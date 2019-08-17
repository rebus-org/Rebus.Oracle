// ReSharper disable once RedundantUsingDirective (because .NET Core)
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Rebus.Extensions;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Oracle.Reflection;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.Oracle.Schema;

namespace Rebus.Oracle.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaStorage"/> that uses Oracle to do its thing
    /// </summary>
    public class OracleSqlSagaStorage : ISagaStorage
    {
        const string IdPropertyName = nameof(ISagaData.Id);

        readonly ObjectSerializer _objectSerializer = new ObjectSerializer();
        readonly OracleConnectionHelper _connectionHelper;
        readonly DbName _dataTable;
        readonly DbName _indexTable;
        readonly ILog _log;

        /// <summary>
        /// Constructs the saga storage
        /// </summary>
        public OracleSqlSagaStorage(OracleConnectionHelper connectionHelper, string dataTableName,
            string indexTableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _dataTable = new DbName(dataTableName) ?? throw new ArgumentNullException(nameof(dataTableName));
            _indexTable = new DbName(indexTableName) ?? throw new ArgumentNullException(nameof(indexTableName));
            _log = rebusLoggerFactory.GetLogger<OracleSqlSagaStorage>();
        }

        /// <summary>
        /// Checks to see if the configured saga data and saga index table exists. If they both exist, we'll continue, if
        /// neither of them exists, we'll try to create them. If one of them exists, we'll throw an error.
        /// </summary>
        public void EnsureTablesAreCreated()
        {
            using (var connection = _connectionHelper.GetConnection())
            {
                if (connection.Connection.CreateRebusSaga(_dataTable, _indexTable))
                    _log.Info("Saga tables {tableName} (data) and {tableName} (index) do not exist - they will be created now",
                              _dataTable, _indexTable);
            }
        }

        /// <summary>
        /// Finds an already-existing instance of the given saga data type that has a property with the given <paramref name="propertyName" />
        /// whose value matches <paramref name="propertyValue" />. Returns null if no such instance could be found
        /// </summary>
        public Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            using (var connection = _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.InitialLOBFetchSize=-1;
                    if (propertyName == IdPropertyName)
                    {
                        command.CommandText = $@"
                            SELECT s.data 
                                FROM {_dataTable} s 
                                WHERE s.id = :id 
                            ";
                        command.Parameters.Add("id", OracleDbType.Raw).Value = ToGuid(propertyValue).ToByteArray();
                    }
                    else
                    {
                        command.CommandText = $@"
                            SELECT s.data
                                FROM {_dataTable} s
                                JOIN {_indexTable} i on s.id = i.saga_id 
                                WHERE i.saga_type = :saga_type AND i.key = :key AND i.value = :value
                            ";
                        command.BindByName = true;
                        command.Parameters.Add(new OracleParameter("key", OracleDbType.NVarchar2, propertyName, ParameterDirection.Input));
                        command.Parameters.Add(new OracleParameter("saga_type", OracleDbType.NVarchar2, GetSagaTypeName(sagaDataType), ParameterDirection.Input));
                        command.Parameters.Add(new OracleParameter("value", OracleDbType.NVarchar2, (propertyValue ?? "").ToString(), ParameterDirection.Input));
                    }

                    var data = (byte[]) command.ExecuteScalar();

                    if (data == null) return Task.FromResult<ISagaData>(null);

                    try
                    {
                        var sagaData = (ISagaData) _objectSerializer.Deserialize(data);

                        if (!sagaDataType.IsInstanceOfType(sagaData))
                        {
                            return Task.FromResult<ISagaData>(null);
                        }

                        return Task.FromResult(sagaData);
                    }
                    catch (Exception exception)
                    {
                        var message =
                            $"An error occurred while attempting to deserialize '{data}' into a {sagaDataType}";

                        throw new RebusApplicationException(exception, message);
                    }
                    finally
                    {
                        connection.Complete();
                    }
                }
            }
        }

        static Guid ToGuid(object propertyValue)
        {
            return (Guid) Convert.ChangeType(propertyValue, typeof(Guid));
        }

        /// <summary>
        /// Inserts the given saga data as a new instance. Throws a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if another saga data instance
        /// already exists with a correlation property that shares a value with this saga data.
        /// </summary>
        public Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            try
            {
                if (sagaData.Id == Guid.Empty)
                {
                    throw new InvalidOperationException(
                        $"Saga data {sagaData.GetType()} has an uninitialized Id property!");
                }

                if (sagaData.Revision != 0)
                {
                    throw new InvalidOperationException(
                        $"Attempted to insert saga data with ID {sagaData.Id} and revision {sagaData.Revision}, but revision must be 0 on first insert!");
                }

                using (var connection = _connectionHelper.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.BindByName = true;
                        command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                        command.Parameters.Add("revision", OracleDbType.Int64).Value = sagaData.Revision;
                        command.Parameters.Add("data", OracleDbType.Blob).Value = _objectSerializer.Serialize(sagaData);

                        command.CommandText =
                            $@"
                            INSERT 
                                INTO {_dataTable} (id, revision, data) 
                                VALUES (:id, :revision, :data)
                            ";

                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch (OracleException exception)
                        {
                            throw new ConcurrencyException(exception,
                                $"Saga data {sagaData.GetType()} with ID {sagaData.Id} in table {_dataTable} could not be inserted!");
                        }
                    }

                    var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);

                    if (propertiesToIndex.Any())
                    {
                        CreateIndex(sagaData, connection, propertiesToIndex);
                    }

                    connection.Complete();
                    return Task.CompletedTask;
                }
            }
            catch (Exception ex)
            {
                // Wrap in AggregateException to comply with Rebus contract. Tests do look for this specific exception type.
                throw new AggregateException(ex);
            }
        }

        /// <summary>
        /// Updates the already-existing instance of the given saga data, throwing a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if another
        /// saga data instance exists with a correlation property that shares a value with this saga data, or if the saga data
        /// instance no longer exists.
        /// </summary>
        public Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            try
            {
                using (var connection = _connectionHelper.GetConnection())
                {
                    var revisionToUpdate = sagaData.Revision;

                    sagaData.Revision++;

                    var nextRevision = sagaData.Revision;

                    // first, delete existing index
                    using (var command = connection.CreateCommand())
                    {
                        command.BindByName = true;
                        command.CommandText = $@"DELETE FROM {_indexTable} WHERE saga_id = :id";
                        command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                        command.ExecuteNonQuery();
                    }

                    // next, update or insert the saga
                    using (var command = connection.CreateCommand())
                    {
                        command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id.ToByteArray();
                        command.Parameters.Add("current_revision", OracleDbType.Int64).Value = revisionToUpdate;
                        command.Parameters.Add("next_revision", OracleDbType.Int64).Value = nextRevision;
                        command.Parameters.Add("data", OracleDbType.Blob).Value = _objectSerializer.Serialize(sagaData);
                        command.BindByName = true;

                        command.CommandText =
                            $@"
                            UPDATE {_dataTable} 
                                SET data = :data, revision = :next_revision 
                                WHERE id = :id AND revision = :current_revision
                            ";

                        var rows = command.ExecuteNonQuery();

                        if (rows == 0)
                        {
                            throw new ConcurrencyException(
                                $"Update of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                        }
                    }

                    var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);

                    if (propertiesToIndex.Any())
                    {
                        CreateIndex(sagaData, connection, propertiesToIndex);
                    }

                    connection.Complete();
                    return Task.CompletedTask;
                }
            }
            catch (Exception ex)
            {
                // Wrap in AggregateException to comply with Rebus contract. Tests do look for this specific exception type.
                throw new AggregateException(ex);
            }
        }

        /// <summary>
        /// Deletes the saga data instance, throwing a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if the instance no longer exists
        /// </summary>
        public Task Delete(ISagaData sagaData)
        {
            try
            {
                using (var connection = _connectionHelper.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
                            DELETE 
                                FROM {_dataTable} 
                                WHERE id = :id AND revision = :current_revision
                            ";

                        command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                        command.Parameters.Add("current_revision", OracleDbType.Int64).Value = sagaData.Revision;

                        var rows = command.ExecuteNonQuery();

                        if (rows == 0)
                        {
                            throw new ConcurrencyException(
                                $"Delete of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                        }
                    }

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
                            DELETE 
                                FROM {_indexTable} 
                                WHERE saga_id = :id
                            ";
                        command.BindByName = true;
                        command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;

                        command.ExecuteNonQuery();
                    }

                    connection.Complete();
                }

                sagaData.Revision++;

                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                // Wrap in AggregateException to comply with Rebus contract. Tests do look for this specific exception type.
                throw new AggregateException(ex);
            }
        }

        void CreateIndex(ISagaData sagaData, OracleDbConnection connection,
            IEnumerable<KeyValuePair<string, string>> propertiesToIndex)
        {
            var sagaTypeName = GetSagaTypeName(sagaData.GetType());
            var parameters = propertiesToIndex
                .Select((p, i) => new
                {
                    SagaType = sagaTypeName,
                    SagaId = sagaData.Id.ToByteArray(),
                    PropertyName = p.Key,
                    PropertyValue = p.Value ?? "",
                })
                .ToList();

            // lastly, generate new index
            using (var command = connection.CreateCommand())
            {
                // generate batch insert with SQL for each entry in the index
                command.CommandText =
                    $@"INSERT INTO {_indexTable} (saga_type, key, value, saga_id) VALUES (:saga_type, :key, :value, :saga_id)";
                command.BindByName = true;
                command.ArrayBindCount = parameters.Count;
                command.Parameters.Add(new OracleParameter("saga_type", OracleDbType.NVarchar2, parameters.Select(x => x.SagaType).ToArray(), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("key", OracleDbType.NVarchar2, parameters.Select(x => x.PropertyName).ToArray(), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("value", OracleDbType.NVarchar2, parameters.Select(x => x.PropertyValue).ToArray(), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("saga_id", OracleDbType.Raw, parameters.Select(x => x.SagaId).ToArray(), ParameterDirection.Input));
                command.ExecuteNonQuery();
            }
        }

        string GetSagaTypeName(Type sagaDataType) => sagaDataType.FullName;

        static List<KeyValuePair<string, string>> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            return correlationProperties
                .Select(p => p.PropertyName)
                .Select(path =>
                {
                    var value = Reflect.Value(sagaData, path);

                    return new KeyValuePair<string, string>(path, value?.ToString());
                })
                .Where(kvp => kvp.Value != null)
                .ToList();
        }
    }
}