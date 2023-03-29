﻿// ReSharper disable once RedundantUsingDirective (because .NET Core)
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Rebus.Extensions;
using System.Reflection;
using System.Threading.Tasks;
using Devart.Data.Oracle;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Oracle.Reflection;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.Oracle.Sagas;

/// <summary>
/// Implementation of <see cref="ISagaStorage"/> that uses Oracle to do its thing
/// </summary>
public class OracleSqlSagaStorage : ISagaStorage
{
    const string IdPropertyName = nameof(ISagaData.Id);

    readonly ObjectSerializer _objectSerializer = new ObjectSerializer();
    readonly OracleConnectionHelper _connectionHelper;
    readonly string _dataTableName;
    readonly string _indexTableName;
    readonly ILog _log;

    /// <summary>
    /// Constructs the saga storage
    /// </summary>
    public OracleSqlSagaStorage(OracleConnectionHelper connectionHelper, string dataTableName,
        string indexTableName, IRebusLoggerFactory rebusLoggerFactory)
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
        _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
        _dataTableName = dataTableName ?? throw new ArgumentNullException(nameof(dataTableName));
        _indexTableName = indexTableName ?? throw new ArgumentNullException(nameof(indexTableName));
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
            var tableNames = connection.GetTableNames();

            var hasDataTable = tableNames.Any(tableName => _dataTableName.Equals(tableName, StringComparison.InvariantCultureIgnoreCase));
            var hasIndexTable = tableNames.Any(tableName => _indexTableName.Equals(tableName, StringComparison.InvariantCultureIgnoreCase));

            if (hasDataTable && hasIndexTable)
            {
                return;
            }

            if (hasDataTable)
            {
                throw new RebusApplicationException(
                    $"The saga index table '{_indexTableName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_dataTableName}', which was supposed to be created as the data table");
            }

            if (hasIndexTable)
            {
                throw new RebusApplicationException(
                    $"The saga data table '{_dataTableName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_indexTableName}', which was supposed to be created as the index table");
            }

            _log.Info(
                "Saga tables {tableName} (data) and {tableName} (index) do not exist - they will be created now",
                _dataTableName, _indexTableName);

            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"
                        CREATE TABLE {_dataTableName} (
                            id RAW(16) NOT NULL,
                            revision NUMBER(10) NOT NULL,
                            data BLOB NOT NULL,
                            CONSTRAINT {_dataTableName}_pk PRIMARY KEY(id)
                        )";

                command.ExecuteNonQuery();
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                        CREATE TABLE {_indexTableName} (
                            saga_type NVARCHAR2(500) NOT NULL,
                            key NVARCHAR2(500) NOT NULL,
                            value NVARCHAR2(2000) NOT NULL,
                            saga_id RAW(16) NOT NULL,
                            CONSTRAINT {_indexTableName}_pk PRIMARY KEY(key, value, saga_type)
                        )";
                command.ExecuteNonQuery();
            }
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"CREATE INDEX {_indexTableName}_IDX ON {_indexTableName} (saga_id)";
                command.ExecuteNonQuery();
            }

            connection.Complete();
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
                command.InitialLobFetchSize = -1;
                if (propertyName == IdPropertyName)
                {
                    command.CommandText = $@"
                            SELECT s.data 
                                FROM {_dataTableName} s 
                                WHERE s.id = :id 
                            ";
                    command.Parameters.Add("id", OracleDbType.Raw).Value = ToGuid(propertyValue).ToByteArray();
                }
                else
                {
                    command.CommandText = $@"
                            SELECT s.data
                                FROM {_dataTableName} s
                                JOIN {_indexTableName} i on s.id = i.saga_id 
                                WHERE i.saga_type = :saga_type AND i.key = :key AND i.value = :value
                            ";
                    command.Parameters.Add(new OracleParameter("key", OracleDbType.NVarChar, propertyName, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("saga_type", OracleDbType.NVarChar, GetSagaTypeName(sagaDataType), ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("value", OracleDbType.NVarChar, (propertyValue ?? "").ToString(), ParameterDirection.Input));
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
                command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                command.Parameters.Add("revision", OracleDbType.Int64).Value = sagaData.Revision;
                command.Parameters.Add("data", OracleDbType.Blob).Value = _objectSerializer.Serialize(sagaData);

                command.CommandText =
                    $@"
                        INSERT 
                            INTO {_dataTableName} (id, revision, data) 
                            VALUES (:id, :revision, :data)
                        ";

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (OracleException exception)
                {
                    throw new ConcurrencyException(exception,
                        $"Saga data {sagaData.GetType()} with ID {sagaData.Id} in table {_dataTableName} could not be inserted!");
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

    /// <summary>
    /// Updates the already-existing instance of the given saga data, throwing a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if another
    /// saga data instance exists with a correlation property that shares a value with this saga data, or if the saga data
    /// instance no longer exists.
    /// </summary>
    public Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
    {
        using (var connection = _connectionHelper.GetConnection())
        {
            var revisionToUpdate = sagaData.Revision;

            sagaData.Revision++;

            var nextRevision = sagaData.Revision;

            // first, delete existing index
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"DELETE FROM {_indexTableName} WHERE saga_id = :id";
                command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                command.ExecuteNonQuery();
            }

            // next, update or insert the saga
            using (var command = connection.CreateCommand())
            {
                command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                command.Parameters.Add("current_revision", OracleDbType.Int64).Value = revisionToUpdate;
                command.Parameters.Add("next_revision", OracleDbType.Int64).Value = nextRevision;
                command.Parameters.Add("data", OracleDbType.Blob).Value = _objectSerializer.Serialize(sagaData);

                command.CommandText =
                    $@"
                        UPDATE {_dataTableName} 
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
                                FROM {_dataTableName} 
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
                                FROM {_indexTableName} 
                                WHERE saga_id = :id
                            ";
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
                $@"INSERT INTO {_indexTableName} (saga_type, key, value, saga_id) VALUES (:saga_type, :key, :value, :saga_id)";
            command.Parameters.Add(new OracleParameter("saga_type", OracleDbType.NVarChar, parameters.Select(x => x.SagaType).ToArray(), ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("key", OracleDbType.NVarChar, parameters.Select(x => x.PropertyName).ToArray(), ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("value", OracleDbType.NVarChar, parameters.Select(x => x.PropertyValue).ToArray(), ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("saga_id", OracleDbType.Raw, parameters.Select(x => x.SagaId).ToArray(), ParameterDirection.Input));
            command.ExecuteArray(parameters.Count);
        }

    }

    string GetSagaTypeName(Type sagaDataType)
    {
        return sagaDataType.FullName;
    }

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