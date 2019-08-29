using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.DataBus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Oracle.Schema;
using Rebus.Serialization;
using Rebus.Time;
// ReSharper disable SimplifyLinqExpression

namespace Rebus.Oracle.DataBus
{
    /// <summary>
    /// Implementation of <see cref="IDataBusStorage"/> that uses Oracle to store data
    /// </summary>
    public class OracleDataBusStorage : IDataBusStorage
    {
        static readonly Encoding TextEncoding = Encoding.UTF8;
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly OracleFactory _connectionHelper;
        readonly DbName _table;
        readonly ILog _log;
        readonly IRebusTime _rebusTime;

        /// <summary>
        /// Creates the data storage
        /// </summary>
        public OracleDataBusStorage(OracleFactory connectionHelper, string tableName, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _table = new DbName(tableName) ?? throw new ArgumentNullException(nameof(tableName));
            _log = rebusLoggerFactory.GetLogger<OracleDataBusStorage>();
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
        }

        /// <summary>
        /// Creates the necessary table if it does not already exist
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionHelper.OpenRaw())
            {
                if (connection.CreateRebusDataBus(_table))
                    _log.Info("Creating data bus table {tableName}", _table);
                else
                    _log.Info("Database already contains a table named {tableName} - will not create anything", _table);
            }
        }

        /// <summary>
        /// Saves the data from the given source stream under the given ID
        /// </summary>
        public Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
        {
            try
            {
                using (var connection = _connectionHelper.Open())
                {
                    using (var blob = connection.CreateBlob())
                    {
                        source.CopyTo(blob);

                        using (var command = connection.CreateCommand())
                        {
                            var metadataBytes = metadata == null ? 
                                null : 
                                TextEncoding.GetBytes(_dictionarySerializer.SerializeToString(metadata));

                            command.CommandText = $"INSERT INTO {_table} (id, meta, data, creationTime) VALUES (:id, :meta, :data, :now)";
                            command.Parameters.Add("id", id);
                            command.Parameters.Add("meta", (object)metadataBytes ?? DBNull.Value);
                            command.Parameters.Add("data", blob);
                            command.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());

                            command.ExecuteNonQuery();
                        }
                    }
                    
                    connection.Complete();
                    return Task.CompletedTask;
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not save data with ID {id}");
            }
        }

        /// <summary>
        /// Opens the data stored under the given ID for reading
        /// </summary>
        public Task<Stream> Read(string id)
        {
            try
            {
                // update last read time quickly
                UpdateLastReadTime(id);

                OracleCommand command = null;
                OracleDataReader reader = null;
                UnitOfWork connection = _connectionHelper.Open();

                try
                {
                    command = connection.CreateCommand();
                    command.CommandText = $"SELECT data FROM {_table} WHERE id = :id";
                    command.Parameters.Add("id", id);
                    command.InitialLOBFetchSize = 4000;

                    reader = command.ExecuteReader(CommandBehavior.SingleRow);

                    if (!reader.Read())
                        throw new ArgumentException($"DataBus row with ID {id} not found");

                    var blob = reader.GetOracleBlob(0);

                    return Task.FromResult<Stream>(new StreamWrapper(blob, /* dispose with stream: */ reader, command, connection));
                }
                catch
                {
                    // if something of the above fails, we did not pass ownership to someone who can dispose it... therefore:
                    reader?.Dispose();
                    command?.Dispose();
                    connection.Dispose();
                    throw;
                }
            }
            catch (Exception exception)
            {
                // Wrap in AggregateException to comply with Rebus contract. Tests do look for this specific exception type.
                throw new AggregateException(exception);
            }
        }

        void UpdateLastReadTime(string id)
        {
            using (var connection = _connectionHelper.Open())
            {
                UpdateLastReadTime(id, connection);
                connection.Complete();
            }
        }

        void UpdateLastReadTime(string id, UnitOfWork connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"UPDATE {_table} SET lastReadTime = :now WHERE id = :id";
                command.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());
                command.Parameters.Add("id", id);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Loads the metadata stored with the given ID
        /// </summary>
        public Task<Dictionary<string, string>> ReadMetadata(string id)
        {
            try
            {
                using (var connection = _connectionHelper.Open())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT meta, creationTime, lastReadTime, LENGTHB(data) AS dataLength FROM {_table} WHERE id = :id";
                    command.Parameters.Add("id", id);

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        if (!reader.Read())
                            throw new ArgumentException($"DataBus row with ID {id} not found");

                        var metaDbValue = reader["meta"];
                        var dataLength = reader.GetInt64(reader.GetOrdinal("dataLength"));
                        var creationTime = reader.GetOracleTimeStampTZ(reader.GetOrdinal("creationTime")).ToDateTimeOffset();
                        var lastReadTimeIndex = reader.GetOrdinal("lastReadTime");
                        var lastReadTime = reader.IsDBNull(lastReadTimeIndex) ?
                            (DateTimeOffset?)null :
                            reader.GetOracleTimeStampTZ(lastReadTimeIndex).ToDateTimeOffset();

                        var metadata = metaDbValue is DBNull ?
                            new Dictionary<string, string>() :
                            _dictionarySerializer.DeserializeFromString(TextEncoding.GetString((byte[])metaDbValue));

                        metadata[MetadataKeys.Length] = dataLength.ToString();
                        metadata[MetadataKeys.SaveTime] = creationTime.ToString("O");

                        if (lastReadTime != null)
                        {
                            metadata[MetadataKeys.ReadTime] = lastReadTime.Value.ToString("O");
                        }

                        return Task.FromResult(metadata);
                    }
                }
            }
            catch (Exception exception) when (!(exception is ArgumentException))
            {
                throw new RebusApplicationException(exception, $"Could not load metadata for data with ID {id}");
            }
        }
    }
}