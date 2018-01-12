using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Time;
using Rebus.Timeouts;

// ReSharper disable AccessToDisposedClosure

#pragma warning disable 1998

namespace Rebus.Oracle.Timeouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that uses Oracle to do its thing. Can be used safely by multiple processes competing
    /// over the same table of timeouts because row-level locking is used when querying for due timeouts.
    /// </summary>
    public class OracleTimeoutManager : ITimeoutManager
    {
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly OracleConnectionHelper _connectionHelper;
        readonly string _tableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager
        /// </summary>
        public OracleTimeoutManager(OracleConnectionHelper connectionHelper, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _log = rebusLoggerFactory.GetLogger<OracleTimeoutManager>();
        }

        /// <summary>
        /// Stores the message with the given headers and body data, delaying it until the specified <paramref name="approximateDueTime" />
        /// </summary>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"INSERT INTO {_tableName} (due_time, headers, body) VALUES (:due_time, :headers, :body)"; 
                    command.BindByName = true;
                    command.Parameters.Add(new OracleParameter("due_time", OracleDbType.TimeStampTZ, approximateDueTime.ToUniversalTime().DateTime, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("headers", OracleDbType.Clob, _dictionarySerializer.SerializeToString(headers), ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("body", OracleDbType.Blob, body, ParameterDirection.Input));
                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Gets due messages as of now, given the approximate due time that they were stored with when <see cref="M:Rebus.Timeouts.ITimeoutManager.Defer(System.DateTimeOffset,System.Collections.Generic.Dictionary{System.String,System.String},System.Byte[])" /> was called
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var connection = await _connectionHelper.GetConnection();

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                        SELECT
                            id,
                            headers, 
                            body 

                        FROM {_tableName} 

                        WHERE due_time <= :current_time 

                        ORDER BY due_time
                        FOR UPDATE";
                    command.BindByName = true;
                    command.Parameters.Add(new OracleParameter("current_time", OracleDbType.TimeStampTZ, RebusTime.Now.ToUniversalTime().DateTime, ParameterDirection.Input));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dueMessages = new List<DueMessage>();

                        while (reader.Read())
                        {
                            var id = (long)reader["id"];
                            var headers = _dictionarySerializer.DeserializeFromString((string) reader["headers"]);
                            var body = (byte[]) reader["body"];

                            dueMessages.Add(new DueMessage(headers, body, async () =>
                            {
                                using (var deleteCommand = connection.CreateCommand())
                                {
                                    deleteCommand.BindByName = true;
                                    deleteCommand.CommandText = $@"DELETE FROM {_tableName} WHERE id = :id";
                                    deleteCommand.Parameters.Add(new OracleParameter("id", OracleDbType.Int64, id, ParameterDirection.Input));
                                    await deleteCommand.ExecuteNonQueryAsync();
                                }
                            }));
                        }

                        return new DueMessagesResult(dueMessages, async () =>
                        {
                            connection.Complete();
                            connection.Dispose();
                        });
                    }
                }
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Checks if the configured timeouts table exists - if it doesn't, it will be created.
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionHelper.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
                {
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                        CREATE TABLE {_tableName} (
                            id  NUMBER(10) NOT NULL,
                            due_time TIMESTAMP(7) WITH TIME ZONE NOT NULL,
                            headers CLOB,
                            body  BLOB,
                            CONSTRAINT {_tableName}_pk PRIMARY KEY(id)
                         )";

                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"CREATE SEQUENCE {_tableName}_SEQ";
                    command.ExecuteNonQuery();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                        CREATE OR REPLACE TRIGGER {_tableName}_on_insert
                             BEFORE INSERT ON  {_tableName}
                             FOR EACH ROW
                        BEGIN
                            if :new.Id is null then
                              :new.id := {_tableName}_seq.nextval;
                            END IF;
                        END;
                        ";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        CREATE INDEX {_tableName}_due_idx ON {_tableName} (due_time)";

                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }
    }
}