using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Internals;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.Oracle.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses Oracle to move messages around
    /// </summary>
    public class OracleTransport : ITransport, IInitializable, IDisposable
    {
        const string CurrentConnectionKey = "oracle-transport-current-connection";

        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        readonly OracleConnectionHelper _connectionHelper;
        readonly string _tableName;
        readonly string _inputQueueName;
        readonly AsyncBottleneck _receiveBottleneck = new AsyncBottleneck(20);
        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly ILog _log;

        bool _disposed;

        /// <summary>
        /// Header key of message priority which happens to be supported by this transport
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Indicates the default interval between which expired messages will be cleaned up
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        const int OperationCancelledNumber = 3980;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName"></param>
        /// <param name="inputQueueName"></param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        public OracleTransport(OracleConnectionHelper connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            _log = rebusLoggerFactory.GetLogger<OracleTransport>();
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _inputQueueName = inputQueueName;
            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);

            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;
        }

        /// <inheritdoc />
        public void Initialize()
        {
            if (_inputQueueName == null) return;
            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>The SQL transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        {
        }

        /// <inheritdoc />
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context);
            var semaphore = connection.Semaphore;

            // serialize access to the connection
            await semaphore.WaitAsync();

            try
            {
                await InnerSend(destinationAddress, message, connection);
            }
            finally
            {
                semaphore.Release();
            }
        }

        async Task InnerSend(string destinationAddress, TransportMessage message, ConnectionWrapper connection)
        {
            using (var command = connection.Connection.CreateCommand())
            {
                command.CommandText = $@"
INSERT INTO {_tableName}
(
    recipient,
    headers,
    body,
    priority,
    visible,
    expiration
)
VALUES
(
    :recipient,
    :headers,
    :body,
    :priority,
    systimestamp(6) + :visible,
    systimestamp(6) + :ttlseconds
)";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = new TimeSpan(0, 0, 0, GetInitialVisibilityDelay(headers));
                var ttlSeconds = new TimeSpan(0, 0, 0, GetTtlSeconds(headers));

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add(new OracleParameter("recipient", OracleDbType.Varchar2, destinationAddress, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("headers", OracleDbType.Blob, serializedHeaders, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("body", OracleDbType.Blob, message.Body, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("priority", OracleDbType.Int32, priority, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("visible", OracleDbType.IntervalDS, initialVisibilityDelay, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("ttlseconds", OracleDbType.IntervalDS, ttlSeconds, ParameterDirection.Input));

                await command.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _receiveBottleneck.Enter(cancellationToken))
            {
                var connection = await GetConnection(context);

                TransportMessage receivedTransportMessage;

                using (var selectCommand = connection.Connection.CreateCommand())
                {
                    selectCommand.CommandText = "rebus_dequeue_message";
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    selectCommand.Parameters.Add(new OracleParameter("recipient", OracleDbType.Varchar2, _inputQueueName, ParameterDirection.Input));
                    selectCommand.Parameters.Add(new OracleParameter("output", OracleDbType.RefCursor ,ParameterDirection.Output));
                    selectCommand.InitialLOBFetchSize = -1;

                    try
                    {
                        selectCommand.ExecuteNonQuery();
                        using (var reader = (selectCommand.Parameters["output"].Value as OracleRefCursor).GetDataReader()){
                            if (!await reader.ReadAsync(cancellationToken))
                            {
                                _log.Warn("Returned no messages from query");
                                return null;
                            }

                            var headers = reader["headers"];
                            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
                            var body = (byte[])reader["body"];

                            receivedTransportMessage = new TransportMessage(headersDictionary, body);
                        }
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                }

                return receivedTransportMessage;
            }
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await _connectionHelper.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
	            delete from {_tableName} 
				where recipient = :recipient 
				and expiration < systimestamp(6)
";
                        command.Parameters.Add(new OracleParameter("recipient", OracleDbType.Varchar2, _inputQueueName, ParameterDirection.Input));
                        affectedRows = await command.ExecuteNonQueryAsync();
                    }

                    results += affectedRows;
                    await connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info(
                    "Performed expired messages cleanup in {0} - {1} expired messages with recipient {2} were deleted",
                    stopwatch.Elapsed, results, _inputQueueName);
            }
        }

        /// <summary>
        /// Gets the address of the transport
        /// </summary>
        public string Address => _inputQueueName;

        /// <summary>
        /// Creates the necessary table
        /// </summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                CreateSchema();
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{_tableName}]");
            }
        }

        void CreateSchema()
        {
            using (var connection = _connectionHelper.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", _tableName);
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName);

                ExecuteCommands(connection, $@"
CREATE TABLE {_tableName}
(
	id NUMBER(20) NOT NULL,
	recipient VARCHAR2(255) NOT NULL,
	priority NUMBER(20) NOT NULL,
    expiration timestamp with time zone NOT NULL,
    visible timestamp with time zone NOT NULL,
	headers blob NOT NULL,
	body blob NOT NULL
)
----
ALTER TABLE {_tableName} ADD CONSTRAINT {_tableName}_pk PRIMARY KEY(recipient, priority, id)
----
CREATE SEQUENCE {_tableName}_SEQ;

----
CREATE OR REPLACE TRIGGER {_tableName}_on_insert
     BEFORE INSERT ON  {_tableName}
     FOR EACH ROW
BEGIN
    if :new.Id is null then
      :new.id := {_tableName}_seq.nextval;
    END IF;
END;
----
CREATE INDEX idx_receive_{_tableName} ON {_tableName}
(
	recipient ASC,
    expiration ASC,
    visible ASC
)
----
create or replace PROCEDURE  rebus_dequeue_message(myRecipient IN varchar, output OUT SYS_REFCURSOR ) AS
  messageId number;
  readCursor SYS_REFCURSOR; 
begin

    open readCursor for 
    SELECT id
    FROM {_tableName}
    WHERE recipient = myRecipient
            and visible < current_timestamp(6)
            and expiration > current_timestamp(6)          
    ORDER BY priority ASC, id ASC
    for update skip locked;
    
    fetch readCursor into messageId;
    close readCursor;      
  open output for select * from {_tableName} where id = messageId;
  delete from {_tableName} where  id = messageId;
END;
");

                AsyncHelpers.RunSync(() => connection.Complete());
            }
        }

        static void ExecuteCommands(OracleDbConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (OracleException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
            }
        }

        class ConnectionWrapper : IDisposable
        {
            public ConnectionWrapper(OracleDbConnection connection)
            {
                Connection = connection;
                Semaphore = new SemaphoreSlim(1, 1);
            }

            public OracleDbConnection Connection { get; }
            public SemaphoreSlim Semaphore { get; }

            public void Dispose()
            {
                Connection?.Dispose();
                Semaphore?.Dispose();
            }
        }

        Task<ConnectionWrapper> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    async () =>
                    {
                        var dbConnection = await _connectionHelper.GetConnection();
                        var connectionWrapper = new ConnectionWrapper(dbConnection);
                        context.OnCommitted(async () => await dbConnection.Complete());
                        context.OnDisposed(() => connectionWrapper.Dispose());
                        return connectionWrapper;
                    });
        }


        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        static int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return defaultTtlSecondsAbout60Years;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
        }
    }
}
