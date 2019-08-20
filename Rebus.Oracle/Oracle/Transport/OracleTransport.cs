using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Oracle.Schema;
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
        readonly DbName _table;
        readonly string _inputQueueName;
        readonly AsyncBottleneck _receiveBottleneck = new AsyncBottleneck(20);
        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly ILog _log;
        readonly IRebusTime _rebusTime;

        bool _disposed;

        /// <summary>
        /// Header key of message priority which happens to be supported by this transport
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Indicates the default interval between which expired messages will be cleaned up
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        /// <summary> </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName"></param>
        /// <param name="inputQueueName"></param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="rebusTime"></param>
        public OracleTransport(OracleConnectionHelper connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            _log = rebusLoggerFactory.GetLogger<OracleTransport>();
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _table = new DbName(tableName) ?? throw new ArgumentNullException(nameof(tableName));
            _inputQueueName = inputQueueName;
            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));

            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;
        }

        /// <inheritdoc />
        public void Initialize()
        {
            if (_inputQueueName == null) return;
            _expiredMessagesCleanupTask.Start();
        }

        /// <summary> </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>The Oracle transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        {
        }

        /// <inheritdoc />
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = GetConnection(context);
            var semaphore = connection.Semaphore;

            // serialize access to the connection
            await semaphore.WaitAsync();

            try
            {
                InnerSend(destinationAddress, message, connection);
            }
            finally
            {
                semaphore.Release();
            }
        }

        void InnerSend(string destinationAddress, TransportMessage message, ConnectionWrapper connection)
        {
            using (var command = connection.Connection.CreateCommand())
            {
                command.CommandText = $@"
                    INSERT INTO {_table}
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
                        :now + :visible,
                        :now + :ttlseconds
                    )";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = new TimeSpan(0, 0, 0, GetInitialVisibilityDelay(headers));
                var ttlSeconds = new TimeSpan(0, 0, 0, GetTtlSeconds(headers));

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.BindByName = true;
                command.Parameters.Add(new OracleParameter("recipient", OracleDbType.Varchar2, destinationAddress, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("headers", OracleDbType.Blob, serializedHeaders, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("body", OracleDbType.Blob, message.Body, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("priority", OracleDbType.Int32, priority, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("visible", OracleDbType.IntervalDS, initialVisibilityDelay, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("now", _rebusTime.Now.ToOracleTimeStamp()));
                command.Parameters.Add(new OracleParameter("ttlseconds", OracleDbType.IntervalDS, ttlSeconds, ParameterDirection.Input));

                command.ExecuteNonQuery();
            }
        }

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _receiveBottleneck.Enter(cancellationToken))
            {
                var connection = GetConnection(context);

                TransportMessage receivedTransportMessage;

                using (var selectCommand = connection.Connection.CreateCommand())
                {
                    selectCommand.CommandText = $"{_table.Prefix}rebus_dequeue_{_table.Name}";
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    selectCommand.Parameters.Add(new OracleParameter("recipientQueue", OracleDbType.Varchar2, _inputQueueName, ParameterDirection.Input));
                    selectCommand.Parameters.Add(new OracleParameter("now", _rebusTime.Now.ToOracleTimeStamp()));
                    selectCommand.Parameters.Add(new OracleParameter("output", OracleDbType.RefCursor, ParameterDirection.Output));
                    selectCommand.InitialLOBFetchSize = -1;

                    selectCommand.ExecuteNonQuery();
                    using (var reader = (selectCommand.Parameters["output"].Value as OracleRefCursor).GetDataReader()) 
                    {
                        if (!reader.Read())
                        {
                            return null;
                        }

                        var headers = reader["headers"];
                        var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
                        var body = (byte[])reader["body"];

                        receivedTransportMessage = new TransportMessage(headersDictionary, body);
                    }
                }

                return receivedTransportMessage;
            }
        }

        Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = _connectionHelper.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
                            delete from {_table} 
                            where recipient = :recipient 
                            and expiration < systimestamp(6)
                            ";
                        command.BindByName = true;
                        command.Parameters.Add(new OracleParameter("recipient", OracleDbType.Varchar2, _inputQueueName, ParameterDirection.Input));
                        affectedRows = command.ExecuteNonQuery();
                    }

                    results += affectedRows;
                    connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info(
                    "Performed expired messages cleanup in {0} - {1} expired messages with recipient {2} were deleted",
                    stopwatch.Elapsed, results, _inputQueueName);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Gets the address of the transport
        /// </summary>
        public string Address => _inputQueueName;

        /// <summary>Creates the necessary DB objects</summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                using (var connection = _connectionHelper.GetConnection())
                {
                    if (connection.Connection.CreateRebusTransport(_table))
                        _log.Info("Table {tableName} does not exist - it will be created now", _table);
                    else
                        _log.Info("Database already contains a table named {tableName} - will not create anything", _table);
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize Oracle transport schema with mesages table {_table}");
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

        ConnectionWrapper GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    () =>
                    {
                        var dbConnection = _connectionHelper.GetConnection();
                        var connectionWrapper = new ConnectionWrapper(dbConnection);
                        context.OnCommitted(() =>
                        {
                            dbConnection.Complete();
                            return Task.CompletedTask;
                        });
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

        int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - _rebusTime.Now).TotalSeconds;
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
