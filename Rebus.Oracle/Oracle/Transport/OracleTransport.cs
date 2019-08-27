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
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        readonly OracleFactory _factory;
        readonly DbName _table;
        readonly AsyncBottleneck _receiveBottleneck = new AsyncBottleneck(20);
        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly ILog _log;
        readonly IRebusTime _rebusTime;

        // SQL are cached so that strings are not built up at every command
        readonly string _sendSql, _receiveSql, _expiredSql;

        /// <summary>Gets the address of the transport</summary>
        public string Address { get; }

        /// <summary>
        /// Header key of message priority which happens to be supported by this transport
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary> </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName"></param>
        /// <param name="inputQueueName"></param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="rebusTime"></param>
        public OracleTransport(OracleFactory connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            _log = rebusLoggerFactory.GetLogger<OracleTransport>();
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _factory = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _table = new DbName(tableName);
            _sendSql = SendCommand.Sql(_table);
       
            // One-way clients don't have an input queue to receive from or cleanup
            if (inputQueueName != null)
            {
                Address = inputQueueName;
                _receiveSql = BuildReceiveSql();
                _expiredSql = BuildExpiredSql();
                _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
            }
        }

        /// <inheritdoc />
        public void Initialize()
        {
            _expiredMessagesCleanupTask?.Start();
        }

        /// <summary>The Oracle transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        { }

        /// <inheritdoc />
        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var headers = message.Headers.Clone();
            var priority = GetMessagePriority(headers);
            var initialVisibilityDelay = new TimeSpan(0, 0, 0, GetInitialVisibilityDelay(headers));
            var ttlSeconds = new TimeSpan(0, 0, 0, GetTtlSeconds(headers));
            // must be last because the other functions on the headers might change them
            var serializedHeaders = HeaderSerializer.Serialize(headers);

            var command = context.GetSendCommand(_factory, _sendSql);
            // Lock is blocking, but we're not async anyway (Oracle provider is blocking).
            // As a bonus: 
            // (1) Monitor is faster than SemaphoreSlim when there's no contention, which is usually the case; 
            // (2) we don't need to allocate any extra object (command is private and not exposed to end-users).
            lock (command)
            {
                new SendCommand(command)
                {
                    Recipient = destinationAddress,
                    Headers = serializedHeaders,
                    Body = message.Body,
                    Priority = priority,
                    Visible = initialVisibilityDelay,
                    Now = _rebusTime.Now.ToOracleTimeStamp(),
                    TtlSeconds = ttlSeconds,
                }
                .ExecuteNonQuery();
            }

            return Task.CompletedTask;
        }      

        string BuildReceiveSql() => $"{_table.Prefix}rebus_dequeue_{_table.Name}";

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _receiveBottleneck.Enter(cancellationToken))
            {
                var connection = context.GetConnection(_factory);

                using (var selectCommand = connection.CreateCommand())
                {
                    selectCommand.CommandText = _receiveSql;
                    selectCommand.CommandType = CommandType.StoredProcedure;
                    selectCommand.Parameters.Add("recipientQueue", Address);
                    selectCommand.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());
                    selectCommand.Parameters.Add("output", OracleDbType.RefCursor, ParameterDirection.Output);
                    selectCommand.InitialLOBFetchSize = -1;
                    selectCommand.ExecuteNonQuery();

                    using (var reader = (selectCommand.Parameters["output"].Value as OracleRefCursor).GetDataReader()) 
                    {
                        if (!reader.Read()) return null;

                        var headers = (byte[])reader["headers"];
                        var body = (byte[])reader["body"];
                        var headersDictionary = HeaderSerializer.Deserialize(headers);
                        return new TransportMessage(headersDictionary, body);
                    }
                }
            }
        }

        string BuildExpiredSql() => $"delete from {_table} where recipient = :recipient and expiration < :now";

        Task PerformExpiredMessagesCleanupCycle()
        {
            var stopwatch = Stopwatch.StartNew();           

            using (var connection = _factory.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = _expiredSql;
                command.Parameters.Add("recipient", Address);
                command.Parameters.Add("now", _rebusTime.Now.ToOracleTimeStamp());            

                int deletedRows = command.ExecuteNonQuery();
                
                connection.Complete();

                if (deletedRows > 0)
                {
                    _log.Info(
                        "Performed expired messages cleanup in {cleanupTime} - {deletedCount} expired messages with recipient {recipient} were deleted",
                        stopwatch.Elapsed, deletedRows, Address);
                }

                return Task.CompletedTask;
            }
        }

        /// <summary>Creates the necessary DB objects</summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                using (var connection = _factory.OpenRaw())
                {
                    if (connection.CreateRebusTransport(_table))
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

        /// <inheritdoc />
        // Note: IAsyncTask can be disposed multiple times without side-effects
        public void Dispose() => _expiredMessagesCleanupTask?.Dispose();

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;
            
            if (!int.TryParse(valueOrNull, out int priority))
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!");
            
            return priority;
        }

        int GetInitialVisibilityDelay(Dictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
                return 0;

            headers.Remove(Headers.DeferredUntil);
            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            return (int)(deferredUntilTime - _rebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(Dictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr))
                return int.MaxValue;    // about 60 years

            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);
            return (int)timeToBeReceived.TotalSeconds;
        }
    }
}
