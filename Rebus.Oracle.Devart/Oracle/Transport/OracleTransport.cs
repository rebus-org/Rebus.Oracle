using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Devart.Data.Oracle;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.Oracle.Transport;

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

    const int OperationCancelledNumber = 3980;

    /// <summary>
    /// 
    /// </summary>
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
        _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
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
        var connection = context.GetOrAdd(CurrentConnectionKey, () =>
        {
            var dbConnection = _connectionHelper.GetConnection();
            var connectionWrapper = new ConnectionWrapper(dbConnection);
            context.OnCommit(_ =>
            {
                dbConnection.Complete();
                return Task.CompletedTask;
            });
            context.OnDisposed(_ => connectionWrapper.Dispose());
            return connectionWrapper;
        });

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
                        :now + :visible,
                        :now + :ttlseconds
                    )";

            var headers = message.Headers.Clone();

            var priority = GetMessagePriority(headers);
            var initialVisibilityDelay = new TimeSpan(0, 0, 0, GetInitialVisibilityDelay(headers, _rebusTime.Now));
            var ttlSeconds = new TimeSpan(0, 0, 0, GetTtlSeconds(headers));

            // must be last because the other functions on the headers might change them
            var serializedHeaders = HeaderSerializer.Serialize(headers);

            command.Parameters.Add(new OracleParameter("recipient", OracleDbType.VarChar, destinationAddress, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("headers", OracleDbType.Blob, serializedHeaders, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("body", OracleDbType.Blob, message.Body, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("priority", OracleDbType.Int64, priority, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("now", OracleDbType.TimeStampTZ, _rebusTime.Now.ToUniversalTime().DateTime, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("visible", OracleDbType.IntervalDS, initialVisibilityDelay, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("ttlseconds", OracleDbType.IntervalDS, ttlSeconds, ParameterDirection.Input));

            command.ExecuteNonQuery();
        }
    }

    /// <inheritdoc />
    public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        using (await _receiveBottleneck.Enter(cancellationToken))
        {
            var connection = context.GetOrAdd(CurrentConnectionKey, () =>
            {
                var dbConnection = _connectionHelper.GetConnection();
                var connectionWrapper = new ConnectionWrapper(dbConnection);
                context.OnAck(_ =>
                {
                    dbConnection.Complete();
                    return Task.CompletedTask;
                });
                context.OnDisposed(_ => connectionWrapper.Dispose());
                return connectionWrapper;
            });

            using (var selectCommand = connection.Connection.CreateCommand())
            {
                selectCommand.CommandText = $"rebus_dequeue_{_tableName}";
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.Parameters.Add(new OracleParameter("recipientQueue", OracleDbType.VarChar, _inputQueueName, ParameterDirection.Input));
                selectCommand.Parameters.Add(new OracleParameter("now", OracleDbType.TimeStampTZ, _rebusTime.Now.ToUniversalTime().DateTime, ParameterDirection.Input));
                selectCommand.Parameters.Add(new OracleParameter("output", OracleDbType.Cursor, ParameterDirection.Output));
                selectCommand.InitialLobFetchSize = -1;
                selectCommand.ExecuteNonQuery();

                using (var reader = (selectCommand.Parameters["output"].Value as OracleCursor).GetDataReader())
                {
                    if (!reader.Read())
                    {
                        return null;
                    }

                    var headers = (byte[])reader["headers"];
                    var body = (byte[])reader["body"];
                    var headersDictionary = HeaderSerializer.Deserialize(headers);

                    return new TransportMessage(headersDictionary, body);
                }
            }
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
                            delete from {_tableName} 
                            where recipient = :recipient 
                            and expiration < :now
                            ";
                    command.Parameters.Add(new OracleParameter("recipient", OracleDbType.VarChar, _inputQueueName, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("now", OracleDbType.TimeStampTZ, _rebusTime.Now.ToUniversalTime().DateTime, ParameterDirection.Input));
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
        using (var connection = _connectionHelper.GetConnection())
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
CREATE SEQUENCE {_tableName}_SEQ
----
CREATE OR REPLACE TRIGGER {_tableName}_on_insert
     BEFORE INSERT ON {_tableName}
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
create or replace PROCEDURE rebus_dequeue_{_tableName}(recipientQueue IN varchar, now IN timestamp with time zone, output OUT SYS_REFCURSOR ) AS
  messageId number;
  readCursor SYS_REFCURSOR; 
begin

    open readCursor for 
    SELECT id
    FROM {_tableName}
    WHERE recipient = recipientQueue
            and visible < now
            and expiration > now         
    ORDER BY priority ASC, visible ASC, id ASC
    for update skip locked;
    
    fetch readCursor into messageId;
    close readCursor;      
  open output for select * from {_tableName} where id = messageId;
  delete from {_tableName} where id = messageId;
END;
");

            connection.Complete();
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

    static int GetInitialVisibilityDelay(IDictionary<string, string> headers, DateTimeOffset now)
    {
        if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
        {
            return 0;
        }

        var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

        headers.Remove(Headers.DeferredUntil);

        return (int)(deferredUntilTime - now).TotalSeconds;
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