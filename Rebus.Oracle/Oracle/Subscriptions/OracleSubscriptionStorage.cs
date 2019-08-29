using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Oracle.Schema;
using Rebus.Subscriptions;

namespace Rebus.Oracle.Subscriptions
{
    /// <summary>
    /// Implementation of <see cref="ISubscriptionStorage"/> that uses Oracle to do its thing
    /// </summary>
    public class OracleSubscriptionStorage : ISubscriptionStorage
    {
        const int UniqueKeyViolation = 1;

        readonly OracleFactory _connectionHelper;
        readonly DbName _table;
        readonly ILog _log;

        /// <summary>
        /// Constructs the subscription storage, storing subscriptions in the specified <paramref name="tableName"/>.
        /// If <paramref name="isCentralized"/> is true, subscribing/unsubscribing will be short-circuited by manipulating
        /// subscriptions directly, instead of requesting via messages
        /// </summary>
        public OracleSubscriptionStorage(OracleFactory connectionHelper, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _table = new DbName(tableName) ?? throw new ArgumentNullException(nameof(tableName));
            IsCentralized = isCentralized;
            _log = rebusLoggerFactory.GetLogger<OracleSubscriptionStorage>();
        }

        /// <summary>
        /// Creates the subscriptions table if no table with the specified name exists
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionHelper.OpenRaw())
            {
                if (connection.CreateRebusSubscription(_table))
                    _log.Info("Table {tableName} does not exist - it will be created now", _table);
            }
        }

        /// <summary>
        /// Gets all destination addresses for the given topic
        /// </summary>
        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            using (var connection = _connectionHelper.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"select address from {_table} where topic = :topic";

                command.Parameters.Add(new OracleParameter("topic", OracleDbType.Varchar2, topic, ParameterDirection.Input));

                var endpoints = new List<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        endpoints.Add((string)reader["address"]);
                    }
                }

                return Task.FromResult(endpoints.ToArray());
            }
        }

        /// <summary>
        /// Registers the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = _connectionHelper.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"insert into {_table} (topic, address) values (:topic, :address)";

                command.Parameters.Add(new OracleParameter("topic", OracleDbType.Varchar2, topic, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("address", OracleDbType.Varchar2, subscriberAddress, ParameterDirection.Input));

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (OracleException exception) when (exception.Number == UniqueKeyViolation)
                {
                    // it's already there
                }

                connection.Complete();
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Unregisters the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = _connectionHelper.Open())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"delete from {_table} where topic = :topic and address = :address";

                command.Parameters.Add(new OracleParameter("topic", OracleDbType.Varchar2, topic, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("address", OracleDbType.Varchar2, subscriberAddress, ParameterDirection.Input));

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (OracleException exception)
                {
                    throw new RebusApplicationException(exception, "Failed to delete subscription from storage");
                }

                connection.Complete();
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Gets whether the subscription storage is centralized and thus supports bypassing the usual subscription request
        /// </summary>
        public bool IsCentralized { get; }
    }
}
