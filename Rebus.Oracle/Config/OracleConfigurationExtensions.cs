using System;
using Oracle.ManagedDataAccess.Client;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Oracle;
using Rebus.Oracle.Sagas;
using Rebus.Oracle.Subscriptions;
using Rebus.Oracle.Timeouts;
using Rebus.Sagas;
using Rebus.Subscriptions;
using Rebus.Timeouts;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for Oracle persistence
    /// </summary>
    public static class OracleConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use Oracle to store saga data snapshots, using the specified table to store the data
        /// </summary>
        public static void StoreInOracle(this StandardConfigurer<ISagaSnapshotStorage> configurer,
            string connectionString, string tableName, bool automaticallyCreateTables = true, 
            Action<OracleConnection> additionalConnectionSetup = null)
        {
            configurer.Register(c =>
            {
                var sagaStorage = new OracleSagaSnapshotStorage(new OracleConnectionHelper(connectionString, additionalConnectionSetup), tableName);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTableIsCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use Oracle to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInOracle(this StandardConfigurer<ISagaStorage> configurer,
            string connectionString, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true, Action<OracleConnection> additionalConnectionSetup = null)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var sagaStorage = new OracleSqlSagaStorage(new OracleConnectionHelper(connectionString, additionalConnectionSetup), dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use Oracle to store timeouts.
        /// </summary>
        public static void StoreInOracle(this StandardConfigurer<ITimeoutManager> configurer, string connectionString, string tableName, 
            bool automaticallyCreateTables = true, Action<OracleConnection> additionalConnectionSetup = null)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var subscriptionStorage = new OracleTimeoutManager(new OracleConnectionHelper(connectionString, additionalConnectionSetup), tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use Oracle to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInOracle(this StandardConfigurer<ISubscriptionStorage> configurer,
            string connectionString, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true, Action<OracleConnection> additionalConnectionSetup = null)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionHelper = new OracleConnectionHelper(connectionString, additionalConnectionSetup);
                var subscriptionStorage = new OracleSubscriptionStorage(
                    connectionHelper, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

    }
}