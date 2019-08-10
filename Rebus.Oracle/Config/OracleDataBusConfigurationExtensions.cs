using System;
using Oracle.ManagedDataAccess.Client;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.Oracle;
using Rebus.Oracle.DataBus;
using Rebus.Time;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for Oracle data bus
    /// </summary>
    public static class OracleDataBusConfigurationExtensions
    {
        /// <summary>
        /// Configures the data bus to store data in a central Oracle table
        /// </summary>
        public static void StoreInOracle(this StandardConfigurer<IDataBusStorage> configurer, string connectionString, string tableName, Action<OracleConnection> additionalConnectionSetup = null, bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();
                var connectionHelper = new OracleConnectionHelper(connectionString, additionalConnectionSetup, enlistInAmbientTransaction);
                return new OracleDataBusStorage(connectionHelper, tableName, automaticallyCreateTables, loggerFactory, rebusTime);
            });
        }
    }
}