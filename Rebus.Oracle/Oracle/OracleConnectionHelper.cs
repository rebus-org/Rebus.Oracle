using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Rebus.Oracle
{
    /// <summary>
    /// Helps with managing <see cref="OracleConnection"/>s
    /// </summary>
    public class OracleConnectionHelper
    {
        readonly string _connectionString;
        private readonly Action<OracleConnection> _additionalConnectionSetupCallback;
        private readonly bool _enlistInAmbientTransaction;

        /// <summary>
        /// Constructs this thingie
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        public OracleConnectionHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Constructs this thingie
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an OracleTransaction and enlist in it</param>
        public OracleConnectionHelper(string connectionString, bool enlistInAmbientTransaction) 
            : this(connectionString, null, enlistInAmbientTransaction)
        {
        }

        /// <summary>
        /// Constructs this thingie
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="additionalConnectionSetupCallback">Additional setup to be performed prior to opening each connection. 
        /// Useful for configuring client certificate authentication, as well as set up other callbacks.</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an OracleTransaction and enlist in it</param>
        public OracleConnectionHelper(string connectionString, Action<OracleConnection> additionalConnectionSetupCallback, bool enlistInAmbientTransaction)
        {
            _connectionString = connectionString;
            _additionalConnectionSetupCallback = additionalConnectionSetupCallback;
            _enlistInAmbientTransaction = enlistInAmbientTransaction;
        }


        /// <summary>
        /// Gets a fresh, open and ready-to-use connection wrapper
        /// </summary>
        public OracleDbConnection GetConnection()
        {
            OracleConnection connection = null;
            OracleTransaction transaction = null;
            try
            {
                if (!_enlistInAmbientTransaction)
                {
                    connection = CreateOracleConnectionSuppressingAPossibleAmbientTransaction();
                    transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                }
                else
                {
                    connection = CreateOracleConnectionInAPossiblyAmbientTransaction();
                }

                return new OracleDbConnection(connection, transaction);
            }
            catch (Exception)
            {
                connection?.Dispose();
                throw;
            }
        }

        private OracleConnection CreateOracleConnectionInAPossiblyAmbientTransaction()
        {
            var connection = new OracleConnection(_connectionString);

            _additionalConnectionSetupCallback?.Invoke(connection);

            // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
            connection.Open();

            var transaction = System.Transactions.Transaction.Current;
            if (transaction != null)
            {
                connection.EnlistTransaction(transaction);
            }

            return connection;
        }

        private OracleConnection CreateOracleConnectionSuppressingAPossibleAmbientTransaction()
        {
            OracleConnection connection;

            using (new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
            {
                connection = new OracleConnection(_connectionString);

                _additionalConnectionSetupCallback?.Invoke(connection);

                // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
                connection.Open();
            }

            return connection;
        }
    }
}