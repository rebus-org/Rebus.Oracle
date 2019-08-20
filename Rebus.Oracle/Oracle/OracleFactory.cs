using System;
using System.Runtime.CompilerServices;
using Oracle.ManagedDataAccess.Client;

namespace Rebus.Oracle
{
    /// <summary>
    /// Helps with managing <see cref="OracleConnection"/>s
    /// </summary>
    public class OracleFactory
    {
        private readonly string _connectionString;
        private readonly Action<OracleConnection> _connectionSetup;
        private readonly bool _enlistInAmbientTransaction;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="connectionSetup">Additional setup to be performed prior to opening each connection. 
        /// Useful for configuring client certificate authentication, as well as set up other callbacks.</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an OracleTransaction and enlist in it</param>
        public OracleFactory(string connectionString, Action<OracleConnection> connectionSetup = null, bool enlistInAmbientTransaction = false)
        {
            _connectionString = connectionString;
            _connectionSetup = connectionSetup;
            _enlistInAmbientTransaction = enlistInAmbientTransaction;
        }

        /// <summary> Gets a new, open Oracle connection, without transaction.</summary>
        /// <remarks> This connection does not enlist into ambient transactions, regardless of enlistInAmbienTransaction</remarks>
        public OracleConnection OpenRaw()
        {
            var connection = new OracleConnection(_connectionString);
            try
            {
                _connectionSetup?.Invoke(connection);
                // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
                connection.Open();
                return connection;
            }
            catch
            {
                connection.Dispose();
                throw;
            } 
        }

        /// <summary>
        /// Gets a fresh, open and ready-to-use unit work (open connection + transaction)
        /// </summary>
        public UnitOfWork Open()
        {
            var connection = OpenRaw();
            try
            {
                var transaction = _enlistInAmbientTransaction && Enlist(connection) ? 
                    null : 
                    connection.BeginTransaction();

                return new UnitOfWork(connection, transaction);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }        

        // Keeping this method distinct should avoid JIT and loading System.Transactions when enlist == false (common case).
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool Enlist(OracleConnection connection)
        {
            var ambientTransaction = System.Transactions.Transaction.Current;
            if (ambientTransaction != null)
            {
                connection.EnlistTransaction(ambientTransaction);
                return true;
            }
            else
                return false;
        }
    }
}