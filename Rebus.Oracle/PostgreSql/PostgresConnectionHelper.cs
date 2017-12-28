using System;
using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace Rebus.PostgreSql
{
    /// <summary>
    /// Helps with managing <see cref="NpgsqlConnection"/>s
    /// </summary>
    public class OracleConnectionHelper
    {
        readonly string _connectionString;
        private readonly Action<OracleConnection> _additionalConnectionSetupCallback;

        /// <summary>
        /// Constructs this thingie
        /// </summary>
        public OracleConnectionHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Constructs this thingie
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="additionalConnectionSetupCallback">Additional setup to be performed prior to opening each connection. 
        /// Useful for configuring client certificate authentication, as well as set up other callbacks.</param>
        public OracleConnectionHelper(string connectionString, Action<OracleConnection> additionalConnectionSetupCallback)
        {
            _connectionString = connectionString;
            _additionalConnectionSetupCallback = additionalConnectionSetupCallback;
        }


        /// <summary>
        /// Gets a fresh, open and ready-to-use connection wrapper
        /// </summary>
        public async Task<OracleDbConnection> GetConnection()
        {
            var connection = new OracleConnection(_connectionString);
            
            if (_additionalConnectionSetupCallback != null)
                _additionalConnectionSetupCallback.Invoke(connection);

            await connection.OpenAsync();

            var currentTransaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            return new OracleDbConnection(connection, currentTransaction);
        }
    }
}