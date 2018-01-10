using System;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

// ReSharper disable EmptyGeneralCatchClause
#pragma warning disable 1998

namespace Rebus.Oracle
{
    /// <summary>
    /// Wraps an opened <see cref="OracleConnection"/> and makes it easier to work with it
    /// </summary>
    public class OracleDbConnection : IDisposable
    {
        readonly OracleConnection _currentConnection;
        OracleTransaction _currentTransaction;

        bool _disposed;

        /// <summary>
        /// Constructs the wrapper with the given connection and transaction
        /// </summary>
        public OracleDbConnection(OracleConnection currentConnection, OracleTransaction currentTransaction)
        {
            if (currentConnection == null) throw new ArgumentNullException(nameof(currentConnection));
            if (currentTransaction == null) throw new ArgumentNullException(nameof(currentTransaction));
            _currentConnection = currentConnection;
            _currentTransaction = currentTransaction;
        }

        /// <summary>
        /// Creates a new command, enlisting it in the current transaction
        /// </summary>
        public OracleCommand CreateCommand()
        {
            var command = _currentConnection.CreateCommand();
            command.Transaction = _currentTransaction;
            return command;
        }

        /// <summary>
        /// Completes the transaction
        /// </summary>

        public void Complete()
        {
            if (_currentTransaction == null) return;
            using (_currentTransaction)
            {
                _currentTransaction.Commit();
                _currentTransaction = null;
            }
        }

        /// <summary>
        /// Rolls back the transaction if it hasn't been completed
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                try
                {
                    if (_currentTransaction == null) return;
                    using (_currentTransaction)
                    {
                        try
                        {
                            _currentTransaction.Rollback();
                        }
                        catch { }
                        _currentTransaction = null;
                    }
                }
                finally
                {
                    _currentConnection.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}