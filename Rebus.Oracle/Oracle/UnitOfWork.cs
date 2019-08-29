using System;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace Rebus.Oracle
{
    /// <summary>
    /// Wraps an opened <see cref="OracleConnection"/> and makes it easier to work with it
    /// </summary>
    /// <remarks>
    /// The embedded transaction can only be used once (you can't open a new transaction),
    /// effectively making this a unit of work.
    /// </remarks>
    public struct UnitOfWork : IDisposable
    {
        private readonly OracleConnection _connection;
        private readonly OracleTransaction _transaction;

        /// <summary>
        /// Constructs the wrapper with the given connection and transaction
        /// </summary>
        public UnitOfWork(OracleConnection connection, OracleTransaction transaction)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _transaction = transaction;
        }

        /// <summary>
        /// Creates a new command, enlisting it in the current transaction
        /// </summary>
        public OracleCommand CreateCommand()
        {
            // There's no need to set the Transaction, it's a no-op in Oracle.
            // Transactions are entirely managed at the connection level.
            var command = _connection.CreateCommand();
            command.BindByName = true;  // You're welcome
            return command;
        }

        /// <summary>
        /// Creates a new blob, associated with the current connection
        /// </summary>
        public OracleBlob CreateBlob() => new OracleBlob(_connection);

        /// <summary>
        /// Completes the transaction
        /// </summary>
        public void Complete()
        {
            // Note: if the local transaction is null, it means that we're part of a larger transaction 
            // (such as TransactionScope or externally managed) and so we don't control commit.

            _transaction?.Commit();

            // We don't nullify the _transaction, but this should not be used after a commit anyway,
            // as there is no more active transaction.
            // Note: keeping the transaction means it will be disposed... well in Dispose() of course.
        }

        /// <summary>
        /// Rolls back the transaction if it hasn't been completed
        /// </summary>
        public void Dispose()
        {
            // It's better not to call Dispose() twice, but it doesn't crash
            // Transaction automatically rollbacks if it wasn't committed before calling Dispose
            _transaction?.Dispose();
            _connection.Dispose();
        }
    }
}