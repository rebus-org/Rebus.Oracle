using System;
using System.Data;
using Devart.Data.Oracle;

namespace Rebus.Oracle;

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
    public OracleConnectionHelper(string connectionString,
        Action<OracleConnection> additionalConnectionSetupCallback, bool enlistInAmbientTransaction)
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
        var connection = new OracleConnection(_connectionString);

        _additionalConnectionSetupCallback?.Invoke(connection);

        // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
        connection.Open();

        try
        {
            return !_enlistInAmbientTransaction
                ? CreateOracleDbConnection(connection)
                : CreateOracleDbConnectionInAPossiblyAmbientTransaction(connection);
        }
        catch (Exception)
        {
            connection.Dispose();
            throw;
        }
    }

    private OracleDbConnection CreateOracleDbConnectionInAPossiblyAmbientTransaction(OracleConnection connection)
    {
        var ambientTransaction = System.Transactions.Transaction.Current;
        if (ambientTransaction != null)
        {
            connection.EnlistTransaction(ambientTransaction);
            return new OracleDbConnection(connection, null);
        }

        var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
        return new OracleDbConnection(connection, transaction);
    }

    private OracleDbConnection CreateOracleDbConnection(OracleConnection connection)
    {
        var currentTransaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
        return new OracleDbConnection(connection, currentTransaction);
    }
}