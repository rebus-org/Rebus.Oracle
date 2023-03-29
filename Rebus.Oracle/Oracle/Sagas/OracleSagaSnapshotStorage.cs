using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.Auditing.Sagas;
using Rebus.Oracle.Schema;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.Oracle.Sagas;

/// <summary>
/// Implementation of <see cref="ISagaSnapshotStorage"/> that uses Oracle to store the snapshots
/// </summary>
public class OracleSagaSnapshotStorage : ISagaSnapshotStorage
{
    readonly ObjectSerializer _objectSerializer = new ObjectSerializer();
    readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
    readonly OracleFactory _connectionHelper;
    readonly DbName _table;

    /// <summary>
    /// Constructs the storage
    /// </summary>
    public OracleSagaSnapshotStorage(OracleFactory connectionHelper, string tableName)
    {
        _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
        _table = new DbName(tableName) ?? throw new ArgumentNullException(nameof(tableName));
    }

    /// <summary>
    /// Saves the <paramref name="sagaData"/> snapshot and the accompanying <paramref name="sagaAuditMetadata"/>
    /// </summary>
    public Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
    {
        using (var connection = _connectionHelper.Open())
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"
                        INSERT
                            INTO {_table} (id, revision, data, metadata)
                            VALUES (:id, :revision, :data, :metadata)
                        ";
                command.Parameters.Add("id", OracleDbType.Raw).Value = sagaData.Id;
                command.Parameters.Add("revision", OracleDbType.Int64).Value = sagaData.Revision;
                command.Parameters.Add("data", OracleDbType.Blob).Value = _objectSerializer.Serialize(sagaData);
                command.Parameters.Add("metadata", OracleDbType.Clob).Value = 
                    _dictionarySerializer.SerializeToString(sagaAuditMetadata);

                command.ExecuteNonQuery();
            }
                
            connection.Complete();
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Creates the necessary table if it does not already exist
    /// </summary>
    public void EnsureTableIsCreated()
    {
        using (var connection = _connectionHelper.OpenRaw())
        {
            connection.CreateRebusSagaSnapshot(_table);
        }
    }
}