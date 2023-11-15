﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Devart.Data.Oracle;
using Rebus.Auditing.Sagas;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.Oracle.Sagas;

/// <summary>
/// Implementation of <see cref="ISagaSnapshotStorage"/> that uses Oracle to store the snapshots
/// </summary>
public class OracleSagaSnapshotStorage : ISagaSnapshotStorage
{
    readonly DictionarySerializer _dictionarySerializer = new();
    readonly ObjectSerializer _objectSerializer = new();
    readonly OracleConnectionHelper _connectionHelper;
    readonly string _tableName;

    /// <summary>
    /// Constructs the storage
    /// </summary>
    public OracleSagaSnapshotStorage(OracleConnectionHelper connectionHelper, string tableName)
    {
        _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
    }

    /// <summary>
    /// Saves the <paramref name="sagaData"/> snapshot and the accompanying <paramref name="sagaAuditMetadata"/>
    /// </summary>
    public Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
    {
        using var connection = _connectionHelper.GetConnection();
        
        using (var command = connection.CreateCommand())
        {
            command.CommandText =
                $@"
                        INSERT
                            INTO {_tableName} (id, revision, data, metadata)
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

    /// <summary>
    /// Creates the necessary table if it does not already exist
    /// </summary>
    public void EnsureTableIsCreated()
    {
        using var connection = _connectionHelper.GetConnection();
        
        var tableNames = new HashSet<string>(connection.GetTableNames(), StringComparer.OrdinalIgnoreCase);

        if (tableNames.Contains(_tableName)) return;

        using (var command = connection.CreateCommand())
        {
            command.CommandText =
                $@"
                        CREATE TABLE {_tableName} (
                            id RAW(16) NOT NULL,
                            revision NUMBER(10) NOT NULL,
                            metadata CLOB NOT NULL,
                            data BLOB NOT NULL,
                            CONSTRAINT {_tableName}_pk PRIMARY KEY (id, revision)
                        )
                        ";

            command.ExecuteNonQuery();
        }

        connection.Complete();
    }
}