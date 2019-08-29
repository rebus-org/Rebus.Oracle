using System;
using Oracle.ManagedDataAccess.Client;
using Rebus.Oracle.Schema;

namespace Rebus.Oracle.Transport
{
    // Struct wrapper (free) around an OracleCommand that enables efficient typed access to command parameters
    struct SendCommand
    {       
        private readonly OracleCommand _command;

        public SendCommand(OracleCommand command)
        {
            _command = command;
        }

        public static string Sql(DbName table) => 
            $@"INSERT INTO {table} (recipient,  headers,  body,  priority,  visible,         expiration)
               VALUES              (:recipient, :headers, :body, :priority, :now + :visible, :now + :ttlseconds)";

        public static OracleCommand Create(UnitOfWork connection, string sql) 
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            // Keep those parameters in correct order as used in properties below!
            command.Parameters.Add("recipient", OracleDbType.Varchar2);     // 0
            command.Parameters.Add("headers", OracleDbType.Blob);           // 1
            command.Parameters.Add("body", OracleDbType.Blob);              // 2
            command.Parameters.Add("priority", OracleDbType.Int32);         // 3
            command.Parameters.Add("now", OracleDbType.TimeStampTZ);        // 4
            command.Parameters.Add("visible", OracleDbType.IntervalDS);     // 5
            command.Parameters.Add("ttlseconds", OracleDbType.IntervalDS);  // 6
            return command;
        }

        public string Recipient { set => _command.Parameters[0].Value = value; }
        public byte[] Headers { set => _command.Parameters[1].Value = value; }
        public byte[] Body { set => _command.Parameters[2].Value = value; }
        public int Priority { set => _command.Parameters[3].Value = value; }
        public DateTimeOffset Now { set => _command.Parameters[4].Value = value.ToOracleTimeStamp(); }
        public TimeSpan Visible { set => _command.Parameters[5].Value = value; }
        public TimeSpan TtlSeconds { set => _command.Parameters[6].Value = value; }

        public int ExecuteNonQuery() => _command.ExecuteNonQuery();
    }
}