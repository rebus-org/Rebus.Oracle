using System;
using Oracle.ManagedDataAccess.Client;

namespace Rebus.Oracle.Schema
{
    /// <summary>Methods to create the required DB objects supporting Rebus.Oracle.</summary>
    /// <remarks>These methods are automatically called by Rebus unless you pass automaticallyCreateTables = false during configuration.</remarks>
    public static class OracleConnectionExtensions
    {
        /// <summary>Create objects supporting Transport.</summary>
        public static bool CreateRebusTransport(this OracleConnection connection, DbName tableName) 
            => connection.CreateIfNotExists(tableName, DDL.transport);

        /// <summary>Create objects supporting Timeouts.</summary>
        public static bool CreateRebusTimeout(this OracleConnection connection, DbName tableName)
            => connection.CreateIfNotExists(tableName, DDL.timeout);

        /// <summary>Create objects supporting Subscriptions.</summary>
        public static bool CreateRebusSubscription(this OracleConnection connection, DbName tableName)
            => connection.CreateIfNotExists(tableName, DDL.subscription);

        private static bool Exists(this OracleConnection connection, DbName objectName)
        {
            using (var command = connection.CreateCommand())
            {
                command.BindByName = true;

                if (objectName.Owner == null)
                {
                    command.CommandText = "select 1 from user_objects where object_name = upper(:name)";
                    command.Parameters.Add("name", objectName.Name);
                }
                else
                {
                    command.CommandText = "select 1 from all_objects where owner = upper(:owner) and object_name = upper(:name)";
                    command.Parameters.Add("owner", objectName.Owner);
                    command.Parameters.Add("name", objectName.Name);
                }

                return command.ExecuteScalar() != null;
            }
        }

        private static bool CreateIfNotExists(this OracleConnection connection, DbName tableName, Func<DbName, string[]> ddl)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            if (connection.Exists(tableName)) return false;

            try
            {
                using (var command = connection.CreateCommand())
                {
                    foreach (var sql in ddl(tableName))
                    {
                        command.CommandText = sql;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch 
            {
                // We might fail if another process created the same objects concurrently
                if (connection.Exists(tableName)) return false;
                // Otherwise propagate the error
                throw;
            }
            
            return true;
        }
    }
}