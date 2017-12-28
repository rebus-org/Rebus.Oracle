using System.Collections.Generic;

namespace Rebus.PostgreSql
{
    static class PostgreSqlMagic
    {
        public static List<string> GetTableNames(this OracleDbConnection connection)
        {
            var tableNames = new List<string>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "select table_name from USER_TABLES";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableNames.Add(reader["table_name"].ToString());
                    }
                }
            }

            return tableNames;
        }
    }
}