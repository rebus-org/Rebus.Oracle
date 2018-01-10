using System;
using Npgsql;
using Rebus.Tests.Contracts;

namespace Rebus.Oracle.Tests
{
    public class OracleTestHelper
    {
        const string TableDoesNotExist = "42P01";
        static readonly OracleConnectionHelper OracleConnectionHelper = new OracleConnectionHelper(ConnectionString);

        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');

        public static string ConnectionString => GetConnectionStringForDatabase(DatabaseName);

        public static OracleConnectionHelper ConnectionHelper => OracleConnectionHelper;

        public static void DropTable(string tableName)
        {
            using (var connection = OracleConnectionHelper.GetConnection().Result)
            {
                using (var comand = connection.CreateCommand())
                {
                    comand.CommandText = $@"drop table ""{tableName}"";";

                    try
                    {
                        comand.ExecuteNonQuery();

                        Console.WriteLine("Dropped postgres table '{0}'", tableName);
                    }
                    catch (PostgresException exception) when (exception.SqlState == TableDoesNotExist)
                    {
                    }
                }

                connection.Complete();
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_POSTGRES")
                   ?? $"server=localhost; database={databaseName}; user id=postgres; password=postgres;maximum pool size=30;";
        }
    }
}