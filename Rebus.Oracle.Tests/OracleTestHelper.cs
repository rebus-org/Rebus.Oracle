using System;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Rebus.Tests.Contracts;

namespace Rebus.Oracle.Tests
{
    public class OracleTestHelper
    {
        const int TableDoesNotExist = 942;
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
                    comand.CommandText = $@"drop table ""{tableName}""";

                    try
                    {
                        comand.ExecuteNonQuery();

                        Console.WriteLine("Dropped oracle table '{0}'", tableName);
                    }
                    catch (OracleException exception) when (exception.Number == TableDoesNotExist)
                    {
                    }
                }

                connection.Complete();
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_ORACLE")
                   ?? $"User Id={databaseName}; Password=rebus; Data Source=localhost/xe;";
        }
    }
}