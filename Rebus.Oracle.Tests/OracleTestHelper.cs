using System;
using Oracle.ManagedDataAccess.Client;
using Rebus.Tests.Contracts;

namespace Rebus.Oracle.Tests
{
    public class OracleTestHelper
    {
        const int TableDoesNotExist = 942;
        const int SequenceDoesNotExist = 2289;
        const int ProcedureDoesNotExist = 4043;
        
        static readonly OracleFactory OracleConnectionHelper = new OracleFactory(ConnectionString);

        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');

        public static string ConnectionString => GetConnectionStringForDatabase(DatabaseName);

        public static OracleFactory ConnectionHelper => OracleConnectionHelper;

        static void DropTable(string tableName, bool dropSequence)
        {
            using (var connection = OracleConnectionHelper.OpenRaw())
            using (var command = connection.CreateCommand())
            {
                try
                {
                    command.CommandText = "drop table " + tableName;
                    command.ExecuteNonQuery();
                    Console.WriteLine($"Dropped oracle table '{tableName}'");                    
                }
                catch (OracleException exception) when (exception.Number == TableDoesNotExist)
                { }
                
                try
                {
                    command.CommandText = $"drop sequence {tableName}_SEQ";
                    command.ExecuteNonQuery();
                    Console.WriteLine("Dropped oracle sequence '{0}_SEQ'", tableName);
                }
                catch (OracleException exception) when (exception.Number == SequenceDoesNotExist)
                { }
            }
        }

        public static void DropTable(string tableName) => DropTable(tableName, dropSequence: false);
        public static void DropTableAndSequence(string tableName) => DropTable(tableName, dropSequence: true);

        public static void DropProcedure(string procedureName)
        {
            using (var connection = OracleConnectionHelper.OpenRaw())
            using (var command = connection.CreateCommand())
            {
                try
                {
                    command.CommandText = "drop procedure " + procedureName;
                    command.ExecuteNonQuery();
                    Console.WriteLine($"Dropped oracle procedure '{procedureName}'");                    
                }
                catch (OracleException exception) when (exception.Number == ProcedureDoesNotExist)
                { }
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            const string dataSource = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCLCDB.LOCALDOMAIN)))";
            const string userId = "rebus";
            const string password = "rebus";
            return Environment.GetEnvironmentVariable("REBUS_ORACLE")
                   ?? $"Data Source={dataSource};User Id={userId};Password={password};Pooling=true;";
        }
    }
}