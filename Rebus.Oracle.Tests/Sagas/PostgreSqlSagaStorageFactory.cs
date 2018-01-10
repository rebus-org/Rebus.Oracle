using Rebus.Logging;
using Rebus.Oracle.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.Oracle.Tests.Sagas
{
    public class PostgreSqlSagaStorageFactory : ISagaStorageFactory
    {
        public PostgreSqlSagaStorageFactory()
        {
            OracleTestHelper.DropTable("saga_index");
            OracleTestHelper.DropTable("saga_data");
        }

        public ISagaStorage GetSagaStorage()
        {
            var postgreSqlSagaStorage = new OracleSqlSagaStorage(OracleTestHelper.ConnectionHelper, "saga_data", "saga_index", new ConsoleLoggerFactory(false));
            postgreSqlSagaStorage.EnsureTablesAreCreated();
            return postgreSqlSagaStorage;
        }

        public void CleanUp()
        {
            //OracleTestHelper.DropTable("saga_index");
            //OracleTestHelper.DropTable("saga_data");
        }
    }
}