using Rebus.Logging;
using Rebus.Oracle.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.Oracle.Tests.Sagas
{
    public class OracleSagaStorageFactory : ISagaStorageFactory
    {
        public OracleSagaStorageFactory()
        {
            OracleTestHelper.DropTable("saga_index");
            OracleTestHelper.DropTable("saga_data");
        }

        public ISagaStorage GetSagaStorage()
        {
            var OracleSagaStorage = new OracleSqlSagaStorage(OracleTestHelper.ConnectionHelper, "saga_data", "saga_index", new ConsoleLoggerFactory(false));
            OracleSagaStorage.EnsureTablesAreCreated();
            return OracleSagaStorage;
        }

        public void CleanUp()
        {
            //OracleTestHelper.DropTable("saga_index");
            //OracleTestHelper.DropTable("saga_data");
        }
    }
}