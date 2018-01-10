using NUnit.Framework;
using Rebus.Logging;
using Rebus.Oracle.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.Oracle.Tests.Timeouts
{
    [TestFixture, Category(TestCategory.Oracle)]
    public class TestPostgreSqlTimeoutManager : BasicStoreAndRetrieveOperations<PostgreSqlTimeoutManagerFactory>
    {
    }

    public class PostgreSqlTimeoutManagerFactory : ITimeoutManagerFactory
    {
        public PostgreSqlTimeoutManagerFactory()
        {
            OracleTestHelper.DropTable("timeouts");
        }

        public ITimeoutManager Create()
        {
            var postgreSqlTimeoutManager = new OracleTimeoutManager(OracleTestHelper.ConnectionHelper, "timeouts", new ConsoleLoggerFactory(false));
            postgreSqlTimeoutManager.EnsureTableIsCreated();
            return postgreSqlTimeoutManager;
        }

        public void Cleanup()
        {
            OracleTestHelper.DropTable("timeouts");
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }
    }

}