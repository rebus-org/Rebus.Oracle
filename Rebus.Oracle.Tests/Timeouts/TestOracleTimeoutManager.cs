using System;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.Oracle.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.Oracle.Tests.Timeouts
{
    [TestFixture, Category(TestCategory.Oracle)]
    public class TestOracleTimeoutManager : BasicStoreAndRetrieveOperations<OracleTimeoutManagerFactory>
    {
    }

    public class OracleTimeoutManagerFactory : ITimeoutManagerFactory
    {
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public ITimeoutManager Create()
        {
            var oracleTimeoutManager = new OracleTimeoutManager(OracleTestHelper.ConnectionHelper, "timeouts", new ConsoleLoggerFactory(false), _fakeRebusTime);
            oracleTimeoutManager.EnsureTableIsCreated();
            return oracleTimeoutManager;
        }

        public void Cleanup()
        {
            OracleTestHelper.DropTableAndSequence("timeouts");
        }

        public void FakeIt(DateTimeOffset fakeTime) => _fakeRebusTime.SetNow(fakeTime);

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }
    }
}