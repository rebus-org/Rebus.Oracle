using System;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.Oracle.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.Oracle.Tests.DataBus
{
    public class OracleDataBusStorageFactory : IDataBusStorageFactory
    {        
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();
        
        public IDataBusStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var sqlServerDataBusStorage = new OracleDataBusStorage(OracleTestHelper.ConnectionHelper, "databus", true, consoleLoggerFactory, _fakeRebusTime);
            sqlServerDataBusStorage.Initialize();
            return sqlServerDataBusStorage;
        }

        public void CleanUp()
        {
            OracleTestHelper.DropTable("databus");
        }

        public void FakeIt(DateTimeOffset fakeTime) => _fakeRebusTime.SetNow(fakeTime);
    }
}