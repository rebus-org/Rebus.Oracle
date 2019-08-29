using System;
using System.Collections.Generic;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Oracle.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.Oracle.Tests.Transport
{
    public class OracleTransportFactory : ITransportFactory
    {
        readonly string _tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');
        readonly List<IDisposable> _disposables = new List<IDisposable>();
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        [TestFixture, Category(Categories.Oracle)]
        public class OracleTransportBasicSendReceive : BasicSendReceive<OracleTransportFactory> 
        { }

        [TestFixture, Category(Categories.Oracle)]
        public class OracleTransportMessageExpiration : MessageExpiration<OracleTransportFactory> 
        { }

        public ITransport CreateOneWayClient()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new OracleFactory(OracleTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new OracleTransport(connectionHelper, _tableName, null, consoleLoggerFactory, asyncTaskFactory, _fakeRebusTime);

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new OracleFactory(OracleTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new OracleTransport(connectionHelper, _tableName, inputQueueAddress, consoleLoggerFactory, asyncTaskFactory, _fakeRebusTime);

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            OracleTestHelper.DropTableAndSequence(_tableName);
            OracleTestHelper.DropProcedure("rebus_dequeue_" + _tableName);
        }

        public void FakeIt(DateTimeOffset fakeTime) => _fakeRebusTime.SetNow(fakeTime);
    }
}
