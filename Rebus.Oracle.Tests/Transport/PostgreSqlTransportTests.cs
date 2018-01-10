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
    public class PostgreSqlTransportFactory : ITransportFactory
    {

         readonly HashSet<string> _tablesToDrop = new HashSet<string>();
        readonly List<IDisposable> _disposables = new List<IDisposable>();


        [TestFixture, Category(Categories.Oracle)]
        public class PostgreSqlTransportBasicSendReceive : BasicSendReceive<PostgreSqlTransportFactory> { }

        [TestFixture, Category(Categories.Oracle)]
        public class PostgreSqlTransportMessageExpiration : MessageExpiration<PostgreSqlTransportFactory> { }


        public ITransport CreateOneWayClient()
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');
             _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new OracleConnectionHelper(OracleTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new OracleTransport(connectionHelper, tableName, null, consoleLoggerFactory, asyncTaskFactory);

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');

            _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new OracleConnectionHelper(OracleTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new OracleTransport(connectionHelper, tableName, inputQueueAddress, consoleLoggerFactory, asyncTaskFactory);

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            _tablesToDrop.ForEach(OracleTestHelper.DropTable);
            _tablesToDrop.Clear();
        }
    }
}
