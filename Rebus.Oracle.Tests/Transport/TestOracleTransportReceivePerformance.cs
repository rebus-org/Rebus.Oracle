﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Oracle.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;

#pragma warning disable 1998

namespace Rebus.Oracle.Tests.Transport
{
    [TestFixture, Category(Categories.Oracle)]
    public class TestOracleTransportReceivePerformance : FixtureBase
    {
        BuiltinHandlerActivator _adapter;

        const string QueueName = "perftest";

        static readonly string TableName = TestConfig.GetName("Messages");

        protected override void SetUp()
        {
            _adapter = Using(new BuiltinHandlerActivator());

            Configure.With(_adapter)
                .Logging(l => l.ColoredConsole(LogLevel.Warn))
                .Transport(t => t.UseOracle(OracleTestHelper.ConnectionString, TableName, QueueName))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(20);
                })
                .Start();
        }

        protected override void TearDown()
        {
            OracleTestHelper.DropTableAndSequence(TableName);
            OracleTestHelper.DropProcedure("rebus_dequeue_" + TableName);
        }

        [TestCase(1000)]
        public async Task NizzleName(int messageCount)
        {

            Console.WriteLine($"Sending {messageCount} messages...");

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => _adapter.Bus.SendLocal($"THIS IS MESSAGE {i}")));

            var counter = new SharedCounter(messageCount);

            _adapter.Handle<string>(async message => counter.Decrement());

            Console.WriteLine("Waiting for messages to be received...");

            var stopwtach = Stopwatch.StartNew();

            _adapter.Bus.Advanced.Workers.SetNumberOfWorkers(3);

            counter.WaitForResetEvent(messageCount / 500 + 7);

            var elapsedSeconds = stopwtach.Elapsed.TotalSeconds;

            Console.WriteLine(
                $"{messageCount} messages received in {elapsedSeconds:0.0} s - that's {messageCount/elapsedSeconds:0.0} msg/s");
        }
    }
}
