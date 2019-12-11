using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.Oracle.Tests.Integration
{
    [TestFixture, Category(Categories.Oracle)]
    public class TestOracleAllTheWay : FixtureBase
    {
        static readonly string ConnectionString = OracleTestHelper.ConnectionString;

        BuiltinHandlerActivator _activator;
        IBus _bus;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            _bus = Configure.With(_activator)
                .Transport(x => x.UseOracle(ConnectionString, "transports", "test.input", true))
                .Subscriptions(_ => _.StoreInOracle(ConnectionString, "subscriptions", true, enlistInAmbientTransaction: true))
                .Options(x =>
                {
                    x.SetNumberOfWorkers(1);
                    x.SetMaxParallelism(1);
                })
                .Start();
        }

        protected override void TearDown()
        {
            OracleTestHelper.DropTableAndSequence("transports");
            OracleTestHelper.DropTableAndSequence("subscriptions");
            OracleTestHelper.DropProcedure("rebus_dequeue_transports");
        }

        [Test]
        public async Task SendAndReceiveOneSingleMessage()
        {
            var gotTheMessage = new ManualResetEvent(false);
            var receivedMessageCount = 0;

            _activator.Handle<string>(async message =>
            {
                await Task.CompletedTask;
                Interlocked.Increment(ref receivedMessageCount);
                Console.WriteLine("w00000t! Got message: {0}", message);
                gotTheMessage.Set();
            });

            await _bus.SendLocal("hej med dig min ven!");

            gotTheMessage.WaitOrDie(TimeSpan.FromSeconds(10));

            await Task.Delay(500);

            Assert.That(receivedMessageCount, Is.EqualTo(1));
        }

        [TestCase(true, false, true)]
        [TestCase(false, false, false)]
        [TestCase(true, true, false)]
        public async Task SendAndReceiveOneSingleMessageInAnAmbientTransaction(bool completeTransaction, bool throwInsideTransactionScope, bool expectMessageReceived)
        {
            var gotTheMessage = new ManualResetEvent(false);
            var receivedMessageCount = 0;

            _activator.Handle<string>(async message =>
            {
                await Task.CompletedTask;
                Interlocked.Increment(ref receivedMessageCount);
                Console.WriteLine("w00000t! Got message: {0}", message);
                gotTheMessage.Set();
            });

            try
            {
                using (var tx = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted,
                    Timeout = TimeSpan.FromSeconds(60)
                }, TransactionScopeAsyncFlowOption.Enabled))
                {
                    await _bus.SendLocal("alles cavakes?");

                    if (throwInsideTransactionScope)
                    {
                        throw new RebusApplicationException("the ting goes skraa");
                    }

                    if (completeTransaction)
                    {
                        tx.Complete();
                    }
                }
            }
            catch (RebusApplicationException) when (throwInsideTransactionScope)
            {
                // Intended
            }

            if (expectMessageReceived)
            {
                gotTheMessage.WaitOrDie(TimeSpan.FromSeconds(10));
            }

            await Task.Delay(500);

            Assert.That(receivedMessageCount, Is.EqualTo(expectMessageReceived ? 1 : 0));
        }

        [Test]
        public async Task PublishAndReceivesOneSingleMessage()
        {
            await _bus.Subscribe<string>();

            var gotTheMessage = new ManualResetEvent(false);
            var receivedMessageCount = 0;

            _activator.Handle<string>(async message =>
            {
                await Task.CompletedTask;
                Interlocked.Increment(ref receivedMessageCount);
                Console.WriteLine("Received message: {0}", message);
                gotTheMessage.Set();
            });

            await _bus.Publish("hallo daar mijn vriend!");

            gotTheMessage.WaitOrDie(TimeSpan.FromSeconds(10));

            await Task.Delay(500);

            Assert.That(receivedMessageCount, Is.EqualTo(1));
        }

        [TestCase(true, false, true)]
        [TestCase(false, false, false)]
        [TestCase(true, true, false)]
        public async Task PublishAndReceiveOneSingleMessageInAnAmbientTransaction(bool completeTransaction, bool throwInsideTransactionScope, bool expectMessageReceived)
        {
            await _bus.Subscribe<string>();
            var gotTheMessage = new ManualResetEvent(false);
            var receivedMessageCount = 0;

            _activator.Handle<string>(async message =>
            {
                await Task.CompletedTask;
                Interlocked.Increment(ref receivedMessageCount);
                Console.WriteLine("Received message: {0}", message);
                gotTheMessage.Set();
            });

            try
            {
                using (var tx = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted,
                    Timeout = TimeSpan.FromSeconds(60)
                }, TransactionScopeAsyncFlowOption.Enabled))
                {
                    await _bus.Publish("alles cavakes?");

                    if (throwInsideTransactionScope)
                    {
                        throw new RebusApplicationException("the ting goes skraa");
                    }

                    if (completeTransaction)
                    {
                        tx.Complete();
                    }
                }
            }
            catch (RebusApplicationException) when (throwInsideTransactionScope)
            {
                // Intended
            }

            if (expectMessageReceived)
            {
                gotTheMessage.WaitOrDie(TimeSpan.FromSeconds(10));
            }

            await Task.Delay(500);

            Assert.That(receivedMessageCount, Is.EqualTo(expectMessageReceived ? 1 : 0));
        }
    }
}
