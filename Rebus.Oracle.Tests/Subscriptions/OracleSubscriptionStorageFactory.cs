using Rebus.Logging;
using Rebus.Oracle.Subscriptions;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.Oracle.Tests.Subscriptions
{
    public class OracleSubscriptionStorageFactory : ISubscriptionStorageFactory
    {
        public OracleSubscriptionStorageFactory()
        {
            Cleanup();
        }

        public ISubscriptionStorage Create()
        {
            var subscriptionStorage = new OracleSubscriptionStorage(OracleTestHelper.ConnectionHelper, "subscriptions", true, new ConsoleLoggerFactory(false));
            subscriptionStorage.EnsureTableIsCreated();
            return subscriptionStorage;
        }

        public void Cleanup()
        {
            OracleTestHelper.DropTableAndSequence("subscriptions");
        }
    }
}