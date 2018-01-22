using NUnit.Framework;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.Oracle.Tests.Subscriptions
{
    [TestFixture, Category(TestCategory.Oracle)]
    public class OracleSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<OracleSubscriptionStorageFactory>
    {
    }
}
