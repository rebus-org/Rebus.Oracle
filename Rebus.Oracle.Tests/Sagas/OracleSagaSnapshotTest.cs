using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.Oracle.Tests.Sagas
{
    [TestFixture, Category(TestCategory.Oracle)]
    public class OracleSagaSnapshotTest : SagaSnapshotStorageTest<OracleSnapshotStorageFactory> 
    { 
        protected override void TearDown()
        {
            base.TearDown();
            OracleTestHelper.DropTable(OracleSnapshotStorageFactory.TableName);
        }
    }
}