using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.Oracle.Tests.Sagas
{
    [TestFixture, Category(TestCategory.Postgres)]
    public class OracleSagaSnapshotTest : SagaSnapshotStorageTest<OracleSnapshotStorageFactory> { }
}