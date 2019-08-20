using NUnit.Framework;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.Oracle.Tests.DataBus
{
    [TestFixture]
    public class SqlServerDataBusStorageTest : GeneralDataBusStorageTests<OracleDataBusStorageFactory> { }
}