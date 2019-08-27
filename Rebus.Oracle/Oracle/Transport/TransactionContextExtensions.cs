using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Rebus.Transport;

namespace Rebus.Oracle.Transport
{
    static class TransactionContextExtensions
    {
        const string ConnectionKey = "oracle-transport-connection";
        const string SendCommandKey = "oracle-transport-send-command";

        public static UnitOfWork GetConnection(this ITransactionContext context, OracleFactory factory)
        {
            return context.GetOrAdd(ConnectionKey, () =>
            {
                var connection = factory.Open();
                context.OnCommitted(() =>
                {
                    connection.Complete();
                    return Task.CompletedTask;
                });
                context.OnDisposed(connection.Dispose);
                return connection;
            });
        }

        public static OracleCommand GetSendCommand(this ITransactionContext context, OracleFactory factory, string sql)
        {
            return context.GetOrAdd(SendCommandKey, () => 
            {
                var connection = context.GetConnection(factory);
                var command = SendCommand.Create(connection, sql);
                context.OnDisposed(command.Dispose);
                return command;
            });
        }
    }
}