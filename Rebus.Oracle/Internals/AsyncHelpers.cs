using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Internals
{

    /// <summary>
    /// Used to remove context within an asynchronous method. All further async calls
    /// after awaiting this class will be performed without a synchronization context. 
    /// this avoids the need to call ConfigureAwait(false), all the way down the call
    /// stack. Instead, doing await new SynchronizationContextRemover() at an api entry 
    /// point should suffice. Previous synchronization context will be restored upon return
    /// from the method that uses this.
    /// 
    /// </summary>
    public class SynchronizationContextRemover : INotifyCompletion
    {
        /// <summary>
        /// Implementation of IsCompleted for awaitable pattern
        /// If current sync context is already null no need to run OnCompleted....
        /// </summary>
        public bool IsCompleted => SynchronizationContext.Current == null;

        /// <summary>
        /// 
        /// Implemetnation of OnCompleted for awaitable pattern
        /// Will run all continuations with no synchronization context.
        /// After the continuation chain has finished executing it will restore
        /// the previous context
        /// </summary>
        /// <param name="continuation"></param>
        public void OnCompleted(Action continuation)
        {
            var previousContext = SynchronizationContext.Current;
            try
            {
                SynchronizationContext.SetSynchronizationContext(null);
                continuation();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previousContext);
            }
        }

        /// <summary>
        /// GetAwaiter() awaitable pattern 
        /// </summary>
        /// <returns></returns>
        public SynchronizationContextRemover GetAwaiter()
        {
            return this;
        }

        /// <summary>
        /// GetResult for awaitable pattern 
        /// </summary>
        public void GetResult()
        {
        }
    }
}