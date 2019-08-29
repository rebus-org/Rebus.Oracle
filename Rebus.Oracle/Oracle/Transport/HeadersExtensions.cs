using System;
using System.Collections.Generic;
using Rebus.Extensions;
using Rebus.Messages;

namespace Rebus.Oracle.Transport
{
    static class HeadersExtensions
    {
        public static int GetMessagePriority(this Dictionary<string, string> headers)
        {
            if (!headers.TryGetValue(OracleTransport.MessagePriorityHeaderKey, out var priorityString))
                return 0;
            
            if (!int.TryParse(priorityString, out int priority))
                throw new FormatException($"Could not parse '{priorityString}' into an Int32!");
            
            return priority;
        }

        public static TimeSpan GetInitialVisibilityDelay(this Dictionary<string, string> headers, DateTimeOffset now)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilString))
                return TimeSpan.Zero;

            return deferredUntilString.ToDateTimeOffset() - now;
        }

        public static TimeSpan GetTtlSeconds(this Dictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedString))
                return TimeSpan.FromDays(36500); // about 100 years. Don't use TimeSpan.MaxValue (~10 million days) because Oracle doesn't support dates after year 9999

            if (!TimeSpan.TryParse(timeToBeReceivedString, out var timeToBeReceived))
                throw new FormatException($"Could not parse '{timeToBeReceivedString}' into a TimeSpan!");

            return timeToBeReceived;
        }
    }
}