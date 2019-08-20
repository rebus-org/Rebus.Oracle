using System;
using System.Globalization;
using Oracle.ManagedDataAccess.Types;

namespace Rebus.Oracle
{
    static class OracleMagic
    {
        public static DateTimeOffset ToDateTimeOffset(this OracleTimeStampTZ value)
        {
            return new DateTimeOffset(value.Value, value.GetTimeZoneOffset());
        }

        public static OracleTimeStampTZ ToOracleTimeStamp(this DateTimeOffset value)
        {
            return new OracleTimeStampTZ(value.DateTime, value.Offset.ToString("c", CultureInfo.InvariantCulture));
        }
    }
}