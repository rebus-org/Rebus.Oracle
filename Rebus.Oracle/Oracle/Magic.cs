using System;
using System.Collections.Generic;
using System.Globalization;
using Oracle.ManagedDataAccess.Types;

namespace Rebus.Oracle
{
    static class OracleMagic
    {
        public static List<string> GetTableNames(this OracleDbConnection connection)
        {
            var tableNames = new List<string>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "select table_name from USER_TABLES";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableNames.Add(reader["table_name"].ToString());
                    }
                }
            }

            return tableNames;
        }

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