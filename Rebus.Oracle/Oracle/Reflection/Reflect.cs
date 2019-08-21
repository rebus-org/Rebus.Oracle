namespace Rebus.Oracle.Reflection
{
    class Reflect
    {
        public static object Value(object obj, string path)
        {
            var empty = new object[0];
            var dots = path.Split('.');

            foreach (var dot in dots)
            {
                var propertyInfo = obj.GetType().GetProperty(dot);
                if (propertyInfo == null) return null;
                obj = propertyInfo.GetValue(obj, empty);
                if (obj == null) break;
            }

            return obj;
        }
    }
}