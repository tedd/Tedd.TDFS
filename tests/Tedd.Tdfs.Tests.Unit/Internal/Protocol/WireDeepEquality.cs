using System.Collections;
using System.Reflection;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Structural comparison of decoded protocol values: records are compared property by property, lists element by
/// element, sets and maps by content, byte memories by bytes. Reports the path of the first difference.
/// </summary>
internal static class WireDeepEquality
{
    /// <summary>Asserts that <paramref name="expected"/> and <paramref name="actual"/> are structurally equal.</summary>
    public static void AssertEqual(object? expected, object? actual, string root = "$")
    {
        string? difference = FindDifference(expected, actual, root);
        if (difference is not null)
            Assert.Fail($"Structural mismatch at {difference}");
    }

    /// <summary>Returns the path and description of the first difference, or null when equal.</summary>
    public static string? FindDifference(object? expected, object? actual, string path)
    {
        if (expected is null || actual is null)
            return expected is null && actual is null ? null : $"{path}: expected {Describe(expected)}, actual {Describe(actual)}";

        Type type = expected.GetType();
        if (expected is ReadOnlyMemory<byte> em)
        {
            if (actual is not ReadOnlyMemory<byte> am)
                return $"{path}: type {actual.GetType()} instead of ReadOnlyMemory<byte>";
            return em.Span.SequenceEqual(am.Span) ? null : $"{path}: bytes differ ({Convert.ToHexString(em.Span)} vs {Convert.ToHexString(am.Span)})";
        }

        if (expected is WireExtension ee)
        {
            if (actual is not WireExtension ae)
                return $"{path}: type {actual.GetType()} instead of WireExtension";
            if (ee.FieldId != ae.FieldId || ee.Required != ae.Required)
                return $"{path}: extension header differs";
            return ee.Value.Span.SequenceEqual(ae.Value.Span) ? null : $"{path}: extension value differs";
        }

        if (IsScalar(type))
            return Equals(expected, actual) ? null : $"{path}: expected {Describe(expected)}, actual {Describe(actual)}";

        if (expected is IDictionary ed)
        {
            if (actual is not IDictionary ad)
                return $"{path}: actual is not a dictionary";
            if (ed.Count != ad.Count)
                return $"{path}: dictionary count {ed.Count} vs {ad.Count}";
            foreach (DictionaryEntry entry in ed)
            {
                if (!ad.Contains(entry.Key))
                    return $"{path}: missing key {entry.Key}";
                string? d = FindDifference(entry.Value, ad[entry.Key], $"{path}[{entry.Key}]");
                if (d is not null)
                    return d;
            }

            return null;
        }

        if (expected is IReadOnlySet<string> es)
        {
            if (actual is not IReadOnlySet<string> @as)
                return $"{path}: actual is not a string set";
            return es.Count == @as.Count && es.All(@as.Contains) ? null : $"{path}: sets differ ({string.Join(",", es)} vs {string.Join(",", @as)})";
        }

        if (expected is IEnumerable ee2 && expected is not string)
        {
            if (actual is not IEnumerable ae2)
                return $"{path}: actual is not enumerable";
            object?[] el = ee2.Cast<object?>().ToArray();
            object?[] al = ae2.Cast<object?>().ToArray();
            if (el.Length != al.Length)
                return $"{path}: count {el.Length} vs {al.Length}";
            for (int i = 0; i < el.Length; i++)
            {
                string? d = FindDifference(el[i], al[i], $"{path}[{i}]");
                if (d is not null)
                    return d;
            }

            return null;
        }

        if (actual.GetType() != type)
            return $"{path}: type {actual.GetType()} vs {type}";

        foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0)
                continue;
            string? d = FindDifference(property.GetValue(expected), property.GetValue(actual), $"{path}.{property.Name}");
            if (d is not null)
                return d;
        }

        return null;
    }

    private static bool IsScalar(Type type)
        => type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(Guid) || type == typeof(UInt128) ||
           type == typeof(DateTimeOffset) || type == typeof(TimeSpan) || type == typeof(decimal) ||
           (type.IsValueType && type.Namespace == "Tedd.Tdfs"); // identifier structs and CommitToken (value equality)

    private static string Describe(object? value) => value switch
    {
        null => "null",
        string s => $"\"{s}\"",
        _ => value.ToString() ?? value.GetType().Name,
    };
}
