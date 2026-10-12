using System.Collections;
using System.Text;

namespace Tedd.Tdfs.Testing;

/// <summary>Compact, bounded descriptions of history operations and values for checker explanations.</summary>
internal static class HistoryFormat
{
    private const int MaxItems = 24;

    public static string Op(HistoryOp op)
    {
        var sb = new StringBuilder();
        sb.Append('#').Append(op.Id).Append(' ').Append(op.Process).Append(' ').Append(op.Kind);
        if (op.Input is not null)
            sb.Append(" in=").Append(Value(op.Input));
        if (op.Outcome == OpOutcome.Ok && op.Output is not null)
            sb.Append(" out=").Append(Value(op.Output));
        sb.Append(' ').Append(op.Outcome);
        sb.Append(" [").Append(op.InvokeTime).Append(", ");
        sb.Append(op.EffectDeadline?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "∞").Append(']');
        if (op.WasResolved)
            sb.Append(" resolved");
        return sb.ToString();
    }

    public static string Ops(IEnumerable<string> descriptions)
    {
        List<string> list = descriptions.ToList();
        if (list.Count == 0)
            return "(none)";
        if (list.Count <= MaxItems)
            return string.Join("; ", list);
        return $"... ({list.Count - MaxItems} earlier); " + string.Join("; ", list.Skip(list.Count - MaxItems));
    }

    public static string Value(object? value)
    {
        switch (value)
        {
            case null:
                return "null";
            case byte[] bytes:
                return Bytes(bytes);
            case ReadOnlyMemory<byte> rom:
                return Bytes(rom.Span);
            case Memory<byte> mem:
                return Bytes(mem.Span);
            case string s:
                return Text(s);
            case Exception ex:
                return ex.GetType().Name;
            case IEnumerable<byte[]> entries:
                return "[" + string.Join(", ", entries.Take(8).Select(b => Bytes(b))) + (entries.Skip(8).Any() ? ", ..." : string.Empty) + "]";
            case IEnumerable enumerable:
                return "[" + string.Join(", ", enumerable.Cast<object?>().Take(8).Select(Value)) + "]";
            default:
                return Text(value.ToString() ?? string.Empty);
        }
    }

    public static string Bytes(ReadOnlySpan<byte> bytes)
    {
        const int Shown = 12;
        string hex = Convert.ToHexString(bytes[..Math.Min(bytes.Length, Shown)]);
        return bytes.Length <= Shown ? $"bytes[{bytes.Length}]:{hex}" : $"bytes[{bytes.Length}]:{hex}…";
    }

    public static string Text(string s, int max = 64) =>
        s.Length <= max ? "\"" + s + "\"" : "\"" + s[..max] + "…\"(" + s.Length + ")";
}
