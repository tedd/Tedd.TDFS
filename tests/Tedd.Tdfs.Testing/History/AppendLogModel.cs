using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// An append-only log (for example a file written only through atomic appends); the state is the ordered list of
/// appended entries.
/// </summary>
/// <remarks>
/// Kinds: <c>"append"</c> (input: the entry bytes; output ignored) and <c>"read"</c>. A read's output is compared when the
/// operation is Ok: a byte payload (byte[]/ReadOnlyMemory&lt;byte&gt;) must equal the concatenation of all entries (whole
/// file read), a sequence of byte arrays must equal the entries one by one, and null means "absent", which matches only
/// an empty log. Duplicate, lost or reordered appends are therefore rejected.
/// </remarks>
public sealed class AppendLogModel : IPrunableSequentialModel<ImmutableList<byte[]>>
{
    private readonly ConditionalWeakTable<ImmutableList<byte[]>, string> _keys = new();

    /// <inheritdoc />
    public ImmutableList<byte[]> Initial { get; } = [];

    /// <inheritdoc />
    public bool Step(ImmutableList<byte[]> state, HistoryOp op, out ImmutableList<byte[]> next)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(op);
        switch (op.Kind)
        {
            case "append":
                byte[] entry = ModelValues.ToBytes(op.Input, op, "append input")
                    ?? throw new ArgumentException($"Operation #{op.Id}: append input must not be null.");
                next = state.Add(entry);
                return true;
            case "read":
                next = state;
                return op.Outcome != OpOutcome.Ok || Matches(state, op);
            default:
                throw new ArgumentException($"Operation #{op.Id}: AppendLogModel does not support kind '{op.Kind}'.");
        }
    }

    /// <inheritdoc />
    /// <remarks>Appends only extend the log, so a completed read can only be satisfied later if the log so far is a prefix of what it observed.</remarks>
    public bool CanStillSatisfy(ImmutableList<byte[]> state, HistoryOp pending)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(pending);
        if (!string.Equals(pending.Kind, "read", StringComparison.Ordinal) || pending.Outcome != OpOutcome.Ok)
            return true;
        switch (pending.Output)
        {
            case null:
                return state.Count == 0;
            case IEnumerable<byte[]> entries:
            {
                int i = 0;
                foreach (byte[] observed in entries)
                {
                    if (i >= state.Count)
                        return true;
                    if (observed is null || !observed.AsSpan().SequenceEqual(state[i]))
                        return false;
                    i++;
                }

                return i >= state.Count;
            }

            default:
            {
                byte[] observed = ModelValues.ToBytes(pending.Output, pending, "read output")!;
                int offset = 0;
                foreach (byte[] e in state)
                {
                    if (offset + e.Length > observed.Length || !observed.AsSpan(offset, e.Length).SequenceEqual(e))
                        return false;
                    offset += e.Length;
                }

                return true;
            }
        }
    }

    /// <inheritdoc />
    public string Key(ImmutableList<byte[]> state)
    {
        ArgumentNullException.ThrowIfNull(state);
        return _keys.GetValue(state, static s => ComputeKey(s));
    }

    private static bool Matches(ImmutableList<byte[]> state, HistoryOp op)
    {
        switch (op.Output)
        {
            case null:
                return state.Count == 0;
            case IEnumerable<byte[]> entries:
            {
                int i = 0;
                foreach (byte[] observed in entries)
                {
                    if (i >= state.Count || observed is null || !observed.AsSpan().SequenceEqual(state[i]))
                        return false;
                    i++;
                }

                return i == state.Count;
            }

            default:
            {
                byte[] observed = ModelValues.ToBytes(op.Output, op, "read output")!;
                long total = 0;
                foreach (byte[] e in state)
                    total += e.Length;
                if (total != observed.Length)
                    return false;
                int offset = 0;
                foreach (byte[] e in state)
                {
                    if (!observed.AsSpan(offset, e.Length).SequenceEqual(e))
                        return false;
                    offset += e.Length;
                }

                return true;
            }
        }
    }

    private static string ComputeKey(ImmutableList<byte[]> state)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[sizeof(int)];
        foreach (byte[] entry in state)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(length, entry.Length);
            hash.AppendData(length);
            hash.AppendData(entry);
        }

        return state.Count.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + Convert.ToHexString(hash.GetHashAndReset());
    }
}
