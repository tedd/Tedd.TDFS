using System.Runtime.CompilerServices;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// A single byte-array register (for example one file's whole content); the state is the content or null when absent.
/// </summary>
/// <remarks>
/// Kinds: <c>"write"</c> (input: the new content, byte[]/ReadOnlyMemory&lt;byte&gt;; output ignored),
/// <c>"read"</c> (output: the content read, or null for "absent"; must equal the state when the operation is Ok),
/// <c>"delete"</c> (state becomes null). Unknown kinds throw <see cref="ArgumentException"/>.
/// </remarks>
public sealed class RegisterModel : ISequentialModel<byte[]?>
{
    private const string AbsentKey = "-";
    private readonly ConditionalWeakTable<byte[], string> _keys = new();

    /// <summary>Creates a register that is initially absent.</summary>
    public RegisterModel()
    {
    }

    /// <summary>Creates a register with initial content (null = absent).</summary>
    public RegisterModel(byte[]? initial) => Initial = initial;

    /// <inheritdoc />
    public byte[]? Initial { get; }

    /// <inheritdoc />
    public bool Step(byte[]? state, HistoryOp op, out byte[]? next)
    {
        ArgumentNullException.ThrowIfNull(op);
        switch (op.Kind)
        {
            case "write":
                next = ModelValues.ToBytes(op.Input, op, "write input")
                    ?? throw new ArgumentException($"Operation #{op.Id}: write input must not be null (use \"delete\").");
                return true;
            case "delete":
                next = null;
                return true;
            case "read":
                next = state;
                if (op.Outcome != OpOutcome.Ok)
                    return true;
                byte[]? observed = ModelValues.ToBytes(op.Output, op, "read output");
                if (observed is null || state is null)
                    return observed is null && state is null;
                return observed.AsSpan().SequenceEqual(state);
            default:
                throw new ArgumentException($"Operation #{op.Id}: RegisterModel does not support kind '{op.Kind}'.");
        }
    }

    /// <inheritdoc />
    public string Key(byte[]? state) =>
        state is null ? AbsentKey : _keys.GetValue(state, static s => ModelValues.BytesKey(s));
}
