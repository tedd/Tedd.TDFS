namespace Tedd.Tdfs;

/// <summary>
/// Durable transaction identity; wraps the <see cref="DfsOperationId"/> of the transaction's begin operation.
/// String form: <c>tx:{Session:x}-{Sequence:x}</c>.
/// </summary>
/// <param name="Operation">Operation id of the transaction's begin.</param>
public readonly record struct DfsTransactionId(DfsOperationId Operation)
{
    /// <summary>String prefix of the canonical form.</summary>
    public const string Prefix = "tx:";

    /// <summary>True when the wrapped operation id is empty.</summary>
    public bool IsEmpty => Operation.IsEmpty;

    /// <summary>Returns <c>tx:{Session:x}-{Sequence:x}</c>.</summary>
    public override string ToString() => DfsOperationId.Format(Prefix, Operation.Session, Operation.Sequence);

    /// <summary>Parses the form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="FormatException">The string is not a valid transaction id.</exception>
    public static DfsTransactionId Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return TryParse(s, out var id) ? id : throw new FormatException("Invalid transaction id; expected 'tx:{session:x}-{sequence:x}'.");
    }

    /// <summary>Tries to parse the form produced by <see cref="ToString"/>.</summary>
    public static bool TryParse(string? s, out DfsTransactionId id)
    {
        id = default;
        if (s is null || !DfsOperationId.TryParseBody(s.AsSpan(), Prefix, out var op))
            return false;
        id = new DfsTransactionId(op);
        return true;
    }
}
