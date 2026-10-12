namespace Tedd.Tdfs;

/// <summary>Identifies a live or backup node. Persisted in the node superblock.</summary>
/// <param name="Value">The underlying GUID.</param>
public readonly record struct NodeId(Guid Value)
{
    /// <summary>Creates a new random node id from the supplied random source (deterministic under simulation).</summary>
    public static NodeId New(IRandomSource r)
    {
        ArgumentNullException.ThrowIfNull(r);
        return new NodeId(r.NextGuid());
    }

    /// <summary>True for the all-zero id.</summary>
    public bool IsEmpty => Value == Guid.Empty;

    /// <summary>Parses the canonical "D" GUID form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="FormatException">The string is not a canonical GUID.</exception>
    public static NodeId Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return TryParse(s, out var id) ? id : throw new FormatException("Invalid node id; expected a GUID in 'D' format.");
    }

    /// <summary>Tries to parse the canonical "D" GUID form.</summary>
    public static bool TryParse(string? s, out NodeId id)
    {
        id = default;
        if (s is null || !IdentifierFormat.TryParseGuid(s, out var g))
            return false;
        id = new NodeId(g);
        return true;
    }

    /// <summary>Returns the GUID in lowercase "D" format.</summary>
    public override string ToString() => IdentifierFormat.FormatGuid(Value);
}
