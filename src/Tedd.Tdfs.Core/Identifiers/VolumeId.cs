namespace Tedd.Tdfs;

/// <summary>Identifies a volume. Stable for the lifetime of the volume; a restored volume gets a new id.</summary>
/// <param name="Value">The underlying GUID.</param>
public readonly record struct VolumeId(Guid Value)
{
    /// <summary>Creates a new random volume id from the supplied random source (deterministic under simulation).</summary>
    public static VolumeId New(IRandomSource r)
    {
        ArgumentNullException.ThrowIfNull(r);
        return new VolumeId(r.NextGuid());
    }

    /// <summary>True for the all-zero id.</summary>
    public bool IsEmpty => Value == Guid.Empty;

    /// <summary>Parses the canonical "D" GUID form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="FormatException">The string is not a canonical GUID.</exception>
    public static VolumeId Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return TryParse(s, out var id) ? id : throw new FormatException("Invalid volume id; expected a GUID in 'D' format.");
    }

    /// <summary>Tries to parse the canonical "D" GUID form.</summary>
    public static bool TryParse(string? s, out VolumeId id)
    {
        id = default;
        if (s is null || !IdentifierFormat.TryParseGuid(s, out var g))
            return false;
        id = new VolumeId(g);
        return true;
    }

    /// <summary>Returns the GUID in lowercase "D" format.</summary>
    public override string ToString() => IdentifierFormat.FormatGuid(Value);
}
