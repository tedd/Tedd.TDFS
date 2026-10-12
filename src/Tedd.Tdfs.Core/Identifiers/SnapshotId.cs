namespace Tedd.Tdfs;

/// <summary>Identifies a snapshot (unique name per volume is tracked separately).</summary>
/// <param name="Value">The underlying GUID.</param>
public readonly record struct SnapshotId(Guid Value)
{
    /// <summary>Creates a new random snapshot id from the supplied random source.</summary>
    public static SnapshotId New(IRandomSource r)
    {
        ArgumentNullException.ThrowIfNull(r);
        return new SnapshotId(r.NextGuid());
    }

    /// <summary>True for the all-zero id.</summary>
    public bool IsEmpty => Value == Guid.Empty;

    /// <summary>Returns the GUID in lowercase "D" format.</summary>
    public override string ToString() => IdentifierFormat.FormatGuid(Value);
}
