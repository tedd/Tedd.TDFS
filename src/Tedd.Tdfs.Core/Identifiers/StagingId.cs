using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>Identifies a staging session (upload scope for unpublished objects); allocated by the volume state machine.</summary>
/// <param name="Value">The numeric staging id.</param>
public readonly record struct StagingId(ulong Value)
{
    /// <summary>True for the reserved zero value (no staging session).</summary>
    public bool IsEmpty => Value == 0;

    /// <summary>Returns the decimal value.</summary>
    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);
}
