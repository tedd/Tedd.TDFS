using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>Identifies an open handle; allocated by the volume state machine.</summary>
/// <param name="Value">The numeric handle id.</param>
public readonly record struct HandleId(ulong Value)
{
    /// <summary>True for the reserved zero value (no handle).</summary>
    public bool IsEmpty => Value == 0;

    /// <summary>Returns the decimal value.</summary>
    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);
}
