using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>Stable identity of a file or directory, independent of its path. Allocated monotonically, never reused.</summary>
/// <param name="Value">The numeric inode id.</param>
public readonly record struct InodeId(ulong Value)
{
    /// <summary>The root directory inode (value 1).</summary>
    public static readonly InodeId Root = new(1);

    /// <summary>True for the reserved zero value (no inode).</summary>
    public bool IsEmpty => Value == 0;

    /// <summary>Returns the decimal value.</summary>
    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);
}
