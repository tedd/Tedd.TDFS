namespace Tedd.Tdfs;

/// <summary>Entry kinds to include in an enumeration.</summary>
[Flags]
public enum DfsEntryKinds
{
    /// <summary>Include files.</summary>
    Files = 1,

    /// <summary>Include directories.</summary>
    Directories = 2,

    /// <summary>Include files and directories.</summary>
    All = Files | Directories,
}
