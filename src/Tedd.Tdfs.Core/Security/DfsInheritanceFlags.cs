namespace Tedd.Tdfs;

/// <summary>Which kinds of children inherit an access control entry (NTFS inheritance flags).</summary>
[Flags]
public enum DfsInheritanceFlags
{
    /// <summary>Not inherited.</summary>
    None = 0,
    /// <summary>Inherited by child directories.</summary>
    ContainerInherit = 1,
    /// <summary>Inherited by child files.</summary>
    ObjectInherit = 2,
}
