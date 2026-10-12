namespace Tedd.Tdfs;

/// <summary>How an inheritable access control entry propagates (NTFS propagation flags).</summary>
[Flags]
public enum DfsPropagationFlags
{
    /// <summary>Normal propagation; the entry also applies to the object it is set on.</summary>
    None = 0,
    /// <summary>Inherited by immediate children only; the inherited copy is not inheritable further.</summary>
    NoPropagateInherit = 1,
    /// <summary>The entry does not apply to the object it is set on, only to inheriting children.</summary>
    InheritOnly = 2,
}
