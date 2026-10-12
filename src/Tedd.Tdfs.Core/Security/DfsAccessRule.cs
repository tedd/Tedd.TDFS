namespace Tedd.Tdfs;

/// <summary>An access control entry (ACE) of a discretionary ACL (Addendum A, ACL-02).</summary>
/// <param name="Identity">The principal the entry applies to.</param>
/// <param name="Rights">Rights allowed or denied (NTFS bit values; non-zero, only defined bits).</param>
/// <param name="Type">Allow or deny.</param>
/// <param name="Inheritance">Which children inherit the entry.</param>
/// <param name="Propagation">How the entry propagates (requires a non-<c>None</c> <paramref name="Inheritance"/>).</param>
public sealed record DfsAccessRule(
    DfsIdentity Identity,
    DfsFileSystemRights Rights,
    DfsAccessControlType Type = DfsAccessControlType.Allow,
    DfsInheritanceFlags Inheritance = DfsInheritanceFlags.None,
    DfsPropagationFlags Propagation = DfsPropagationFlags.None)
{
    /// <summary>Every defined rights bit (<see cref="DfsFileSystemRights.FullControl"/>).</summary>
    public const DfsFileSystemRights AllRights = DfsFileSystemRights.FullControl;

    private const DfsInheritanceFlags AllInheritance = DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit;
    private const DfsPropagationFlags AllPropagation = DfsPropagationFlags.NoPropagateInherit | DfsPropagationFlags.InheritOnly;

    /// <summary>
    /// True for entries derived from an ancestor. Set only in descriptors returned by the system (and by
    /// <see cref="DfsAccessEvaluator.EffectiveRules(IReadOnlyList{DfsSecurityDescriptor}, bool, DfsIdentity)"/>); ignored
    /// when a descriptor is stored.
    /// </summary>
    public bool IsInherited { get; init; }

    /// <summary>Normalized path of the ancestor whose explicit entry this inherited entry derives from, when known.</summary>
    public string? InheritedFrom { get; init; }

    /// <summary>True when the entry is inherited by some kind of child (<see cref="Inheritance"/> is not <c>None</c>).</summary>
    public bool IsInheritable => (Inheritance & AllInheritance) != 0;

    /// <summary>True when the entry applies to the object it is set on (it is not <see cref="DfsPropagationFlags.InheritOnly"/>).</summary>
    public bool AppliesToObject => (Propagation & DfsPropagationFlags.InheritOnly) == 0;

    // Display order: composites first, then single bits by their file-oriented names (aliases share values).
    private static readonly (DfsFileSystemRights Value, string Name)[] FormatOrder =
    [
        (DfsFileSystemRights.FullControl, nameof(DfsFileSystemRights.FullControl)),
        (DfsFileSystemRights.Modify, nameof(DfsFileSystemRights.Modify)),
        (DfsFileSystemRights.ReadAndExecute, nameof(DfsFileSystemRights.ReadAndExecute)),
        (DfsFileSystemRights.Read, nameof(DfsFileSystemRights.Read)),
        (DfsFileSystemRights.Write, nameof(DfsFileSystemRights.Write)),
        (DfsFileSystemRights.ReadData, nameof(DfsFileSystemRights.ReadData)),
        (DfsFileSystemRights.WriteData, nameof(DfsFileSystemRights.WriteData)),
        (DfsFileSystemRights.AppendData, nameof(DfsFileSystemRights.AppendData)),
        (DfsFileSystemRights.ReadExtendedAttributes, nameof(DfsFileSystemRights.ReadExtendedAttributes)),
        (DfsFileSystemRights.WriteExtendedAttributes, nameof(DfsFileSystemRights.WriteExtendedAttributes)),
        (DfsFileSystemRights.ExecuteFile, nameof(DfsFileSystemRights.ExecuteFile)),
        (DfsFileSystemRights.DeleteSubdirectoriesAndFiles, nameof(DfsFileSystemRights.DeleteSubdirectoriesAndFiles)),
        (DfsFileSystemRights.ReadAttributes, nameof(DfsFileSystemRights.ReadAttributes)),
        (DfsFileSystemRights.WriteAttributes, nameof(DfsFileSystemRights.WriteAttributes)),
        (DfsFileSystemRights.Delete, nameof(DfsFileSystemRights.Delete)),
        (DfsFileSystemRights.ReadPermissions, nameof(DfsFileSystemRights.ReadPermissions)),
        (DfsFileSystemRights.ChangePermissions, nameof(DfsFileSystemRights.ChangePermissions)),
        (DfsFileSystemRights.TakeOwnership, nameof(DfsFileSystemRights.TakeOwnership)),
        (DfsFileSystemRights.Synchronize, nameof(DfsFileSystemRights.Synchronize)),
    ];

    /// <summary>
    /// Formats rights unambiguously (<see cref="Enum.ToString()"/> may pick a directory alias such as <c>CreateFiles</c>):
    /// composites first, then file-oriented bit names, joined by <c>'|'</c>; undefined bits as hex; <c>None</c> for zero.
    /// </summary>
    public static string FormatRights(DfsFileSystemRights rights)
    {
        if (rights == 0)
            return "None";
        var parts = new List<string>();
        DfsFileSystemRights rest = rights;
        foreach ((DfsFileSystemRights value, string name) in FormatOrder)
        {
            if ((rest & value) == value)
            {
                parts.Add(name);
                rest &= ~value;
            }
        }

        if (rest != 0)
            parts.Add("0x" + ((int)rest).ToString("X", System.Globalization.CultureInfo.InvariantCulture));
        return string.Join("|", parts);
    }

    /// <summary>Validates the entry as an explicit entry of a descriptor.</summary>
    /// <exception cref="ArgumentException">The entry is invalid; the message names the violated rule.</exception>
    public void Validate()
    {
        string? error = GetValidationError();
        if (error is not null)
            throw new ArgumentException(error, "rule");
    }

    /// <summary>Returns the first violated rule, or null when the entry is a valid explicit entry.</summary>
    /// <remarks>
    /// Rules: the identity is not <c>default</c>; rights are non-zero and use only defined bits; the type is defined;
    /// only defined inheritance/propagation bits; <see cref="DfsPropagationFlags.InheritOnly"/> and
    /// <see cref="DfsPropagationFlags.NoPropagateInherit"/> require an inheritance flag; <see cref="DfsIdentity.CreatorOwner"/>
    /// is only allowed in inheritable entries.
    /// </remarks>
    public string? GetValidationError()
    {
        if (Identity.IsDefault)
            return "An access rule needs an identity (default(DfsIdentity) is not valid).";
        if (Rights == 0)
            return $"The access rule for '{Identity}' has no rights.";
        if ((Rights & ~AllRights) != 0)
            return $"The access rule for '{Identity}' uses undefined rights bits 0x{(int)(Rights & ~AllRights):X}.";
        if (Type is not (DfsAccessControlType.Allow or DfsAccessControlType.Deny))
            return $"The access rule for '{Identity}' has an undefined type {(int)Type}.";
        if ((Inheritance & ~AllInheritance) != 0)
            return $"The access rule for '{Identity}' uses undefined inheritance flags 0x{(int)Inheritance:X}.";
        if ((Propagation & ~AllPropagation) != 0)
            return $"The access rule for '{Identity}' uses undefined propagation flags 0x{(int)Propagation:X}.";
        if (Propagation != DfsPropagationFlags.None && !IsInheritable)
            return $"The access rule for '{Identity}' sets propagation flags ({Propagation}) without an inheritance flag.";
        if (Identity == DfsIdentity.CreatorOwner && !IsInheritable)
            return $"'{DfsIdentity.CreatorOwnerName}' is only allowed in inheritable access rules.";
        return null;
    }
}
