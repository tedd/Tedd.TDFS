namespace Tedd.Tdfs;

/// <summary>
/// Per-inode access flags, checked in addition to volume grants. Since Addendum A these are shortcuts for security
/// descriptors (<see cref="DfsSecurityDescriptor.FromAccessFlags"/>): <paramref name="OwnerOnly"/> → protected
/// {owner FullControl, Administrators FullControl}; <paramref name="ReadOnlyForOthers"/> → protected {owner FullControl,
/// Administrators FullControl, Everyone Read}; both false → not protected, no explicit entries (inherit only).
/// </summary>
/// <param name="OwnerOnly">Only the owner (and administrators) may access the inode.</param>
/// <param name="ReadOnlyForOthers">Principals other than the owner may only read.</param>
public sealed record DfsInodeAccess(bool OwnerOnly = false, bool ReadOnlyForOthers = false);
