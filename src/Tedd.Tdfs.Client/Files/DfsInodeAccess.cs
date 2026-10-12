namespace Tedd.Tdfs;

/// <summary>Per-inode access flags, checked in addition to volume grants.</summary>
/// <param name="OwnerOnly">Only the owner (and administrators) may access the inode.</param>
/// <param name="ReadOnlyForOthers">Principals other than the owner may only read.</param>
public sealed record DfsInodeAccess(bool OwnerOnly = false, bool ReadOnlyForOthers = false);
