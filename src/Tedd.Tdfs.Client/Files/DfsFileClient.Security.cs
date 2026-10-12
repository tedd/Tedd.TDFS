using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

// Addendum A: NTFS-like security descriptors (ACL-02..ACL-07) and named streams (ADS-03).
public sealed partial class DfsFileClient
{
    /// <summary>
    /// Reads the file's security descriptor: owner, protection flag, explicit entries and the inherited entries
    /// (<see cref="DfsAccessRule.IsInherited"/> = true, with <see cref="DfsAccessRule.InheritedFrom"/>) in evaluation
    /// order. Requires ReadPermissions. A stream path reads its file's descriptor.
    /// </summary>
    public Task<DfsSecurityDescriptor> GetAccessControlAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.GetAccessControlAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetAccessControlAsync"/>.</summary>
    public DfsSecurityDescriptor GetAccessControl(string path) =>
        SyncBridge.Run(() => GetAccessControlAsync(path, CancellationToken.None));

    /// <summary>
    /// Atomically replaces the explicit entries and the protection flag. Inherited entries in
    /// <paramref name="descriptor"/> are ignored, and so is <see cref="DfsSecurityDescriptor.Owner"/> (use
    /// <see cref="SetOwnerAsync"/> to change the owner). Requires ChangePermissions. Bumps the inode revision (ACL-07).
    /// </summary>
    /// <exception cref="ArgumentException">The descriptor is invalid (<see cref="DfsSecurityDescriptor.Validate"/>).</exception>
    public Task<CommitToken> SetAccessControlAsync(string path, DfsSecurityDescriptor descriptor, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.SetAccessControlAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetAccessControlAsync"/>.</summary>
    public CommitToken SetAccessControl(string path, DfsSecurityDescriptor descriptor) =>
        SyncBridge.Run(() => SetAccessControlAsync(path, descriptor, CancellationToken.None));

    /// <summary>
    /// Changes the owner: with TakeOwnership the caller may make itself the owner; Administrators may set any owner
    /// (ACL-05, ACL-014). <see cref="DfsIdentity.CreatorOwner"/> is rejected.
    /// </summary>
    public Task<CommitToken> SetOwnerAsync(string path, DfsIdentity owner, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.SetOwnerAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetOwnerAsync"/>.</summary>
    public CommitToken SetOwner(string path, DfsIdentity owner) =>
        SyncBridge.Run(() => SetOwnerAsync(path, owner, CancellationToken.None));

    /// <summary>
    /// The rights <paramref name="identity"/> (null = the caller) would be granted on the file by
    /// <see cref="DfsAccessEvaluator"/>, including inherited and group-derived bits (ACL-034). Volume grants are not
    /// folded in.
    /// </summary>
    public Task<DfsFileSystemRights> GetEffectiveRightsAsync(string path, DfsIdentity? identity = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.GetEffectiveRightsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetEffectiveRightsAsync"/>.</summary>
    public DfsFileSystemRights GetEffectiveRights(string path, DfsIdentity? identity = null) =>
        SyncBridge.Run(() => GetEffectiveRightsAsync(path, identity, CancellationToken.None));

    /// <summary>
    /// Lists the named streams of a file or directory (not the default stream) with lengths and revisions,
    /// ordinal-sorted by name (ADS-03, ADS-025). <see cref="FileNotFoundException"/> when the path is absent; a stream
    /// path is rejected as an invalid path.
    /// </summary>
    public Task<IReadOnlyList<DfsStreamInfo>> ListStreamsAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsFileClient.ListStreamsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ListStreamsAsync"/>.</summary>
    public IReadOnlyList<DfsStreamInfo> ListStreams(string path) =>
        SyncBridge.Run(() => ListStreamsAsync(path, CancellationToken.None));
}
