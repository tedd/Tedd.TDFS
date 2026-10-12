using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

// Addendum A: NTFS-like security descriptors on directories (ACL-02..ACL-07). Directory paths never carry stream
// syntax (ADS-06): a stream path is rejected as an invalid path.
public sealed partial class DfsDirectoryClient
{
    /// <summary>
    /// Reads the directory's security descriptor including inherited entries (<see cref="DfsAccessRule.IsInherited"/> =
    /// true) in evaluation order. Requires ReadPermissions.
    /// </summary>
    public Task<DfsSecurityDescriptor> GetAccessControlAsync(string path, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.GetAccessControlAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetAccessControlAsync"/>.</summary>
    public DfsSecurityDescriptor GetAccessControl(string path) =>
        SyncBridge.Run(() => GetAccessControlAsync(path, CancellationToken.None));

    /// <summary>
    /// Atomically replaces the explicit entries and the protection flag; inherited entries and the owner in the input
    /// are ignored (use <see cref="SetOwnerAsync"/>). Requires ChangePermissions. The change takes effect for the whole
    /// subtree with this commit (ACL-03).
    /// </summary>
    /// <exception cref="ArgumentException">The descriptor is invalid (<see cref="DfsSecurityDescriptor.Validate"/>).</exception>
    public Task<CommitToken> SetAccessControlAsync(string path, DfsSecurityDescriptor descriptor, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.SetAccessControlAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetAccessControlAsync"/>.</summary>
    public CommitToken SetAccessControl(string path, DfsSecurityDescriptor descriptor) =>
        SyncBridge.Run(() => SetAccessControlAsync(path, descriptor, CancellationToken.None));

    /// <summary>Changes the owner (TakeOwnership for the caller itself; Administrators for any owner).</summary>
    public Task<CommitToken> SetOwnerAsync(string path, DfsIdentity owner, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.SetOwnerAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetOwnerAsync"/>.</summary>
    public CommitToken SetOwner(string path, DfsIdentity owner) =>
        SyncBridge.Run(() => SetOwnerAsync(path, owner, CancellationToken.None));

    /// <summary>The rights <paramref name="identity"/> (null = the caller) would be granted on the directory.</summary>
    public Task<DfsFileSystemRights> GetEffectiveRightsAsync(string path, DfsIdentity? identity = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsDirectoryClient.GetEffectiveRightsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetEffectiveRightsAsync"/>.</summary>
    public DfsFileSystemRights GetEffectiveRights(string path, DfsIdentity? identity = null) =>
        SyncBridge.Run(() => GetEffectiveRightsAsync(path, identity, CancellationToken.None));
}
