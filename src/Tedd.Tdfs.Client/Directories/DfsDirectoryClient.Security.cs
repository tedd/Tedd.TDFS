using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

// Addendum A: NTFS-like security descriptors on directories (ACL-02..ACL-07). Directory paths never carry stream
// syntax (ADS-06): a stream path is rejected as an invalid path.
public sealed partial class DfsDirectoryClient
{
    /// <summary>Reads the security descriptor of a directory (explicit entries, then inherited entries in evaluation order).</summary>
    public Task<DfsSecurityDescriptor> GetAccessControlAsync(string path, CancellationToken ct = default) =>
        SecurityOperations.GetAccessControlAsync(Volume, Begin, Validation.DirectoryPath, path, Pin, TxId, ct, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="GetAccessControlAsync"/>.</summary>
    public DfsSecurityDescriptor GetAccessControl(string path) =>
        SyncBridge.Run(() => GetAccessControlAsync(path, CancellationToken.None));

    /// <summary>Atomically replaces the explicit entries and protection flag of a directory.</summary>
    public Task<CommitToken> SetAccessControlAsync(string path, DfsSecurityDescriptor descriptor, CancellationToken ct = default) =>
        SecurityOperations.SetAccessControlAsync(Volume, Begin, Validation.DirectoryPath, path, descriptor, TxId, ct, EnsureWritable, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="SetAccessControlAsync"/>.</summary>
    public CommitToken SetAccessControl(string path, DfsSecurityDescriptor descriptor) =>
        SyncBridge.Run(() => SetAccessControlAsync(path, descriptor, CancellationToken.None));

    /// <summary>Changes the owner of a directory.</summary>
    public Task<CommitToken> SetOwnerAsync(string path, DfsIdentity owner, CancellationToken ct = default) =>
        SecurityOperations.SetOwnerAsync(Volume, Begin, Validation.DirectoryPath, path, owner, TxId, ct, EnsureWritable, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="SetOwnerAsync"/>.</summary>
    public CommitToken SetOwner(string path, DfsIdentity owner) =>
        SyncBridge.Run(() => SetOwnerAsync(path, owner, CancellationToken.None));

    /// <summary>The rights <paramref name="identity"/> (null = the caller) would be granted on a directory.</summary>
    public Task<DfsFileSystemRights> GetEffectiveRightsAsync(string path, DfsIdentity? identity = null, CancellationToken ct = default) =>
        SecurityOperations.GetEffectiveRightsAsync(Volume, Begin, Validation.DirectoryPath, path, identity, Pin, TxId, ct, EnterTransaction);

    /// <summary>Synchronous counterpart of <see cref="GetEffectiveRightsAsync"/>.</summary>
    public DfsFileSystemRights GetEffectiveRights(string path, DfsIdentity? identity = null) =>
        SyncBridge.Run(() => GetEffectiveRightsAsync(path, identity, CancellationToken.None));
}
