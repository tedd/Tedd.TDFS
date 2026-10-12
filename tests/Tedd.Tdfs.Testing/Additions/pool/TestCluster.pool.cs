namespace Tedd.Tdfs.Testing;

// POOL suite additions (docs/api-additions/pool.md): restoring a snapshot into a chosen storage pool (POOL-016).
public sealed partial class TestCluster
{
    /// <summary>
    /// Restores snapshot <paramref name="snapshotName"/> of <paramref name="volume"/> into a new volume
    /// <paramref name="newVolumeName"/> bound to <paramref name="storagePool"/> (Addendum A, POOL-02/POOL-016), as the admin
    /// principal, and returns the new volume's information once its group has a leader and the restore is complete
    /// (the same completion point as <see cref="DfsSnapshotClient.RestoreAsync"/>). The new volume keeps the snapshot's
    /// default and named policies; its data objects are copied only onto <paramref name="storagePool"/>'s nodes and its
    /// metadata voters are chosen from them. Errors are the SDK's typed exceptions: <see cref="DfsErrorCode.PoolNotFound"/>,
    /// <see cref="DfsErrorCode.InvalidPolicy"/> (the pool cannot satisfy the restored policies, POOL-03),
    /// <see cref="DfsErrorCode.SnapshotNotFound"/>, <see cref="DfsAlreadyExistsException"/> (name taken),
    /// <see cref="DfsPlacementUnavailableException"/>; a failed restore leaves no volume behind.
    /// </summary>
    public Task<DfsVolumeInfo> RestoreSnapshotToPoolAsync(string volume, string snapshotName, string newVolumeName, string storagePool,
        CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.RestoreSnapshotToPoolAsync");
}
