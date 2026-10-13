using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

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
    /// <remarks>
    /// Sent through the harness's own protocol channel as "admin": a <c>RestoreSnapshot</c> request to the source volume's
    /// leader carrying the required <c>StoragePool</c> extension (PROTOCOL.md, <c>RestoreSnapshot</c> field 1); a refusal is
    /// rethrown as the SDK's typed exception for its error code.
    /// </remarks>
    public async Task<DfsVolumeInfo> RestoreSnapshotToPoolAsync(string volume, string snapshotName, string newVolumeName, string storagePool,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotName);
        ArgumentException.ThrowIfNullOrWhiteSpace(newVolumeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(storagePool);
        TestNode entry = RunningNodes().FirstOrDefault() ?? throw new InvalidOperationException("No node of the cluster is running.");
        await using RawProtocolClient raw = await RawProtocolClient.ConnectAsync(this, HarnessName("restore"), entry.Name, entry.Endpoint, ClusterId, ct,
            isHarness: true);
        DfsErrorCode authentication = await raw.AuthenticateAsync(AdminCredentials);
        if (authentication != DfsErrorCode.None)
            throw DfsErrors.ToException(authentication, "The harness could not authenticate as admin.");

        Record($"restore snapshot {snapshotName} of {volume} into new volume {newVolumeName} on pool {storagePool}");
        (DfsErrorCode code, IWireMessage? reply) = await raw.SendVolumeRequestAsync(volume, mutating: true,
            header => RawProtocolClient.Encode(new RestoreSnapshotMessage(header, snapshotName, newVolumeName).WithStoragePool(storagePool)));
        if (reply is ErrorReplyMessage error)
            throw error.ToException();
        if (reply is not RestoreSnapshotResultMessage restored)
            throw DfsErrors.ToException(code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, $"The restore of {snapshotName} got no result ({code}).");

        if (!Options.OutOfProcess)
            await WaitForGroupLeaderAsync(newVolumeName, DefaultWaitTimeout, ct);
        VolumeEntry v = restored.Volume;
        Record($"volume {newVolumeName} restored (id {v.Id}, pool {v.StoragePool})");
        return new DfsVolumeInfo(v.Id, v.Name, v.Epoch, v.Metadata, v.DefaultPolicy, v.NamedPolicies, v.InlineThreshold, v.Limits, restored.LastCommit,
            v.StoragePool, v.EnforceTraverseChecking);
    }
}
