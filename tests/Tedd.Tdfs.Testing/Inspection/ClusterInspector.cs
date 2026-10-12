using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

/// <summary>Cluster-wide inspection over all running nodes (reads durable/applied state, not client views).</summary>
public sealed partial class ClusterInspector
{
    internal ClusterInspector()
    {
    }

    /// <summary>The control group as seen by its leader.</summary>
    public Task<GroupInspection> GetControlGroupAsync() => throw Stub.NotYet("ClusterInspector.GetControlGroupAsync");

    /// <summary>A volume group as seen by its leader.</summary>
    public Task<GroupInspection> GetVolumeGroupAsync(string volume) => throw Stub.NotYet("ClusterInspector.GetVolumeGroupAsync");

    /// <summary>A replica's applied volume state (default: the leader).</summary>
    public Task<VolumeStateInspection> GetVolumeStateAsync(string volume, string? node = null) =>
        throw Stub.NotYet("ClusterInspector.GetVolumeStateAsync");

    /// <summary>A file's inode/extent view on the leader; null when absent.</summary>
    public Task<FileInspection?> GetFileAsync(string volume, string path) => throw Stub.NotYet("ClusterInspector.GetFileAsync");

    /// <summary>Nodes durably holding a verified copy of an object.</summary>
    public Task<IReadOnlyList<string>> GetObjectHoldersAsync(ObjectId id) => throw Stub.NotYet("ClusterInspector.GetObjectHoldersAsync");

    /// <summary>Distinct hosts and zones durably holding an object.</summary>
    public Task<(int Copies, int Zones)> CountDurableDomainsAsync(ObjectId id) => throw Stub.NotYet("ClusterInspector.CountDurableDomainsAsync");

    /// <summary>Replica health of a volume.</summary>
    public Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume) => throw Stub.NotYet("ClusterInspector.GetReplicaHealthAsync");

    /// <summary>All objects stored on any node (optionally of one volume).</summary>
    public Task<IReadOnlySet<ObjectId>> GetAllStoredObjectsAsync(string? volume = null) =>
        throw Stub.NotYet("ClusterInspector.GetAllStoredObjectsAsync");

    /// <summary>Sum of all running nodes' counters.</summary>
    public Task<NodeCounters> GetTotalCountersAsync() => throw Stub.NotYet("ClusterInspector.GetTotalCountersAsync");

    /// <summary>Asserts at most one leader per term per group.</summary>
    public Task AssertNoSplitBrainAsync() => throw Stub.NotYet("ClusterInspector.AssertNoSplitBrainAsync");

    /// <summary>Asserts identical digests on all caught-up replicas of a volume.</summary>
    public Task AssertReplicasConvergedAsync(string volume) => throw Stub.NotYet("ClusterInspector.AssertReplicasConvergedAsync");
}
