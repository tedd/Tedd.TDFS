namespace Tedd.Tdfs.Testing;

// Addendum A (API.md §H.3): storage pool inspection.
public sealed partial class ClusterInspector
{
    /// <summary>Member node names of a pool as applied by the control-group leader, ordinal-sorted (<c>"default"</c> included).</summary>
    public Task<IReadOnlyList<string>> GetPoolNodesAsync(string pool) => PoolNodesAsync(pool);

    /// <summary>Objects durably stored on one node (optionally only those of one volume), read from its object store.</summary>
    public Task<IReadOnlySet<ObjectId>> GetStoredObjectsOnNodeAsync(string node, string? volume = null) => StoredObjectsOnNodeAsync(node, volume);
}
