namespace Tedd.Tdfs;

/// <summary>Status of a storage pool (Addendum A, POOL-05).</summary>
/// <param name="Info">Pool membership, volumes and capacity.</param>
/// <param name="Replicas">Replica health of the objects placed in the pool.</param>
/// <param name="DrainingNodes">Nodes being removed from the pool whose objects are still migrating (POOL-04), ordinal-sorted.</param>
public sealed record DfsPoolStatus(DfsPoolInfo Info, DfsReplicaHealth Replicas, IReadOnlyList<string> DrainingNodes);
