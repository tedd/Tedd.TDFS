namespace Tedd.Tdfs.Server;

/// <summary>A retained snapshot.</summary>
/// <param name="Id">Snapshot id.</param>
/// <param name="Name">Snapshot name.</param>
/// <param name="BoundaryIndex">Raft index the snapshot captures.</param>
/// <param name="Label">Consistency label.</param>
/// <param name="InodeCount">Inodes in the snapshot root.</param>
public sealed record SnapshotInspection(SnapshotId Id, string Name, ulong BoundaryIndex, DfsConsistencyLabel Label, int InodeCount);
