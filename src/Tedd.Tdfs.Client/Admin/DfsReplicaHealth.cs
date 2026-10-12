namespace Tedd.Tdfs;

/// <summary>Replica health of a volume's data.</summary>
/// <param name="Objects">Referenced objects.</param>
/// <param name="UnderReplicatedObjects">Objects with fewer durable copies than the target.</param>
/// <param name="ZoneDeficitObjects">Objects spanning fewer zones than the target.</param>
/// <param name="MissingObjects">Objects with no verified copy.</param>
/// <param name="PlacementViolations">Objects violating placement constraints.</param>
/// <param name="RepairBacklogBytes">Bytes awaiting repair.</param>
/// <param name="PendingGcObjects">Objects eligible for collection but not yet deleted.</param>
/// <param name="StagedBytes">Staged (unpublished) bytes.</param>
/// <param name="PinnedBytes">Bytes retained by pins.</param>
public sealed record DfsReplicaHealth(
    long Objects,
    long UnderReplicatedObjects,
    long ZoneDeficitObjects,
    long MissingObjects,
    long PlacementViolations,
    long RepairBacklogBytes,
    long PendingGcObjects,
    long StagedBytes,
    long PinnedBytes);
