namespace Tedd.Tdfs;

/// <summary>Snapshot metadata.</summary>
/// <param name="Id">Snapshot id.</param>
/// <param name="Name">Snapshot name (unique per volume).</param>
/// <param name="Volume">Volume name.</param>
/// <param name="Boundary">Commit boundary the snapshot captures.</param>
/// <param name="Label">Consistency label.</param>
/// <param name="Adapters">Adapters recorded in the snapshot.</param>
/// <param name="CreatedUtc">Creation time (leader clock).</param>
/// <param name="Retention">Current retention.</param>
/// <param name="AdapterResumeFailure">Adapter resume failure reported during creation; the snapshot remains valid.</param>
public sealed record DfsSnapshotInfo(
    SnapshotId Id,
    string Name,
    string Volume,
    CommitToken Boundary,
    DfsConsistencyLabel Label,
    IReadOnlyList<DfsAdapterRecord> Adapters,
    DateTimeOffset CreatedUtc,
    DfsRetention Retention,
    Exception? AdapterResumeFailure);
