namespace Tedd.Tdfs.Server;

/// <summary>An active pin protecting old versions from garbage collection.</summary>
/// <param name="Id">Pin id.</param>
/// <param name="Kind">"Handle", "SnapshotReader", "Transfer" or "Enumeration".</param>
/// <param name="CreatedIndex">Raft index at which the pin was taken.</param>
/// <param name="Snapshot">Snapshot name for snapshot-reader and transfer pins.</param>
public sealed record PinInspection(ulong Id, string Kind, ulong CreatedIndex, string? Snapshot);
