namespace Tedd.Tdfs.Server;

/// <summary>Backup progress recorded in volume state for one target.</summary>
/// <param name="Target">Target name.</param>
/// <param name="LastSnapshot">Last acknowledged snapshot name.</param>
/// <param name="CoveredIndex">Raft index covered by the last acknowledged snapshot.</param>
/// <param name="ReseedRequired">True when the transfer lineage was abandoned and a full reseed is required.</param>
/// <param name="Lineage">Current transfer lineage id.</param>
public sealed record BackupProgressInspection(string Target, string? LastSnapshot, ulong CoveredIndex, bool ReseedRequired, Guid Lineage);
