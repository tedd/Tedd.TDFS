namespace Tedd.Tdfs;

/// <summary>Administrative status of a volume.</summary>
/// <param name="Info">Volume metadata.</param>
/// <param name="Group">Volume consensus group status.</param>
/// <param name="Replicas">Replica health.</param>
/// <param name="Backups">Status of each registered backup target.</param>
public sealed record DfsVolumeStatus(DfsVolumeInfo Info, DfsGroupStatus Group, DfsReplicaHealth Replicas, IReadOnlyList<DfsBackupStatus> Backups);
