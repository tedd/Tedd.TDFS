namespace Tedd.Tdfs.Storage;

/// <summary>Admission class of an object put: how much of the store's reserved capacity it may use.</summary>
public enum ObjectAdmission
{
    /// <summary>Client payload (uploads, restores): admitted only while the reserve stays untouched.</summary>
    Normal = 0,

    /// <summary>
    /// Repair and re-replication copies: may use the reserve down to <see cref="ObjectStoreOptions.EffectiveRepairFloorBytes"/>,
    /// which stays free for metadata progress and compaction, so lost redundancy can be restored on nodes that normal
    /// admission already considers full.
    /// </summary>
    Repair = 1,
}
