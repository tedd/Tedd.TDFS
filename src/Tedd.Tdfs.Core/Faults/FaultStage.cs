namespace Tedd.Tdfs;

/// <summary>
/// Stage boundaries at which faults can be injected (test plan fault stages F00-F21). Each site in
/// <see cref="FaultSites"/> belongs to exactly one stage.
/// </summary>
public enum FaultStage
{
    /// <summary>Client operation start.</summary>
    F00,
    /// <summary>Client validation complete.</summary>
    F01,
    /// <summary>Client connect.</summary>
    F02,
    /// <summary>Client request send / chunk upload.</summary>
    F03,
    /// <summary>Server request accepted.</summary>
    F04,
    /// <summary>Staging registered.</summary>
    F05,
    /// <summary>Object write.</summary>
    F06,
    /// <summary>Object written, before flush.</summary>
    F07,
    /// <summary>Object flushed, before receipt.</summary>
    F08,
    /// <summary>Commit receipts validated.</summary>
    F09,
    /// <summary>Raft propose / local append.</summary>
    F10,
    /// <summary>Raft committed, before apply.</summary>
    F11,
    /// <summary>Applied, before reply.</summary>
    F12,
    /// <summary>Reply send.</summary>
    F13,
    /// <summary>Client reply received.</summary>
    F14,
    /// <summary>Cleanup/release.</summary>
    F15,
    /// <summary>Recovery (journal replay, checkpoint/snapshot install).</summary>
    F16,
    /// <summary>Snapshot pin / backup inventory.</summary>
    F17,
    /// <summary>Backup object receipt / closure verification.</summary>
    F18,
    /// <summary>Backup pointer publication and acknowledgement.</summary>
    F19,
    /// <summary>GC, compaction and replica deletion.</summary>
    F20,
    /// <summary>Reconfiguration and key rotation.</summary>
    F21,
}
