using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace Tedd.Tdfs;

/// <summary>Stable fault-injection site names. Each constant documents its <see cref="FaultStage"/>.</summary>
public static class FaultSites
{
    // Client (F00-F03, F13-F14)

    /// <summary>F00: client operation start.</summary>
    public const string ClientOperationStart = "client.op.start";
    /// <summary>F01: client validation complete.</summary>
    public const string ClientValidated = "client.op.validated";
    /// <summary>F02: client connect.</summary>
    public const string ClientConnect = "client.connect";
    /// <summary>F03: client request send.</summary>
    public const string ClientRequestSend = "client.request.send";
    /// <summary>F03: client chunk upload (Occurrence = chunk index).</summary>
    public const string ClientUploadChunk = "client.upload.chunk";
    /// <summary>F14: client reply received.</summary>
    public const string ClientReplyReceived = "client.reply.received";

    // Server request path

    /// <summary>F04: server accepted an RPC request.</summary>
    public const string RpcRequestAccepted = "rpc.request.accepted";
    /// <summary>F05: staging session registered.</summary>
    public const string StagingRegistered = "staging.registered";
    /// <summary>F06: object write.</summary>
    public const string ObjectWrite = "object.write";
    /// <summary>F07: object written, before flush.</summary>
    public const string ObjectBeforeFlush = "object.before-flush";
    /// <summary>F08: object flushed, before receipt.</summary>
    public const string ObjectAfterFlushBeforeReceipt = "object.after-flush";
    /// <summary>F09: commit receipts validated.</summary>
    public const string CommitReceiptValidated = "commit.receipt-validated";
    /// <summary>F10: before Raft propose.</summary>
    public const string RaftBeforePropose = "raft.before-propose";
    /// <summary>F10: after local Raft append.</summary>
    public const string RaftAfterLocalAppend = "raft.after-local-append";
    /// <summary>F11: committed, before apply.</summary>
    public const string RaftAfterCommitBeforeApply = "raft.after-commit";
    /// <summary>F12: applied, before reply.</summary>
    public const string ApplyAfterBeforeReply = "apply.after";
    /// <summary>F13: reply send.</summary>
    public const string RpcReplySend = "rpc.reply.send";
    /// <summary>F15: cleanup/release.</summary>
    public const string CleanupRelease = "cleanup.release";
    /// <summary>F16: journal replay during recovery.</summary>
    public const string RecoveryJournalReplay = "recovery.journal-replay";
    /// <summary>F16: checkpoint install during recovery.</summary>
    public const string RecoveryCheckpointInstall = "recovery.checkpoint-install";
    /// <summary>F16: Raft snapshot install.</summary>
    public const string RaftSnapshotInstall = "raft.snapshot-install";
    /// <summary>F17: snapshot pin.</summary>
    public const string SnapshotPin = "snapshot.pin";
    /// <summary>F17: backup inventory.</summary>
    public const string BackupInventory = "backup.inventory";
    /// <summary>F18: backup object received.</summary>
    public const string BackupObjectReceived = "backup.object-received";
    /// <summary>F18: backup before closure verification.</summary>
    public const string BackupBeforeClosureVerify = "backup.before-verify";
    /// <summary>F19: backup before pointer publication.</summary>
    public const string BackupBeforePointer = "backup.before-pointer";
    /// <summary>F19: backup after pointer publication.</summary>
    public const string BackupAfterPointer = "backup.after-pointer";
    /// <summary>F19: backup acknowledgement.</summary>
    public const string BackupAck = "backup.ack";
    /// <summary>F20: GC sweep.</summary>
    public const string GcSweep = "gc.sweep";
    /// <summary>F20: compaction copy.</summary>
    public const string CompactionCopy = "compaction.copy";
    /// <summary>F20: compaction publish.</summary>
    public const string CompactionPublish = "compaction.publish";
    /// <summary>F20: compaction delete.</summary>
    public const string CompactionDelete = "compaction.delete";
    /// <summary>F20: replica delete.</summary>
    public const string ReplicaDelete = "replica.delete";
    /// <summary>F21: reconfiguration transition.</summary>
    public const string Reconfiguration = "reconfig.transition";
    /// <summary>F21: key rotation.</summary>
    public const string KeyRotation = "key.rotation";

    private static readonly (string Site, FaultStage Stage)[] Table =
    [
        (ClientOperationStart, FaultStage.F00),
        (ClientValidated, FaultStage.F01),
        (ClientConnect, FaultStage.F02),
        (ClientRequestSend, FaultStage.F03),
        (ClientUploadChunk, FaultStage.F03),
        (ClientReplyReceived, FaultStage.F14),
        (RpcRequestAccepted, FaultStage.F04),
        (StagingRegistered, FaultStage.F05),
        (ObjectWrite, FaultStage.F06),
        (ObjectBeforeFlush, FaultStage.F07),
        (ObjectAfterFlushBeforeReceipt, FaultStage.F08),
        (CommitReceiptValidated, FaultStage.F09),
        (RaftBeforePropose, FaultStage.F10),
        (RaftAfterLocalAppend, FaultStage.F10),
        (RaftAfterCommitBeforeApply, FaultStage.F11),
        (ApplyAfterBeforeReply, FaultStage.F12),
        (RpcReplySend, FaultStage.F13),
        (CleanupRelease, FaultStage.F15),
        (RecoveryJournalReplay, FaultStage.F16),
        (RecoveryCheckpointInstall, FaultStage.F16),
        (RaftSnapshotInstall, FaultStage.F16),
        (SnapshotPin, FaultStage.F17),
        (BackupInventory, FaultStage.F17),
        (BackupObjectReceived, FaultStage.F18),
        (BackupBeforeClosureVerify, FaultStage.F18),
        (BackupBeforePointer, FaultStage.F19),
        (BackupAfterPointer, FaultStage.F19),
        (BackupAck, FaultStage.F19),
        (GcSweep, FaultStage.F20),
        (CompactionCopy, FaultStage.F20),
        (CompactionPublish, FaultStage.F20),
        (CompactionDelete, FaultStage.F20),
        (ReplicaDelete, FaultStage.F20),
        (Reconfiguration, FaultStage.F21),
        (KeyRotation, FaultStage.F21),
    ];

    private static readonly FrozenDictionary<string, FaultStage> Stages =
        Table.ToFrozenDictionary(e => e.Site, e => e.Stage, StringComparer.Ordinal);

    private static readonly string[] AllSites = Table.Select(e => e.Site).ToArray();

    /// <summary>Every site name, in declaration order.</summary>
    public static IReadOnlyList<string> All { get; } = Array.AsReadOnly(AllSites);

    /// <summary>Returns the stage a site belongs to.</summary>
    /// <exception cref="ArgumentException">The site name is unknown.</exception>
    public static FaultStage StageOf(string site)
    {
        ArgumentNullException.ThrowIfNull(site);
        return TryGetStage(site, out var stage) ? stage : throw new ArgumentException($"Unknown fault site '{site}'.", nameof(site));
    }

    /// <summary>Tries to look up the stage of a site (ordinal match).</summary>
    public static bool TryGetStage([NotNullWhen(true)] string? site, out FaultStage stage)
    {
        stage = default;
        return site is not null && Stages.TryGetValue(site, out stage);
    }

    /// <summary>All sites belonging to <paramref name="stage"/>, in declaration order.</summary>
    public static IReadOnlyList<string> SitesOf(FaultStage stage)
        => Table.Where(e => e.Stage == stage).Select(e => e.Site).ToArray();
}
