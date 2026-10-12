# API additions for suites FMT, OPS and PERF (FMT-001..FMT-012, OPS-001..OPS-016, PERF-001..PERF-016)

All additions are stubs (`NotImplementedException`) in separate files; no existing file or signature was changed. Exact
required behaviour is in the XML docs of each member; this file summarises it.

## New harness members and types (`tests/Tedd.Tdfs.Testing/Additions/fmt-ops-perf/`)

| Member / type | Required behaviour | Needed by |
|---|---|---|
| `public sealed record SoftwareVersionProfile(string Name, int ProtocolVersionMin, int ProtocolVersionMax, int DiskFormatVersion, int MaxReadableDiskFormatVersion, IReadOnlySet<string> Features, IReadOnlySet<string> IrreversibleFeatures, bool IsEmulated)` (`SoftwareVersionProfile.cs`) | Describes a release the harness can run a node as: negotiated protocol range, disk format written, highest format readable, understood features, irreversible features. Shared refusal contract: a node started as a profile that cannot interpret its durable image (minimum-reader marker or highest format above `MaxReadableDiskFormatVersion`, or an activated irreversible feature it does not know) fails with `DfsException(Unsupported)` before writing anything to the medium. | FMT-005..FMT-010 |
| `const string SoftwareVersionProfile.ProbeFeature = "tdfs.test.probe"` | Test-only reversible feature flag understood by `Current`, not by `Previous`. No data effect; appears in `DfsClusterStatus.ActiveFeatures` and negotiated `DfsServerCapabilities.Features`; may only become active while every live member understands it. | FMT-007, FMT-009 |
| `const string SoftwareVersionProfile.IrreversibleProbeFeature = "tdfs.test.irreversible-probe"` | Test-only irreversible feature understood by `Current`, not by `Previous`. Same eligibility rule; once active every node durably records it and raises its minimum-reader marker above `Previous.MaxReadableDiskFormatVersion`; `SetFeatureAsync(f, false)` is refused. | FMT-006 |
| `static SoftwareVersionProfile Current { get; }` | The build under test (protocol `ProtocolInfo.MinSupportedVersion..CurrentVersion`, its disk format, features include both probe features). Harness-started nodes run it. | FMT-005..FMT-010 |
| `static SoftwareVersionProfile Previous { get; }` | Oldest release in the supported window; while the window has one release, an emulation of `Current` (`IsEmulated`) without the two probe features and unable to read images marked by `IrreversibleProbeFeature`. | FMT-005, FMT-007..FMT-010 |
| `static SoftwareVersionProfile BelowSupportedWindow { get; }` | A release older than the window (emulated if necessary) whose `MaxReadableDiskFormatVersion` is below `Current.DiskFormatVersion`; must refuse every current image. | FMT-006 |
| `static IReadOnlyList<SoftwareVersionProfile> SupportedWindow { get; }` | Every supported release, oldest first; first = `Previous`, last = `Current`. | FMT-005, FMT-007 |
| `static SoftwareVersionProfile Get(string name)` (implemented) / `bool Supports(string feature)` (implemented) | Name lookup for theory data ("Current", "Previous", "BelowSupportedWindow"); feature membership. | FMT-005, FMT-010 |
| `public static class VersionedClusterTest` with `static Task RunAsync(TestClusterOptions options, SoftwareVersionProfile initialRelease, Func<TestCluster, Task> scenario, CancellationToken ct = default)` (`VersionedClusterTest.cs`) | Exactly `ClusterTest.RunAsync`, except every live and backup node is created and bootstrapped by `initialRelease` (fresh media formatted in its disk format, its protocol range and features); `SoftwareVersion` reports it until a node is restarted as another release. Clients always run the current SDK. `ArgumentException` outside the supported window. | FMT-005, FMT-007, FMT-008, FMT-009, FMT-010 |
| `SoftwareVersionProfile TestNode.SoftwareVersion { get; }` (`TestNode.fmt-ops-perf.cs`) | Release the node runs (or last ran). | FMT-005..FMT-010 |
| `Task TestNode.RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile, Func<NodeOptions, NodeOptions>? reconfigure = null)` | Stops a running/paused node gracefully first, then restarts it from its durable image as `profile` (incarnation + 1); completes when running and registered. Refusal: `DfsException(Unsupported)`, node stays `Stopped`, medium untouched. Armed faults apply during startup; if one crashes the node the task completes without throwing and `State == Crashed`. | FMT-005..FMT-010 |
| `SoftwareVersionProfile TestBackupNode.SoftwareVersion { get; }`, `Task TestBackupNode.RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile)` (`TestBackupNode.fmt-ops-perf.cs`) | Same contract for backup nodes; registration name, endpoint and credentials unchanged. | FMT-010 |
| `Task<DfsErrorCode> RawProtocolClient.SendPathOperationWithExtensionsAsync(string volume, DfsOperationKind kind, string rawPath, IReadOnlyList<WireExtension> extensions, string? rawPath2 = null)` (`RawProtocolClient.fmt-ops-perf.cs`) | Same request as `SendPathOperationAsync` but the message's terminal extension block additionally carries exactly `extensions` (in order). Server must refuse an unknown *required* extension with `Unsupported` and no effect, and treat unknown *optional* ones as absent. Returns `None` on success. | FMT-002, FMT-003 |
| `IReadOnlyList<MessageAttempt> TestNetwork.GetMessageAttempts(MessageFilter filter)` (`TestNetwork.fmt-ops-perf.cs`) | Every transmission matching the filter observed by the link policy (including dropped/held/duplicated/black-holed), in order. | OPS-007 |
| `public sealed record MessageAttempt(long Sequence, string From, string To, LinkDirection Direction, MessageType Type, ulong CorrelationId, DfsOperationId? OperationId, int Bytes, LinkDecisionKind Decision)` (`MessageAttempt.cs`) | `CorrelationId` is distinct for every request transmission (each retry/reroute is a new one; duplicates of one transmission share it; a reply carries its request's id). `OperationId` is the id in the request's operation header (or the answered request's id for replies). | OPS-007 |

## New server inspector members (`src/Tedd.Tdfs.Server/Additions/fmt-ops-perf/`)

| Member / type | Required behaviour | Needed by |
|---|---|---|
| `Task<DiskFormatInspection> NodeInspector.GetDiskFormatAsync()` (`NodeInspector.fmt-ops-perf.cs`) | Reads, on the node loop, the format versions durably recorded on the medium (not in-memory intent). | FMT-005, FMT-006, FMT-008 |
| `public sealed record DiskFormatInspection(int SuperblockVersion, int JournalFormatVersion, int CheckpointFormatVersion, int ObjectSegmentFormatVersion, int MinimumReaderVersion, IReadOnlyList<string> ActivatedIrreversibleFeatures)` + `int HighestComponentVersion` (implemented) (`DiskFormatInspection.cs`) | Component format versions (0 for absent components), the never-lowered minimum-reader marker, and activated irreversible features (ordinal order). | FMT-005, FMT-006, FMT-008 |

## Behaviour of existing members the tests rely on

| Member / behaviour | Needed by |
|---|---|
| `MessageLog.CountMessages/CountBytes` count every message offered to the network since the last `Reset`, including dropped and black-holed ones; `MessageFilter.Direction = Request` selects request frames. | OPS-011, PERF-001, PERF-002, PERF-008, PERF-009, PERF-016 |
| Data reads travel as `GetObject`/`GetObjectResult`; directory pages as `ListDirectory`/`ListDirectoryResult`; live read metadata as `ReadRange`. | PERF-002, PERF-008, PERF-009 |
| `NodeCounters.ObjectBytesReceived`, `RepairBytes`, `BackupBytesSent`, `GcDeletedObjects` are cumulative per node; `ClusterInspector.GetTotalCountersAsync` sums them over running nodes. | OPS-014, OPS-015, PERF-002, PERF-005, PERF-010..PERF-012 |
| `FaultSites.ObjectWrite` is hit with `FaultContext.Operation = Repair` for repair writes on the receiving node; `FaultSites.GcSweep` on every GC sweep; `FaultSites.BackupBeforePointer` on the backup node (node name "b0") before the published-pointer update. | OPS-003, OPS-005, OPS-006 |
| `PinInspection.Kind` is `"Enumeration"` for directory-enumeration pins. | PERF-008 |
| `DfsReplicaHealth` counts objects the leader can verify as durably held; `StagedBytes` lies between distinct and per-replica bytes of objects of open staging sessions. | OPS-003, OPS-004, OPS-005 |
| `TestClientOptions.Diagnostics` receives SDK events carrying the operation id of the mutation they concern. | OPS-007 |
| `DfsFileInfo.Revision` and `CommitToken.Sequence` are both the Raft log index of the entry that published the change (ARCHITECTURE.md identifiers table), so a mutation's returned token sequence equals the resulting revision. | OPS-007 |
| `FaultSites.RecoveryJournalReplay`, `RecoveryCheckpointInstall` and `RaftSnapshotInstall` are reached while a node restarted on a new release recovers (local replay, local checkpoint, leader snapshot because the leader compacted past it); `FaultSites.Reconfiguration` is reached during its protocol/version transition (incarnation and release registration with the control group). | FMT-008 |

## Implemented Core members beyond API.md that the FMT unit tests rely on

These already exist in `src/Tedd.Tdfs.Core` (the codec, error and identifier types are implemented) and are pinned by the
FMT-001/002/003/004/011/012 unit tests; they must not be removed or change encoding.

| Member | Pinned behaviour |
|---|---|
| `WireWriter.WriteNodeId/WriteVolumeId/WriteObjectId/WriteOperationId/WriteOptionalOperationId/WriteCommitToken`, `WireReader.ReadNodeId/ReadVolumeId/ReadObjectId/ReadOperationId/ReadOptionalOperationId/ReadCommitToken` | GUID ids as `WriteGuid`; `ObjectId` as little-endian `UInt128`; operation id = varuint session, varuint sequence; optional = bool presence byte + value; commit token = cluster GUID, volume GUID, varuint epoch, varuint sequence. |
| `MessageCodec.Decode<T>(ReadOnlySpan<byte>, WireLimits?)` | Typed decode of a whole frame. |
| `Tedd.Tdfs.Protocol.Messages.ErrorReplyMessage(Code, Message, OperationId, Retryable, LeaderHint, TransactionId)` + `static FromException(Exception, DfsOperationId?, string?)` | Error frame layout: `[u16 1][zigzag varint code][string message][optional op id][bool retryable][optional string leader hint][optional tx id][extension block]`. Code must fit 32 bits. |
| `ErrorCodec.DecodeToException(ReadOnlySpan<byte>, string? path = null)` | Recreates the canonical exception type (API.md A.2 mapping) with code, op id, retryability, transaction id and leader hint; unknown codes become a plain `DfsException` carrying the code verbatim. |
| `DfsErrors.IsRetryable(Exception)`, `DfsErrors.GetOperationId(Exception)` | Retryability / operation identity of any mapped exception. |
| `DfsUnavailableException(DfsErrorCode, string)` + `string? LeaderHint`, `DfsCommitOutcomeUnknownException(DfsOperationId, DfsTransactionId?, string)`, `DfsTransactionConflictException(DfsTransactionId, string)` | Constructors/properties used to build typed errors for round-trips. |
| Strict canonical decoding | Non-minimal varints, bool/presence bytes other than 0/1, reserved extension flag bits, duplicate extension ids, overlong/invalid UTF-8, trailing bytes and unknown message types are `DfsProtocolException(ProtocolError)`; an unknown required extension is `DfsProtocolException(Unsupported)`; `WriteString` rejects unpaired surrogates with `ArgumentException`. |
