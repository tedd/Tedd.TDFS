# API additions — SNP / APP test suites

Branch `tests/snp-app`. Tests: SNP-001..SNP-018, APP-001..APP-012.

## Harness / product stubs

None. Every SNP/APP scenario is expressed with members already listed in `docs/API.md` (sections B, C.5, D, F).
No partial harness class (`TestCluster`, `TestNode`, `TestNetwork`, `TestFaults`, `ClusterInspector`, ...) and no
server inspector received new members.

## New standalone test helpers (fully implemented, test-side only)

Location: `tests/Tedd.Tdfs.Testing/Additions/snp-app/`, namespace `Tedd.Tdfs.Testing.SnpApp`. They use only the
public SDK (`DfsVolume`, `DfsFileClient`, `DfsDirectoryClient`, `DfsStream`, `DfsSnapshotView`, `IDfsSnapshotAdapter`)
and `Tedd.Tdfs.Data.ObjectChecksum`; they use no clocks other than an injected `TimeProvider`, no threads and no
thread pool, so they run unchanged inside the deterministic simulation. They are test oracles/doubles, not product
behaviour, so they are implemented now (not stubs).

### `AdapterTimeline` / `TimelineEvent`

```csharp
public sealed record TimelineEvent(long Sequence, string Source, string Event, DateTimeOffset At, long Timestamp, string? Detail);
public sealed class AdapterTimeline
{
    public AdapterTimeline(TimeProvider? time = null);
    public TimeProvider Time { get; }
    public IReadOnlyList<TimelineEvent> Events { get; }
    public long Record(string source, string eventName, string? detail = null);   // strictly increasing Sequence under a lock
    public TimelineEvent? Find(string source, string eventName);                 // first
    public TimelineEvent? FindLast(string source, string eventName);
    public TimelineEvent Get(string source, string eventName);                   // Assert.Fail (with dump) when absent
    public bool Has(string source, string eventName);
    public IReadOnlyList<TimelineEvent> WithName(string eventName);
    public void AssertBefore(TimelineEvent earlier, TimelineEvent later, string because);
    public TimeSpan Elapsed(TimelineEvent from, TimelineEvent to);
}
```
Totally ordered, thread-safe log of adapter callbacks and fault-site probes (`FaultAction.Custom` recording e.g.
`server:capture.accepted` at `rpc.request.accepted`/CreateSnapshot, `server:capture.applied` at `apply.after`,
`server:transfer.first` at `backup.*`). Used by APP-002, APP-003, APP-005, APP-007, SNP-016.

### `RecordingSnapshotAdapter`, `AdapterPhaseBehavior`, `AdapterInjectedFailureException`

```csharp
public enum AdapterPhaseBehavior { Succeed, ThrowAsync, ThrowSynchronously, ReturnCanceledTask, HangUntilCancelled, HangIgnoringCancellation }
public sealed class AdapterInjectedFailureException : Exception { public string AdapterId { get; } public string Phase { get; } }
public sealed class RecordingSnapshotAdapter : IDfsSnapshotAdapter
{
    public RecordingSnapshotAdapter(string id, AdapterTimeline timeline, string version = "1.0.0", IReadOnlyDictionary<string, string>? restoreRequirements = null);
    public AdapterPhaseBehavior PrepareBehavior { get; set; }   // also ResumeBehavior, AbortBehavior
    public Func<DfsSnapshotAdapterContext, CancellationToken, Task>? PrepareWork { get; set; }   // also ResumeWork, AbortWork (run before the behaviour)
    public int PrepareCalls { get; }  public int ResumeCalls { get; }  public int AbortCalls { get; }
    public bool PrepareStarted { get; } public bool PrepareSucceeded { get; } public bool ResumeSucceeded { get; } public bool AbortSucceeded { get; }
    public bool CancellationObserved { get; }   // caller's token fired during a callback
    public bool IsPaused { get; }               // prepare started and no resume/abort attempted since
    public bool PauseReleased { get; }          // a resume or abort was attempted after the last prepare
    public IReadOnlyList<DfsSnapshotAdapterContext> Contexts { get; }
    public void ReleaseHangs();                 // ends HangIgnoringCancellation waits (test cleanup)
}
```
Records `{id}:{phase}.start|done|failed|cancelled` and `cancel.observed` into the timeline. Used by APP-002, APP-004,
APP-005, APP-006, APP-007, APP-008, SNP-016.

### `LogSampleApp` (+ `LogSampleAppOptions`, `LogRecordInfo`, `LogRecord`, `LogScan`, `LogRecoveryResult`)

```csharp
public sealed record LogSampleAppOptions { public string Root { get; init; } = "/app"; public bool SplitRecordFlush { get; init; } }
public sealed class LogSampleApp
{
    public LogSampleApp(DfsVolume volume, LogSampleAppOptions? options = null);
    public string Root { get; } public string LogPath { get; } public string DataPath { get; }
    public IReadOnlyList<LogRecordInfo> Records { get; } public long LastSequence { get; } public long AppliedSequence { get; }
    public CommitToken LastDataToken { get; } public CommitToken? LastCheckpointToken { get; } public bool IsPaused { get; }
    public IReadOnlyDictionary<string, byte[]> CommittedState { get; }
    public Task<CommitToken> InitializeAsync(CancellationToken ct = default);
    public Task<LogRecordInfo> UpdateAsync(string key, byte[] value, CancellationToken ct = default);          // log append, then data apply
    public Task<LogRecordInfo> AppendLogAsync(string key, byte[] value, CancellationToken ct = default);       // log only
    public Task<LogRecordInfo> AppendLogAsync(string key, byte[] value, Func<CancellationToken, Task>? betweenFlushes, CancellationToken ct = default);
    public Task<CommitToken> ApplyAsync(CancellationToken ct = default);
    public IDfsSnapshotAdapter CreateAdapter(string id = "log-sample-app", AdapterTimeline? timeline = null, string version = "1.0.0");
    public static IReadOnlyDictionary<string, byte[]> ExpectedStateAt(IEnumerable<LogRecordInfo> records, CommitToken boundary);
    public static long ExpectedLastSequenceAt(IEnumerable<LogRecordInfo> records, CommitToken boundary);
    public static Task<LogRecoveryResult> RecoverAsync(DfsFileClient files, string root, bool repair, CancellationToken ct = default);
    public static byte[] EncodeRecord(long sequence, string key, ReadOnlySpan<byte> value);
    public static LogScan ScanLog(ReadOnlySpan<byte> log);
    public static byte[] EncodeData(IReadOnlyDictionary<string, byte[]> state, long appliedSequence);
    public static (IReadOnlyDictionary<string, byte[]> State, long AppliedSequence) DecodeData(ReadOnlySpan<byte> data);
    public static bool StatesEqual(...); public static void AssertStatesEqual(...);
}
```
A tiny write-ahead-log application on the DFS (`{root}/wal.log` + `{root}/data.bin`, CRC-32C framed). Documented flush
order: log record published first (atomic append, or header + payload as two stream commits with
`SplitRecordFlush`), then the data file is atomically rewritten with the applied sequence. A filesystem-consistent
snapshot may therefore hold a stale data file or a torn log tail but never a data file ahead of the log;
`RecoverAsync` replays the valid log suffix (and with `repair` rewrites the data file and truncates the torn tail).
The adapter pauses updates, drains the in-flight one and checkpoints the data file in prepare (application-consistent
image needs no replay) and unpauses in resume/abort; its restore requirements record `log`, `data`, `scope`, `recovery`.
`ExpectedStateAt` is the oracle: all records whose complete publication token is at or before the boundary.
Used by APP-002, APP-003, APP-006, APP-008, APP-009, APP-012.

### `SnapshotTree`

```csharp
public static class SnapshotTree
{
    public static Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsFileClient files, DfsDirectoryClient directories, string root = "/", CancellationToken ct = default);
    public static Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsVolume volume, string root = "/", CancellationToken ct = default);
    public static Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsSnapshotView view, string root = "/", CancellationToken ct = default);
    public static Task<SortedDictionary<string, byte[]?>> CaptureSharedAsync(DfsVolume volume, string root = "/", CancellationToken ct = default);
    public static SortedDictionary<string, byte[]> FilesOf(IReadOnlyDictionary<string, byte[]?> tree);
    public static SortedDictionary<string, byte[]?> Clone(IReadOnlyDictionary<string, byte[]?> tree);
    public static void AssertEqual(IReadOnlyDictionary<string, byte[]?> expected, IReadOnlyDictionary<string, byte[]?> actual, string because);
    public static bool AreEqual(IReadOnlyDictionary<string, byte[]?> a, IReadOnlyDictionary<string, byte[]?> b);
}
```
Complete namespace + byte image (directories map to `null`) captured by enumeration and full reads through the
public SDK of a live volume, snapshot view or backup view. Used by most SNP tests, APP-010/011/012, SNP-004.
`CaptureSharedAsync` reads every live file through an explicit read-only `VersionPinned` open granting
`FileShare.ReadWrite | FileShare.Delete`, the compatible reader pairing required by test plan §5 while other clients
hold writable handles or transaction reservations (used by SNP-002 and SNP-003 so a sharing conflict of the oracle
reader can never masquerade as a visibility result).

### `CommitHistoryOracle`

```csharp
public sealed class CommitHistoryOracle
{
    public CommitHistoryOracle(IReadOnlyDictionary<string, byte[]>? initial = null);
    public int Count { get; }
    public void Record(CommitToken token, string path, byte[]? content);                      // null = deleted
    public void Record(CommitToken token, IReadOnlyDictionary<string, byte[]?> writes);       // atomic multi-path commit
    public SortedDictionary<string, byte[]> StateAt(CommitToken boundary);
    public CommitToken? LatestAtOrBefore(CommitToken boundary);
    public IReadOnlySet<string> TrackedPaths { get; }
    public void AssertMatches(CommitToken boundary, IReadOnlyDictionary<string, byte[]> actualFiles, string because);
}
```
Independent oracle of file contents as a function of commit position: a snapshot with boundary B must contain exactly
the effects of all recorded commits with sequence ≤ B (same cluster/volume/epoch; incomparable tokens throw).
Used by SNP-001, SNP-009, APP-010, APP-011.
