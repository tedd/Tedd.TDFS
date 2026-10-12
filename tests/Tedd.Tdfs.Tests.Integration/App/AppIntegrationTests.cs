using System.Reflection;
using System.Text;
using Microsoft.Win32.SafeHandles;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;
using static Tedd.Tdfs.Tests.Integration.App.AppHelpers;

namespace Tedd.Tdfs.Tests.Integration.App;

/// <summary>
/// APP integration tests (test plan §7.20, FSD §11.3): adapter callback ordering, failure/timeout/resume handling,
/// multi-adapter labelling, adapter metadata retention, log-based recovery from filesystem-consistent snapshots,
/// intermediate application states, transactions, and adapters' use of the storage contract. All run over QUIC with
/// on-disk storage.
/// </summary>
public sealed class AppIntegrationTests
{
    private static readonly TimeSpan Slack = TimeSpan.FromSeconds(10);

    // ============================================================================================================
    // APP-002 PrepareCaptureResumeOrder
    // ============================================================================================================

    /// <summary>
    /// APP-002: with a live application writing continuously, every adapter's prepare (pause + checkpoint) completes before
    /// the capture request reaches the server, resume starts only after the capture was applied (boundary established),
    /// and the remote backup transfer starts only after every resume completed. The captured image is the checkpoint:
    /// it needs no log replay, contains the checkpoint commit and excludes post-resume writes.
    /// </summary>
    [Theory]
    [InlineData(0, false)]
    [InlineData(2, false)]
    [InlineData(1, true)]
    [Catalog("APP-002", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_002_PrepareCaptureResumeOrder(int extraAdapters, bool syncApi)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(backups: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true));

            var timeline = new AdapterTimeline(TimeProvider.System);
            var app = new LogSampleApp(vol);
            await app.InitializeAsync(ct);
            for (int i = 0; i < 5; i++)
                await app.UpdateAsync($"k{i}", TestData.Bytes(0xA002_0000UL + (ulong)i, 300), ct);

            var adapters = new List<IDfsSnapshotAdapter> { app.CreateAdapter("log-app", timeline) };
            for (int i = 0; i < extraAdapters; i++)
            {
                adapters.Add(new RecordingSnapshotAdapter($"aux-{i}", timeline, $"2.{i}.0",
                    new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = $"/aux/{i}" }));
            }

            FaultArm accepted = ArmProbe(cluster, timeline, FaultSites.RpcRequestAccepted, DfsOperationKind.CreateSnapshot, "capture.accepted");
            FaultArm applied = ArmProbe(cluster, timeline, FaultSites.ApplyAfterBeforeReply, DfsOperationKind.CreateSnapshot, "capture.applied");
            FaultArm transfer = ArmProbe(cluster, timeline, "backup.*", null, "transfer.first");

            using var stop = new CancellationTokenSource();
            Task writer = WriterLoopAsync(app, 0xA002_1000UL, stop.Token);
            await AssertEx.EventuallyAsync(() => Task.FromResult(app.LastSequence >= 10), TimeSpan.FromSeconds(30), "the application is writing");

            var options = new DfsSnapshotOptions { Adapters = adapters, AdapterTimeout = TimeSpan.FromSeconds(10) };
            DfsSnapshotInfo info = syncApi
                ? await Task.Run(() => vol.Snapshots.Create("ordered", options), ct)
                : await vol.Snapshots.CreateAsync("ordered", options, ct);
            long seqAfterCreate = app.LastSequence;
            await AssertEx.EventuallyAsync(() => Task.FromResult(app.LastSequence >= seqAfterCreate + 3), TimeSpan.FromSeconds(30),
                "application writes resume after the snapshot");
            await stop.CancelAsync();
            await writer;

            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info.Label);
            Assert.Null(info.AdapterResumeFailure);
            AssertAdapterRecords(adapters, info.Adapters, "created snapshot");
            cluster.Faults.AssertReached(accepted);
            cluster.Faults.AssertReached(applied);
            TimelineEvent acceptedEvent = timeline.Get("server", "capture.accepted");
            TimelineEvent appliedEvent = timeline.Get("server", "capture.applied");
            timeline.AssertBefore(acceptedEvent, appliedEvent, "request acceptance precedes application");
            foreach (IDfsSnapshotAdapter adapter in adapters)
            {
                TimelineEvent prepareDone = timeline.Get(adapter.Id, "prepare.done");
                timeline.AssertBefore(timeline.Get(adapter.Id, "prepare.start"), prepareDone, "prepare runs to completion");
                timeline.AssertBefore(prepareDone, acceptedEvent, $"{adapter.Id}: prepare/checkpoint completes before the capture reaches the server");
                timeline.AssertBefore(appliedEvent, timeline.Get(adapter.Id, "resume.start"), $"{adapter.Id}: resume only after the snapshot boundary is established");
                Assert.Null(timeline.Find(adapter.Id, "abort.start"));
                Assert.Single(timeline.Events, e => e.Source == adapter.Id && e.Event == "prepare.start");
                Assert.Single(timeline.Events, e => e.Source == adapter.Id && e.Event == "resume.start");
            }

            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "ordered", TimeSpan.FromSeconds(60), ct);
            Assert.Equal(info.Id, checkpoint.SnapshotId);
            Assert.True(checkpoint.IsExactSnapshot);
            cluster.Faults.AssertReached(transfer);
            TimelineEvent transferEvent = timeline.Get("server", "transfer.first");
            foreach (IDfsSnapshotAdapter adapter in adapters)
                timeline.AssertBefore(timeline.Get(adapter.Id, "resume.done"), transferEvent, $"{adapter.Id}: remote transfer occurs after the application resumed");

            Assert.NotNull(app.LastCheckpointToken);
            Assert.True(info.Boundary.IsAtOrAfter(app.LastCheckpointToken!.Value), "the snapshot boundary includes the prepare checkpoint");
            Assert.Contains(app.Records, r => r.CompleteToken.CompareTo(info.Boundary) > 0);
            LogRecoveryResult image = await RecoverFromSnapshotAsync(vol, "ordered", app.Root, ct);
            Assert.Equal(0, image.ReplayedRecords);
            Assert.False(image.TornTail);
            Assert.Equal(image.LastValidSequence, image.DataAppliedSequence);
            LogSampleApp.AssertStatesEqual(LogSampleApp.ExpectedStateAt(app.Records, info.Boundary), image.State, "application-consistent image");
        }, ct);
    }

    // ============================================================================================================
    // APP-003 is a simulation test (Tedd.Tdfs.Tests.Simulation/App).
    // ============================================================================================================

    // ============================================================================================================
    // APP-004 PrepareFailureAbortsLabelledCapture
    // ============================================================================================================

    public enum PrepareFailure
    {
        ThrowAsync,
        ThrowSynchronously,
        ReturnCanceledTask,
    }

    /// <summary>
    /// APP-004: when one of three adapters fails its prepare (faulted task, synchronous throw, or a canceled task), the
    /// creation fails with DfsSnapshotAdapterException naming that adapter and phase; no snapshot is labelled
    /// ApplicationConsistent under the incomplete set (no silent downgrade); every started prepare's pause is released
    /// and successful prepares are resumed; a later capture with a healthy set gets the stronger label.
    /// </summary>
    [Theory]
    [InlineData(0, PrepareFailure.ThrowAsync)]
    [InlineData(1, PrepareFailure.ThrowAsync)]
    [InlineData(2, PrepareFailure.ThrowAsync)]
    [InlineData(0, PrepareFailure.ThrowSynchronously)]
    [InlineData(1, PrepareFailure.ThrowSynchronously)]
    [InlineData(2, PrepareFailure.ThrowSynchronously)]
    [InlineData(0, PrepareFailure.ReturnCanceledTask)]
    [InlineData(1, PrepareFailure.ReturnCanceledTask)]
    [InlineData(2, PrepareFailure.ReturnCanceledTask)]
    [Catalog("APP-004", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_004_PrepareFailureAbortsLabelledCapture(int failingIndex, PrepareFailure failure)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            await vol.Files.WriteAllBytesAsync("/app/state", TestData.Bytes(0xA004_0001, 5000), ct);
            var timeline = new AdapterTimeline(TimeProvider.System);
            RecordingSnapshotAdapter[] adapters = Enumerable.Range(0, 3)
                .Select(i => new RecordingSnapshotAdapter($"a{i}", timeline, $"1.{i}"))
                .ToArray();
            RecordingSnapshotAdapter failing = adapters[failingIndex];
            failing.PrepareBehavior = failure switch
            {
                PrepareFailure.ThrowAsync => AdapterPhaseBehavior.ThrowAsync,
                PrepareFailure.ThrowSynchronously => AdapterPhaseBehavior.ThrowSynchronously,
                _ => AdapterPhaseBehavior.ReturnCanceledTask,
            };

            Exception? error = await CaptureAsync(() => vol.Snapshots.CreateAsync("labelled", new DfsSnapshotOptions { Adapters = adapters }, ct));

            DfsSnapshotAdapterException adapterError = Assert.IsAssignableFrom<DfsSnapshotAdapterException>(error);
            Assert.Equal(DfsErrorCode.AdapterFailed, adapterError.ErrorCode);
            Assert.Equal(failing.Id, adapterError.AdapterId);
            Assert.Equal("prepare", adapterError.Phase, ignoreCase: true);
            if (failure != PrepareFailure.ReturnCanceledTask)
            {
                Assert.True(ChainContains(adapterError, e => e is AdapterInjectedFailureException f && f.AdapterId == failing.Id),
                    "the adapter's own failure is surfaced as the cause");
            }

            await AssertNotApplicationConsistentAsync(vol, "labelled", ct);
            Assert.DoesNotContain(await vol.Snapshots.ListAsync(ct), s => s.Label == DfsConsistencyLabel.ApplicationConsistent);
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.DoesNotContain(state.Snapshots, s => s.Label == DfsConsistencyLabel.ApplicationConsistent);

            foreach (RecordingSnapshotAdapter adapter in adapters)
            {
                if (adapter.PrepareSucceeded)
                    Assert.True(adapter.ResumeCalls >= 1, $"{adapter.Id}: resume must be attempted after a successful prepare");
                if (adapter.PrepareStarted)
                    Assert.True(adapter.PauseReleased, $"{adapter.Id}: the application pause must be released (resume or abort)");
                Assert.False(adapter.IsPaused, $"{adapter.Id} remains paused");
            }

            Assert.True(failing.PrepareStarted);
            Assert.False(failing.PrepareSucceeded);

            // A complete, healthy adapter set later yields the stronger label.
            failing.PrepareBehavior = AdapterPhaseBehavior.Succeed;
            DfsSnapshotInfo ok = await vol.Snapshots.CreateAsync("labelled-retry", new DfsSnapshotOptions { Adapters = adapters }, ct);
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, ok.Label);
            AssertAdapterRecords(adapters, ok.Adapters, "healthy retry");
        }, ct);
    }

    // ============================================================================================================
    // APP-005 AdapterTimeoutReleasesPause
    // ============================================================================================================

    public enum HangPoint
    {
        PrepareIgnoresCancellation,
        PrepareHonoursCancellation,
        SecondAdapterPrepareHangs,
        PrepareFailsThenAbortHangs,
        CaptureBlockedAtServer,
        ResumeHangs,
    }

    /// <summary>
    /// APP-005: a hung prepare (ignoring or honouring cancellation), a hung abort after another adapter's failure, a
    /// capture blocked at the server, or a hung resume never pins the application: CreateAsync completes within a bound
    /// derived from AdapterTimeout and the operation timeout, every started pause is released (resume after every
    /// successful prepare), the failure is surfaced, and a blocked capture can never later produce an
    /// ApplicationConsistent snapshot whose boundary follows post-resume application commits; a definite failure never
    /// commits afterwards.
    /// </summary>
    [Theory]
    [InlineData(HangPoint.PrepareIgnoresCancellation)]
    [InlineData(HangPoint.PrepareHonoursCancellation)]
    [InlineData(HangPoint.SecondAdapterPrepareHangs)]
    [InlineData(HangPoint.PrepareFailsThenAbortHangs)]
    [InlineData(HangPoint.CaptureBlockedAtServer)]
    [InlineData(HangPoint.ResumeHangs)]
    [Catalog("APP-005", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_005_AdapterTimeoutReleasesPause(HangPoint hang)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        TimeSpan adapterTimeout = TimeSpan.FromSeconds(1);
        TimeSpan operationTimeout = TimeSpan.FromSeconds(5);
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = operationTimeout }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            await vol.Files.WriteAllBytesAsync("/app/state", TestData.Bytes(0xA005_0001, 3000), ct);
            var timeline = new AdapterTimeline(TimeProvider.System);
            var first = new RecordingSnapshotAdapter("first", timeline);
            var second = new RecordingSnapshotAdapter("second", timeline);
            RecordingSnapshotAdapter? culprit = null;
            switch (hang)
            {
                case HangPoint.PrepareIgnoresCancellation:
                    first.PrepareBehavior = AdapterPhaseBehavior.HangIgnoringCancellation;
                    culprit = first;
                    break;
                case HangPoint.PrepareHonoursCancellation:
                    first.PrepareBehavior = AdapterPhaseBehavior.HangUntilCancelled;
                    culprit = first;
                    break;
                case HangPoint.SecondAdapterPrepareHangs:
                    second.PrepareBehavior = AdapterPhaseBehavior.HangIgnoringCancellation;
                    culprit = second;
                    break;
                case HangPoint.PrepareFailsThenAbortHangs:
                    second.PrepareBehavior = AdapterPhaseBehavior.ThrowAsync;
                    first.AbortBehavior = AdapterPhaseBehavior.HangIgnoringCancellation;
                    culprit = second;
                    break;
                case HangPoint.ResumeHangs:
                    first.ResumeBehavior = AdapterPhaseBehavior.HangIgnoringCancellation;
                    culprit = first;
                    break;
            }

            FaultArm? block = null;
            if (hang == HangPoint.CaptureBlockedAtServer)
            {
                block = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.CreateSnapshot, Count: 1000),
                    FaultAction.Block());
            }

            try
            {
                DfsOperationId opId = await vol.NewOperationIdAsync(ct);
                TimeSpan bound = (adapterTimeout * 3) + (hang == HangPoint.CaptureBlockedAtServer ? operationTimeout : TimeSpan.Zero) + Slack;
                long start = TimeProvider.System.GetTimestamp();
                Task<DfsSnapshotInfo> create = vol.Snapshots.CreateAsync("bounded", new DfsSnapshotOptions
                {
                    Adapters = [first, second],
                    AdapterTimeout = adapterTimeout,
                    OperationId = opId,
                }, ct);
                Task winner = await Task.WhenAny(create, Task.Delay(bound, ct));
                Assert.True(ReferenceEquals(winner, create), $"{hang}: CreateAsync must complete within {bound} (adapter timeouts are bounded)");
                TimeSpan elapsed = TimeProvider.System.GetElapsedTime(start);
                Exception? error = await CaptureAsync(() => create);
                DfsSnapshotInfo? info = error is null ? create.Result : null;

                // Every started pause is released; every successful prepare is followed by a resume attempt.
                foreach (RecordingSnapshotAdapter adapter in new[] { first, second })
                {
                    if (adapter.PrepareStarted)
                        Assert.True(adapter.PauseReleased, $"{hang}: {adapter.Id} must be resumed or aborted (pause released)");
                    if (adapter.PrepareSucceeded)
                        Assert.True(adapter.ResumeCalls >= 1, $"{hang}: {adapter.Id} prepared successfully and must get a resume attempt");
                    TimelineEvent? paused = timeline.Find(adapter.Id, "prepare.start");
                    TimelineEvent? released = timeline.Events.FirstOrDefault(e => e.Source == adapter.Id && e.Event is "resume.start" or "abort.start");
                    if (paused is not null && released is not null)
                        Assert.True(timeline.Elapsed(paused, released) <= bound, $"{adapter.Id} paused for {timeline.Elapsed(paused, released)} > {bound}");
                }

                switch (hang)
                {
                    case HangPoint.PrepareIgnoresCancellation:
                    case HangPoint.PrepareHonoursCancellation:
                    case HangPoint.SecondAdapterPrepareHangs:
                    case HangPoint.PrepareFailsThenAbortHangs:
                        {
                            DfsSnapshotAdapterException adapterError = Assert.IsAssignableFrom<DfsSnapshotAdapterException>(error);
                            Assert.Equal(culprit!.Id, adapterError.AdapterId);
                            Assert.Equal("prepare", adapterError.Phase, ignoreCase: true);
                            if (hang == HangPoint.PrepareHonoursCancellation)
                                Assert.True(culprit.CancellationObserved, "the hung prepare must receive cancellation at the timeout");
                            if (hang == HangPoint.PrepareFailsThenAbortHangs)
                                Assert.True(first.ResumeCalls >= 1, "a hung abort must not prevent the required resume of a prepared adapter");
                            Assert.True(elapsed <= bound);
                            await AssertNotApplicationConsistentAsync(vol, "bounded", ct);
                            break;
                        }

                    case HangPoint.ResumeHangs:
                        {
                            Assert.Null(error);
                            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info!.Label);
                            Assert.NotNull(info.AdapterResumeFailure);
                            Assert.True(ChainContains(info.AdapterResumeFailure, e => e is TimeoutException or OperationCanceledException
                                    || (e is DfsSnapshotAdapterException d && d.AdapterId == "first")),
                                $"the resume timeout must be reported explicitly: {info.AdapterResumeFailure}");
                            Assert.True(second.ResumeCalls >= 1, "one adapter's hung resume must not skip the other adapters' resume");
                            DfsSnapshotInfo stored = await vol.Snapshots.GetAsync("bounded", ct);
                            Assert.Equal(info.Id, stored.Id);
                            Assert.Equal(info.Boundary, stored.Boundary);
                            break;
                        }

                    case HangPoint.CaptureBlockedAtServer:
                        {
                            Assert.NotNull(error);
                            Assert.True(error is DfsSnapshotAdapterException or DfsCommitOutcomeUnknownException or TimeoutException
                                or DfsUnavailableException or OperationCanceledException,
                                $"a blocked capture must surface as an adapter/timeout/unknown-outcome failure, got {error}");
                            cluster.Faults.AssertReached(block!);
                            Assert.True(first.ResumeCalls >= 1 && second.ResumeCalls >= 1, "both prepared adapters must be resumed");

                            // The application continues; then the blocked request is released.
                            CommitToken afterResume = await vol.Files.WriteAllBytesAsync("/app/after-resume", TestData.Bytes(0xA005_0002, 100), ct);
                            block!.Disarm();
                            block.Release();
                            await Task.Delay(TimeSpan.FromSeconds(2), ct);

                            DfsOperationOutcome outcome = await vol.QueryOperationAsync(opId, true, ct);
                            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                                $"resolve:true must produce a definite outcome, got {outcome.Status}");
                            bool exists = (await vol.Snapshots.ListAsync(ct)).Any(s => s.Name == "bounded");
                            Assert.Equal(outcome.Status == DfsOutcomeStatus.Committed, exists);
                            if (error is not DfsCommitOutcomeUnknownException)
                                Assert.False(exists, "a definite failure reported to the caller must never commit afterwards");
                            if (exists)
                            {
                                DfsSnapshotInfo late = await vol.Snapshots.GetAsync("bounded", ct);
                                if (late.Label == DfsConsistencyLabel.ApplicationConsistent)
                                {
                                    Assert.True(late.Boundary.CompareTo(afterResume) < 0,
                                        "an ApplicationConsistent capture must precede every application commit made after the pause was released");
                                }
                            }

                            break;
                        }
                }

                Assert.False(first.IsPaused);
                Assert.False(second.IsPaused);
            }
            finally
            {
                first.ReleaseHangs();
                second.ReleaseHangs();
                block?.Release();
            }
        }, ct);
    }

    // ============================================================================================================
    // APP-006 ResumeFailureIsVisible
    // ============================================================================================================

    public enum ResumeFailure
    {
        ThrowAsync,
        ThrowSynchronously,
        ReturnCanceledTask,
    }

    /// <summary>
    /// APP-006: when an adapter's resume fails after the source snapshot was created, the snapshot and its
    /// ApplicationConsistent outcome are preserved and returned, the failure is reported explicitly in
    /// AdapterResumeFailure, and the other adapter's resume still runs (its application continues).
    /// </summary>
    [Theory]
    [InlineData(ResumeFailure.ThrowAsync, 0)]
    [InlineData(ResumeFailure.ThrowAsync, 1)]
    [InlineData(ResumeFailure.ThrowSynchronously, 0)]
    [InlineData(ResumeFailure.ThrowSynchronously, 1)]
    [InlineData(ResumeFailure.ReturnCanceledTask, 0)]
    [InlineData(ResumeFailure.ReturnCanceledTask, 1)]
    [Catalog("APP-006", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_006_ResumeFailureIsVisible(ResumeFailure failure, int failingIndex)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            var timeline = new AdapterTimeline(TimeProvider.System);
            var app = new LogSampleApp(vol);
            await app.InitializeAsync(ct);
            for (int i = 0; i < 4; i++)
                await app.UpdateAsync($"k{i}", TestData.Bytes(0xA006_0000UL + (ulong)i, 500), ct);
            await app.AppendLogAsync("pending", TestData.Bytes(0xA006_0100, 64), ct);   // log ahead of data; prepare checkpoints it

            var fragile = new RecordingSnapshotAdapter("fragile", timeline, "0.9")
            {
                ResumeBehavior = failure switch
                {
                    ResumeFailure.ThrowAsync => AdapterPhaseBehavior.ThrowAsync,
                    ResumeFailure.ThrowSynchronously => AdapterPhaseBehavior.ThrowSynchronously,
                    _ => AdapterPhaseBehavior.ReturnCanceledTask,
                },
            };
            IDfsSnapshotAdapter appAdapter = app.CreateAdapter("log-app", timeline);
            IDfsSnapshotAdapter[] adapters = failingIndex == 0 ? [fragile, appAdapter] : [appAdapter, fragile];

            DfsSnapshotInfo info = await vol.Snapshots.CreateAsync("resume-fails", new DfsSnapshotOptions { Adapters = adapters }, ct);

            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info.Label);
            AssertAdapterRecords(adapters, info.Adapters, "snapshot preserved despite the resume failure");
            Assert.NotNull(info.AdapterResumeFailure);
            Assert.True(ChainContains(info.AdapterResumeFailure, e =>
                    (e is AdapterInjectedFailureException f && f.AdapterId == "fragile" && f.Phase == "resume")
                    || (e is DfsSnapshotAdapterException d && d.AdapterId == "fragile" && string.Equals(d.Phase, "resume", StringComparison.OrdinalIgnoreCase))
                    || (failure == ResumeFailure.ReturnCanceledTask && e is OperationCanceledException)),
                $"the cleanup problem of 'fragile' must be reported explicitly: {info.AdapterResumeFailure}");
            Assert.Equal(1, fragile.ResumeCalls);
            Assert.False(app.IsPaused, "the other adapter's resume still ran");
            Assert.NotNull(timeline.Find("log-app", "resume.done"));

            DfsSnapshotInfo stored = await vol.Snapshots.GetAsync("resume-fails", ct);
            Assert.Equal(info.Id, stored.Id);
            Assert.Equal(info.Boundary, stored.Boundary);
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, stored.Label);
            LogRecoveryResult image = await RecoverFromSnapshotAsync(vol, "resume-fails", app.Root, ct);
            Assert.Equal(0, image.ReplayedRecords);
            LogSampleApp.AssertStatesEqual(LogSampleApp.ExpectedStateAt(app.Records, info.Boundary), image.State, "recoverable snapshot");

            // The application keeps working.
            await app.UpdateAsync("after", TestData.Bytes(0xA006_0200, 32), ct).WaitAsync(TimeSpan.FromSeconds(30), ct);
        }, ct);
    }

    // ============================================================================================================
    // APP-007 MultipleAdaptersAllMustSucceed
    // ============================================================================================================

    /// <summary>
    /// APP-007: several cooperating applications each checkpoint their own file in prepare and mark it "resumed" in
    /// resume. The stronger label is published only when all prepares succeed, in which case every prepare completes
    /// before the capture is applied, no resume starts before it, and the snapshot holds every application's checkpoint
    /// (none of the resumed states). If any one fails, nothing is labelled ApplicationConsistent and prepared ones resume.
    /// </summary>
    [Theory]
    [InlineData(2, -1)]
    [InlineData(3, -1)]
    [InlineData(5, -1)]
    [InlineData(2, 0)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(5, 2)]
    [InlineData(5, 4)]
    [Catalog("APP-007", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_007_MultipleAdaptersAllMustSucceed(int count, int failingIndex)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            var timeline = new AdapterTimeline(TimeProvider.System);
            var checkpoints = new Dictionary<int, (byte[] Bytes, CommitToken Token)>();
            var adapters = new RecordingSnapshotAdapter[count];
            for (int i = 0; i < count; i++)
            {
                int index = i;
                string path = $"/coop/{index}";
                await vol.Files.WriteAllBytesAsync(path, Encoding.UTF8.GetBytes($"initial-{index}"), ct);
                adapters[i] = new RecordingSnapshotAdapter($"coop-{index}", timeline, $"1.{index}.0",
                    new Dictionary<string, string>(StringComparer.Ordinal) { ["file"] = path, ["role"] = index % 2 == 0 ? "primary" : "secondary" })
                {
                    PrepareWork = async (context, token) =>
                    {
                        byte[] bytes = Encoding.UTF8.GetBytes($"prepared-{index}-{context.SnapshotName}");
                        CommitToken commit = await vol.Files.WriteAllBytesAsync(path, bytes, token);
                        lock (checkpoints)
                            checkpoints[index] = (bytes, commit);
                    },
                    ResumeWork = async (context, token) =>
                        await vol.Files.WriteAllBytesAsync(path, Encoding.UTF8.GetBytes($"resumed-{index}"), token),
                };
            }

            if (failingIndex >= 0)
                adapters[failingIndex].PrepareBehavior = AdapterPhaseBehavior.ThrowAsync;
            FaultArm applied = ArmProbe(cluster, timeline, FaultSites.ApplyAfterBeforeReply, DfsOperationKind.CreateSnapshot, "capture.applied");

            Exception? error = await CaptureAsync(() => vol.Snapshots.CreateAsync("coop", new DfsSnapshotOptions { Adapters = adapters }, ct));

            if (failingIndex < 0)
            {
                Assert.Null(error);
                DfsSnapshotInfo info = await vol.Snapshots.GetAsync("coop", ct);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info.Label);
                AssertAdapterRecords(adapters, info.Adapters, "all adapters succeeded");
                cluster.Faults.AssertReached(applied);
                TimelineEvent capture = timeline.Get("server", "capture.applied");
                long lastPrepare = adapters.Max(a => timeline.Get(a.Id, "prepare.done").Sequence);
                long firstResume = adapters.Min(a => timeline.Get(a.Id, "resume.start").Sequence);
                Assert.True(lastPrepare < capture.Sequence, "every adapter's preparation completes before the capture");
                Assert.True(capture.Sequence < firstResume, "no application resumes before the capture is applied");
                await using DfsSnapshotView view = await vol.Snapshots.OpenAsync("coop", ct);
                for (int i = 0; i < count; i++)
                {
                    Assert.True(info.Boundary.IsAtOrAfter(checkpoints[i].Token), $"snapshot includes coop-{i}'s checkpoint");
                    byte[] captured = await view.Files.ReadAllBytesAsync($"/coop/{i}", ct);
                    AssertEx.BytesEqual(checkpoints[i].Bytes, captured, $"coop-{i} is captured in its prepared state");
                }
            }
            else
            {
                DfsSnapshotAdapterException adapterError = Assert.IsAssignableFrom<DfsSnapshotAdapterException>(error);
                Assert.Equal(adapters[failingIndex].Id, adapterError.AdapterId);
                Assert.Equal("prepare", adapterError.Phase, ignoreCase: true);
                await AssertNotApplicationConsistentAsync(vol, "coop", ct);
                foreach (RecordingSnapshotAdapter adapter in adapters)
                {
                    if (adapter.PrepareSucceeded)
                        Assert.True(adapter.ResumeCalls >= 1, $"{adapter.Id} prepared and must be resumed");
                    if (adapter.PrepareStarted)
                        Assert.True(adapter.PauseReleased, $"{adapter.Id} must not stay paused");
                }
            }
        }, ct);
    }

    // ============================================================================================================
    // APP-008 AdapterMetadataRetained
    // ============================================================================================================

    /// <summary>
    /// APP-008: adapter ids, versions and restore requirements (log/checkpoint paths, scope, recovery rule; Unicode keys
    /// and values; empty requirement sets) are recorded with the snapshot, identical through create/get/list/view, on
    /// every replica's label, after a cold restart of all nodes, in the backup's published manifest (also after the
    /// backup restarts), and usable by a restore runbook to recover the application from the backup copy.
    /// </summary>
    [Fact]
    [Catalog("APP-008", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_008_AdapterMetadataRetained()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(backups: 1), async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: false));
            var timeline = new AdapterTimeline(TimeProvider.System);
            IDfsSnapshotAdapter[] adapters;
            DfsSnapshotInfo created;
            LogSampleApp app;
            await using (DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "before" }, ct))
            {
                DfsVolume vol = client.GetVolume(Volume);
                app = new LogSampleApp(vol, new LogSampleAppOptions { Root = "/db" });
                await app.InitializeAsync(ct);
                for (int i = 0; i < 6; i++)
                    await app.UpdateAsync($"row{i}", TestData.Bytes(0xA008_0000UL + (ulong)i, 256), ct);

                adapters =
                [
                    app.CreateAdapter("db-main", timeline, "14.2.1"),
                    new RecordingSnapshotAdapter("cache-ü", timeline, "2026.10-β", new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["checkpoint"] = "/cache/checkpoint.bin",
                        ["scope"] = "volume:vol:/cache",
                        ["notes-日本"] = "requires warm-up → rebuild index",
                    }),
                    new RecordingSnapshotAdapter("no-requirements", timeline, "1"),
                ];
                created = await vol.Snapshots.CreateAsync("app-meta", new DfsSnapshotOptions { Adapters = adapters }, ct);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, created.Label);
                AssertAdapterRecords(adapters, created.Adapters, "create result");
                AssertAdapterRecords(adapters, (await vol.Snapshots.GetAsync("app-meta", ct)).Adapters, "GetAsync");
                AssertAdapterRecords(adapters, Assert.Single(await vol.Snapshots.ListAsync(ct), s => s.Name == "app-meta").Adapters, "ListAsync");
                await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("app-meta", ct))
                    AssertAdapterRecords(adapters, view.Info.Adapters, "snapshot view");
                Assert.Equal(Volume, created.Volume);   // the adapters' scope is this volume
            }

            await cluster.WaitForReplicationAsync(Volume, ct);
            foreach (TestNode node in cluster.Nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, node.Name);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, Assert.Single(state.Snapshots, s => s.Name == "app-meta").Label);
            }

            // Cold restart of every source node (on-disk images only).
            TestNode[] nodes = cluster.Nodes.ToArray();
            foreach (TestNode node in nodes)
                await node.StopAsync();
            foreach (TestNode node in nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            await using DfsClient after = await cluster.ConnectAsync(new TestClientOptions { Name = "after" }, ct);
            DfsVolume afterVol = after.GetVolume(Volume);
            DfsSnapshotInfo restarted = await afterVol.Snapshots.GetAsync("app-meta", ct);
            Assert.Equal(created.Id, restarted.Id);
            Assert.Equal(created.Boundary, restarted.Boundary);
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, restarted.Label);
            AssertAdapterRecords(adapters, restarted.Adapters, "after a cold restart of all source nodes");

            // Backup manifest carries the same records, also after the backup node restarts.
            await afterVol.Backups.RequestSyncAsync(backup.TargetName, "app-meta", ct);
            DfsBackupCheckpoint checkpoint = await afterVol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "app-meta", TimeSpan.FromSeconds(60), ct);
            Assert.Equal(created.Id, checkpoint.SnapshotId);
            for (int round = 0; round < 2; round++)
            {
                if (round == 1)
                    await backup.RestartAsync();
                await using DfsClient backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = $"breader{round}", Credentials = backup.ReaderCredentials }, ct);
                DfsVolume backupVol = backupClient.GetVolume(Volume);
                DfsSnapshotInfo onBackup = await backupVol.Snapshots.GetAsync("app-meta", ct);
                Assert.Equal(created.Id, onBackup.Id);
                Assert.Equal(created.Boundary, onBackup.Boundary);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, onBackup.Label);
                AssertAdapterRecords(adapters, onBackup.Adapters, round == 0 ? "backup manifest" : "backup manifest after backup restart");

                // A restore runbook uses the recorded requirements to recover the application from the backup copy.
                DfsAdapterRecord db = onBackup.Adapters.Single(a => a.Id == "db-main");
                Assert.Equal(app.LogPath, db.RestoreRequirements["log"]);
                Assert.Equal(app.DataPath, db.RestoreRequirements["data"]);
                await using DfsSnapshotView view = await backupVol.Snapshots.OpenAsync("app-meta", ct);
                LogRecoveryResult image = await LogSampleApp.RecoverAsync(view.Files, db.RestoreRequirements["scope"], false, ct);
                Assert.Equal(0, image.ReplayedRecords);
                LogSampleApp.AssertStatesEqual(LogSampleApp.ExpectedStateAt(app.Records, created.Boundary), image.State, "recovered from the backup using recorded requirements");
            }
        }, ct);
    }

    // ============================================================================================================
    // APP-009 FilesystemSnapshotMayNeedRecovery
    // ============================================================================================================

    public enum LogScenario
    {
        BetweenLogAndData,
        TornLogTail,
        AfterApply,
        ConcurrentWorkload,
    }

    /// <summary>
    /// APP-009: a log-based sample application (write-ahead log + data file on the DFS, documented flush order) is
    /// snapshotted without adapters: between log append and data apply, inside a split record publication (torn tail),
    /// after apply, and repeatedly under a concurrent workload. Each snapshot is labelled FilesystemConsistent; its raw
    /// image may need recovery (data behind log, torn tail) but never shows the data file ahead of the log; replaying its
    /// valid log yields exactly the state of all records completely published at the boundary. Restored into a new
    /// volume, recovery repairs the image (data file rewritten, torn tail truncated) and is idempotent.
    /// </summary>
    [Theory]
    [InlineData(LogScenario.BetweenLogAndData)]
    [InlineData(LogScenario.TornLogTail)]
    [InlineData(LogScenario.AfterApply)]
    [InlineData(LogScenario.ConcurrentWorkload)]
    [Catalog("APP-009", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_009_FilesystemSnapshotMayNeedRecovery(LogScenario scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, ct);
            await using DfsClient snapClient = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume snapVol = snapClient.GetVolume(Volume);
            bool split = scenario is LogScenario.TornLogTail or LogScenario.ConcurrentWorkload;
            var app = new LogSampleApp(vol, new LogSampleAppOptions { SplitRecordFlush = split });
            await app.InitializeAsync(ct);
            for (int i = 0; i < 5; i++)
                await app.UpdateAsync($"k{i}", TestData.Bytes(0xA009_0000UL + (ulong)i, 100 + (i * 700)), ct);

            var snapshots = new List<DfsSnapshotInfo>();
            switch (scenario)
            {
                case LogScenario.BetweenLogAndData:
                    await app.AppendLogAsync("k5", TestData.Bytes(0xA009_0105, 900), ct);
                    snapshots.Add(await snapVol.Snapshots.CreateAsync("fs-between", null, ct));
                    await app.ApplyAsync(ct);
                    break;
                case LogScenario.TornLogTail:
                    await app.AppendLogAsync("k5", TestData.Bytes(0xA009_0205, 900), async token =>
                        snapshots.Add(await snapVol.Snapshots.CreateAsync("fs-torn", null, token)), ct);
                    await app.ApplyAsync(ct);
                    break;
                case LogScenario.AfterApply:
                    snapshots.Add(await snapVol.Snapshots.CreateAsync("fs-clean", null, ct));
                    break;
                case LogScenario.ConcurrentWorkload:
                    {
                        async Task WorkloadAsync()
                        {
                            for (int n = 0; n < 40; n++)
                            {
                                byte[] value = TestData.Bytes(0xA009_1000UL + (ulong)n, 50 + (n % 4 * 1500));
                                if (n % 3 == 0)
                                {
                                    await app.AppendLogAsync($"k{n % 6}", value, ct);
                                    await Task.Delay(TimeSpan.FromMilliseconds(3), ct);
                                    await app.ApplyAsync(ct);
                                }
                                else
                                {
                                    await app.UpdateAsync($"k{n % 6}", value, ct);
                                }
                            }
                        }

                        async Task SnapshotterAsync()
                        {
                            for (int s = 0; s < 6; s++)
                            {
                                await Task.Delay(TimeSpan.FromMilliseconds(15 + (s * 7)), ct);
                                DfsSnapshotInfo info = await snapVol.Snapshots.CreateAsync($"fs-loop-{s}", null, ct);
                                lock (snapshots)
                                    snapshots.Add(info);
                            }
                        }

                        await Task.WhenAll(WorkloadAsync(), SnapshotterAsync());
                        break;
                    }
            }

            Assert.NotEmpty(snapshots);
            foreach (DfsSnapshotInfo info in snapshots)
            {
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
                Assert.Empty(info.Adapters);
                IReadOnlyDictionary<string, byte[]> expected = LogSampleApp.ExpectedStateAt(app.Records, info.Boundary);
                long expectedLast = LogSampleApp.ExpectedLastSequenceAt(app.Records, info.Boundary);

                await using DfsSnapshotView view = await snapVol.Snapshots.OpenAsync(info.Name, ct);
                (IReadOnlyDictionary<string, byte[]> rawState, long rawApplied) = LogSampleApp.DecodeData(await view.Files.ReadAllBytesAsync(app.DataPath, ct));
                LogScan rawLog = LogSampleApp.ScanLog(await view.Files.ReadAllBytesAsync(app.LogPath, ct));
                long rawLast = rawLog.Records.Count == 0 ? 0 : rawLog.Records[^1].Sequence;
                Assert.True(rawApplied <= rawLast, $"{info.Name}: the data file can never be ahead of the log in a filesystem-consistent snapshot");

                LogRecoveryResult recovered = await LogSampleApp.RecoverAsync(view.Files, app.Root, false, ct);
                Assert.Equal(expectedLast, recovered.LastValidSequence);
                LogSampleApp.AssertStatesEqual(expected, recovered.State, $"{info.Name}: replaying the valid log recovers the boundary state");

                switch (scenario)
                {
                    case LogScenario.BetweenLogAndData:
                        Assert.Equal(5, rawApplied);
                        Assert.Equal(6, rawLast);
                        Assert.False(LogSampleApp.StatesEqual(expected, rawState), "the raw image is stale and needs recovery");
                        Assert.Equal(1, recovered.ReplayedRecords);
                        break;
                    case LogScenario.TornLogTail:
                        Assert.True(rawLog.TornTail, "the snapshot captured the record header without its payload");
                        Assert.Equal(5, rawLast);
                        Assert.True(recovered.TornTail);
                        Assert.Equal(0, recovered.ReplayedRecords);
                        break;
                    case LogScenario.AfterApply:
                        Assert.Equal(rawLast, rawApplied);
                        Assert.False(rawLog.TornTail);
                        Assert.Equal(0, recovered.ReplayedRecords);
                        break;
                }
            }

            // Restore one snapshot into a new volume and repair it there.
            DfsSnapshotInfo chosen = snapshots[^1];
            DfsVolumeInfo restoredInfo = await snapVol.Snapshots.RestoreAsync(chosen.Name, "app-restored", ct);
            Assert.NotEqual(chosen.Boundary.VolumeId, restoredInfo.Id);
            DfsVolume restored = snapClient.GetVolume("app-restored");
            IReadOnlyDictionary<string, byte[]> expectedChosen = LogSampleApp.ExpectedStateAt(app.Records, chosen.Boundary);
            LogRecoveryResult repaired = await LogSampleApp.RecoverAsync(restored.Files, app.Root, true, ct);
            LogSampleApp.AssertStatesEqual(expectedChosen, repaired.State, "recovered state in the restored volume");
            (IReadOnlyDictionary<string, byte[]> dataAfter, long appliedAfter) = LogSampleApp.DecodeData(await restored.Files.ReadAllBytesAsync(app.DataPath, ct));
            LogScan logAfter = LogSampleApp.ScanLog(await restored.Files.ReadAllBytesAsync(app.LogPath, ct));
            Assert.False(logAfter.TornTail, "repair truncates the torn tail");
            Assert.Equal(repaired.LastValidSequence, appliedAfter);
            LogSampleApp.AssertStatesEqual(expectedChosen, dataAfter, "repaired data file");
            LogRecoveryResult again = await LogSampleApp.RecoverAsync(restored.Files, app.Root, false, ct);
            Assert.Equal(0, again.ReplayedRecords);
            Assert.False(again.TornTail);
        }, ct);
    }

    // ============================================================================================================
    // APP-010 NontransactionalApplicationIntermediateState
    // ============================================================================================================

    /// <summary>
    /// APP-010: an application keeps /pair/a and /pair/b at the same version but updates them with two unrelated writes.
    /// A snapshot between the writes captures the application-invalid intermediate state (a ahead of b) and is labelled
    /// only FilesystemConsistent without adapter records. Under a concurrent loop every snapshot is a commit-order prefix:
    /// b is never ahead of a and each snapshot equals the oracle state at its boundary.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("APP-010", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_010_NontransactionalApplicationIntermediateState(bool concurrent)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, ct);
            await using DfsClient snapClient = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume snapVol = snapClient.GetVolume(Volume);
            var oracle = new CommitHistoryOracle();
            oracle.Record(await vol.Files.WriteAllBytesAsync("/pair/a", PairValue(1), ct), "/pair/a", PairValue(1));
            oracle.Record(await vol.Files.WriteAllBytesAsync("/pair/b", PairValue(1), ct), "/pair/b", PairValue(1));

            if (!concurrent)
            {
                CommitToken ta = await vol.Files.WriteAllBytesAsync("/pair/a", PairValue(2), ct);
                DfsSnapshotInfo between = await snapVol.Snapshots.CreateAsync("between", null, ct);
                CommitToken tb = await vol.Files.WriteAllBytesAsync("/pair/b", PairValue(2), ct);
                Assert.True(between.Boundary.CompareTo(ta) > 0 && between.Boundary.CompareTo(tb) < 0);
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, between.Label);
                Assert.Empty(between.Adapters);
                (int a, int b) = await ReadPairAsync(snapVol, "between", ct);
                Assert.Equal(2, a);
                Assert.Equal(1, b);   // application-invalid intermediate state, captured faithfully and not labelled consistent
                return;
            }

            var snapshots = new List<DfsSnapshotInfo>();
            async Task WriterAsync()
            {
                for (int k = 2; k < 32; k++)
                {
                    oracle.Record(await vol.Files.WriteAllBytesAsync("/pair/a", PairValue(k), ct), "/pair/a", PairValue(k));
                    await Task.Delay(TimeSpan.FromMilliseconds(2), ct);
                    oracle.Record(await vol.Files.WriteAllBytesAsync("/pair/b", PairValue(k), ct), "/pair/b", PairValue(k));
                }
            }

            async Task SnapshotterAsync()
            {
                for (int s = 0; s < 8; s++)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10 + (s * 5)), ct);
                    snapshots.Add(await snapVol.Snapshots.CreateAsync($"loop-{s}", null, ct));
                }
            }

            await Task.WhenAll(WriterAsync(), SnapshotterAsync());
            int intermediate = 0;
            foreach (DfsSnapshotInfo snapshot in snapshots)
            {
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, snapshot.Label);
                Assert.Empty(snapshot.Adapters);
                (int a, int b) = await ReadPairAsync(snapVol, snapshot.Name, ct);
                Assert.True(a == b || a == b + 1, $"{snapshot.Name}: (a={a}, b={b}) is not a prefix of the commit order");
                if (a != b)
                    intermediate++;
                await using DfsSnapshotView view = await snapVol.Snapshots.OpenAsync(snapshot.Name, ct);
                SortedDictionary<string, byte[]?> tree = await SnapshotTree.CaptureAsync(view, "/pair", ct);
                oracle.AssertMatches(snapshot.Boundary, SnapshotTree.FilesOf(tree), snapshot.Name);
            }

            TestContext.Current.TestOutputHelper?.WriteLine($"{intermediate} of {snapshots.Count} snapshots captured an application-invalid intermediate state");
        }, ct);
    }

    // ============================================================================================================
    // APP-011 ApplicationConsistencyWithExplicitTransaction
    // ============================================================================================================

    public enum TxScenario
    {
        SnapshotWhileTransactionOpen,
        CommitWithSnapshot,
        ConcurrentLoop,
    }

    /// <summary>
    /// APP-011: the same two-file update done inside a filesystem transaction is never split by a snapshot: a snapshot
    /// while the transaction is open contains neither change, after commit both, commit-and-snapshot exactly both; under
    /// a concurrent loop every snapshot has a == b and equals the oracle state at its boundary.
    /// </summary>
    [Theory]
    [InlineData(TxScenario.SnapshotWhileTransactionOpen)]
    [InlineData(TxScenario.CommitWithSnapshot)]
    [InlineData(TxScenario.ConcurrentLoop)]
    [Catalog("APP-011", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_011_ApplicationConsistencyWithExplicitTransaction(TxScenario scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, ct);
            await using DfsClient snapClient = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume snapVol = snapClient.GetVolume(Volume);
            var oracle = new CommitHistoryOracle();
            oracle.Record(await PairTransactionAsync(vol, 1, null, ct), PairWrites(1));

            switch (scenario)
            {
                case TxScenario.SnapshotWhileTransactionOpen:
                    {
                        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync("/pair/a", PairValue(2), ct);
                        DfsSnapshotInfo mid = await snapVol.Snapshots.CreateAsync("mid", null, ct);
                        await tx.Files.WriteAllBytesAsync("/pair/b", PairValue(2), ct);
                        CommitToken t = await tx.CommitAsync(ct);
                        DfsSnapshotInfo after = await snapVol.Snapshots.CreateAsync("after", new DfsSnapshotOptions { MinimumCommit = t }, ct);
                        Assert.Equal((1, 1), await ReadPairAsync(snapVol, "mid", ct));
                        Assert.Equal((2, 2), await ReadPairAsync(snapVol, "after", ct));
                        Assert.True(mid.Boundary.CompareTo(t) < 0 && after.Boundary.IsAtOrAfter(t));
                        break;
                    }

                case TxScenario.CommitWithSnapshot:
                    {
                        await using (DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct))
                        {
                            await tx.Files.WriteAllBytesAsync("/pair/a", PairValue(2), ct);
                            await tx.Files.WriteAllBytesAsync("/pair/b", PairValue(2), ct);
                            DfsSnapshotInfo exact = await tx.CommitWithSnapshotAsync("exact", ct);
                            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, exact.Label);
                        }

                        await PairTransactionAsync(vol, 3, null, ct);
                        Assert.Equal((2, 2), await ReadPairAsync(snapVol, "exact", ct));
                        Assert.Equal(PairValue(3), await vol.Files.ReadAllBytesAsync("/pair/a", ct));
                        break;
                    }

                case TxScenario.ConcurrentLoop:
                    {
                        var snapshots = new List<DfsSnapshotInfo>();
                        async Task WriterAsync()
                        {
                            for (int k = 2; k < 27; k++)
                                oracle.Record(await PairTransactionAsync(vol, k, TimeSpan.FromMilliseconds(2), ct), PairWrites(k));
                        }

                        async Task SnapshotterAsync()
                        {
                            for (int s = 0; s < 8; s++)
                            {
                                await Task.Delay(TimeSpan.FromMilliseconds(10 + (s * 5)), ct);
                                snapshots.Add(await snapVol.Snapshots.CreateAsync($"loop-{s}", null, ct));
                            }
                        }

                        await Task.WhenAll(WriterAsync(), SnapshotterAsync());
                        foreach (DfsSnapshotInfo snapshot in snapshots)
                        {
                            (int a, int b) = await ReadPairAsync(snapVol, snapshot.Name, ct);
                            Assert.True(a == b, $"{snapshot.Name}: a transaction was split (a={a}, b={b})");
                            await using DfsSnapshotView view = await snapVol.Snapshots.OpenAsync(snapshot.Name, ct);
                            oracle.AssertMatches(snapshot.Boundary, SnapshotTree.FilesOf(await SnapshotTree.CaptureAsync(view, "/pair", ct)), snapshot.Name);
                        }

                        break;
                    }
            }
        }, ct);
    }

    // ============================================================================================================
    // APP-012 AdapterStorageInterfaceCompatibility
    // ============================================================================================================

    public enum AdapterStorage
    {
        DfsStorageContract,
        NativeLocalPath,
    }

    /// <summary>
    /// APP-012: the SDK offers a Stream (not a FileStream, no OS handle or local path), so an adapter only protects state
    /// it writes through the provided storage contract. The DFS-backed adapter's checkpoint is inside the snapshot and a
    /// restore needs no replay; a "native-path" adapter that checkpoints to a local file reports success, yet the
    /// verification finds none of its checkpoint in the snapshot or the restored volume — Stream availability alone
    /// does not make a native-path database recoverable.
    /// </summary>
    [Theory]
    [InlineData(AdapterStorage.DfsStorageContract)]
    [InlineData(AdapterStorage.NativeLocalPath)]
    [Catalog("APP-012", "I", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_012_AdapterStorageInterfaceCompatibility(AdapterStorage storage)
    {
        // The storage contract is a Stream, not an OS file: no FileStream, handle or native path is exposed.
        Assert.True(typeof(Stream).IsAssignableFrom(typeof(DfsStream)));
        Assert.False(typeof(FileStream).IsAssignableFrom(typeof(DfsStream)));
        Assert.Null(typeof(DfsStream).GetProperty("SafeFileHandle"));
        Assert.Null(typeof(DfsStream).GetProperty("Handle"));
        foreach (MethodInfo method in typeof(DfsFileClient).GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            Type returned = method.ReturnType.IsGenericType ? method.ReturnType.GetGenericArguments()[0] : method.ReturnType;
            Assert.False(typeof(FileStream).IsAssignableFrom(returned) || typeof(SafeFileHandle).IsAssignableFrom(returned),
                $"{method.Name} must not expose an OS file");
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            var timeline = new AdapterTimeline(TimeProvider.System);
            var app = new LogSampleApp(vol, new LogSampleAppOptions { Root = "/db" });
            await app.InitializeAsync(ct);
            for (int i = 0; i < 5; i++)
                await app.UpdateAsync($"row{i}", TestData.Bytes(0xA012_0000UL + (ulong)i, 400), ct);
            await app.AppendLogAsync("row5", TestData.Bytes(0xA012_0005, 400), ct);   // log ahead of the data file

            if (storage == AdapterStorage.DfsStorageContract)
            {
                IDfsSnapshotAdapter adapter = app.CreateAdapter("db-on-dfs", timeline);
                DfsSnapshotInfo info = await vol.Snapshots.CreateAsync("compat", new DfsSnapshotOptions { Adapters = [adapter] }, ct);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info.Label);
                Assert.NotNull(app.LastCheckpointToken);
                Assert.True(info.Boundary.IsAtOrAfter(app.LastCheckpointToken!.Value), "the checkpoint written through the SDK is inside the snapshot");
                Assert.True(await VerifyCheckpointCapturedAsync(vol, "compat", app.DataPath, LogSampleApp.EncodeData(app.CommittedState, app.LastSequence), ct),
                    "verification: the adapter used the provided storage contract");

                DfsVolumeInfo restored = await vol.Snapshots.RestoreAsync("compat", "compat-restored", ct);
                Assert.NotEqual(info.Boundary.VolumeId, restored.Id);
                DfsVolume rvol = client.GetVolume("compat-restored");
                (IReadOnlyDictionary<string, byte[]> state, long applied) = LogSampleApp.DecodeData(await rvol.Files.ReadAllBytesAsync(app.DataPath, ct));
                Assert.Equal(app.LastSequence, applied);
                LogSampleApp.AssertStatesEqual(app.CommittedState, state, "the restored data file is the checkpoint (no replay needed)");
                return;
            }

            string nativeDirectory = Path.Combine(cluster.DataRoot, "native-db");
            Directory.CreateDirectory(nativeDirectory);
            var native = new NativePathAdapter(nativeDirectory, app);
            DfsSnapshotInfo nativeInfo = await vol.Snapshots.CreateAsync("native", new DfsSnapshotOptions { Adapters = [native] }, ct);
            Assert.True(native.CheckpointWritten, "the native adapter reported a successful checkpoint");
            byte[] nativeCheckpoint = await File.ReadAllBytesAsync(native.CheckpointPath, ct);
            DfsAdapterRecord record = Assert.Single(nativeInfo.Adapters);
            Assert.Equal("native-path:" + nativeDirectory, record.RestoreRequirements["storage"]);   // the external dependency is recorded, not hidden

            // Verification: none of the native checkpoint is protected by the snapshot or reachable after restore.
            Assert.False(await VerifyCheckpointCapturedAsync(vol, "native", "/db/native.db", nativeCheckpoint, ct),
                "a native-path checkpoint is not captured by a filesystem snapshot");
            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("native", ct))
            {
                foreach (byte[] content in SnapshotTree.FilesOf(await SnapshotTree.CaptureAsync(view, "/", ct)).Values)
                    Assert.False(content.AsSpan().SequenceEqual(nativeCheckpoint), "the native checkpoint bytes must not appear in the snapshot");
                LogRecoveryResult image = await LogSampleApp.RecoverAsync(view.Files, app.Root, false, ct);
                Assert.Equal(1, image.ReplayedRecords);   // only the DFS-resident log protects the last update
            }

            await vol.Snapshots.RestoreAsync("native", "native-restored", ct);
            DfsVolume nrvol = client.GetVolume("native-restored");
            Assert.False(await nrvol.Files.ExistsAsync("/db/native.db", ct), "restoring the snapshot cannot restore native-path state");
        }, ct);
    }

    // ============================================================================================================
    // helpers
    // ============================================================================================================

    private static byte[] PairValue(int version)
    {
        byte[] head = Encoding.ASCII.GetBytes($"v{version:D6}|");
        return [.. head, .. TestData.Bytes(0xA010_0000UL + (ulong)version, 2000)];
    }

    private static Dictionary<string, byte[]?> PairWrites(int version) =>
        new(StringComparer.Ordinal) { ["/pair/a"] = PairValue(version), ["/pair/b"] = PairValue(version) };

    private static int ParseVersion(byte[] value) =>
        int.Parse(Encoding.ASCII.GetString(value, 1, 6), System.Globalization.CultureInfo.InvariantCulture);

    private static async Task<(int A, int B)> ReadPairAsync(DfsVolume vol, string snapshot, CancellationToken ct)
    {
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        byte[] a = await view.Files.ReadAllBytesAsync("/pair/a", ct);
        byte[] b = await view.Files.ReadAllBytesAsync("/pair/b", ct);
        AssertEx.BytesEqual(PairValue(ParseVersion(a)), a, $"{snapshot}: /pair/a is a complete committed version");
        AssertEx.BytesEqual(PairValue(ParseVersion(b)), b, $"{snapshot}: /pair/b is a complete committed version");
        return (ParseVersion(a), ParseVersion(b));
    }

    private static async Task<CommitToken> PairTransactionAsync(DfsVolume vol, int version, TimeSpan? pause, CancellationToken ct)
    {
        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
        await tx.Files.WriteAllBytesAsync("/pair/a", PairValue(version), ct);
        if (pause is not null)
            await Task.Delay(pause.Value, ct);
        await tx.Files.WriteAllBytesAsync("/pair/b", PairValue(version), ct);
        return await tx.CommitAsync(ct);
    }

    private static async Task<bool> VerifyCheckpointCapturedAsync(DfsVolume vol, string snapshot, string path, byte[] checkpoint, CancellationToken ct)
    {
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        if (!await view.Files.ExistsAsync(path, ct))
            return false;
        byte[] captured = await view.Files.ReadAllBytesAsync(path, ct);
        return captured.AsSpan().SequenceEqual(checkpoint);
    }

    /// <summary>
    /// An adapter for a "native-path database": it checkpoints the application's state into a local OS file instead of
    /// through the provided DfsFileClient/DfsStream contract, and records that dependency in its restore requirements.
    /// </summary>
    private sealed class NativePathAdapter : IDfsSnapshotAdapter
    {
        private readonly LogSampleApp _app;

        public NativePathAdapter(string directory, LogSampleApp app)
        {
            _app = app;
            CheckpointPath = Path.Combine(directory, "native.db");
            RestoreRequirements = new Dictionary<string, string>(StringComparer.Ordinal) { ["storage"] = "native-path:" + directory };
        }

        public string Id => "native-db";

        public string Version => "1.0";

        public IReadOnlyDictionary<string, string> RestoreRequirements { get; }

        public string CheckpointPath { get; }

        public bool CheckpointWritten { get; private set; }

        public async Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            await File.WriteAllBytesAsync(CheckpointPath, LogSampleApp.EncodeData(_app.CommittedState, _app.LastSequence), ct);
            CheckpointWritten = true;
        }

        public Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct) => Task.CompletedTask;

        public Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct) => Task.CompletedTask;
    }
}
