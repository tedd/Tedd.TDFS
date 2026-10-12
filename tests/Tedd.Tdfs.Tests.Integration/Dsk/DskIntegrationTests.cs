using System.Globalization;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Integration.Dsk.DskHelpers;

namespace Tedd.Tdfs.Tests.Integration.Dsk;

/// <summary>
/// DSK — local persistence, corruption and crash consistency (test plan §7.18; FSD §§10.3, 13, 14).
/// Storage-engine rows drive <see cref="Journal"/>, <see cref="AtomicFile"/> and <see cref="ObjectStore"/> directly over
/// both <see cref="SimulatedMedium"/> (power loss with every <see cref="CrashMode"/>) and <see cref="FileSystemMedium"/>
/// (real files under TDFS_TEST_ROOT, process-kill crashes), with crash matrices over every write and durability barrier.
/// Whole-node variants crash real nodes (QUIC + disk) with <see cref="TestNode.CrashAsync"/>.
/// </summary>
public sealed class DskIntegrationTests
{
    private const string Inv01 = "INV-01";
    private const string Inv02 = "INV-02";
    private const string Inv09 = "INV-09";
    private const string Inv14 = "INV-14";
    private const string Wal = "wal";

    /// <summary>Journal workloads explored by the DSK-001/013 crash matrices.</summary>
    public enum JournalScenario
    {
        /// <summary>One record, one flush.</summary>
        SingleRecord,

        /// <summary>Three flush batches with record sizes 0, 1, 300, 4103, 17, 70000, 5.</summary>
        BatchedFlushes,

        /// <summary>Tiny segments (512 B) so appends roll over to new segment files.</summary>
        SegmentRollover,

        /// <summary>Flush, truncate the suffix (Raft conflict), append, flush, then an unflushed tail.</summary>
        TruncateThenAppend,
    }

    /// <summary>ENOSPC targets of DSK-005.</summary>
    public enum EnospcStep
    {
        /// <summary>Journal append + flush (metadata log).</summary>
        JournalAppend,

        /// <summary>Object put (data record + index).</summary>
        ObjectPut,

        /// <summary>Atomic checkpoint replacement (temp, flush, rename, directory flush).</summary>
        Checkpoint,

        /// <summary>Object-store compaction (copy, publish, delete).</summary>
        Compaction,
    }

    /// <summary>Corruption applied to every copy of one extent (DSK-010).</summary>
    public enum ReplicaCorruption
    {
        /// <summary>Payload bytes flipped.</summary>
        Payload,

        /// <summary>Identity/length header corrupted.</summary>
        Header,

        /// <summary>Object silently deleted.</summary>
        Missing,
    }

    // ------------------------------------------------------------------------------------------------------------
    // Shared storage-engine scenarios
    // ------------------------------------------------------------------------------------------------------------

    private static JournalOptions JournalOptionsFor(JournalScenario scenario) =>
        scenario == JournalScenario.SegmentRollover ? new JournalOptions(SegmentSize: 512) : new JournalOptions();

    private static async Task RunJournalScenarioAsync(IStorageMedium medium, JournalModel model, JournalScenario scenario)
    {
        Journal journal = await Journal.OpenAsync(medium, Wal, JournalOptionsFor(scenario), Ct);

        async Task Append(ulong seed, int length)
        {
            JournalModel.ModelRecord record = model.BeginAppend(TestData.Bytes(seed, length));
            long index = await journal.AppendAsync(record.Payload, Ct);
            model.Appended(record, index);
        }

        async Task Flush()
        {
            await journal.FlushAsync(Ct);
            model.Flushed();
        }

        switch (scenario)
        {
            case JournalScenario.SingleRecord:
                await Append(1, 100);
                await Flush();
                break;
            case JournalScenario.BatchedFlushes:
                await Append(10, 0);
                await Append(11, 1);
                await Append(12, 300);
                await Flush();
                await Append(13, 4096 + 7);
                await Flush();
                await Append(14, 17);
                await Append(15, 70_000);
                await Append(16, 5);
                await Flush();
                break;
            case JournalScenario.SegmentRollover:
                for (int i = 0; i < 12; i++)
                {
                    await Append(20 + (ulong)i, 60 + (i * 13 % 140));
                    if (i % 3 == 2)
                        await Flush();
                }

                break;
            case JournalScenario.TruncateThenAppend:
                for (int i = 0; i < 5; i++)
                    await Append(40 + (ulong)i, 40 + (i * 10));
                await Flush();
                long keepThrough = model.Records[2].Index!.Value;
                model.BeginTruncateAfter(keepThrough);
                await journal.TruncateAfterAsync(keepThrough, Ct);
                await Append(50, 33);
                await Append(51, 77);
                await Flush();
                await Append(52, 21); // unflushed tail at the end of the run
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario));
        }

        // Deliberately no DisposeAsync: "after the last boundary" is a crash without a clean close.
    }

    private static async Task VerifyJournalRecoveryAsync(IStorageMedium medium, JournalModel model, string context, JournalOptions options)
    {
        Journal journal;
        try
        {
            journal = await Journal.OpenAsync(medium, Wal, options, Ct);
        }
        catch (DfsIntegrityException ex)
        {
            Assert.Fail($"{context}: recovery reported interior corruption although the crash only affected unacknowledged writes: {ex.Message}");
            throw;
        }

        IReadOnlyList<JournalRecord> recovered = journal.Records.ToList();
        model.AssertAllowed(recovered, context);

        // The recovered journal must be usable: the next index follows the recovered tail and survives a reopen.
        byte[] sentinel = TestData.Bytes(0x5E47, 33);
        long index = await journal.AppendAsync(sentinel, Ct);
        if (recovered.Count > 0)
            Assert.True(index == recovered[^1].Index + 1, $"{context}: first append after recovery got index {index}, expected {recovered[^1].Index + 1}.");
        await journal.FlushAsync(Ct);
        await journal.DisposeAsync();

        Journal reopened = await Journal.OpenAsync(medium, Wal, options, Ct);
        try
        {
            Assert.True(reopened.Records.Count == recovered.Count + 1, $"{context}: reopen after recovery returned {reopened.Records.Count} records, expected {recovered.Count + 1}.");
            for (int i = 0; i < recovered.Count; i++)
            {
                Assert.Equal(recovered[i].Index, reopened.Records[i].Index);
                AssertEx.BytesEqual(recovered[i].Payload.Span, reopened.Records[i].Payload.Span, $"{context}: record {i} after second reopen");
            }

            Assert.Equal(index, reopened.Records[^1].Index);
            AssertEx.BytesEqual(sentinel, reopened.Records[^1].Payload.Span, $"{context}: sentinel");
        }
        finally
        {
            await reopened.DisposeAsync();
        }
    }

    private static readonly ObjectStoreOptions CompactionStoreOptions = SmallStoreOptions(segmentSize: 16 << 10);

    private static readonly int[] DeletedBySetup = [0, 2, 3, 5, 7, 8, 10];

    /// <summary>Twelve objects (2000..7500 bytes) over several 16 KiB segments, seven of them deleted (durable tombstones).</summary>
    private static async Task ObjectStoreSetupAsync(IStorageMedium medium, ObjectStoreModel model)
    {
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", CompactionStoreOptions, NoFaults.Instance, Ct);
        for (int i = 0; i < 12; i++)
        {
            byte[] data = TestData.Bytes(100 + (ulong)i, 2000 + (500 * i));
            ObjectStoreModel.Entry entry = model.BeginPut(Oid((ulong)i), i % 2 == 0 ? VolumeA : VolumeB, new StagingId(1 + ((ulong)i % 3)), data);
            ObjectRecordInfo info = await store.PutAsync(entry.Id, entry.Volume, entry.Staging, data, Ct);
            model.PutAcked(entry, info, "setup");
        }

        foreach (int i in DeletedBySetup)
        {
            model.BeginDelete(Oid((ulong)i));
            await store.DeleteAsync(Oid((ulong)i), Ct);
            model.Deleted(Oid((ulong)i));
        }

        await store.DisposeAsync();
    }

    /// <summary>Compaction followed by one put and one delete (work racing a fresh layout).</summary>
    private static async Task CompactionScenarioAsync(IStorageMedium medium, ObjectStoreModel model, IFaultInjector? faults = null)
    {
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", CompactionStoreOptions, faults ?? NoFaults.Instance, Ct);
        await store.CompactAsync(Ct);
        byte[] data = TestData.Bytes(150, 5_555);
        ObjectStoreModel.Entry entry = model.BeginPut(Oid(50), VolumeA, new StagingId(9), data);
        ObjectRecordInfo info = await store.PutAsync(entry.Id, entry.Volume, entry.Staging, data, Ct);
        model.PutAcked(entry, info, "post-compaction put");
        model.BeginDelete(Oid(1));
        await store.DeleteAsync(Oid(1), Ct);
        model.Deleted(Oid(1));
    }

    private static async Task VerifyObjectStoreRecoveryAsync(IStorageMedium medium, ObjectStoreModel model, string context, ObjectStoreOptions options)
    {
        ObjectStore store;
        try
        {
            store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        }
        catch (DfsIntegrityException ex)
        {
            Assert.Fail($"{context}: recovery reported corruption although the crash only interrupted unacknowledged work: {ex.Message}");
            throw;
        }

        try
        {
            await model.AssertAllowedAsync(store, context);

            // Re-putting an unacknowledged object with identical content is idempotent and completes it.
            foreach (ObjectStoreModel.Entry entry in model.Entries.Where(e => e.State == ObjectStoreModel.ObjectState.PutPending).ToList())
            {
                ObjectRecordInfo info = await store.PutAsync(entry.Id, entry.Volume, entry.Staging, entry.Data, Ct);
                model.PutAcked(entry, info, context + " re-put");
            }

            await store.CompactAsync(Ct);
            await model.AssertAllowedAsync(store, context + " after a completed compaction");
        }
        finally
        {
            await store.DisposeAsync();
        }
    }

    /// <summary>What an atomically replaced file may contain after recovery (null = absent).</summary>
    private sealed class AtomicModel
    {
        public List<byte[]?> Allowed { get; } = [];

        public void Begin(byte[] next) => Allowed.Add(next);

        public void Completed(byte[] value)
        {
            Allowed.Clear();
            Allowed.Add(value);
        }

        public void AssertAllowed(byte[]? actual, string context)
        {
            foreach (byte[]? allowed in Allowed)
            {
                if (allowed is null && actual is null)
                    return;
                if (allowed is not null && actual is not null && allowed.AsSpan().SequenceEqual(actual))
                    return;
            }

            Assert.Fail($"{context}: atomic file content ({(actual is null ? "absent" : actual.Length + " bytes")}) is not one complete installed version " +
                $"(allowed: {string.Join(", ", Allowed.Select(a => a is null ? "absent" : a.Length + " bytes"))}).");
        }
    }

    private const string CheckpointPath = "meta/ckpt/state";

    private static async Task AtomicSetupAsync(IStorageMedium medium, AtomicModel model)
    {
        byte[] v1 = TestData.Bytes(301, 3_000);
        await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
        model.Begin(v1);
        await AtomicFile.WriteAsync(medium, CheckpointPath, v1, Ct);
        model.Completed(v1);
    }

    private static async Task AtomicScenarioAsync(IStorageMedium medium, AtomicModel model)
    {
        byte[] v2 = TestData.Bytes(302, 9_000);
        model.Begin(v2);
        await AtomicFile.WriteAsync(medium, CheckpointPath, v2, Ct);
        model.Completed(v2);
        byte[] v3 = TestData.Bytes(303, 17);
        model.Begin(v3);
        await AtomicFile.WriteAsync(medium, CheckpointPath, v3, Ct);
        model.Completed(v3);
    }

    private static async Task VerifyAtomicRecoveryAsync(IStorageMedium medium, AtomicModel model, string context, string path = CheckpointPath)
    {
        byte[]? actual;
        try
        {
            actual = await AtomicFile.ReadAsync(medium, path, Ct);
        }
        catch (DfsIntegrityException ex)
        {
            Assert.Fail($"{context}: a partially installed version was selected/detected instead of a complete one: {ex.Message}");
            throw;
        }

        model.AssertAllowed(actual, context);

        // Leftover temporaries must not disturb later installs. (The parent chain is ensured explicitly: AtomicFile's
        // contract covers the file, not the creation of missing ancestor directories.)
        byte[] next = TestData.Bytes(399, 1_234);
        await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(StoragePath.Normalize(path)), Ct);
        await AtomicFile.WriteAsync(medium, path, next, Ct);
        AssertBytes(next, await AtomicFile.ReadAsync(medium, path, Ct) ?? [], $"{context}: install after recovery");
    }

    public static TheoryData<JournalScenario, DskMediumKind, CrashMode, int> JournalCrashData()
    {
        var data = new TheoryData<JournalScenario, DskMediumKind, CrashMode, int>();
        foreach (JournalScenario scenario in Enum.GetValues<JournalScenario>())
        {
            foreach ((DskMediumKind kind, CrashMode mode) in CrashCases())
            {
                int seeds = mode is CrashMode.RandomSubset or CrashMode.TearLastWrite ? 3 : 1;
                for (int seed = 1; seed <= seeds; seed++)
                    data.Add(scenario, kind, mode, seed);
            }
        }

        return data;
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-001
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(JournalCrashData))]
    [Catalog("DSK-001", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_001_JournalAppendCrashMatrix(JournalScenario scenario, DskMediumKind kind, CrashMode mode, int seed)
    {
        JournalOptions options = JournalOptionsFor(scenario);
        CrashMatrixReport report = await RunCrashMatrixAsync<JournalModel>(
            string.Create(CultureInfo.InvariantCulture, $"DSK-001-{scenario}-{kind}-{mode}-{seed}"),
            kind,
            mode,
            (ulong)seed,
            () => new JournalModel(),
            setup: null,
            scenario: (medium, model) => RunJournalScenarioAsync(medium, model, scenario),
            verify: (medium, model, context) => VerifyJournalRecoveryAsync(medium, model, context, options));

        // Evidence that the matrix crashed between writes and their durability barrier, not only at clean points.
        Assert.Contains(nameof(BoundaryKind.Write), report.BoundaryKinds);
        Assert.Contains(nameof(BoundaryKind.FlushFile), report.BoundaryKinds);
        Assert.True(report.PointsWithUnflushedData > 0, $"{report.Name}: no crash point had unflushed journal data outstanding.");
    }

    public static TheoryData<string, CrashMode> RaftStageCrashData()
    {
        var data = new TheoryData<string, CrashMode>();
        foreach (string site in new[] { FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend, FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply })
        {
            foreach (CrashMode mode in Enum.GetValues<CrashMode>())
                data.Add(site, mode);
        }

        return data;
    }

    /// <summary>
    /// Node-level DSK-001: the volume leader crashes (with each power-loss mode) before proposing, after its local log
    /// append, after commit before apply, and after apply before replying. Whatever the client saw, the resolved outcome
    /// of the operation id matches the recovered bytes exactly; a reported success is never lost.
    /// </summary>
    [Theory]
    [MemberData(nameof(RaftStageCrashData))]
    [Catalog("DSK-001", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_001_JournalAppendCrashMatrix_LeaderLogStages(string site, CrashMode mode)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] v0 = TestData.Bytes(1001, 10_000);
            byte[] v1 = TestData.Bytes(1002, 10_000);
            await volume.Files.WriteAllBytesAsync("/f", v0, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Volume: Volume, Operation: DfsOperationKind.WriteAll), FaultAction.CrashNode(mode));
            DfsOperationId opId = await volume.NewOperationIdAsync(Ct);

            Exception? failure = null;
            try
            {
                await volume.Files.WriteAllBytesAsync("/f", v1, new DfsMutationOptions { OperationId = opId }, Ct);
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            cluster.Faults.AssertReached(arm);
            bool preCommit = site is FaultSites.RaftBeforePropose or FaultSites.RaftAfterLocalAppend;
            bool definiteFailure = false;
            if (failure is DfsCommitOutcomeUnknownException unknown)
            {
                // Every site is after the server accepted the request, so without a definite answer the only truthful
                // report is an unknown outcome carrying the operation id (FSD §13).
                Assert.Equal(opId, unknown.OperationId);
            }
            else if (failure is not null)
            {
                // A retry of the same id at the new leader may obtain a definite refusal before any quorum commit (for
                // example when a receipt came from the crashed node's old incarnation). Only then may the client see an
                // ordinary failure, and the operation must then never commit (checked after resolution below).
                Assert.True(preCommit && failure is DfsUnavailableException,
                    $"a leader crash at {site} must surface as an unknown outcome (or, before commit, a definite availability refusal), got {failure.GetType().Name}: {failure.Message}");
                definiteFailure = true;
            }

            await WaitAsync(cluster, () => Task.FromResult(leader.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10), $"the leader crashes at {site}");
            await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

            DfsOperationOutcome outcome = await volume.QueryOperationAsync(opId, resolve: true, Ct);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve:true must give a definite outcome, got {outcome.Status}.");
            if (failure is null)
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            if (definiteFailure)
                Assert.True(outcome.Status == DfsOutcomeStatus.Aborted, $"the client was told the write failed ({failure!.GetType().Name}), yet operation {opId} committed.");

            // Before proposal either outcome is legal (the SDK may retry the same id at the new leader); once a
            // quorum committed the entry, it must survive the leader's crash.
            if (site is FaultSites.RaftAfterCommitBeforeApply or FaultSites.ApplyAfterBeforeReply)
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);

            byte[] actual = await volume.Files.ReadAllBytesAsync("/f", Ct);
            AssertEx.BytesEqual(outcome.Status == DfsOutcomeStatus.Committed ? v1 : v0, actual, $"/f after leader crash at {site} ({mode}), outcome {outcome.Status}");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);
    }

    /// <summary>
    /// Node-level DSK-001: every node loses power at once (each mode) right after a series of acknowledged commits with
    /// further commits still in flight; every acknowledged commit and its commit token survive the cold restart.
    /// </summary>
    [Theory]
    [InlineData(CrashMode.LoseUnflushed)]
    [InlineData(CrashMode.KeepUnflushed)]
    [InlineData(CrashMode.RandomSubset)]
    [InlineData(CrashMode.TearLastWrite)]
    [Catalog("DSK-001", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_001_JournalAppendCrashMatrix_WholeClusterPowerLoss(CrashMode mode)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/acked", Ct);
            await volume.Directories.CreateDirectoryAsync("/inflight", Ct);
            var acked = new Dictionary<string, (byte[] Data, CommitToken Token)>(StringComparer.Ordinal);
            for (int i = 0; i < 12; i++)
            {
                string path = $"/acked/{i}";
                byte[] data = TestData.Bytes(2000 + (ulong)i, i % 3 == 0 ? 100 : 20_000 + i);
                CommitToken token = await volume.Files.WriteAllBytesAsync(path, data, Ct);
                Assert.False(token.IsNone);
                acked[path] = (data, token);
            }

            var inflight = new List<Task>();
            for (int i = 0; i < 4; i++)
                inflight.Add(volume.Files.WriteAllBytesAsync($"/inflight/{i}", TestData.Bytes(3000 + (ulong)i, 30_000), Ct));

            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(mode);
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(45), Ct);
            var inflightAcked = new HashSet<int>();
            for (int i = 0; i < inflight.Count; i++)
            {
                try
                {
                    await inflight[i];
                    inflightAcked.Add(i); // an SDK retry with the same id may still obtain the definite success
                }
                catch (Exception)
                {
                    // Unacknowledged: checked below (absent or complete).
                }
            }

            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v = fresh.GetVolume(Volume);
            CommitToken last = (await v.GetInfoAsync(Ct)).LastCommit;
            foreach ((string path, (byte[] data, CommitToken token)) in acked)
            {
                AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"acknowledged {path} after whole-cluster {mode} power loss");
                Assert.True(last.IsAtOrAfter(token), $"volume history regressed below acknowledged token {token} (now {last}).");
            }

            for (int i = 0; i < 4; i++)
            {
                string path = $"/inflight/{i}";
                bool exists = await v.Files.ExistsAsync(path, Ct);
                if (inflightAcked.Contains(i))
                    Assert.True(exists, $"{path} was acknowledged after the restart but is missing");
                if (exists)
                    AssertBytes(TestData.Bytes(3000 + (ulong)i, 30_000), await v.Files.ReadAllBytesAsync(path, Ct), $"in-flight {path} must be absent or complete");
            }

            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-002
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DskComponent, DskMediumKind, DskDamage> TornRecordData()
    {
        var data = new TheoryData<DskComponent, DskMediumKind, DskDamage>();
        foreach (DskComponent component in Enum.GetValues<DskComponent>())
        {
            foreach (DskMediumKind kind in Enum.GetValues<DskMediumKind>())
            {
                foreach (DskDamage damage in Enum.GetValues<DskDamage>())
                    data.Add(component, kind, damage);
            }
        }

        return data;
    }

    private enum RecordRegion
    {
        Header,
        Payload,
        Trailer,
        Unclassified,
    }

    private sealed record LocatedRecord(string Path, long Start, long End, long PayloadStart, long PayloadEnd, byte[] Payload, IReadOnlyDictionary<string, byte[]> Before)
    {
        public RecordRegion Classify(long offset) =>
            PayloadStart < 0 ? RecordRegion.Unclassified
            : offset < PayloadStart ? RecordRegion.Header
            : offset < PayloadEnd ? RecordRegion.Payload
            : RecordRegion.Trailer;
    }

    private static LocatedRecord Locate(IReadOnlyDictionary<string, byte[]> pristine, IReadOnlyList<Boundary> window, byte[] payload, IReadOnlyDictionary<string, byte[]> before)
    {
        (string Path, int Offset)? found = LocatePayload(pristine, payload);
        if (found is { } f)
        {
            (long start, long end) = ContiguousRun(window, f.Path, f.Offset);
            end = Math.Min(end, pristine[f.Path].Length);
            return new LocatedRecord(f.Path, start, end, f.Offset, f.Offset + payload.Length, payload, before);
        }

        (string Path, long Start, long End)? main = MainWriteRange(window);
        Assert.True(main is not null, "the record's persistence wrote no bytes");
        long clippedEnd = Math.Min(main!.Value.End, pristine.TryGetValue(main.Value.Path, out byte[]? img) ? img.Length : 0);
        return new LocatedRecord(main.Value.Path, main.Value.Start, clippedEnd, -1, -1, payload, before);
    }

    /// <summary>The device persisted only the first <paramref name="cut"/> - start bytes of the record's write.</summary>
    private static byte[] Tear(byte[] after, byte[] before, long start, long end, long cut)
    {
        if (end >= after.Length && before.Length <= start)
            return after[..(int)cut];
        var torn = (byte[])after.Clone();
        for (long i = cut; i < end && i < torn.Length; i++)
            torn[i] = i < before.Length ? before[i] : (byte)0;
        return torn;
    }

    private static byte[]? Damage(byte[] image, LocatedRecord record, DskDamage damage, long offset)
    {
        switch (damage)
        {
            case DskDamage.TornPrefix:
                return Tear(image, record.Before.GetValueOrDefault(record.Path) ?? [], record.Start, record.End, offset);
            case DskDamage.BitFlip:
            {
                var copy = (byte[])image.Clone();
                copy[offset] ^= 0xFF;
                return copy;
            }

            case DskDamage.ZeroRun:
            {
                var copy = (byte[])image.Clone();
                long end = Math.Min(offset + 8, record.End);
                bool changed = false;
                for (long i = offset; i < end; i++)
                {
                    changed |= copy[i] != 0;
                    copy[i] = 0;
                }

                return changed ? copy : null;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(damage));
        }
    }

    [Theory]
    [MemberData(nameof(TornRecordData))]
    [Catalog("DSK-002", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_002_TornRecordDetection(DskComponent component, DskMediumKind kind, DskDamage damage)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-002-{component}-{kind}-{damage}", 2);
        var regionHits = new Dictionary<RecordRegion, int>();
        int cases = 0;

        switch (component)
        {
            case DskComponent.Journal:
                cases = await TornJournalAsync(disk, damage, regionHits);
                break;
            case DskComponent.ObjectStore:
                cases = await TornObjectStoreAsync(disk, damage, regionHits);
                break;
            case DskComponent.AtomicFile:
                cases = await TornAtomicFileAsync(disk, damage, regionHits);
                break;
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"DSK-002 {component}/{kind}/{damage}: {cases} damaged images; regions {string.Join(", ", regionHits.Select(kv => $"{kv.Key}={kv.Value}"))}");
        Assert.True(cases > 0, "no damaged image was evaluated");
        if (damage != DskDamage.TornPrefix && regionHits.ContainsKey(RecordRegion.Payload))
            Assert.True(regionHits.GetValueOrDefault(RecordRegion.Header) + regionHits.GetValueOrDefault(RecordRegion.Trailer) > 0,
                "header/trailer/checksum bytes were never damaged (only the payload)");
    }

    private static async Task<int> TornJournalAsync(DskMedium disk, DskDamage damage, Dictionary<RecordRegion, int> regionHits)
    {
        InterposedMedium medium = disk.Medium;
        byte[][] payloads = [TestData.Bytes(21, 64), TestData.Bytes(22, 200), TestData.Bytes(23, 48)];
        var windows = new List<IReadOnlyList<Boundary>>();
        var befores = new List<IReadOnlyDictionary<string, byte[]>>();
        var options = new JournalOptions();
        Journal journal = await Journal.OpenAsync(medium, Wal, options, Ct);
        foreach (byte[] payload in payloads)
        {
            befores.Add(await SnapshotFilesAsync(medium, Wal));
            int mark = medium.BoundaryCount;
            await journal.AppendAsync(payload, Ct);
            await journal.FlushAsync(Ct);
            windows.Add(medium.Log.Skip(mark).ToList());
        }

        await journal.DisposeAsync();
        SortedDictionary<string, byte[]> pristine = await SnapshotFilesAsync(medium, Wal);
        var records = payloads.Select((p, i) => Locate(pristine, windows[i], p, befores[i])).ToList();
        int tail = payloads.Length - 1;
        int cases = 0;

        for (int r = 0; r < records.Count; r++)
        {
            bool isTail = r == tail;
            if (damage == DskDamage.TornPrefix && !isTail)
                continue;
            LocatedRecord record = records[r];
            for (long offset = record.Start; offset < record.End; offset++)
            {
                byte[]? damaged = Damage(pristine[record.Path], record, damage, offset);
                if (damaged is null)
                    continue;
                var images = new SortedDictionary<string, byte[]>(pristine, StringComparer.Ordinal) { [record.Path] = damaged };
                await RestoreFilesAsync(medium, Wal, images);
                RecordRegion region = record.Classify(offset);
                regionHits[region] = regionHits.GetValueOrDefault(region) + 1;
                cases++;
                string context = $"journal record {r}{(isTail ? " (tail)" : " (interior)")} {damage} at offset {offset} ({region})";

                Journal reopened;
                try
                {
                    reopened = await Journal.OpenAsync(medium, Wal, options, Ct);
                }
                catch (DfsIntegrityException)
                {
                    Assert.False(damage == DskDamage.TornPrefix, $"{context}: a torn tail must be truncated at recovery, not reported as interior corruption.");
                    continue;
                }

                List<JournalRecord> recovered;
                try
                {
                    recovered = reopened.Records.ToList();
                    bool allIntact = recovered.Count == payloads.Length && payloads.Select((p, i) => recovered[i].Payload.Span.SequenceEqual(p)).All(x => x);
                    bool tailDropped = recovered.Count == tail && payloads.Take(tail).Select((p, i) => recovered[i].Payload.Span.SequenceEqual(p)).All(x => x);
                    if (damage == DskDamage.TornPrefix)
                    {
                        // A cut before the end of the payload leaves the record incomplete, so it must be dropped. A cut
                        // after the payload lands in the record's trailer or in other bytes written in the same window
                        // (for example a durability marker): dropping or keeping the intact record are both legal, but
                        // never altered content.
                        bool payloadIncomplete = record.PayloadStart >= 0 ? offset < record.PayloadEnd : offset == record.Start;
                        if (payloadIncomplete)
                            Assert.True(tailDropped, $"{context}: recovery must yield exactly the {tail} complete records before the torn one, got {recovered.Count}.");
                        else
                            Assert.True(tailDropped || allIntact, $"{context}: a torn tail must be dropped or be byte-identical; got {recovered.Count} records with altered content.");
                    }
                    else if (isTail)
                    {
                        Assert.True(allIntact || tailDropped, $"{context}: a damaged tail must be detected (dropped) or be byte-identical; got {recovered.Count} records with altered content.");
                        if (region == RecordRegion.Payload)
                            Assert.False(allIntact, $"{context}: altered payload bytes were accepted as valid.");
                    }
                    else
                    {
                        Assert.True(allIntact, $"{context}: interior corruption must raise DfsIntegrityException, not silently truncate/skip/alter records (got {recovered.Count}).");
                        Assert.NotEqual(RecordRegion.Payload, region);
                    }

                    if (damage == DskDamage.TornPrefix && (offset == record.Start || offset == record.End - 1 || offset % 16 == 0))
                    {
                        // The torn tail is physically discarded: a new record follows the last complete one and survives a reopen.
                        byte[] next = TestData.Bytes(29, 40);
                        long index = await reopened.AppendAsync(next, Ct);
                        if (recovered.Count > 0)
                            Assert.Equal(recovered[^1].Index + 1, index);
                        await reopened.FlushAsync(Ct);
                        await reopened.DisposeAsync();
                        reopened = await Journal.OpenAsync(medium, Wal, options, Ct);
                        Assert.Equal(recovered.Count + 1, reopened.Records.Count);
                        AssertEx.BytesEqual(next, reopened.Records[^1].Payload.Span, $"{context}: record appended after torn-tail recovery");
                    }
                }
                finally
                {
                    await reopened.DisposeAsync();
                }
            }
        }

        return cases;
    }

    private static async Task<int> TornObjectStoreAsync(DskMedium disk, DskDamage damage, Dictionary<RecordRegion, int> regionHits)
    {
        InterposedMedium medium = disk.Medium;
        ObjectStoreOptions options = SmallStoreOptions();
        (ObjectId Id, byte[] Data)[] objects = [(Oid(1), TestData.Bytes(31, 5000)), (Oid(2), TestData.Bytes(32, 300)), (Oid(3), TestData.Bytes(33, 64))];
        var windows = new List<IReadOnlyList<Boundary>>();
        var befores = new List<IReadOnlyDictionary<string, byte[]>>();
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        foreach ((ObjectId id, byte[] data) in objects)
        {
            befores.Add(await SnapshotFilesAsync(medium, "objects"));
            int mark = medium.BoundaryCount;
            await store.PutAsync(id, VolumeA, new StagingId(7), data, Ct);
            windows.Add(medium.Log.Skip(mark).ToList());
        }

        await store.DisposeAsync();
        SortedDictionary<string, byte[]> pristine = await SnapshotFilesAsync(medium, "objects");
        var records = objects.Select((o, i) => Locate(pristine, windows[i], o.Data, befores[i])).ToList();
        int tail = objects.Length - 1;
        int cases = 0;

        for (int r = 0; r < records.Count; r++)
        {
            if (damage == DskDamage.TornPrefix && r != tail)
                continue;
            LocatedRecord record = records[r];
            for (long offset = record.Start; offset < record.End; offset++)
            {
                byte[]? damaged = Damage(pristine[record.Path], record, damage, offset);
                if (damaged is null)
                    continue;
                var images = new SortedDictionary<string, byte[]>(pristine, StringComparer.Ordinal) { [record.Path] = damaged };
                await RestoreFilesAsync(medium, "objects", images);
                RecordRegion region = record.Classify(offset);
                regionHits[region] = regionHits.GetValueOrDefault(region) + 1;
                cases++;
                string context = $"object record {r} {damage} at offset {offset} ({region})";

                ObjectStore reopened;
                try
                {
                    reopened = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
                }
                catch (DfsIntegrityException ex)
                {
                    // Refusing to open is an explicit detection of corrupted bytes, but a torn last record is an ordinary
                    // crash artifact of an unacknowledged put: recovery must discard it and keep serving the acknowledged
                    // objects before it (as the crash matrices require), not take the whole store offline.
                    Assert.False(damage == DskDamage.TornPrefix, $"{context}: a torn tail record made the store refuse to open: {ex.Message}");
                    continue;
                }

                try
                {
                    foreach (ObjectRecordInfo info in reopened.List())
                    {
                        int known = Array.FindIndex(objects, o => o.Id == info.Id);
                        Assert.True(known >= 0, $"{context}: index lists a phantom object {info.Id} (misinterpreted damaged bytes).");
                        Assert.True(info.Length == objects[known].Data.Length && info.Checksum == ObjectChecksum.Compute(objects[known].Data),
                            $"{context}: index entry {info.Id} carries a wrong length/checksum.");
                    }

                    for (int o = 0; o < objects.Length; o++)
                    {
                        (ObjectId id, byte[] data) = objects[o];
                        bool mustBeIntact = o < r; // records before the damaged one are untouched
                        try
                        {
                            byte[] read = await ReadObjectAsync(reopened, id, data.Length);
                            AssertEx.BytesEqual(data, read, $"{context}: object {o} returned bytes");
                            if (o == r && region == RecordRegion.Payload && damage != DskDamage.TornPrefix)
                                Assert.Fail($"{context}: damaged payload bytes were returned as valid data.");
                            if (o == r && damage == DskDamage.TornPrefix && offset < record.PayloadEnd)
                                Assert.Fail($"{context}: a torn record whose payload never fully reached the device was returned as complete data.");
                        }
                        catch (DfsIntegrityException)
                        {
                            Assert.False(mustBeIntact, $"{context}: undamaged object {o} became unreadable.");
                        }
                    }
                }
                finally
                {
                    await reopened.DisposeAsync();
                }
            }
        }

        return cases;
    }

    private static async Task<int> TornAtomicFileAsync(DskMedium disk, DskDamage damage, Dictionary<RecordRegion, int> regionHits)
    {
        InterposedMedium medium = disk.Medium;
        byte[] content = TestData.Bytes(41, 3000);
        await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
        await AtomicFile.WriteAsync(medium, CheckpointPath, content, Ct);
        byte[] pristine = await ReadFileAsync(medium, CheckpointPath);
        List<int> at = FindAll(pristine, content);
        long payloadStart = at.Count == 1 ? at[0] : -1;
        var record = new LocatedRecord(CheckpointPath, 0, pristine.Length, payloadStart, payloadStart < 0 ? -1 : payloadStart + content.Length, content,
            new Dictionary<string, byte[]>(StringComparer.Ordinal));
        int cases = 0;
        for (long offset = 0; offset < pristine.Length; offset++)
        {
            byte[]? damaged = Damage(pristine, record, damage, offset);
            if (damaged is null)
                continue;
            await WriteImageAsync(medium, CheckpointPath, damaged);
            RecordRegion region = record.Classify(offset);
            regionHits[region] = regionHits.GetValueOrDefault(region) + 1;
            cases++;
            string context = $"atomic file {damage} at offset {offset} ({region})";
            try
            {
                byte[]? read = await AtomicFile.ReadAsync(medium, CheckpointPath, Ct);
                Assert.True(read is not null, $"{context}: a damaged file was reported as absent (false absence).");
                AssertEx.BytesEqual(content, read, $"{context}: returned content");
                Assert.False(damage == DskDamage.TornPrefix, $"{context}: a torn file was accepted as a complete version.");
                Assert.False(region == RecordRegion.Payload, $"{context}: altered payload accepted.");
            }
            catch (DfsIntegrityException)
            {
                // Required detection.
            }
        }

        return cases;
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-003
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DskComponent, DskMediumKind, int> ShortWriteData()
    {
        var data = new TheoryData<DskComponent, DskMediumKind, int>();
        foreach (DskComponent component in Enum.GetValues<DskComponent>())
        {
            foreach (DskMediumKind kind in Enum.GetValues<DskMediumKind>())
            {
                foreach (int count in new[] { 1, 3, 1000 })
                    data.Add(component, kind, count);
            }
        }

        return data;
    }

    private static void ArmShortWrites(DskMedium disk, int count)
    {
        if (disk.SimulatedFaults is { } faults)
            faults.ShortNextWrites = count;
        else
            disk.Medium.ShortWrites = count;
    }

    /// <summary>
    /// One storage operation of the DSK-003/004/005 single-fault families, with its acknowledgement points. An
    /// acknowledging call (journal flush, object put, atomic install) promises durability when it returns; the probe
    /// records the medium boundaries each such call spanned and what it acknowledged, so a check can tell a fault inside
    /// an acknowledging call (which must fail that call) from a fault in auxiliary work after it (for example a
    /// best-effort close marker), and verification knows exactly which state must survive a power loss.
    /// </summary>
    private sealed class OperationProbe
    {
        private readonly List<(int Start, int End)> _ackWindows = [];

        public required Func<InterposedMedium, Task> Setup { get; init; }

        public required Func<InterposedMedium, OperationProbe, Task> Run { get; init; }

        public required Func<InterposedMedium, OperationProbe, string, Task> Verify { get; init; }

        /// <summary>Acknowledged calls per started attempt of <see cref="Operation"/>, in order.</summary>
        public List<int> AttemptAcks { get; } = [];

        /// <summary>Acknowledging calls that returned successfully.</summary>
        public int Acks { get; private set; }

        /// <summary>Object ids whose put was acknowledged.</summary>
        public HashSet<ulong> AckedObjects { get; } = [];

        public Task Operation(InterposedMedium medium)
        {
            AttemptAcks.Add(0);
            return Run(medium, this);
        }

        public Task VerifyAfter(InterposedMedium medium, string context) => Verify(medium, this, context);

        public async Task<T> AckAsync<T>(InterposedMedium medium, Func<Task<T>> call)
        {
            int start = medium.BoundaryCount;
            T result = await call();
            _ackWindows.Add((start, medium.BoundaryCount));
            Acks++;
            if (AttemptAcks.Count > 0)
                AttemptAcks[^1]++;
            return result;
        }

        public Task AckAsync(InterposedMedium medium, Func<Task> call) => AckAsync(medium, async () =>
        {
            await call();
            return true;
        });

        /// <summary>True when medium boundary <paramref name="boundary"/> was performed inside an acknowledging call that returned success.</summary>
        public bool InsideAcknowledgement(int boundary) => _ackWindows.Exists(w => boundary >= w.Start && boundary < w.End);
    }

    private static readonly (ulong Id, int Length)[] ProbeObjects = [(71UL, 1), (72UL, 4096), (73UL, 200_000)];

    private static OperationProbe ProbeFor(DskComponent component) => component switch
    {
        DskComponent.Journal => new OperationProbe
        {
            Setup = async m => await WithJournalAsync(m, Wal, new JournalOptions(), async j =>
            {
                await j.AppendAsync(TestData.Bytes(61, 500), Ct);
                await j.FlushAsync(Ct);
            }),
            Run = async (m, p) =>
            {
                Journal j = await Journal.OpenAsync(m, Wal, new JournalOptions(), Ct);
                await j.AppendAsync(TestData.Bytes(62, 10), Ct);
                await j.AppendAsync(TestData.Bytes(63, 5000), Ct);
                await j.AppendAsync(TestData.Bytes(64, 70_000), Ct);
                await p.AckAsync(m, async () => await j.FlushAsync(Ct));
                await j.DisposeAsync();
            },
            Verify = async (m, p, context) => await WithJournalAsync(m, Wal, new JournalOptions(), j =>
            {
                Assert.True(j.Records.Count >= 1, $"{context}: the previously acknowledged record is missing.");
                AssertEx.BytesEqual(TestData.Bytes(61, 500), j.Records[0].Payload.Span, $"{context}: acknowledged record");
                byte[][] expected = [TestData.Bytes(62, 10), TestData.Bytes(63, 5000), TestData.Bytes(64, 70_000)];
                var rest = j.Records.Skip(1).ToList();

                // Each attempt appended the same three records: an acknowledged attempt must contribute exactly those
                // three, a failed one a complete prefix of them (0..3 records). Nothing else may appear.
                bool Matches(int attempt, int pos)
                {
                    if (attempt == p.AttemptAcks.Count)
                        return pos == rest.Count;
                    if (p.AttemptAcks[attempt] > 0)
                        return pos + 3 <= rest.Count && Enumerable.Range(0, 3).All(i => rest[pos + i].Payload.Span.SequenceEqual(expected[i])) && Matches(attempt + 1, pos + 3);
                    for (int k = 0; k <= 3 && pos + k <= rest.Count; k++)
                    {
                        if (k > 0 && !rest[pos + k - 1].Payload.Span.SequenceEqual(expected[k - 1]))
                            break;
                        if (Matches(attempt + 1, pos + k))
                            return true;
                    }

                    return false;
                }

                Assert.True(Matches(0, 0), $"{context}: the {rest.Count} recovered record(s) after the setup record are not the acknowledged batches plus complete prefixes of failed attempts (acknowledgements per attempt: {string.Join(",", p.AttemptAcks)}).");
                return Task.CompletedTask;
            }),
        },
        DskComponent.ObjectStore => new OperationProbe
        {
            Setup = async m => await WithStoreAsync(m, SmallStoreOptions(), async s => await s.PutAsync(Oid(70), VolumeA, new StagingId(1), TestData.Bytes(70, 2000), Ct)),
            Run = async (m, p) =>
            {
                ObjectStore s = await ObjectStore.OpenAsync(m, "objects", SmallStoreOptions(), NoFaults.Instance, Ct);
                foreach ((ulong id, int length) in ProbeObjects)
                {
                    ObjectRecordInfo info = await p.AckAsync(m, async () => await s.PutAsync(Oid(id), VolumeA, new StagingId(2), TestData.Bytes(id, length), Ct));
                    p.AckedObjects.Add(id);
                    Assert.Equal(length, info.Length);
                    Assert.Equal(ObjectChecksum.Compute(TestData.Bytes(id, length)), info.Checksum);
                }

                await s.DisposeAsync();
            },
            Verify = async (m, p, context) => await WithStoreAsync(m, SmallStoreOptions(), async s =>
            {
                AssertBytes(TestData.Bytes(70, 2000), await ReadObjectAsync(s, Oid(70), 2000), $"{context}: previously acknowledged object");
                foreach ((ulong id, int length) in ProbeObjects)
                {
                    bool present = await s.ContainsAsync(Oid(id), Ct);
                    if (p.AckedObjects.Contains(id))
                        Assert.True(present, $"{context}: acknowledged object {id} missing");
                    if (present)
                        AssertBytes(TestData.Bytes(id, length), await ReadObjectAsync(s, Oid(id), length), $"{context}: object {id} must be complete or absent");
                }
            }),
        },
        DskComponent.AtomicFile => new OperationProbe
        {
            Setup = async m =>
            {
                await m.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
                await AtomicFile.WriteAsync(m, CheckpointPath, TestData.Bytes(80, 700), Ct);
            },
            Run = async (m, p) => await p.AckAsync(m, async () => await AtomicFile.WriteAsync(m, CheckpointPath, TestData.Bytes(81, 100_000), Ct)),
            Verify = async (m, p, context) =>
            {
                byte[]? actual = await AtomicFile.ReadAsync(m, CheckpointPath, Ct);
                Assert.NotNull(actual);
                if (p.Acks > 0)
                    AssertEx.BytesEqual(TestData.Bytes(81, 100_000), actual, $"{context}: acknowledged install");
                else if (!actual.AsSpan().SequenceEqual(TestData.Bytes(80, 700)))
                    AssertEx.BytesEqual(TestData.Bytes(81, 100_000), actual, $"{context}: content must be the old or the complete new version");
            },
        },
        _ => throw new ArgumentOutOfRangeException(nameof(component)),
    };

    private static CrashMode AckLossMode(DskMediumKind kind) => kind == DskMediumKind.Simulated ? CrashMode.LoseUnflushed : CrashMode.KeepUnflushed;

    [Theory]
    [MemberData(nameof(ShortWriteData))]
    [Catalog("DSK-003", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_003_ShortWriteHandling(DskComponent component, DskMediumKind kind, int shortWrites)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-003-{component}-{kind}-{shortWrites}", 3);
        OperationProbe probe = ProbeFor(component);
        await probe.Setup(disk.Medium);
        ArmShortWrites(disk, shortWrites);

        // Failing is allowed (with an I/O error, never a hang); acknowledging a partially written object is not.
        Exception? failure = await Record.ExceptionAsync(() => probe.Operation(disk.Medium).WaitAsync(TimeSpan.FromSeconds(20), Ct));
        if (failure is not null)
        {
            Assert.True(failure is IOException, $"short writes may only fail the operation with an I/O error, got {failure.GetType().Name}: {failure.Message}");
            TestContext.Current.TestOutputHelper?.WriteLine($"operation failed under short writes: {failure.Message}");
        }

        Assert.True(disk.Medium.ShortWritesObserved > 0, "no short write was returned to the component (fault point not reached).");
        if (disk.SimulatedFaults is { } faults)
            faults.ShortNextWrites = 0;
        else
            disk.Medium.ShortWrites = 0;

        // Power loss right after the acknowledgement: everything acknowledged must be fully durable.
        InterposedMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
        await probe.VerifyAfter(restarted, $"{component} after {shortWrites} short writes ({probe.Acks} acknowledged call(s), {(failure is null ? "completed" : "failed")})");
    }

    [Theory]
    [InlineData(DskComponent.Journal, DskMediumKind.Simulated)]
    [InlineData(DskComponent.Journal, DskMediumKind.FileSystem)]
    [InlineData(DskComponent.ObjectStore, DskMediumKind.Simulated)]
    [InlineData(DskComponent.ObjectStore, DskMediumKind.FileSystem)]
    [InlineData(DskComponent.AtomicFile, DskMediumKind.Simulated)]
    [InlineData(DskComponent.AtomicFile, DskMediumKind.FileSystem)]
    [Catalog("DSK-003", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_003_ShortWriteHandling_ZeroProgressFailsNotHangs(DskComponent component, DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-003z-{component}-{kind}", 4);
        OperationProbe probe = ProbeFor(component);
        await probe.Setup(disk.Medium);
        disk.Medium.ZeroProgressWrites = int.MaxValue;

        Exception ex = await Assert.ThrowsAnyAsync<IOException>(() => probe.Operation(disk.Medium).WaitAsync(TimeSpan.FromSeconds(20), Ct));
        Assert.IsNotType<TimeoutException>(ex);
        Assert.True(disk.Medium.ShortWritesObserved > 0, "zero-progress write was never returned (fault point not reached).");
        disk.Medium.ZeroProgressWrites = 0;

        InterposedMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
        await probe.VerifyAfter(restarted, $"{component} after zero-progress writes");
    }

    /// <summary>Node-level DSK-003: every node's disk returns short writes; commits still land complete and durable.</summary>
    [Fact]
    [Catalog("DSK-003", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_003_ShortWriteHandling_NodeDisks()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            const int armed = 200;
            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.ShortNextWrites = armed;

            byte[] data = TestData.Bytes(3100, 300_000);
            await volume.Files.WriteAllBytesAsync("/short", data, Ct);
            byte[] small = TestData.Bytes(3101, 1000);
            await volume.Files.WriteAllBytesAsync("/short-inline", small, Ct);
            Assert.Contains(cluster.Nodes, n => n.Disk.Faults.ShortNextWrites < armed);

            foreach (TestNode node in cluster.Nodes)
            {
                node.Disk.Faults.ShortNextWrites = 0;
                await node.CrashAsync(CrashMode.LoseUnflushed);
            }

            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(45), Ct);
            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            AssertBytes(data, await fresh.GetVolume(Volume).Files.ReadAllBytesAsync("/short", Ct), "/short after short writes + power loss");
            AssertBytes(small, await fresh.GetVolume(Volume).Files.ReadAllBytesAsync("/short-inline", Ct), "/short-inline after short writes + power loss");
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-004
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DskComponent, DskMediumKind> ComponentKindData()
    {
        var data = new TheoryData<DskComponent, DskMediumKind>();
        foreach (DskComponent component in Enum.GetValues<DskComponent>())
        {
            foreach (DskMediumKind kind in Enum.GetValues<DskMediumKind>())
                data.Add(component, kind);
        }

        return data;
    }

    /// <summary>
    /// Every durability barrier (file and directory flush) of the operation fails in turn. A barrier failing inside an
    /// acknowledging call (journal flush, object put, atomic install) must fail that call with an I/O error: no durable
    /// receipt may follow a failed flush. A barrier of auxiliary work after the last acknowledgement (for example a
    /// best-effort close marker) may be absorbed. After power loss exactly the acknowledged state is guaranteed; the
    /// failed work may be absent or complete, never partial.
    /// </summary>
    [Theory]
    [MemberData(nameof(ComponentKindData))]
    [Catalog("DSK-004", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_004_FlushErrorPropagates(DskComponent component, DskMediumKind kind)
    {
        List<int> flushBoundaries;
        await using (DskMedium dry = DskMedium.Create(kind, $"DSK-004-dry-{component}-{kind}", 5))
        {
            OperationProbe probe = ProbeFor(component);
            await probe.Setup(dry.Medium);
            int before = dry.Medium.BoundaryCount;
            await probe.Operation(dry.Medium);
            flushBoundaries = dry.Medium.Log.Skip(before)
                .Where(b => b.Kind is BoundaryKind.FlushFile or BoundaryKind.FlushDirectory)
                .Select(b => b.Index - before)
                .ToList();
        }

        Assert.NotEmpty(flushBoundaries);
        int failedAcknowledgements = 0;
        foreach (int relative in flushBoundaries)
        {
            await using DskMedium disk = DskMedium.Create(kind, $"DSK-004-{component}-{kind}-{relative}", 5 + (ulong)relative);
            OperationProbe probe = ProbeFor(component);
            await probe.Setup(disk.Medium);
            int target = disk.Medium.BoundaryCount + relative;
            bool injected = false;
            disk.Medium.Inject = b =>
            {
                if (b.Index != target)
                    return null;
                injected = true;
                return StorageErrors.DeviceError("flush", b.Path);
            };

            Exception? failure = await Record.ExceptionAsync(() => probe.Operation(disk.Medium));
            Assert.True(injected, $"flush boundary {relative} was not reached");
            if (failure is null)
            {
                Assert.False(probe.InsideAcknowledgement(target),
                    $"{component}: flush boundary {relative} failed inside an acknowledging call, yet the call reported success (durable receipt after a failed flush).");
            }
            else
            {
                Assert.True(failure is IOException, $"{component}: a failed flush (boundary {relative}) must surface as an I/O error, got {failure.GetType().Name}: {failure.Message}");
                failedAcknowledgements++;
            }

            disk.Medium.Inject = null;
            InterposedMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
            await probe.VerifyAfter(restarted, $"{component} after failed flush at boundary {relative} ({probe.Acks} acknowledged call(s))");
        }

        Assert.True(failedAcknowledgements > 0, $"{component}: no failed durability barrier ever failed the operation.");

        // Native device flush failure on the simulated disk, then a retried barrier: a later successful flush must
        // really make the earlier data durable (no "fsyncgate" loss of dirty state).
        if (kind == DskMediumKind.Simulated && component == DskComponent.Journal)
        {
            await using DskMedium disk = DskMedium.Create(kind, "DSK-004-retry", 6);
            Journal journal = await Journal.OpenAsync(disk.Medium, Wal, new JournalOptions(), Ct);
            byte[] r0 = TestData.Bytes(90, 100), r1 = TestData.Bytes(91, 100);
            await journal.AppendAsync(r0, Ct);
            await journal.FlushAsync(Ct);
            await journal.AppendAsync(r1, Ct);
            disk.SimulatedFaults!.FailNextFlushes = 1;
            await Assert.ThrowsAnyAsync<IOException>(async () => await journal.FlushAsync(Ct));
            Assert.Equal(0, disk.SimulatedFaults.FailNextFlushes);
            bool retried;
            try
            {
                await journal.FlushAsync(Ct);
                retried = true;
            }
            catch (IOException)
            {
                retried = false; // a journal may poison itself after a failed barrier
            }

            InterposedMedium restarted = await disk.PowerLossAsync(CrashMode.LoseUnflushed);
            await WithJournalAsync(restarted, Wal, new JournalOptions(), j =>
            {
                Assert.True(j.Records.Count >= 1);
                AssertEx.BytesEqual(r0, j.Records[0].Payload.Span, "acknowledged record");
                if (retried)
                {
                    Assert.True(j.Records.Count == 2, "a flush reported success after an earlier failure, yet the record it covered was lost.");
                    AssertEx.BytesEqual(r1, j.Records[1].Payload.Span, "record made durable by the retried flush");
                }

                return Task.CompletedTask;
            });
        }
    }

    /// <summary>
    /// Node-level DSK-004: one node's device fails every flush. It must issue no receipt; ordinary commits proceed on
    /// the healthy nodes (ack 2 of 3), while a volume requiring three durable copies refuses to commit.
    /// </summary>
    [Fact]
    [Catalog("DSK-004", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_004_FlushErrorPropagates_NoReceiptFromFailingNode()
    {
        var strict = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        TestClusterOptions options = QuicDisk(3) with { Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("strict") { DefaultPolicy = strict }] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            TestNode leaderA = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode leaderB = await cluster.GetLeaderAsync("strict", Ct);
            TestNode bad = cluster.Nodes.FirstOrDefault(n => n.Name != leaderA.Name && n.Name != leaderB.Name)
                ?? cluster.Nodes.First(n => n.Name != leaderA.Name);
            long receiptsBefore = (await bad.GetCountersAsync()).ReceiptsIssued;
            bad.Disk.Faults.FailNextFlushes = int.MaxValue;

            byte[] data = TestData.Bytes(4001, 150_000);
            await client.GetVolume(Volume).Files.WriteAllBytesAsync("/ok", data, Ct);
            foreach (ObjectId id in await FileObjectsAsync(cluster, "/ok"))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.DoesNotContain(bad.Name, holders);
                Assert.True(holders.Count >= 2, $"object {id} acknowledged with only {holders.Count} durable holders");
            }

            // Receipts are validated before anything is proposed (FSD §10.3 step 3): without the failing node's receipt
            // the commit is a definite placement refusal, never an unknown outcome or a success.
            DfsPlacementUnavailableException refused = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => client.GetVolume("strict").Files.WriteAllBytesAsync("/needs3", TestData.Bytes(4002, 150_000), Ct), DfsErrorCode.PlacementUnavailable);
            Assert.Equal(3, refused.RequiredCopies);
            Assert.True(refused.AchievedCopies < 3, $"placement reported {refused.AchievedCopies} durable copies although one device fails every flush");

            Assert.True(bad.Disk.Faults.FailNextFlushes < int.MaxValue, "the failing device never saw a flush (fault point not reached).");
            if (bad.State == TestNodeState.Running)
                Assert.Equal(receiptsBefore, (await bad.GetCountersAsync()).ReceiptsIssued);
            Assert.False(await client.GetVolume("strict").Files.ExistsAsync("/needs3", Ct), "a refused commit became visible");

            bad.Disk.Faults.FailNextFlushes = 0;
            if (bad.State != TestNodeState.Running)
                await bad.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            byte[] later = TestData.Bytes(4003, 150_000);
            await WaitAsync(cluster, async () =>
            {
                try
                {
                    await client.GetVolume("strict").Files.WriteAllBytesAsync("/needs3", later, Ct);
                    return true;
                }
                catch (DfsUnavailableException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), "the strict volume commits again once the device flushes");
            AssertBytes(later, await client.GetVolume("strict").Files.ReadAllBytesAsync("/needs3", Ct), "/needs3");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-005
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<EnospcStep, DskMediumKind> EnospcData()
    {
        var data = new TheoryData<EnospcStep, DskMediumKind>();
        foreach (EnospcStep step in Enum.GetValues<EnospcStep>())
        {
            foreach (DskMediumKind kind in Enum.GetValues<DskMediumKind>())
                data.Add(step, kind);
        }

        return data;
    }

    private static OperationProbe EnospcProbe(EnospcStep step) => step switch
    {
        EnospcStep.JournalAppend => ProbeFor(DskComponent.Journal),
        EnospcStep.ObjectPut => ProbeFor(DskComponent.ObjectStore),
        EnospcStep.Checkpoint => ProbeFor(DskComponent.AtomicFile),
        EnospcStep.Compaction => new OperationProbe
        {
            Setup = async m => await ObjectStoreSetupAsync(m, new ObjectStoreModel()),

            // Compaction promises nothing durable to a caller; it must only keep every reference valid.
            Run = async (m, _) => await WithStoreAsync(m, CompactionStoreOptions, async s => await s.CompactAsync(Ct)),
            Verify = async (m, _, context) =>
            {
                var model = new ObjectStoreModel();
                await using (DskMedium scratch = DskMedium.Create(DskMediumKind.Simulated, "enospc-model", 1))
                    await ObjectStoreSetupAsync(scratch.Medium, model);
                await WithStoreAsync(m, CompactionStoreOptions, async s => await model.AssertAllowedAsync(s, context));
            },
        },
        _ => throw new ArgumentOutOfRangeException(nameof(step)),
    };

    /// <summary>
    /// ENOSPC at every allocating local step (write, length change, create, mkdir, rename) of journal appends, object
    /// puts, checkpoint installs and compaction. The failure surfaces as a capacity/disk-full error, or the component
    /// recovers by itself; in both cases a power loss right afterwards leaves exactly the acknowledged state (never a
    /// false durability acknowledgement), the previous valid state is intact, and the operation succeeds once space
    /// returns. Real exhaustion of the simulated medium's capacity is checked at budgets just below the need.
    /// </summary>
    [Theory]
    [MemberData(nameof(EnospcData))]
    [Catalog("DSK-005", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_005_EnospcAtEveryLocalStep(EnospcStep step, DskMediumKind kind)
    {
        List<int> steps;
        await using (DskMedium dry = DskMedium.Create(kind, $"DSK-005-dry-{step}-{kind}", 7))
        {
            OperationProbe probe = EnospcProbe(step);
            await probe.Setup(dry.Medium);
            int before = dry.Medium.BoundaryCount;
            await probe.Operation(dry.Medium);
            steps = dry.Medium.Log.Skip(before)
                .Where(b => b.Kind is BoundaryKind.Write or BoundaryKind.SetLength or BoundaryKind.Open or BoundaryKind.CreateDirectory or BoundaryKind.Rename)
                .Select(b => b.Index - before)
                .ToList();
        }

        Assert.NotEmpty(steps);
        int failedCleanly = 0;
        foreach (int relative in steps)
        {
            await using DskMedium disk = DskMedium.Create(kind, $"DSK-005-{step}-{kind}-{relative}", 7 + (ulong)relative);
            OperationProbe probe = EnospcProbe(step);
            await probe.Setup(disk.Medium);
            int target = disk.Medium.BoundaryCount + relative;
            bool injected = false;
            disk.Medium.Inject = b =>
            {
                if (b.Index != target)
                    return null;
                injected = true;
                return StorageErrors.DiskFull(b.Path);
            };

            Exception? failure = await Record.ExceptionAsync(() => probe.Operation(disk.Medium));
            Assert.True(injected, $"ENOSPC step {relative} not reached");
            if (failure is not null)
            {
                Assert.True(IsCapacityFailure(failure),
                    $"{step}: ENOSPC at step {relative} must surface as a capacity/disk-full failure, got {failure.GetType().Name}: {failure.Message}");
                failedCleanly++;
            }

            disk.Medium.Inject = null;

            // Exactly the acknowledged state survives a power loss, and the operation succeeds once space is available.
            InterposedMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
            await probe.VerifyAfter(restarted, $"{step} after ENOSPC at step {relative} ({probe.Acks} acknowledged call(s), {(failure is null ? "absorbed" : "failed")})");
            await probe.Operation(restarted);
            await probe.VerifyAfter(restarted, $"{step} retried after space returned (step {relative})");
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"DSK-005 {step}/{kind}: ENOSPC injected at {steps.Count} local steps, {failedCleanly} surfaced as capacity failures.");
        if (step != EnospcStep.Compaction)
            Assert.True(failedCleanly > 0, $"{step}: ENOSPC never failed the operation (no allocation is checked).");

        // Real exhaustion (medium capacity) at every capacity just below what the operation needs.
        if (kind == DskMediumKind.Simulated)
        {
            long needed;
            await using (DskMedium probeDisk = DskMedium.Create(kind, $"DSK-005-cap-{step}", 8))
            {
                OperationProbe probe = EnospcProbe(step);
                await probe.Setup(probeDisk.Medium);
                long used = probeDisk.Simulated!.UsedBytes;
                await probe.Operation(probeDisk.Medium);
                needed = Math.Max(1, probeDisk.Simulated.UsedBytes - used);
            }

            foreach (long budget in new[] { 0L, 1, needed / 4, needed / 2, needed - 1 }.Distinct())
            {
                await using DskMedium disk = DskMedium.Create(kind, $"DSK-005-cap-{step}-{budget}", 9);
                OperationProbe probe = EnospcProbe(step);
                await probe.Setup(disk.Medium);
                disk.SimulatedFaults!.CapacityOverride = disk.Simulated!.UsedBytes + budget;
                Exception? failure = await Record.ExceptionAsync(() => probe.Operation(disk.Medium));
                if (failure is not null)
                    Assert.True(IsCapacityFailure(failure), $"capacity budget {budget}: expected a capacity/disk-full failure, got {failure.GetType().Name}: {failure.Message}");
                disk.SimulatedFaults.CapacityOverride = null;
                InterposedMedium restarted = await disk.PowerLossAsync(CrashMode.LoseUnflushed);
                await probe.VerifyAfter(restarted, $"{step} with capacity budget {budget} ({probe.Acks} acknowledged call(s))");
            }
        }
    }

    /// <summary>
    /// DSK-005 reserve: object admission stops with CapacityExhausted before consuming the reserved bytes, so journal
    /// and checkpoint progress (metadata) still fits; deleting + compacting returns admission.
    /// </summary>
    [Theory]
    [InlineData(DskMediumKind.Simulated)]
    [InlineData(DskMediumKind.FileSystem)]
    [Catalog("DSK-005", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_005_EnospcAtEveryLocalStep_ReservePreservedForMetadata(DskMediumKind kind)
    {
        const long capacity = 4L << 20;
        const long reserve = 512L << 10;
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-005-reserve-{kind}", 10, capacity);
        IStorageMedium medium = disk.Medium;
        ObjectStoreOptions options = SmallStoreOptions(segmentSize: 256 << 10, reserved: reserve);
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        var stored = new List<ObjectId>();
        DfsCapacityException? refused = null;
        for (ulong i = 0; i < 200 && refused is null; i++)
        {
            try
            {
                await store.PutAsync(Oid(500 + i), VolumeA, new StagingId(1), TestData.Bytes(500 + i, 64 << 10), Ct);
                stored.Add(Oid(500 + i));
            }
            catch (DfsCapacityException ex)
            {
                refused = ex;
            }
        }

        Assert.NotNull(refused);
        Assert.Equal(DfsErrorCode.CapacityExhausted, refused.ErrorCode);
        Assert.True(capacity - medium.UsedBytes >= reserve - (64 << 10),
            $"admission consumed the reserve: free {capacity - medium.UsedBytes} B < reserve {reserve} B (minus one object of slack).");
        foreach (ObjectId id in stored)
            Assert.True(await store.ContainsAsync(id, Ct), $"acknowledged object {id} lost while hitting the admission limit");

        // Metadata progress inside the reserve (64 KiB journal segments: the segment size must not exceed the reserve).
        await WithJournalAsync(medium, Wal, new JournalOptions(SegmentSize: 64 << 10), async j =>
        {
            await j.AppendAsync(TestData.Bytes(600, 4096), Ct);
            await j.FlushAsync(Ct);
        });
        await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
        await AtomicFile.WriteAsync(medium, CheckpointPath, TestData.Bytes(601, 32 << 10), Ct);
        AssertBytes(TestData.Bytes(601, 32 << 10), await AtomicFile.ReadAsync(medium, CheckpointPath, Ct) ?? [], "checkpoint written within the reserve");

        // Reclaim and recover admission: compaction itself must fit in what is left of the reserve (FSD §14 reserves
        // capacity for compaction), otherwise a store filled to its admission limit could never reclaim space.
        foreach (ObjectId id in stored.Take(stored.Count / 2))
            await store.DeleteAsync(id, Ct);
        await store.CompactAsync(Ct);
        await store.PutAsync(Oid(999), VolumeA, new StagingId(1), TestData.Bytes(999, 64 << 10), Ct);
        AssertBytes(TestData.Bytes(999, 64 << 10), await ReadObjectAsync(store, Oid(999), 64 << 10), "object admitted after reclamation");
        await store.DisposeAsync();
    }

    /// <summary>Node-level DSK-005: ENOSPC at the object write and pre-flush stages on every node.</summary>
    [Theory]
    [InlineData(FaultSites.ObjectWrite)]
    [InlineData(FaultSites.ObjectBeforeFlush)]
    [Catalog("DSK-005", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_005_EnospcAtEveryLocalStep_NodeStages(string site)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] old = TestData.Bytes(5001, 120_000);
            await volume.Files.WriteAllBytesAsync("/f", old, Ct);

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Count: int.MaxValue), FaultAction.DiskFull());
            DfsException failure = await AssertEx.ThrowsDfsAsync<DfsException>(() => volume.Files.WriteAllBytesAsync("/f", TestData.Bytes(5002, 120_000), Ct));
            cluster.Faults.AssertReached(arm);
            Assert.True(failure is DfsCapacityException or DfsPlacementUnavailableException,
                $"ENOSPC on every node at {site} must be a typed capacity/placement failure, got {failure.GetType().Name} ({failure.ErrorCode}).");
            AssertBytes(old, await volume.Files.ReadAllBytesAsync("/f", Ct), "old revision after ENOSPC");

            // Metadata-only work still progresses (reserve), and data work resumes when space returns.
            await volume.Directories.CreateDirectoryAsync("/meta-progress", Ct);
            arm.Disarm();
            byte[] next = TestData.Bytes(5003, 120_000);
            await volume.Files.WriteAllBytesAsync("/f", next, Ct);
            AssertBytes(next, await volume.Files.ReadAllBytesAsync("/f", Ct), "/f after space returned");
        }, Ct);
    }

    /// <summary>Node-level DSK-005: every node's disk is genuinely full except less than the reserve.</summary>
    [Fact]
    [Catalog("DSK-005", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_005_EnospcAtEveryLocalStep_NodeDisksFull()
    {
        await ClusterTest.RunAsync(QuicDisk(3) with { ReservedBytes = 8L << 20 }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] old = TestData.Bytes(5101, 200_000);
            await volume.Files.WriteAllBytesAsync("/kept", old, Ct);
            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.CapacityOverride = node.Disk.UsedBytes + (4L << 20); // less than the 8 MiB reserve
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Volume);

            DfsException refused = await AssertEx.ThrowsDfsAsync<DfsException>(() => volume.Files.WriteAllBytesAsync("/big", TestData.Bytes(5102, 2 << 20), Ct));
            Assert.True(refused is DfsCapacityException { ErrorCode: DfsErrorCode.CapacityExhausted } or DfsPlacementUnavailableException,
                $"admission must refuse payload work before the reserve: got {refused.GetType().Name} ({refused.ErrorCode})");
            Assert.False(await volume.Files.ExistsAsync("/big", Ct));

            // Admission refused before writing into the reserve: no node stored any payload of the refused write.
            IReadOnlySet<ObjectId> storedAfter = await cluster.Inspect.GetAllStoredObjectsAsync(Volume);
            Assert.True(storedAfter.IsSubsetOf(storedBefore),
                $"{storedAfter.Except(storedBefore).Count()} object(s) of the refused write were stored although every node's free space was below its reserve");
            AssertBytes(old, await volume.Files.ReadAllBytesAsync("/kept", Ct), "/kept while disks are full");
            await volume.Directories.CreateDirectoryAsync("/still-works", Ct);
            await volume.Files.DeleteAsync("/kept", Ct);

            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.CapacityOverride = null;
            byte[] big = TestData.Bytes(5103, 2 << 20);
            await volume.Files.WriteAllBytesAsync("/big", big, Ct);
            AssertBytes(big, await volume.Files.ReadAllBytesAsync("/big", Ct), "/big after space returned");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-006
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Metadata voters (n0..n2) have full disks (reserve included) while data nodes (n3..n5) stay free. Payload uploads and
    /// their durable receipts still succeed, but no metadata publication may: neither a whole-file write nor the flush
    /// of a stream whose data was already uploaded with valid receipts. Once space returns, every pending operation
    /// resolves to one definite outcome that matches the bytes exactly, and service resumes.
    /// </summary>
    [Fact]
    [Catalog("DSK-006", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_006_MetadataDiskFullSeparateFromData()
    {
        TestClusterOptions options = QuicDisk(6) with
        {
            ControlVoters = 3,
            Volumes =
            [
                new TestVolumeSpec(Volume)
                {
                    Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] },
                },
            ],
            ReservedBytes = 1L << 20,
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] oldF = TestData.Bytes(6001, 50_000);
            byte[] oldG = TestData.Bytes(6010, 50_000);
            await volume.Files.WriteAllBytesAsync("/f", oldF, Ct);
            await volume.Files.WriteAllBytesAsync("/g", oldG, Ct);
            CommitToken before = (await volume.GetInfoAsync(Ct)).LastCommit;
            DfsOperationId opId = await volume.NewOperationIdAsync(Ct);
            TestNode[] metadata = [cluster.Node("n0"), cluster.Node("n1"), cluster.Node("n2")];

            // A replacement of /g is staged first: its complete chunks are uploaded and receipted while there is space.
            long receiptsBefore = (await cluster.Inspect.GetTotalCountersAsync()).ReceiptsIssued;
            byte[] newG = TestData.Bytes(6011, 300_000);
            DfsStream staged = await volume.Files.OpenAsync("/g", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
            await staged.WriteAsync(newG, Ct);
            await WaitAsync(cluster, async () => (await cluster.Inspect.GetTotalCountersAsync()).ReceiptsIssued > receiptsBefore,
                TimeSpan.FromSeconds(10), "the staged chunks are uploaded and receipted before metadata storage fills");
            Assert.True(staged.PendingBytes > 0, "nothing is staged");

            foreach (TestNode node in metadata)
                node.Disk.Faults.CapacityOverride = node.Disk.UsedBytes; // completely full, reserve included
            long receiptsAtFull = (await cluster.Inspect.GetTotalCountersAsync()).ReceiptsIssued;

            // 1. A whole-file write cannot publish.
            Exception? failure = await Record.ExceptionAsync(() =>
                volume.Files.WriteAllBytesAsync("/f", TestData.Bytes(6002, 50_000), new DfsMutationOptions { OperationId = opId }, Ct));
            Assert.True(failure is DfsException, $"a commit whose metadata cannot be persisted must not succeed, got {failure?.GetType().Name ?? "success"}.");

            // 2. Receipted payload cannot bypass the metadata decision either.
            Exception? flushFailure = await Record.ExceptionAsync(() => staged.FlushAsync(Ct));
            Assert.True(flushFailure is DfsException, $"a flush whose metadata cannot be persisted must not succeed although its payload receipts are valid, got {flushFailure?.GetType().Name ?? "success"}.");
            long receiptsDuring = (await cluster.Inspect.GetTotalCountersAsync()).ReceiptsIssued - receiptsAtFull;
            TestContext.Current.TestOutputHelper?.WriteLine($"receipts issued while metadata storage was full: {receiptsDuring}");

            // Nothing became visible. Reads may fail explicitly (they need metadata authority) but never show new bytes.
            Exception? queryFailure = await Record.ExceptionAsync(async () =>
                Assert.NotEqual(DfsOutcomeStatus.Committed, (await volume.QueryOperationAsync(opId, resolve: false, Ct)).Status));
            Assert.True(queryFailure is null or DfsException, $"outcome query while metadata storage is full: {queryFailure}");
            foreach ((string path, byte[] expected) in new[] { ("/f", oldF), ("/g", oldG) })
            {
                Exception? readFailure = await Record.ExceptionAsync(async () => AssertBytes(expected, await volume.Files.ReadAllBytesAsync(path, Ct), $"{path} while metadata storage is full"));
                Assert.True(readFailure is null or DfsException, $"a read while metadata storage is full may only fail explicitly: {readFailure}");
            }

            foreach (TestNode node in metadata)
                node.Disk.Faults.CapacityOverride = null;
            await RestartStoppedNodesAsync(cluster); // a voter may stop itself after a fatal journal write error
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

            DfsOperationOutcome resolved = await volume.QueryOperationAsync(opId, resolve: true, Ct);
            Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve:true must be definite, got {resolved.Status}");
            AssertBytes(resolved.Status == DfsOutcomeStatus.Committed ? TestData.Bytes(6002, 50_000) : oldF, await volume.Files.ReadAllBytesAsync("/f", Ct), "/f matches the resolved outcome");

            try
            {
                await staged.DisposeAsync();
            }
            catch (IOException)
            {
                // A faulted stream reports its fault again on disposal.
            }

            byte[] g = await volume.Files.ReadAllBytesAsync("/g", Ct);
            Assert.True(g.SequenceEqual(oldG) || g.SequenceEqual(newG), $"/g is neither the old nor the complete new revision ({g.Length} bytes)");
            Assert.True((await volume.GetInfoAsync(Ct)).LastCommit.IsAtOrAfter(before));

            byte[] next = TestData.Bytes(6003, 50_000);
            await volume.Files.WriteAllBytesAsync("/f", next, Ct);
            AssertBytes(next, await volume.Files.ReadAllBytesAsync("/f", Ct), "/f after metadata space returned");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-007
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Read errors on every replica but one: reads fail over to the verified copy and return exact bytes; with every
    /// replica failing, the read fails explicitly (never zeros, never a short/EOF result).
    /// </summary>
    [Fact]
    [Catalog("DSK-007", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_007_ReadIoErrorFailsOver()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient writer = await cluster.ConnectAsync(ct: Ct);
            byte[] data = TestData.Bytes(7001, (5 * 64 * 1024) + 123);
            await writer.GetVolume(Volume).Files.WriteAllBytesAsync("/f", data, Ct);
            IReadOnlyList<ObjectId> objects = await FileObjectsAsync(cluster, "/f");
            Assert.True(objects.Count >= 2, "the file must span several objects so each extent picks its own replica order");

            var holderSets = new List<IReadOnlyList<string>>();
            foreach (ObjectId id in objects)
                holderSets.Add(await cluster.Inspect.GetObjectHoldersAsync(id));
            var common = holderSets.Select(h => (IEnumerable<string>)h).Aggregate((a, b) => a.Intersect(b, StringComparer.Ordinal)).ToList();
            Assert.NotEmpty(common);
            TestNode volumeLeader = await cluster.GetLeaderAsync(Volume, Ct);
            string survivor = common.Contains(volumeLeader.Name) ? volumeLeader.Name : common.Order(StringComparer.Ordinal).First();
            TestNode[] failing = cluster.Nodes.Where(n => n.Name != survivor).ToArray();
            foreach (TestNode node in failing)
                node.Disk.Faults.FailNextReads = int.MaxValue;

            for (int i = 0; i < 5; i++)
            {
                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = $"r{i}" }, Ct);
                AssertBytes(data, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), $"read {i} with read errors on {string.Join(",", failing.Select(n => n.Name))}");
                AssertBytes(data, await ReadViaStreamAsync(reader.GetVolume(Volume), "/f"), $"stream read {i}");
            }

            Assert.Contains(failing, n => n.Disk.Faults.FailNextReads < int.MaxValue);

            // No verified copy left: explicit failure.
            cluster.Node(survivor).Disk.Faults.FailNextReads = int.MaxValue;
            await using (DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r-none" }, Ct))
            {
                Exception? ex = await Record.ExceptionAsync(() => reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct));
                Assert.NotNull(ex);
                AssertExplicitReadFailure(ex, "every replica returns read errors");
            }

            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.FailNextReads = 0;
            await using DfsClient after = await cluster.ConnectAsync(new TestClientOptions { Name = "r-after" }, Ct);
            AssertBytes(data, await after.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), "read after devices recovered");
        }, Ct);
    }

    /// <summary>DSK-007 at the storage engine: a single store has no other copy, so a device read error is surfaced explicitly and is transient.</summary>
    [Theory]
    [InlineData(DskMediumKind.Simulated)]
    [InlineData(DskMediumKind.FileSystem)]
    [Catalog("DSK-007", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_007_ReadIoErrorFailsOver_LocalStoreSurfacesError(DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-007-{kind}", 7);
        byte[] data = TestData.Bytes(7101, 100_000);
        await WithStoreAsync(disk.Medium, SmallStoreOptions(), async s => await s.PutAsync(Oid(1), VolumeA, new StagingId(1), data, Ct));
        InterposedMedium medium = await disk.RestartAsync();
        await WithStoreAsync(medium, SmallStoreOptions(), async store =>
        {
            if (disk.SimulatedFaults is { } faults)
                faults.FailNextReads = 1;
            else
                medium.FailReads = 1;
            Exception? ex = await Record.ExceptionAsync(() => ReadObjectAsync(store, Oid(1), data.Length));
            bool reached = disk.SimulatedFaults is { } f ? f.FailNextReads == 0 : medium.ReadFailuresInjected == 1;
            Assert.True(reached, "the object read never touched the device (fault point not reached)");
            Assert.True(ex is IOException, $"a device read error must surface as an I/O failure, got {ex?.GetType().Name ?? "success"}");
            Assert.True(await store.ContainsAsync(Oid(1), Ct), "a transient read error must not drop the object from the index");
            AssertBytes(data, await ReadObjectAsync(store, Oid(1), data.Length), "read after the transient error");
        });
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-008
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DskMediumKind, int> BitFlipData()
    {
        var data = new TheoryData<DskMediumKind, int>();
        foreach (DskMediumKind kind in Enum.GetValues<DskMediumKind>())
        {
            foreach (int size in new[] { 1, 4096, 65_539, 1 << 20 })
                data.Add(kind, size);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(BitFlipData))]
    [Catalog("DSK-008", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_008_PayloadBitFlipDetected(DskMediumKind kind, int size)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-008-{kind}-{size}", 8);
        byte[] data = TestData.Bytes(8000 + (ulong)size, size);
        byte[] neighbour = TestData.Bytes(8999, 3000);
        IReadOnlyList<Boundary> window = [];
        await WithStoreAsync(disk.Medium, SmallStoreOptions(segmentSize: 4 << 20), async s =>
        {
            int mark = disk.Medium.BoundaryCount;
            await s.PutAsync(Oid(1), VolumeA, new StagingId(1), data, Ct);
            window = disk.Medium.Log.Skip(mark).ToList();
            await s.PutAsync(Oid(2), VolumeA, new StagingId(1), neighbour, Ct);
        });

        SortedDictionary<string, byte[]> pristine = await SnapshotFilesAsync(disk.Medium, "objects");
        (string Path, int Offset)? where = size >= 16 ? LocatePayload(pristine, data) : null;
        if (where is null)
        {
            Assert.True(size < 16, "the plaintext payload of an unencrypted store was not found on disk");

            // Too short to locate uniquely: within the bytes written by this object's put, flip every byte equal to the
            // payload byte. Each flip is detected or lands outside the object's protected bytes (then the object still
            // reads exactly); a misdelivered byte fails the check, and the real payload byte is among the candidates.
            (string Path, long Start, long End)? range = MainWriteRange(window);
            Assert.True(range is not null, "the put wrote no bytes");
            byte[] image = pristine[range!.Value.Path];
            int candidates = 0, detected = 0;
            for (long i = range.Value.Start; i < Math.Min(range.Value.End, image.Length); i++)
            {
                if (image[i] != data[0])
                    continue;
                detected += await CheckFlipAsync(disk, pristine, range.Value.Path, (int)i, data, neighbour, mustDetect: false) ? 1 : 0;
                candidates++;
            }

            Assert.True(candidates > 0, "the payload byte was not found among the bytes the put wrote");
            Assert.True(detected > 0, "flipping the one-byte payload was never detected");
            return;
        }

        var flipAt = new SortedSet<int> { 0, size / 2, size - 1 };
        if (size > 65_536)
        {
            flipAt.Add(65_535);
            flipAt.Add(65_536);
        }

        foreach (int relative in flipAt)
            await CheckFlipAsync(disk, pristine, where.Value.Path, where.Value.Offset + relative, data, neighbour, mustDetect: true, relative);
    }

    /// <summary>Flips one stored bit, reopens the store and checks the object is never delivered altered. Returns whether the damage was detected.</summary>
    private static async Task<bool> CheckFlipAsync(DskMedium disk, SortedDictionary<string, byte[]> pristine, string path, int offset, byte[] data, byte[] neighbour, bool mustDetect, int relative = -1)
    {
        bool detected = false;
        var images = new SortedDictionary<string, byte[]>(pristine, StringComparer.Ordinal);
        byte[] damaged = (byte[])pristine[path].Clone();
        damaged[offset] ^= 0x01;
        images[path] = damaged;
        await RestoreFilesAsync(disk.Medium, "objects", images);
        InterposedMedium medium = await disk.RestartAsync();
        string context = $"bit flip at {path}+{offset} (payload offset {relative})";
        await WithStoreAsync(medium, SmallStoreOptions(segmentSize: 4 << 20), async store =>
        {
            Exception? whole = await Record.ExceptionAsync(async () => AssertBytes(data, await ReadObjectAsync(store, Oid(1), data.Length), context));
            detected = whole is not null;
            if (mustDetect)
            {
                DfsIntegrityException integrity = Assert.IsType<DfsIntegrityException>(whole, exactMatch: false);
                Assert.Equal(DfsErrorCode.Integrity, integrity.ErrorCode);
                if (integrity.ObjectId is { } reported)
                    Assert.Equal(Oid(1), reported);

                // A range read covering only the flipped byte is verified too.
                await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => ReadObjectAsync(store, Oid(1), 1, relative), DfsErrorCode.Integrity);

                // Detection is durable bookkeeping, not a one-off read error: either the store already quarantined the
                // record while opening (verifying records it cannot vouch for), or a scrub reports it now.
                bool quarantinedAtOpen = !await store.ContainsAsync(Oid(1), Ct);
                ScrubResult scrub = await store.ScrubAsync(100, Ct);
                Assert.True(quarantinedAtOpen || scrub.Corrupt.Contains(Oid(1)), $"{context}: the corrupt object is neither quarantined nor reported by a scrub.");
                Assert.DoesNotContain(Oid(2), scrub.Corrupt);
            }
            else if (whole is not null)
            {
                Assert.IsType<DfsIntegrityException>(whole, exactMatch: false);
            }

            if (!mustDetect && whole is null && await store.ContainsAsync(Oid(2), Ct))
                AssertBytes(neighbour, await ReadObjectAsync(store, Oid(2), neighbour.Length), $"{context}: neighbouring object");
            else if (mustDetect)
                AssertBytes(neighbour, await ReadObjectAsync(store, Oid(2), neighbour.Length), $"{context}: neighbouring object");
        });
        await RestoreFilesAsync(disk.Medium, "objects", pristine);
        return detected;
    }

    /// <summary>Node-level DSK-008: two of three replicas have flipped payload bits; every read still returns exact bytes and scrubbing counts the failures.</summary>
    [Fact]
    [Catalog("DSK-008", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_008_PayloadBitFlipDetected_ReplicaFailover()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            byte[] data = TestData.Bytes(8101, 3 * 64 * 1024);
            await client.GetVolume(Volume).Files.WriteAllBytesAsync("/f", data, Ct);
            IReadOnlyList<ObjectId> objects = await FileObjectsAsync(cluster, "/f");
            long failuresBefore = (await cluster.Inspect.GetTotalCountersAsync()).ChecksumFailures;
            int corrupted = 0;
            foreach (ObjectId id in objects)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.True(holders.Count >= 3, $"object {id} has {holders.Count} holders before the test");
                foreach (string name in holders.Take(holders.Count - 1))
                {
                    await cluster.Node(name).Disk.CorruptObjectAsync(id, offset: 1000, xorMask: 0x10);
                    corrupted++;
                }
            }

            for (int i = 0; i < 4; i++)
            {
                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = $"r{i}" }, Ct);
                AssertBytes(data, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), $"read {i} with corrupted replicas");
            }

            await cluster.Admin.StartScrubAsync(ct: Ct);
            await WaitAsync(cluster, async () => (await cluster.Inspect.GetTotalCountersAsync()).ChecksumFailures - failuresBefore >= corrupted,
                TimeSpan.FromSeconds(30), "scrubbing detects every corrupted replica");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-009
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(DskMediumKind.Simulated, false)]
    [InlineData(DskMediumKind.Simulated, true)]
    [InlineData(DskMediumKind.FileSystem, false)]
    [InlineData(DskMediumKind.FileSystem, true)]
    [Catalog("DSK-009", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_009_IdentityMetadataCorruptionDetected(DskMediumKind kind, bool encrypted)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-009-{kind}-{encrypted}", 9);
        ObjectStoreOptions options = SmallStoreOptions(cipher: encrypted ? new TestAesGcmCipher() : null);
        (ObjectId Id, byte[] Data)[] objects = [(Oid(11), TestData.Bytes(911, 4096)), (Oid(12), TestData.Bytes(912, 4096)), (Oid(13), TestData.Bytes(913, 4096))];
        var windows = new List<IReadOnlyList<Boundary>>();
        var receipts = new Dictionary<ObjectId, ObjectRecordInfo>();
        await WithStoreAsync(disk.Medium, options, async s =>
        {
            foreach ((ObjectId id, byte[] data) in objects)
            {
                int mark = disk.Medium.BoundaryCount;
                ObjectRecordInfo info = await s.PutAsync(id, VolumeA, new StagingId(3), data, Ct);
                windows.Add(disk.Medium.Log.Skip(mark).ToList());
                Assert.Equal(id, info.Id);
                Assert.Equal(VolumeA, info.Volume);
                Assert.Equal(new StagingId(3), info.Staging);
                Assert.Equal(data.Length, info.Length);
                Assert.Equal(ObjectChecksum.Compute(data), info.Checksum);
                receipts[id] = info;
            }
        });

        SortedDictionary<string, byte[]> pristine = await SnapshotFilesAsync(disk.Medium, "objects");
        LocatedRecord a = Locate(pristine, windows[0], objects[0].Data, new Dictionary<string, byte[]>());
        if (!encrypted)
            Assert.True(a.PayloadStart >= 0, "the plaintext payload of an unencrypted store was not found on disk");
        long metadataEnd = a.PayloadStart >= 0 ? a.PayloadStart : a.End; // plaintext: header only; encrypted: whole record
        int outcomes = 0, detected = 0;
        for (long offset = a.Start; offset < metadataEnd; offset++)
        {
            var images = new SortedDictionary<string, byte[]>(pristine, StringComparer.Ordinal);
            byte[] damaged = (byte[])pristine[a.Path].Clone();
            damaged[offset] ^= 0x5A;
            images[a.Path] = damaged;
            await RestoreFilesAsync(disk.Medium, "objects", images);
            detected += await CheckIdentityAsync(await disk.RestartAsync(), options, objects, receipts, $"metadata byte {offset - a.Start} of object A") ? 1 : 0;
            outcomes++;
        }

        // Swap the identities of A and B where they are stored verbatim (payload bytes stay intact).
        if (!encrypted)
        {
            LocatedRecord b = Locate(pristine, windows[1], objects[1].Data, new Dictionary<string, byte[]>());
            foreach (bool littleEndian in new[] { true, false })
            {
                if (b.Path != a.Path || b.PayloadStart < 0)
                    break;
                byte[] idA = IdBytes(objects[0].Id, littleEndian), idB = IdBytes(objects[1].Id, littleEndian);
                List<int> hitsA = FindAll(pristine[a.Path].AsSpan((int)a.Start, (int)(a.PayloadStart - a.Start)), idA);
                List<int> hitsB = FindAll(pristine[b.Path].AsSpan((int)b.Start, (int)(b.PayloadStart - b.Start)), idB);
                if (hitsA.Count != 1 || hitsB.Count != 1)
                    continue;
                byte[] swapped = (byte[])pristine[a.Path].Clone();
                idB.CopyTo(swapped, a.Start + hitsA[0]);
                idA.CopyTo(swapped, b.Start + hitsB[0]);
                var images = new SortedDictionary<string, byte[]>(pristine, StringComparer.Ordinal) { [a.Path] = swapped };
                await RestoreFilesAsync(disk.Medium, "objects", images);
                Assert.True(await CheckIdentityAsync(await disk.RestartAsync(), options, objects, receipts, $"swapped object ids ({(littleEndian ? "LE" : "BE")})"),
                    "swapping two records' identities while keeping their payloads intact went undetected");
                outcomes++;
            }
        }

        await RestoreFilesAsync(disk.Medium, "objects", pristine);
        TestContext.Current.TestOutputHelper?.WriteLine($"DSK-009 {kind} encrypted={encrypted}: {outcomes} corruptions, {detected} detected, the rest byte-identical (unused bytes).");
        Assert.True(outcomes > 0);
        Assert.True(detected > 0, "no identity/length/encoding corruption was ever detected");
    }

    private static byte[] IdBytes(ObjectId id, bool littleEndian)
    {
        var bytes = new byte[16];
        if (littleEndian)
            System.Buffers.Binary.BinaryPrimitives.WriteUInt128LittleEndian(bytes, id.Value);
        else
            System.Buffers.Binary.BinaryPrimitives.WriteUInt128BigEndian(bytes, id.Value);
        return bytes;
    }

    /// <summary>
    /// Returns true when the damage was detected (open failed, object missing or read failed); fails on any misdirected
    /// read and on any index entry whose identity metadata (volume, staging session, length, checksum, encoding, key
    /// version) differs from what the put acknowledged: an undetected change there would bind the object to another
    /// volume or session (GC ownership, authorization) even when its payload bytes still read back intact.
    /// </summary>
    private static async Task<bool> CheckIdentityAsync(IStorageMedium medium, ObjectStoreOptions options, (ObjectId Id, byte[] Data)[] objects,
        IReadOnlyDictionary<ObjectId, ObjectRecordInfo> receipts, string context)
    {
        ObjectStore store;
        try
        {
            store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        }
        catch (DfsIntegrityException)
        {
            return true;
        }

        bool detected = false;
        try
        {
            foreach (ObjectRecordInfo info in store.List())
            {
                int known = Array.FindIndex(objects, o => o.Id == info.Id);
                Assert.True(known >= 0, $"{context}: the index lists a misdirected/phantom id {info.Id}.");
                Assert.True(info.Length == objects[known].Data.Length && info.Checksum == ObjectChecksum.Compute(objects[known].Data),
                    $"{context}: index entry {info.Id} is bound to another object's length/checksum.");
                Assert.True(info == receipts[info.Id],
                    $"{context}: index entry {info} differs from the acknowledged record {receipts[info.Id]} (undetected identity metadata corruption).");
            }

            foreach ((ObjectId id, byte[] data) in objects)
            {
                try
                {
                    if (!await store.ContainsAsync(id, Ct))
                    {
                        detected = true;
                        continue;
                    }

                    AssertBytes(data, await ReadObjectAsync(store, id, data.Length), $"{context}: object {id} returned another record's bytes");
                }
                catch (DfsIntegrityException)
                {
                    detected = true;
                }
            }
        }
        finally
        {
            await store.DisposeAsync();
        }

        return detected;
    }

    /// <summary>Node-level DSK-009: one replica's identity header is corrupted; reads use a verified copy and scrub stops counting it until repaired.</summary>
    [Fact]
    [Catalog("DSK-009", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_009_IdentityMetadataCorruptionDetected_ReplicaHeader()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            byte[] data = TestData.Bytes(9101, 100_000);
            await client.GetVolume(Volume).Files.WriteAllBytesAsync("/f", data, Ct);
            ObjectId victim = (await FileObjectsAsync(cluster, "/f"))[0];
            string badName = (await cluster.Inspect.GetObjectHoldersAsync(victim))[0];
            TestNode bad = cluster.Node(badName);
            long repairBefore = (await bad.GetCountersAsync()).RepairBytes;
            await bad.Disk.CorruptObjectHeaderAsync(victim);

            for (int i = 0; i < 3; i++)
            {
                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = $"r{i}" }, Ct);
                AssertBytes(data, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), $"read {i} with a corrupted identity header on {badName}");
            }

            await cluster.Admin.StartScrubAsync(badName, Ct);
            await WaitAsync(cluster, async () =>
                !(await cluster.Inspect.GetObjectHoldersAsync(victim)).Contains(badName) || (await bad.GetCountersAsync()).RepairBytes > repairBefore,
                TimeSpan.FromSeconds(30), "the replica with corrupted identity metadata is no longer counted as a verified holder (or was re-repaired)");
            await cluster.WaitForRepairAsync(Volume, TimeSpan.FromSeconds(60), Ct);
            Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(victim));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-010
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(ReplicaCorruption.Payload)]
    [InlineData(ReplicaCorruption.Header)]
    [InlineData(ReplicaCorruption.Missing)]
    [Catalog("DSK-010", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_010_AllReplicasCorruptIsNotSuccess(ReplicaCorruption corruption)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            byte[] data = TestData.Bytes(10_001, (4 * 64 * 1024) + 1);
            await client.GetVolume(Volume).Files.WriteAllBytesAsync("/f", data, Ct);
            FileInspection file = (await cluster.Inspect.GetFileAsync(Volume, "/f"))!;
            Assert.True(file.Extents.Count >= 3, "need several extents");
            ExtentInspection victim = file.Extents[1];
            ExtentInspection first = file.Extents[0];
            ExtentInspection last = file.Extents[^1];

            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(victim.Object);
            Assert.NotEmpty(holders);
            foreach (string name in holders)
            {
                TestDisk disk = cluster.Node(name).Disk;
                switch (corruption)
                {
                    case ReplicaCorruption.Payload:
                        await disk.CorruptObjectAsync(victim.Object, offset: victim.ObjectOffset + 3);
                        break;
                    case ReplicaCorruption.Header:
                        await disk.CorruptObjectHeaderAsync(victim.Object);
                        break;
                    case ReplicaCorruption.Missing:
                        await disk.DeleteObjectAsync(victim.Object);
                        break;
                }
            }

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r0" }, Ct);
            DfsVolume volume = reader.GetVolume(Volume);
            DfsIntegrityException whole = await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => volume.Files.ReadAllBytesAsync("/f", Ct), DfsErrorCode.Integrity);
            if (whole.ObjectId is { } reported)
                Assert.Equal(victim.Object, reported);

            await using (DfsStream stream = await volume.Files.OpenReadAsync("/f", Ct))
            {
                // Bytes outside the damaged extent remain exactly readable.
                byte[] head = new byte[(int)Math.Min(1000, first.Length)];
                stream.Seek(first.Offset, SeekOrigin.Begin);
                await stream.ReadExactlyAsync(head, Ct);
                AssertEx.BytesEqual(data.AsSpan((int)first.Offset, head.Length), head, "range in an intact extent");
                byte[] tail = new byte[(int)Math.Min(500, last.Length)];
                stream.Seek(last.Offset, SeekOrigin.Begin);
                await stream.ReadExactlyAsync(tail, Ct);
                AssertEx.BytesEqual(data.AsSpan((int)last.Offset, tail.Length), tail, "range in the last intact extent");

                // The damaged range fails explicitly: no zeros, no short read, no EOF.
                stream.Seek(victim.Offset + 5, SeekOrigin.Begin);
                var buffer = new byte[100];
                await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(async () =>
                {
                    int n = await stream.ReadAsync(buffer, Ct);
                    Assert.Fail($"a read inside a corrupt extent returned {n} bytes instead of failing");
                }, DfsErrorCode.Integrity);

                // (Caller-buffer contents after an exception are unspecified by FSD §6.2, so they are not inspected.)
                Assert.Equal(victim.Offset + 5, stream.Position);
            }

            DfsFileInfo info = await volume.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(data.Length, info.Length);
            await cluster.Admin.StartScrubAsync(ct: Ct);
            await WaitAsync(cluster, async () =>
            {
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                return health.MissingObjects >= 1;
            }, TimeSpan.FromSeconds(30), "the lost extent is reported as missing (not healthy)");
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            Assert.True((await cluster.Inspect.GetReplicaHealthAsync(Volume)).MissingObjects >= 1, "with no verified source, the loss must stay reported (no invented repair)");
            await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => volume.Files.ReadAllBytesAsync("/f", Ct), DfsErrorCode.Integrity);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-011
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DskMediumKind, CrashMode> CrashCasesData() => CrashCaseData();

    [Theory]
    [MemberData(nameof(CrashCasesData))]
    [Catalog("DSK-011", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_011_InterruptedCompactionPreservesReferences(DskMediumKind kind, CrashMode mode)
    {
        // Compaction reclaims space when it completes.
        await using (DskMedium scratch = DskMedium.Create(kind, $"DSK-011-space-{kind}", 11))
        {
            var model = new ObjectStoreModel();
            await ObjectStoreSetupAsync(scratch.Medium, model);
            await WithStoreAsync(scratch.Medium, CompactionStoreOptions, async s =>
            {
                long before = s.TotalBytes;
                await s.CompactAsync(Ct);
                Assert.True(s.TotalBytes < before, $"compaction did not reclaim dead records ({before} -> {s.TotalBytes} bytes)");
                Assert.True(s.LiveBytes <= s.TotalBytes);
                await model.AssertAllowedAsync(s, "after a clean compaction");
            });
        }

        CrashMatrixReport report = await RunCrashMatrixAsync<ObjectStoreModel>(
            $"DSK-011-{kind}-{mode}",
            kind,
            mode,
            11,
            () => new ObjectStoreModel(),
            setup: ObjectStoreSetupAsync,
            scenario: (medium, model) => CompactionScenarioAsync(medium, model),
            verify: (medium, model, context) => VerifyObjectStoreRecoveryAsync(medium, model, context, CompactionStoreOptions));
        Assert.Contains(nameof(BoundaryKind.Write), report.BoundaryKinds);
        Assert.Contains(nameof(BoundaryKind.Delete), report.BoundaryKinds);
    }

    public static TheoryData<string, DskMediumKind, CrashMode> CompactionSiteData()
    {
        var data = new TheoryData<string, DskMediumKind, CrashMode>();
        foreach (string site in new[] { FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete })
        {
            foreach ((DskMediumKind kind, CrashMode mode) in CrashCases())
                data.Add(site, kind, mode);
        }

        return data;
    }

    /// <summary>
    /// DSK-011 by named stage: power is lost while copying records, publishing the new layout and deleting old segments,
    /// at every occurrence of the stage in the pass (counted by an uninterrupted dry run, so the matrix follows the
    /// store's real stage graph instead of assuming one).
    /// </summary>
    [Theory]
    [MemberData(nameof(CompactionSiteData))]
    [Catalog("DSK-011", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_011_InterruptedCompactionPreservesReferences_Stages(string site, DskMediumKind kind, CrashMode mode)
    {
        int occurrences;
        await using (DskMedium dry = DskMedium.Create(kind, $"DSK-011s-dry-{site}-{kind}-{mode}", 12))
        {
            var model = new ObjectStoreModel();
            await ObjectStoreSetupAsync(dry.Medium, model);
            var recorder = new RecordingFaultInjector();
            await CompactionScenarioAsync(dry.Medium, model, recorder);
            occurrences = recorder.Count(site);
            TestContext.Current.TestOutputHelper?.WriteLine($"{site}: {occurrences} occurrence(s) in one compaction pass; hits: {string.Join(", ", recorder.Hits.GroupBy(h => h.Site).Select(g => $"{g.Key}={g.Count()}"))}");
        }

        Assert.True(occurrences > 0, $"one compaction pass over a store with dead records never reached {site} (stage not instrumented).");
        for (int occurrence = 1; occurrence <= Math.Min(occurrences, 16); occurrence++)
        {
            await using DskMedium disk = DskMedium.Create(kind, $"DSK-011s-{site}-{kind}-{mode}-{occurrence}", 12 + (ulong)occurrence);
            var model = new ObjectStoreModel();
            await ObjectStoreSetupAsync(disk.Medium, model);
            var faults = new RecordingFaultInjector { CrashSite = site, CrashOccurrence = occurrence };
            try
            {
                await CompactionScenarioAsync(disk.Medium, model, faults);
            }
            catch (Exception) when (faults.Triggered)
            {
                // Power lost at the requested stage.
            }

            Assert.True(faults.Triggered, $"compaction never reached {site} occurrence {occurrence} (hits: {faults.Count(site)}).");
            InterposedMedium restarted = await disk.PowerLossAsync(mode);
            await VerifyObjectStoreRecoveryAsync(restarted, model, $"compaction crash at {site}#{occurrence} ({kind}/{mode})", CompactionStoreOptions);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-012
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(CrashCasesData))]
    [Catalog("DSK-012", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_012_CheckpointAtomicInstallation(DskMediumKind kind, CrashMode mode)
    {
        CrashMatrixReport replace = await RunCrashMatrixAsync<AtomicModel>(
            $"DSK-012-replace-{kind}-{mode}",
            kind,
            mode,
            12,
            () => new AtomicModel(),
            setup: AtomicSetupAsync,
            scenario: AtomicScenarioAsync,
            verify: (medium, model, context) => VerifyAtomicRecoveryAsync(medium, model, context));
        Assert.Contains(nameof(BoundaryKind.FlushFile), replace.BoundaryKinds);
        Assert.Contains(nameof(BoundaryKind.Rename), replace.BoundaryKinds);
        Assert.Contains(nameof(BoundaryKind.FlushDirectory), replace.BoundaryKinds);

        const string fresh = "fresh/deep/dir/pointer";
        await RunCrashMatrixAsync<AtomicModel>(
            $"DSK-012-first-{kind}-{mode}",
            kind,
            mode,
            13,
            () =>
            {
                var model = new AtomicModel();
                model.Allowed.Add(null);
                return model;
            },
            setup: null,
            scenario: async (medium, model) =>
            {
                // A first install into directories that do not exist yet: the directory chain is created durably
                // (the crash points include every mkdir and directory flush), then the file is installed.
                byte[] v = TestData.Bytes(1201, 777);
                await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(fresh), Ct);
                model.Begin(v);
                await AtomicFile.WriteAsync(medium, fresh, v, Ct);
                model.Completed(v);
            },
            verify: (medium, model, context) => VerifyAtomicRecoveryAsync(medium, model, context, fresh));
    }

    public static TheoryData<string, CrashMode> CheckpointInstallData()
    {
        var data = new TheoryData<string, CrashMode>();
        foreach (string site in new[] { FaultSites.RaftSnapshotInstall, FaultSites.RecoveryCheckpointInstall })
        {
            foreach (CrashMode mode in new[] { CrashMode.LoseUnflushed, CrashMode.KeepUnflushed, CrashMode.TearLastWrite, CrashMode.RandomSubset })
                data.Add(site, mode);
        }

        return data;
    }

    /// <summary>
    /// Node-level DSK-012: a lagging follower crashes while installing a metadata checkpoint (from the leader, or its
    /// own during restart). It must never select a partial checkpoint: after restart it converges to the exact
    /// replicated state and stays converged across another restart.
    /// </summary>
    [Theory]
    [MemberData(nameof(CheckpointInstallData))]
    [Catalog("DSK-012", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_012_CheckpointAtomicInstallation_NodeCheckpointInstall(string site, CrashMode mode)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] data = TestData.Bytes(1210, 50_000);
            await volume.Files.WriteAllBytesAsync("/base", data, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode lagger = cluster.Nodes.First(n => n.Name != leader.Name);

            // Far more entries than the snapshot threshold, so the leader's log is compacted past the lagger's position
            // (even if it retains a trailing window) and the lagger must install a checkpoint.
            await lagger.StopAsync();
            int ops = (3 * cluster.Options.Timings.RaftSnapshotThreshold) + 60;
            for (int i = 0; i < ops; i++)
                await volume.Directories.CreateDirectoryAsync($"/d/{i}", Ct);

            FaultArm arm;
            if (site == FaultSites.RaftSnapshotInstall)
            {
                arm = cluster.Faults.Arm(new FaultMatch(site, Node: lagger.Name), FaultAction.CrashNode(mode));
                await lagger.RestartAsync();
            }
            else
            {
                await lagger.RestartAsync();
                await cluster.WaitForReplicationAsync(Volume, Ct);
                await lagger.StopAsync();
                arm = cluster.Faults.Arm(new FaultMatch(site, Node: lagger.Name), FaultAction.CrashNode(mode));
                try
                {
                    await lagger.RestartAsync();
                }
                catch (Exception)
                {
                    // The node crashed during its own startup.
                }
            }

            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);
            await WaitAsync(cluster, () => Task.FromResult(lagger.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10), "the fault crashes the installing node");
            arm.Disarm();

            await lagger.RestartAsync();
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            VolumeStateInspection leaderState = await cluster.Inspect.GetVolumeStateAsync(Volume);
            VolumeStateInspection laggerState = await cluster.Inspect.GetVolumeStateAsync(Volume, lagger.Name);
            Assert.Equal(leaderState.InodeCount, laggerState.InodeCount);

            await lagger.StopAsync();
            await lagger.RestartAsync();
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);

            // The recovered follower can serve as leader with the exact state.
            await leader.StopAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            AssertBytes(data, await volume.Files.ReadAllBytesAsync("/base", Ct), "/base served after checkpoint recovery");
            Assert.True(await volume.Directories.ExistsAsync($"/d/{ops - 1}", Ct));
            await leader.RestartAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-013
    // ------------------------------------------------------------------------------------------------------------

    private sealed class OutcomeTally
    {
        public int Persisted { get; set; }

        public int Disappeared { get; set; }
    }

    [Theory]
    [InlineData(DskComponent.Journal)]
    [InlineData(DskComponent.ObjectStore)]
    [InlineData(DskComponent.AtomicFile)]
    [Catalog("DSK-013", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_013_UnflushedWritesMayPersistOrDisappear(DskComponent component)
    {
        var tallies = new Dictionary<string, OutcomeTally>(StringComparer.Ordinal);
        var cases = new List<(DskMediumKind Kind, CrashMode Mode, int Seed)>();
        foreach ((DskMediumKind kind, CrashMode mode) in CrashCases())
        {
            int seeds = mode is CrashMode.RandomSubset or CrashMode.TearLastWrite ? 4 : 1;
            for (int seed = 1; seed <= seeds; seed++)
                cases.Add((kind, mode, seed));
        }

        foreach ((DskMediumKind kind, CrashMode mode, int seed) in cases)
        {
            string key = $"{kind}/{mode}";
            if (!tallies.TryGetValue(key, out OutcomeTally? tally))
                tallies[key] = tally = new OutcomeTally();
            string name = $"DSK-013-{component}-{kind}-{mode}-{seed}";
            CrashMatrixReport report = component switch
            {
                DskComponent.Journal => await RunCrashMatrixAsync<JournalModel>(name, kind, mode, (ulong)seed, () => new JournalModel(), null,
                    (m, model) => RunJournalScenarioAsync(m, model, JournalScenario.BatchedFlushes),
                    async (m, model, context) =>
                    {
                        await WithJournalAsync(m, Wal, new JournalOptions(), j =>
                        {
                            if (model.Records.Count > model.AckedCount)
                            {
                                if (j.Records.Count > model.AckedCount)
                                    tally.Persisted++;
                                else
                                    tally.Disappeared++;
                            }

                            return Task.CompletedTask;
                        });
                        await VerifyJournalRecoveryAsync(m, model, context, new JournalOptions());
                    }),
                DskComponent.ObjectStore => await RunCrashMatrixAsync<ObjectStoreModel>(name, kind, mode, (ulong)seed, () => new ObjectStoreModel(), null,
                    async (m, model) =>
                    {
                        ObjectStore s = await ObjectStore.OpenAsync(m, "objects", CompactionStoreOptions, NoFaults.Instance, Ct);
                        for (int i = 0; i < 5; i++)
                        {
                            byte[] d = TestData.Bytes(1300 + (ulong)i, 1500 + (i * 1500));
                            ObjectStoreModel.Entry e = model.BeginPut(Oid(1300 + (ulong)i), VolumeA, new StagingId(4), d);
                            model.PutAcked(e, await s.PutAsync(e.Id, e.Volume, e.Staging, d, Ct), "put");
                        }

                        model.BeginDelete(Oid(1301));
                        await s.DeleteAsync(Oid(1301), Ct);
                        model.Deleted(Oid(1301));
                    },
                    async (m, model, context) =>
                    {
                        await WithStoreAsync(m, CompactionStoreOptions, async s =>
                        {
                            foreach (ObjectStoreModel.Entry e in model.Entries.Where(e => e.State is ObjectStoreModel.ObjectState.PutPending))
                            {
                                if (await s.ContainsAsync(e.Id, Ct))
                                    tally.Persisted++;
                                else
                                    tally.Disappeared++;
                            }
                        });
                        await VerifyObjectStoreRecoveryAsync(m, model, context, CompactionStoreOptions);
                    }),
                _ => await RunCrashMatrixAsync<AtomicModel>(name, kind, mode, (ulong)seed, () => new AtomicModel(), AtomicSetupAsync, AtomicScenarioAsync,
                    async (m, model, context) =>
                    {
                        if (model.Allowed.Count > 1)
                        {
                            byte[]? actual = await AtomicFile.ReadAsync(m, CheckpointPath, Ct);
                            if (actual is not null && model.Allowed[^1] is { } newest && newest.AsSpan().SequenceEqual(actual))
                                tally.Persisted++;
                            else
                                tally.Disappeared++;
                        }

                        await VerifyAtomicRecoveryAsync(m, model, context);
                    }),
            };
            Assert.True(report.PointsWithUnflushedData > 0, $"{name}: no crash point had unflushed data outstanding (both storage outcomes unexplored).");
        }

        foreach ((string key, OutcomeTally tally) in tallies)
            TestContext.Current.TestOutputHelper?.WriteLine($"DSK-013 {component} {key}: unacknowledged work persisted {tally.Persisted}x, disappeared {tally.Disappeared}x");
        // Both storage outcomes were explored: every case above crashed with unflushed bytes outstanding, and the modes
        // include the device dropping them (LoseUnflushed), keeping them (KeepUnflushed, the real-directory process
        // kill) and keeping an arbitrary subset or a torn last write. Whether the component then exposes surviving
        // unacknowledged work is its own legal choice (a journal may ignore an unflushed tail), so only the inherent
        // outcome is required here: work the device dropped is gone. Each recovered state was checked against the model.
        Assert.True(tallies[$"{DskMediumKind.Simulated}/{CrashMode.LoseUnflushed}"].Disappeared > 0, "LoseUnflushed never removed unacknowledged work");
        Assert.True(tallies.Values.Sum(t => t.Persisted) + tallies.Values.Sum(t => t.Disappeared) > 0, "no crash interrupted unacknowledged work");
    }

    /// <summary>
    /// Node-level DSK-013: whole-cluster power loss with commits in flight. Each in-flight operation resolves to a
    /// definite outcome that matches the bytes exactly, whichever way the unflushed state fell, and service continues.
    /// </summary>
    [Theory]
    [InlineData(CrashMode.LoseUnflushed, 1)]
    [InlineData(CrashMode.KeepUnflushed, 1)]
    [InlineData(CrashMode.RandomSubset, 1)]
    [InlineData(CrashMode.RandomSubset, 2)]
    [InlineData(CrashMode.TearLastWrite, 1)]
    [Catalog("DSK-013", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_013_UnflushedWritesMayPersistOrDisappear_InFlightResolution(CrashMode mode, int seed)
    {
        await ClusterTest.RunAsync(QuicDisk(3) with { Seed = (ulong)(1300 + seed) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            var old = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 6; i++)
            {
                old[$"/f{i}"] = TestData.Bytes(1310 + (ulong)i, 40_000);
                await volume.Files.WriteAllBytesAsync($"/f{i}", old[$"/f{i}"], Ct);
            }

            var ops = new List<(string Path, DfsOperationId Id, byte[] Data, Task Task)>();
            for (int i = 0; i < 6; i++)
            {
                DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                byte[] next = TestData.Bytes(1320 + (ulong)i, 40_000);
                ops.Add(($"/f{i}", id, next, volume.Files.WriteAllBytesAsync($"/f{i}", next, new DfsMutationOptions { OperationId = id }, Ct)));
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(5 * seed), Ct);
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(mode);
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            var acknowledged = new HashSet<string>(StringComparer.Ordinal);
            foreach ((string path, _, _, Task task) in ops)
            {
                try
                {
                    await task;
                    acknowledged.Add(path);
                }
                catch (Exception)
                {
                    // Unknown/unavailable: resolved below.
                }
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(45), Ct);

            int committed = 0, aborted = 0;
            foreach ((string path, DfsOperationId id, byte[] next, _) in ops)
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{path}: outcome {outcome.Status} is not definite after resolve");
                if (acknowledged.Contains(path))
                    Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                byte[] actual = await volume.Files.ReadAllBytesAsync(path, Ct);
                AssertBytes(outcome.Status == DfsOutcomeStatus.Committed ? next : old[path], actual, $"{path} after {mode} with outcome {outcome.Status}");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    committed++;
                else
                    aborted++;
            }

            TestContext.Current.TestOutputHelper?.WriteLine($"DSK-013 {mode}/{seed}: {committed} in-flight commits survived, {aborted} were aborted.");
            byte[] after = TestData.Bytes(1330, 40_000);
            await volume.Files.WriteAllBytesAsync("/after", after, Ct);
            AssertBytes(after, await volume.Files.ReadAllBytesAsync("/after", Ct), "service continues after the power loss");
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-014
    // ------------------------------------------------------------------------------------------------------------

    private const string DeepDir = "deep/a/b/c";

    /// <summary>One acknowledged storage step.</summary>
    private sealed record AckStep(string Name, Func<IStorageMedium, Task> Run);

    private static IReadOnlyList<AckStep> DirectoryDurabilitySteps(DskComponent component)
    {
        var steps = new List<AckStep>();
        switch (component)
        {
            case DskComponent.Journal:
                for (int n = 0; n < 8; n++)
                {
                    int i = n;
                    steps.Add(new AckStep($"append+flush r{i}", async m => await WithJournalAsync(m, DeepDir + "/wal", new JournalOptions(SegmentSize: 512), async j =>
                    {
                        await j.AppendAsync(TestData.Bytes(1400 + (ulong)i, 150 + (i * 20)), Ct);
                        await j.FlushAsync(Ct);
                    })));
                }

                break;
            case DskComponent.ObjectStore:
                for (int n = 0; n < 6; n++)
                {
                    int i = n;
                    steps.Add(new AckStep($"put o{i}", async m => await WithStoreAsync(m, SmallStoreOptions(segmentSize: 8 << 10), async s =>
                        await s.PutAsync(Oid(1400 + (ulong)i), VolumeA, new StagingId(1), TestData.Bytes(1400 + (ulong)i, 3000), Ct))));
                }

                steps.Add(new AckStep("delete o1", async m => await WithStoreAsync(m, SmallStoreOptions(segmentSize: 8 << 10), async s => await s.DeleteAsync(Oid(1401), Ct))));
                steps.Add(new AckStep("compact", async m => await WithStoreAsync(m, SmallStoreOptions(segmentSize: 8 << 10), async s => await s.CompactAsync(Ct))));
                break;
            default:
                steps.Add(new AckStep("create directories + install v1", async m =>
                {
                    await m.CreateDirectoryDurableAsync(DeepDir, Ct);
                    await AtomicFile.WriteAsync(m, DeepDir + "/ckpt", TestData.Bytes(1450, 900), Ct);
                }));
                steps.Add(new AckStep("install v2", async m => await AtomicFile.WriteAsync(m, DeepDir + "/ckpt", TestData.Bytes(1451, 2900), Ct)));
                break;
        }

        return steps;
    }

    private static async Task VerifyDirectoryDurabilityAsync(DskComponent component, IStorageMedium medium, int acked, string context)
    {
        switch (component)
        {
            case DskComponent.Journal:
                await WithJournalAsync(medium, DeepDir + "/wal", new JournalOptions(SegmentSize: 512), j =>
                {
                    Assert.True(j.Records.Count >= acked, $"{context}: {acked} acknowledged records, {j.Records.Count} recovered (directory entries of new segments were not made durable).");
                    for (int i = 0; i < acked; i++)
                        AssertEx.BytesEqual(TestData.Bytes(1400 + (ulong)i, 150 + (i * 20)), j.Records[i].Payload.Span, $"{context}: record {i}");
                    return Task.CompletedTask;
                });
                break;
            case DskComponent.ObjectStore:
                await WithStoreAsync(medium, SmallStoreOptions(segmentSize: 8 << 10), async s =>
                {
                    int puts = Math.Min(acked, 6);
                    for (int i = 0; i < puts; i++)
                    {
                        bool deleted = i == 1 && acked >= 7;
                        Assert.True(await s.ContainsAsync(Oid(1400 + (ulong)i), Ct) != deleted, $"{context}: object o{i} {(deleted ? "resurrected" : "lost")}");
                        if (!deleted)
                            AssertBytes(TestData.Bytes(1400 + (ulong)i, 3000), await ReadObjectAsync(s, Oid(1400 + (ulong)i), 3000), $"{context}: o{i}");
                    }
                });
                break;
            default:
                byte[] expected = acked == 1 ? TestData.Bytes(1450, 900) : TestData.Bytes(1451, 2900);
                AssertBytes(expected, await AtomicFile.ReadAsync(medium, DeepDir + "/ckpt", Ct), $"{context}: installed version");
                break;
        }
    }

    [Theory]
    [MemberData(nameof(ComponentKindData))]
    [Catalog("DSK-014", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_014_DirectoryEntryDurability(DskComponent component, DskMediumKind kind)
    {
        IReadOnlyList<AckStep> steps = DirectoryDurabilitySteps(component);
        for (int acked = 1; acked <= steps.Count; acked++)
        {
            await using DskMedium disk = DskMedium.Create(kind, $"DSK-014-{component}-{kind}-{acked}", 14 + (ulong)acked);
            for (int i = 0; i < acked; i++)
                await steps[i].Run(disk.Medium);

            // Power loss that discards every directory entry, rename and creation not explicitly persisted.
            IStorageMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
            await VerifyDirectoryDurabilityAsync(component, restarted, acked, $"{component} after '{steps[acked - 1].Name}' ({acked}/{steps.Count}) + {AckLossMode(kind)}");
        }
    }

    [Theory]
    [InlineData("graceful")]
    [InlineData("process-crash")]
    [InlineData("power-loss")]
    [Catalog("DSK-014", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_014_DirectoryEntryDurability_NodeRestart(string how)
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int d = 0; d < 3; d++)
                await volume.Directories.CreateDirectoryAsync($"/dir{d}/sub", Ct);
            await volume.Directories.CreateDirectoryAsync("/moved", Ct);
            for (int i = 0; i < 8; i++)
            {
                string path = $"/dir{i % 3}/sub/file{i}";
                files[path] = TestData.Bytes(1460 + (ulong)i, i % 2 == 0 ? 2000 : 90_000);
                await volume.Files.WriteAllBytesAsync(path, files[path], Ct);
            }

            await volume.Files.MoveAsync("/dir0/sub/file0", "/moved/file0", ct: Ct);
            files["/moved/file0"] = files["/dir0/sub/file0"];
            files.Remove("/dir0/sub/file0");
            await cluster.WaitForReplicationAsync(Volume, Ct);

            var identities = cluster.Nodes.ToDictionary(n => n.Name, n => (n.Id, n.Incarnation));
            var listings = new Dictionary<string, List<ObjectId>>(StringComparer.Ordinal);
            foreach (TestNode node in cluster.Nodes)
                listings[node.Name] = [.. (await node.Inspect.ListObjectsAsync(Volume)).Select(o => o.Id).OrderBy(o => o.Value)];

            foreach (TestNode node in cluster.Nodes)
            {
                switch (how)
                {
                    case "graceful":
                        await node.StopAsync();
                        break;
                    case "process-crash":
                        await node.CrashAsync(CrashMode.KeepUnflushed);
                        break;
                    default:
                        await node.CrashAsync(CrashMode.LoseUnflushed);
                        break;
                }
            }

            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(45), Ct);

            IReadOnlySet<ObjectId> reachable = (await cluster.Inspect.GetVolumeStateAsync(Volume)).ReachableObjects();
            foreach (TestNode node in cluster.Nodes)
            {
                Assert.Equal(identities[node.Name].Id, node.Id);
                Assert.True(node.Incarnation > identities[node.Name].Incarnation, $"{node.Name}: incarnation did not advance durably across the restart");
                var after = (await node.Inspect.ListObjectsAsync(Volume)).Select(o => o.Id).ToHashSet();
                foreach (ObjectId id in listings[node.Name].Where(reachable.Contains))
                    Assert.True(after.Contains(id), $"{node.Name}: acknowledged object {id} vanished across a {how} restart");
            }

            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            foreach ((string path, byte[] data) in files)
                AssertBytes(data, await fresh.GetVolume(Volume).Files.ReadAllBytesAsync(path, Ct), $"{path} after {how} restart of every node");
            Assert.False(await fresh.GetVolume(Volume).Files.ExistsAsync("/dir0/sub/file0", Ct), "a renamed entry resurrected at its old name");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-015
    // ------------------------------------------------------------------------------------------------------------

    private static void SetReadOnly(DskMedium disk, bool readOnly)
    {
        if (disk.SimulatedFaults is { } faults)
            faults.ReadOnly = readOnly;
        else
            disk.Medium.Inject = readOnly ? b => StorageErrors.ReadOnlyDevice(b.Path) : null;
    }

    [Theory]
    [MemberData(nameof(ComponentKindData))]
    [Catalog("DSK-015", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_015_ReadOnlyDeviceTransition(DskComponent component, DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-015-{component}-{kind}", 15);
        InterposedMedium medium = disk.Medium;
        switch (component)
        {
            case DskComponent.Journal:
            {
                byte[] r0 = TestData.Bytes(1501, 300);
                await WithJournalAsync(medium, Wal, new JournalOptions(), async j =>
                {
                    await j.AppendAsync(r0, Ct);
                    await j.FlushAsync(Ct);
                });

                // Journal.Records are the records replayed at open, so r0 is reopened from disk before the transition.
                Journal journal = await Journal.OpenAsync(medium, Wal, new JournalOptions(), Ct);
                SetReadOnly(disk, true);
                Exception? failure = await Record.ExceptionAsync(async () =>
                {
                    await journal.AppendAsync(TestData.Bytes(1502, 300), Ct);
                    await journal.FlushAsync(Ct);
                });
                Assert.True(failure is IOException, $"durability work on a read-only device must fail, got {failure?.GetType().Name ?? "success"}");
                Assert.True(failure is DfsException || StorageErrors.IsReadOnly(failure), $"the failure must be a typed storage failure or identify the read-only device: {failure!.Message}");
                Assert.Single(journal.Records);
                AssertEx.BytesEqual(r0, journal.Records[0].Payload.Span, "replayed records stay readable");
                await Record.ExceptionAsync(async () => await journal.DisposeAsync());
                SetReadOnly(disk, false);
                IStorageMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
                await WithJournalAsync(restarted, Wal, new JournalOptions(), async j =>
                {
                    Assert.True(j.Records.Count is 1 or 2);
                    AssertEx.BytesEqual(r0, j.Records[0].Payload.Span, "acknowledged record");
                    await j.AppendAsync(TestData.Bytes(1503, 10), Ct);
                    await j.FlushAsync(Ct);
                });
                break;
            }

            case DskComponent.ObjectStore:
            {
                ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", SmallStoreOptions(), NoFaults.Instance, Ct);
                byte[] kept = TestData.Bytes(1511, 70_000);
                await store.PutAsync(Oid(1511), VolumeA, new StagingId(1), kept, Ct);
                SetReadOnly(disk, true);
                Exception? failure = await Record.ExceptionAsync(() => store.PutAsync(Oid(1512), VolumeA, new StagingId(1), TestData.Bytes(1512, 9000), Ct).AsTask());
                Assert.True(failure is IOException, $"a put on a read-only device must fail, got {failure?.GetType().Name ?? "success"}");
                Exception? deleteFailure = await Record.ExceptionAsync(() => store.DeleteAsync(Oid(1511), Ct).AsTask());
                Assert.True(deleteFailure is IOException, "a delete on a read-only device must fail rather than pretend");
                AssertBytes(kept, await ReadObjectAsync(store, Oid(1511), kept.Length), "verified reads continue on a read-only device");
                Assert.True(await store.ContainsAsync(Oid(1511), Ct));
                await Record.ExceptionAsync(async () => await store.DisposeAsync());
                SetReadOnly(disk, false);
                IStorageMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
                await WithStoreAsync(restarted, SmallStoreOptions(), async s =>
                {
                    AssertBytes(kept, await ReadObjectAsync(s, Oid(1511), kept.Length), "acknowledged object after the transition");
                    if (await s.ContainsAsync(Oid(1512), Ct))
                        AssertBytes(TestData.Bytes(1512, 9000), await ReadObjectAsync(s, Oid(1512), 9000), "a refused put must be absent or complete");
                    await s.PutAsync(Oid(1513), VolumeA, new StagingId(1), TestData.Bytes(1513, 100), Ct);
                });
                break;
            }

            default:
            {
                byte[] v1 = TestData.Bytes(1521, 800);
                await medium.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
                await AtomicFile.WriteAsync(medium, CheckpointPath, v1, Ct);
                SetReadOnly(disk, true);
                Exception? failure = await Record.ExceptionAsync(() => AtomicFile.WriteAsync(medium, CheckpointPath, TestData.Bytes(1522, 800), Ct).AsTask());
                Assert.True(failure is IOException, $"an install on a read-only device must fail, got {failure?.GetType().Name ?? "success"}");
                AssertBytes(v1, await AtomicFile.ReadAsync(medium, CheckpointPath, Ct), "reads continue on a read-only device");
                SetReadOnly(disk, false);
                IStorageMedium restarted = await disk.PowerLossAsync(AckLossMode(kind));
                AssertBytes(v1, await AtomicFile.ReadAsync(restarted, CheckpointPath, Ct), "the old version survives the refused install");
                break;
            }
        }
    }

    /// <summary>Node-level DSK-015: devices switch to read-only while serving.</summary>
    [Fact]
    [Catalog("DSK-015", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_015_ReadOnlyDeviceTransition_NodeDisks()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            byte[] a = TestData.Bytes(1531, 150_000);
            await volume.Files.WriteAllBytesAsync("/a", a, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();

            // One follower's device becomes read-only: reads continue, writes succeed on the healthy pair, and the
            // read-only node issues no receipt.
            TestNode ro = followers[0];
            long receipts = (await ro.GetCountersAsync()).ReceiptsIssued;
            ro.Disk.SetReadOnly(true);
            AssertBytes(a, await volume.Files.ReadAllBytesAsync("/a", Ct), "read with one read-only device");
            byte[] b = TestData.Bytes(1532, 150_000);
            await volume.Files.WriteAllBytesAsync("/b", b, Ct);
            foreach (ObjectId id in await FileObjectsAsync(cluster, "/b"))
                Assert.DoesNotContain(ro.Name, await cluster.Inspect.GetObjectHoldersAsync(id));
            if (ro.State == TestNodeState.Running)
                Assert.Equal(receipts, (await ro.GetCountersAsync()).ReceiptsIssued);

            // Two read-only devices: no durable majority and no ack set; new work fails explicitly, reads continue.
            followers[1].Disk.SetReadOnly(true);
            DfsException refused = await AssertEx.ThrowsDfsAsync<DfsException>(() => volume.Files.WriteAllBytesAsync("/c", TestData.Bytes(1533, 150_000), Ct));
            Assert.True(refused is DfsUnavailableException or DfsCommitOutcomeUnknownException, $"expected an availability failure, got {refused.GetType().Name} ({refused.ErrorCode})");
            await using (DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct))
            {
                Exception? readFailure = await Record.ExceptionAsync(async () => AssertBytes(a, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/a", Ct), "read with two read-only devices"));
                if (readFailure is not null)
                {
                    // The read helper needs a committed reader pin, which cannot commit without two writable logs.
                    Assert.True(readFailure is DfsUnavailableException or DfsException { ErrorCode: DfsErrorCode.Timeout },
                        $"a read may only fail for lack of authority, got {readFailure.GetType().Name}: {readFailure.Message}");
                }
            }

            foreach (TestNode node in followers)
                node.Disk.SetReadOnly(false);
            foreach (TestNode node in followers.Where(n => n.State != TestNodeState.Running))
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            byte[] c = TestData.Bytes(1534, 150_000);
            await WaitAsync(cluster, async () =>
            {
                try
                {
                    await volume.Files.WriteAllBytesAsync("/c", c, Ct);
                    return true;
                }
                catch (DfsUnavailableException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), "writes resume once devices are writable");
            AssertBytes(c, await volume.Files.ReadAllBytesAsync("/c", Ct), "/c");
            AssertBytes(b, await volume.Files.ReadAllBytesAsync("/b", Ct), "/b");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-016
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(DskMediumKind.Simulated)]
    [InlineData(DskMediumKind.FileSystem)]
    [Catalog("DSK-016", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_016_LatentCorruptionScrubRepair_StoreScrub(DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-016-{kind}", 16);
        // 16 KiB segments: the damaged objects end up in sealed segments that nothing reads until the scrub.
        ObjectStoreOptions options = SmallStoreOptions(segmentSize: 16 << 10);
        var objects = Enumerable.Range(0, 6).Select(i => (Id: Oid(1600 + (ulong)i), Data: TestData.Bytes(1600 + (ulong)i, 3000 + (i * 1000)))).ToArray();
        await WithStoreAsync(disk.Medium, options, async s =>
        {
            foreach ((ObjectId id, byte[] data) in objects)
                await s.PutAsync(id, VolumeB, new StagingId(2), data, Ct);
        });

        SortedDictionary<string, byte[]> images = await SnapshotFilesAsync(disk.Medium, "objects");
        int[] corrupt = [1, 4];
        foreach (int i in corrupt)
        {
            (string Path, int Offset) at = LocatePayload(images, objects[i].Data) ?? throw new InvalidOperationException("plaintext payload not found on disk");
            images[at.Path][at.Offset + 17] ^= 0x40;
        }

        await RestoreFilesAsync(disk.Medium, "objects", images);
        InterposedMedium medium = await disk.RestartAsync();
        await WithStoreAsync(medium, options, async store =>
        {
            // A store may verify records it cannot vouch for while opening; anything it dropped then counts as
            // detected. Healthy objects must all be indexed.
            var indexed = store.List().Select(i => i.Id).ToHashSet();
            var quarantinedAtOpen = objects.Select(o => o.Id).Where(id => !indexed.Contains(id)).ToHashSet();
            TestContext.Current.TestOutputHelper?.WriteLine($"quarantined while opening: {quarantinedAtOpen.Count}");

            ScrubResult bounded = await store.ScrubAsync(2, Ct);
            Assert.True(bounded.Checked <= 2, $"scrub exceeded its bound ({bounded.Checked} > 2)");

            ScrubResult full = await store.ScrubAsync(100, Ct);
            var found = new HashSet<ObjectId>(full.Corrupt.Concat(bounded.Corrupt).Concat(quarantinedAtOpen));
            Assert.True(full.Checked + bounded.Checked >= indexed.Count, $"scrub checked only {full.Checked + bounded.Checked} of {indexed.Count} indexed objects");
            Assert.Equal(corrupt.Select(i => objects[i].Id).OrderBy(o => o.Value), found.OrderBy(o => o.Value));

            for (int i = 0; i < objects.Length; i++)
            {
                if (corrupt.Contains(i))
                {
                    // Quarantined: never served as healthy.
                    if (await store.ContainsAsync(objects[i].Id, Ct))
                        await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => ReadObjectAsync(store, objects[i].Id, objects[i].Data.Length), DfsErrorCode.Integrity);
                }
                else
                {
                    AssertBytes(objects[i].Data, await ReadObjectAsync(store, objects[i].Id, objects[i].Data.Length), $"healthy object {i}");
                }
            }

            // Repair from a verified copy (re-put with the correct bytes), then a clean scrub.
            foreach (int i in corrupt)
            {
                await store.PutAsync(objects[i].Id, VolumeB, new StagingId(2), objects[i].Data, Ct);
                AssertBytes(objects[i].Data, await ReadObjectAsync(store, objects[i].Id, objects[i].Data.Length), $"repaired object {i}");
            }

            ScrubResult clean = await store.ScrubAsync(100, Ct);
            Assert.Empty(clean.Corrupt);
        });
    }

    [Fact]
    [Catalog("DSK-016", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_016_LatentCorruptionScrubRepair()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            byte[] data = TestData.Bytes(1650, 200_000);
            await client.GetVolume(Volume).Files.WriteAllBytesAsync("/f", data, Ct);
            ObjectId victim = (await FileObjectsAsync(cluster, "/f"))[0];
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(victim);
            TestNode bad = cluster.Node(holders[^1]);
            NodeCounters before = await bad.GetCountersAsync();
            await bad.Disk.CorruptObjectAsync(victim, offset: 99);

            using (MessageHold repairs = cluster.Network.Hold(new MessageFilter(Type: MessageType.ReplicateObject)))
            {
                await cluster.Admin.StartScrubAsync(bad.Name, Ct);
                await WaitAsync(cluster, async () => (await bad.GetCountersAsync()).ChecksumFailures > before.ChecksumFailures,
                    TimeSpan.FromSeconds(30), "scrubbing detects the latent corruption without any client read");

                // Quarantined before any repair can land (repair copies are held): the corrupt replica is no longer a
                // verified holder, the deficit is reported, and reads keep returning exact bytes from the healthy copies.
                await WaitAsync(cluster, async () => !(await cluster.Inspect.GetObjectHoldersAsync(victim)).Contains(bad.Name),
                    TimeSpan.FromSeconds(30), "the corrupt replica is quarantined (no longer counted as a verified holder) while its repair is pending");
                (int copies, _) = await cluster.Inspect.CountDurableDomainsAsync(victim);
                Assert.True(copies < 3, $"the object is counted with {copies} durable copies although one replica is corrupt and its repair is held");
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                Assert.True(health.UnderReplicatedObjects + health.MissingObjects >= 1, "the object is reported healthy while its repair is still pending");
                await using (DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r-quarantine" }, Ct))
                    AssertBytes(data, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), "read while the corrupt replica is quarantined");
            }

            await cluster.WaitForRepairAsync(Volume, TimeSpan.FromSeconds(60), Ct);
            Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(victim));
            DfsReplicaHealth healed = await cluster.Inspect.GetReplicaHealthAsync(Volume);
            Assert.Equal(0, healed.UnderReplicatedObjects);
            Assert.Equal(0, healed.MissingObjects);
            for (int i = 0; i < 3; i++)
            {
                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = $"r{i}" }, Ct);
                AssertBytes(data, await reader.GetVolume(Volume).Files.ReadAllBytesAsync("/f", Ct), $"read {i} after scrub repair");
            }

            // The repaired copy itself verifies: another scrub finds nothing new.
            long failures = (await cluster.Inspect.GetTotalCountersAsync()).ChecksumFailures;
            await cluster.Admin.StartScrubAsync(ct: Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            Assert.Equal(failures, (await cluster.Inspect.GetTotalCountersAsync()).ChecksumFailures);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-017
    // ------------------------------------------------------------------------------------------------------------

    private static readonly byte[] Marker = "TENANT1-SECRET!!"u8.ToArray();

    private static byte[] MarkerPayload(int length)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++)
            data[i] = Marker[i % Marker.Length];
        return data;
    }

    private static void AssertNoMarker(ReadOnlySpan<byte> data, string context) =>
        Assert.True(data.IndexOf(Marker.AsSpan(0, 8)) < 0, $"{context}: a previous tenant's bytes leaked into a read.");

    [Theory]
    [InlineData(DskMediumKind.Simulated)]
    [InlineData(DskMediumKind.FileSystem)]
    [Catalog("DSK-017", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_017_SegmentReuseDoesNotLeakOldData(DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-017-{kind}", 17, capacity: 3L << 20);
        ObjectStoreOptions options = SmallStoreOptions(segmentSize: 128 << 10);
        await WithStoreAsync(disk.Medium, options, async store =>
        {
            for (ulong i = 0; i < 20; i++)
                await store.PutAsync(Oid(1700 + i), VolumeA, new StagingId(1), MarkerPayload(40_000), Ct);
            for (ulong i = 0; i < 20; i++)
                await store.DeleteAsync(Oid(1700 + i), Ct);
            await store.CompactAsync(Ct);

            // Tenant B reuses the reclaimed space.
            var b = new List<(ObjectId Id, byte[] Data)>();
            for (ulong i = 0; i < 20; i++)
            {
                byte[] data = TestData.Bytes(1750 + i, 1 + (int)(i * 2111));
                await store.PutAsync(Oid(1750 + i), VolumeB, new StagingId(2), data, Ct);
                b.Add((Oid(1750 + i), data));
            }

            foreach ((ObjectId id, byte[] data) in b)
            {
                byte[] read = await ReadObjectAsync(store, id, data.Length);
                AssertEx.BytesEqual(data, read, $"tenant B object {id}");

                // Reading past the end returns nothing, not stale bytes.
                byte[] beyond = new byte[4096];
                int n = await store.ReadAsync(id, data.Length, beyond, Ct);
                Assert.Equal(0, n);
                AssertNoMarker(beyond, $"read beyond the end of {id}");
            }

            for (ulong i = 0; i < 20; i++)
            {
                ObjectId gone = Oid(1700 + i);
                await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => ReadObjectAsync(store, gone, 40_000), DfsErrorCode.Integrity);
            }

            Assert.Empty(store.List(VolumeA));
            Assert.Equal(20, store.List(VolumeB).Count);

            // A failed allocation (ENOSPC) leaves no readable remnant.
            if (disk.SimulatedFaults is { } faults)
                faults.CapacityOverride = disk.Simulated!.UsedBytes + 10_000;
            else
                disk.Medium.Inject = boundary => boundary.Kind == BoundaryKind.Write ? StorageErrors.DiskFull(boundary.Path) : null;
            Exception? failed = await Record.ExceptionAsync(() => store.PutAsync(Oid(1799), VolumeB, new StagingId(2), TestData.Bytes(1799, 60_000), Ct).AsTask());
            Assert.True(IsCapacityFailure(failed), $"the allocation must fail for lack of space, got {failed?.GetType().Name ?? "success"}: {failed?.Message}");
            if (disk.SimulatedFaults is { } f2)
                f2.CapacityOverride = null;
            disk.Medium.Inject = null;
            Assert.False(await store.ContainsAsync(Oid(1799), Ct), "a failed allocation produced a readable object");
        });

        // Whatever stays on the device, a restarted store never exposes it.
        InterposedMedium restarted = await disk.RestartAsync();
        await WithStoreAsync(restarted, options, async store =>
        {
            Assert.Empty(store.List(VolumeA));
            Assert.False(await store.ContainsAsync(Oid(1799), Ct), "the failed allocation became an object after restart");
            Assert.Equal(20, store.List(VolumeB).Count);
            foreach (ObjectRecordInfo info in store.List())
                AssertNoMarker(await ReadObjectAsync(store, info.Id, (int)info.Length), $"object {info.Id} after restart");
        });
    }

    [Fact]
    [Catalog("DSK-017", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_017_SegmentReuseDoesNotLeakOldData_CrossPrincipal()
    {
        TestClusterOptions options = QuicDisk(3) with { Volumes = [new TestVolumeSpec("v1"), new TestVolumeSpec("v2")] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TestPrincipal p1 = await cluster.CreatePrincipalAsync("tenant1", new Dictionary<string, DfsPermissions> { ["v1"] = DfsPermissions.Read | DfsPermissions.Write });
            TestPrincipal p2 = await cluster.CreatePrincipalAsync("tenant2", new Dictionary<string, DfsPermissions> { ["v2"] = DfsPermissions.Read | DfsPermissions.Write });
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "t1", Credentials = p1.Credentials }, Ct);
            await using DfsClient c2 = await cluster.ConnectAsync(new TestClientOptions { Name = "t2", Credentials = p2.Credentials }, Ct);
            DfsVolume v1 = c1.GetVolume("v1");
            DfsVolume v2 = c2.GetVolume("v2");

            for (int i = 0; i < 16; i++)
                await v1.Files.WriteAllBytesAsync($"/secret{i}", MarkerPayload(128 * 1024), Ct);
            IReadOnlySet<ObjectId> secretObjects = await cluster.Inspect.GetAllStoredObjectsAsync("v1");
            Assert.NotEmpty(secretObjects);
            for (int i = 0; i < 16; i++)
                await v1.Files.DeleteAsync($"/secret{i}", Ct);
            await WaitAsync(cluster, async () => !(await cluster.Inspect.GetAllStoredObjectsAsync("v1")).Overlaps(secretObjects),
                TimeSpan.FromSeconds(30), "tenant 1's deleted objects are reclaimed");

            // Sparse file: holes read as zeros.
            await using (DfsStream sparse = await v2.Files.CreateAsync("/sparse", Ct))
            {
                await sparse.SetLengthAsync(1 << 20, Ct);
                sparse.Seek(500_000, SeekOrigin.Begin);
                await sparse.WriteAsync(TestData.Bytes(1771, 100), Ct);
                await sparse.FlushAsync(Ct);
            }

            byte[] sparseRead = await v2.Files.ReadAllBytesAsync("/sparse", Ct);
            Assert.Equal(1 << 20, sparseRead.Length);
            AssertNoMarker(sparseRead, "sparse file");
            Assert.True(sparseRead.AsSpan(0, 500_000).IndexOfAnyExcept((byte)0) < 0, "a hole returned non-zero bytes");
            AssertEx.BytesEqual(TestData.Bytes(1771, 100), sparseRead.AsSpan(500_000, 100), "written range in the sparse file");
            Assert.True(sparseRead.AsSpan(500_100).IndexOfAnyExcept((byte)0) < 0, "the hole after the written range returned non-zero bytes");

            for (int i = 0; i < 16; i++)
            {
                byte[] data = TestData.Bytes(1780 + (ulong)i, (64 * 1024) + i);
                await v2.Files.WriteAllBytesAsync($"/new{i}", data, Ct);
                byte[] read = await v2.Files.ReadAllBytesAsync($"/new{i}", Ct);
                AssertEx.BytesEqual(data, read, $"/new{i}");
            }

            // Failed allocation: the file keeps its old committed bytes, nothing foreign appears.
            byte[] keep = TestData.Bytes(1790, 10_000);
            await v2.Files.WriteAllBytesAsync("/keep", keep, Ct);
            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.CapacityOverride = node.Disk.UsedBytes + 1024;
            DfsException allocation = await AssertEx.ThrowsDfsAsync<DfsException>(() => v2.Files.WriteAllBytesAsync("/keep", TestData.Bytes(1791, 3 << 20), Ct));
            Assert.True(allocation is DfsCapacityException { ErrorCode: DfsErrorCode.CapacityExhausted } or DfsPlacementUnavailableException,
                $"a write that cannot be allocated must fail as a typed capacity/placement refusal, got {allocation.GetType().Name} ({allocation.ErrorCode})");
            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.CapacityOverride = null;
            byte[] kept = await v2.Files.ReadAllBytesAsync("/keep", Ct);
            AssertEx.BytesEqual(keep, kept, "/keep after a failed allocation");
            AssertNoMarker(kept, "/keep");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-018
    // ------------------------------------------------------------------------------------------------------------

    private static async Task CopyTreeAsync(string from, string to)
    {
        foreach (string dir in Directory.EnumerateDirectories(from, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        foreach (string file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            string target = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            await using FileStream source = File.OpenRead(file);
            await using FileStream dest = File.Create(target);
            await source.CopyToAsync(dest, Ct);
        }
    }

    [Theory]
    [InlineData(DskMediumKind.Simulated)]
    [InlineData(DskMediumKind.FileSystem)]
    [Catalog("DSK-018", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_018_LocalEngineReopenIsSelfContained(DskMediumKind kind)
    {
        await using DskMedium disk = DskMedium.Create(kind, $"DSK-018-{kind}", 18);
        ObjectStoreOptions options = SmallStoreOptions(segmentSize: 64 << 10);
        var expected = new List<(ObjectRecordInfo Info, byte[] Data)>();
        byte[][] journalPayloads = [.. Enumerable.Range(0, 9).Select(i => TestData.Bytes(1830 + (ulong)i, 100 * (i + 1)))];
        byte[] checkpoint = TestData.Bytes(1840, 4000);
        int[] deleted = [3, 8, 11];

        await WithStoreAsync(disk.Medium, options, async store =>
        {
            for (int i = 0; i < 14; i++)
            {
                byte[] data = TestData.Bytes(1800 + (ulong)i, 2000 + (i * 1500));
                ObjectRecordInfo info = await store.PutAsync(Oid(1800 + (ulong)i), i % 2 == 0 ? VolumeA : VolumeB, new StagingId(10 + (ulong)(i % 4)), data, Ct);
                expected.Add((info, data));
            }

            foreach (int i in deleted)
            {
                await store.DeleteAsync(Oid(1800 + (ulong)i), Ct);
                expected.RemoveAll(e => e.Info.Id == Oid(1800 + (ulong)i));
            }
        });
        await WithJournalAsync(disk.Medium, Wal, new JournalOptions(SegmentSize: 1024), async j =>
        {
            foreach (byte[] payload in journalPayloads)
                await j.AppendAsync(payload, Ct);
            await j.FlushAsync(Ct);
        });
        await disk.Medium.CreateDirectoryDurableAsync(StoragePath.GetParent(CheckpointPath), Ct);
        await AtomicFile.WriteAsync(disk.Medium, CheckpointPath, checkpoint, Ct);

        async Task VerifyAsync(IStorageMedium medium, string context)
        {
            await WithStoreAsync(medium, options, async store =>
            {
                var listed = store.List().OrderBy(i => i.Id.Value).ToList();
                var wanted = expected.Select(e => e.Info).OrderBy(i => i.Id.Value).ToList();
                Assert.Equal(wanted, listed);
                Assert.Equal(wanted.Where(i => i.Volume == VolumeA).ToList(), store.List(VolumeA).OrderBy(i => i.Id.Value).ToList());
                foreach ((ObjectRecordInfo info, byte[] data) in expected)
                    AssertBytes(data, await ReadObjectAsync(store, info.Id, data.Length), $"{context}: object {info.Id}");
                foreach (int i in deleted)
                    Assert.False(await store.ContainsAsync(Oid(1800 + (ulong)i), Ct), $"{context}: deleted object resurrected");
            });
            await WithJournalAsync(medium, Wal, new JournalOptions(SegmentSize: 1024), j =>
            {
                Assert.Equal(journalPayloads.Length, j.Records.Count);
                for (int i = 0; i < journalPayloads.Length; i++)
                    AssertEx.BytesEqual(journalPayloads[i], j.Records[i].Payload.Span, $"{context}: journal record {i}");
                return Task.CompletedTask;
            });
            AssertBytes(checkpoint, await AtomicFile.ReadAsync(medium, CheckpointPath, Ct), $"{context}: checkpoint");
        }

        // 1. Cold reopen in a new process view (no caches).
        await VerifyAsync(await disk.RestartAsync(), "cold reopen");

        // 2. Crash without clean close (no shutdown metadata written).
        await VerifyAsync(await disk.PowerLossAsync(AckLossMode(kind)), "after power loss");

        // 3. The durable files alone, copied to a different device, are sufficient.
        if (disk.Simulated is { } simulated)
        {
            await VerifyAsync(new InterposedMedium(simulated.CloneDurable("dsk-018-copy")), "cloned durable image");
        }
        else
        {
            string copy = disk.Root + "-copy";
            Directory.CreateDirectory(copy);
            try
            {
                await CopyTreeAsync(disk.Root!, copy);
                await VerifyAsync(new InterposedMedium(new FileSystemMedium(copy)), "copied directory");
            }
            finally
            {
                Directory.Delete(copy, recursive: true);
            }
        }
    }

    [Fact]
    [Catalog("DSK-018", "I", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_018_LocalEngineReopenIsSelfContained_NodeRestart()
    {
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 10; i++)
            {
                files[$"/f{i}"] = TestData.Bytes(1850 + (ulong)i, 30_000 * (i + 1));
                await volume.Files.WriteAllBytesAsync($"/f{i}", files[$"/f{i}"], Ct);
            }

            await volume.Files.DeleteAsync("/f3", Ct);
            files.Remove("/f3");
            await cluster.WaitForReplicationAsync(Volume, Ct);

            var indexes = new Dictionary<string, List<ObjectRecordInfo>>(StringComparer.Ordinal);
            foreach (TestNode node in cluster.Nodes)
                indexes[node.Name] = [.. (await node.Inspect.ListObjectsAsync()).OrderBy(o => o.Id.Value)];

            foreach (TestNode node in cluster.Nodes)
                await node.StopAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(45), Ct);

            IReadOnlySet<ObjectId> reachable = (await cluster.Inspect.GetVolumeStateAsync(Volume)).ReachableObjects();
            foreach (TestNode node in cluster.Nodes)
            {
                List<ObjectRecordInfo> rebuilt = [.. (await node.Inspect.ListObjectsAsync()).OrderBy(o => o.Id.Value)];

                // GC may reclaim unreachable objects in between; every reachable one is rebuilt with identical metadata.
                foreach (ObjectRecordInfo info in indexes[node.Name].Where(i => reachable.Contains(i.Id)))
                    Assert.Contains(info, rebuilt);
                Assert.True(rebuilt.All(r => indexes[node.Name].Any(i => i.Id == r.Id) || reachable.Contains(r.Id)), $"{node.Name}: index rebuilt with unknown objects");
            }

            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            foreach ((string path, byte[] data) in files)
                AssertBytes(data, await fresh.GetVolume(Volume).Files.ReadAllBytesAsync(path, Ct), path);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DSK-019 / DSK-020 (H)
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Prepares acknowledged commits on real QUIC + disk nodes and verifies them before the power cut. Interrupting
    /// device power needs an external rig, so the test is reported as not run (never as a pass).
    /// </summary>
    [Fact]
    [Catalog("DSK-019", "H", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_019_ColdPowerLossDurability()
    {
        var expected = new Dictionary<string, (byte[] Data, CommitToken Token)>(StringComparer.Ordinal);
        string? rig = Environment.GetEnvironmentVariable("TDFS_POWER_RIG");
        await ClusterTest.RunAsync(QuicDisk(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/durable", Ct);
            for (int i = 0; i < 16; i++)
            {
                byte[] data = TestData.Bytes(1900 + (ulong)i, i % 4 == 0 ? 512 : 300_000);
                CommitToken token = await volume.Files.WriteAllBytesAsync($"/durable/{i}", data, Ct);
                expected[$"/durable/{i}"] = (data, token);
            }

            foreach ((string path, (byte[] data, _)) in expected)
                AssertBytes(data, await volume.Files.ReadAllBytesAsync(path, Ct), $"{path} before the power cut");
            foreach (TestNode node in cluster.Nodes)
                TestContext.Current.TestOutputHelper?.WriteLine($"{node.Name}: data at {node.Disk.RootDirectory} (incarnation {node.Incarnation})");
        }, Ct);

        Assert.Skip($"requires a power-cut rig (TDFS_POWER_RIG={rig ?? "unset"}) on supported devices with documented persistence settings to cut power after {expected.Count} acknowledged commits and cold-restart from the surviving disks; status: not run");
    }

    public static TheoryData<string, string, string> DeviceMatrixData() => new()
    {
        { "NTFS", "Windows", "process-kill" },
        { "NTFS", "Windows", "power-cut, volatile write cache enabled" },
        { "NTFS", "Windows", "power-cut, write cache disabled" },
        { "ReFS", "Windows", "process-kill" },
        { "ReFS", "Windows", "power-cut" },
        { "ext4", "Linux", "process-kill" },
        { "ext4", "Linux", "power-cut, data=ordered, barrier=1" },
        { "xfs", "Linux", "power-cut" },
        { "btrfs", "Linux", "power-cut" },
        { "apfs", "macOS", "power-cut" },
    };

    /// <summary>
    /// Repeats the storage-engine crash/flush/rename/recovery families on each supported local filesystem and
    /// persistence configuration. Process-kill rows run when the test root is on that filesystem; power-cut rows and
    /// absent filesystems are recorded explicitly as not run.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeviceMatrixData))]
    [Catalog("DSK-020", "H", Inv01, Inv02, Inv09, Inv14)]
    public async Task DSK_020_FilesystemAndDeviceMatrix(string fileSystem, string os, string persistence)
    {
        string root = Path.GetFullPath(TestRoot);
        Directory.CreateDirectory(root);
        string actualFs = new DriveInfo(Path.GetPathRoot(root)!).DriveFormat;
        bool osMatches = os switch
        {
            "Windows" => OperatingSystem.IsWindows(),
            "Linux" => OperatingSystem.IsLinux(),
            "macOS" => OperatingSystem.IsMacOS(),
            _ => false,
        };
        TestContext.Current.TestOutputHelper?.WriteLine($"test root {root} is on '{actualFs}' ({System.Runtime.InteropServices.RuntimeInformation.OSDescription})");
        if (!osMatches || !string.Equals(actualFs, fileSystem, StringComparison.OrdinalIgnoreCase))
            Assert.Skip($"{fileSystem} on {os} ({persistence}) is not available on this host (test root on '{actualFs}'); combination recorded as not run");
        if (persistence.StartsWith("power-cut", StringComparison.Ordinal))
            Assert.Skip($"{fileSystem}/{os} '{persistence}' requires a power-cut rig with that device cache configuration; status: not run");

        // Process-kill crash matrices on the real filesystem: journal, atomic rename, compaction.
        foreach (JournalScenario scenario in new[] { JournalScenario.BatchedFlushes, JournalScenario.SegmentRollover, JournalScenario.TruncateThenAppend })
        {
            JournalOptions options = JournalOptionsFor(scenario);
            await RunCrashMatrixAsync<JournalModel>($"DSK-020-{fileSystem}-{scenario}", DskMediumKind.FileSystem, CrashMode.KeepUnflushed, 20,
                () => new JournalModel(), null, (m, model) => RunJournalScenarioAsync(m, model, scenario),
                (m, model, context) => VerifyJournalRecoveryAsync(m, model, context, options));
        }

        await RunCrashMatrixAsync<AtomicModel>($"DSK-020-{fileSystem}-atomic", DskMediumKind.FileSystem, CrashMode.KeepUnflushed, 21,
            () => new AtomicModel(), AtomicSetupAsync, AtomicScenarioAsync, (m, model, context) => VerifyAtomicRecoveryAsync(m, model, context));
        await RunCrashMatrixAsync<ObjectStoreModel>($"DSK-020-{fileSystem}-compaction", DskMediumKind.FileSystem, CrashMode.KeepUnflushed, 22,
            () => new ObjectStoreModel(), ObjectStoreSetupAsync, (m, model) => CompactionScenarioAsync(m, model),
            (m, model, context) => VerifyObjectStoreRecoveryAsync(m, model, context, CompactionStoreOptions));
    }
}
