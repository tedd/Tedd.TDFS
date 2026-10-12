using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Str.StrHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Str;

/// <summary>Where the in-flight live read of STR-014 is held while a replacement commits.</summary>
public enum ReadBlockPoint { AfterRevisionSelected, FirstObjectFetch, SecondObjectFetch, LastObjectFetch }

/// <summary>The committed replacement of STR-014.</summary>
public enum ReplacementKind { WriteAllSameLength, WriteAllShorter, StreamOverwriteEveryChunk, TruncateViaSetLength }

/// <summary>How another permitted writer publishes the new revision in STR-015.</summary>
public enum CommitPath { WriteAllBytes, StreamCommit, WriteThroughWrite, AppendAllBytes, TransactionCommit, CopyOverwrite, StreamTruncate }

/// <summary>Namespace/data change applied while a pinned stream is open (STR-016).</summary>
public enum PinnedMutation { Overwrite, Truncate, Append, Rename, Delete, DeleteAndRecreate, RenameOver, Sequence }

/// <summary>Why the pinned reader of STR-017 stops renewing.</summary>
public enum PinExpiryCause { ClientIsolated, ClientIsolatedWithLeaderChange, ClientClockFrozen }

/// <summary>
/// STR — stream reads, writes, seeking and interoperability: the S-level rows (FSD §§6.2, 7; test plan §7.5). Every row runs
/// in deterministic simulation (seeded, replayable), uses async APIs only and checks its safety property both while the
/// interfering transition is in progress and after it settled.
/// </summary>
public sealed class StrSimulationTests
{
    // ------------------------------------------------------------------ STR-014

    public static TheoryData<ReadBlockPoint, ReplacementKind> LiveReadCases()
    {
        var data = new TheoryData<ReadBlockPoint, ReplacementKind>();
        foreach (ReadBlockPoint point in Enum.GetValues<ReadBlockPoint>())
        {
            foreach (ReplacementKind kind in Enum.GetValues<ReplacementKind>())
                data.Add(point, kind);
        }

        return data;
    }

    /// <summary>
    /// A multi-extent live read is held after it selected its revision, or at the first/second/last object fetch, while
    /// another client commits a replacement (same length, shorter, every chunk overwritten, truncated) and GC runs.
    /// The read returns one revision only — all of A or all of B, never mixed extents — and the next live call observes
    /// the replacement.
    /// </summary>
    [Theory]
    [MemberData(nameof(LiveReadCases))]
    [Catalog("STR-014", "S", "INV-03", "INV-07")]
    public async Task STR_014_LiveReadPinsOneRevisionPerCall(ReadBlockPoint point, ReplacementKind kind)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            DfsVolume wv = writer.GetVolume(Vol);
            const string path = "/str014.bin";
            int lenA = 6 * Chunk + 123;
            byte[] a = NonZero(0x5014_0001, lenA);
            // One object per chunk (seven objects): a single read call necessarily spans several extents/objects, whatever
            // object packing WriteAll would choose.
            await SeedOneObjectPerChunkAsync(wv, path, a);
            FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, path);
            Assert.True(inspected is { IsInline: false } && inspected.Extents.Select(e => e.Object).Distinct().Count() >= 2,
                $"precondition: a multi-extent file backed by several objects (got {inspected?.Extents.Count ?? 0} extents)");

            // Calibrate the number of object fetches of one cold live read call (separate client, never reused).
            int fetches;
            await using (DfsClient calibrator = await ConnectAsync(cluster, "cal"))
            {
                FaultArm counter = cluster.Faults.Arm(
                    new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.ReadObject, Count: int.MaxValue),
                    FaultAction.Delay(TimeSpan.Zero));
                await using DfsStream probe = await calibrator.GetVolume(Vol).Files.OpenAsync(path, Reader(), Ct);
                int got = await probe.ReadAsync(new byte[lenA], Ct);
                Assert.True(got > 0, "calibration read delivered data");
                fetches = counter.Hits;
                counter.Disarm();
            }

            Assert.True(fetches >= 1, "precondition: a multi-extent live read fetches object data from storage nodes");
            byte[] b = BuildReplacement(kind, a);

            await using DfsClient reader = await ConnectAsync(cluster, "r");
            DfsStream s = await reader.GetVolume(Vol).Files.OpenAsync(path, Reader(), Ct);
            FaultMatch match = point switch
            {
                ReadBlockPoint.AfterRevisionSelected => new FaultMatch(FaultSites.ClientReplyReceived, Operation: DfsOperationKind.Read),
                ReadBlockPoint.FirstObjectFetch => new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.ReadObject, Occurrence: 1),
                ReadBlockPoint.SecondObjectFetch => new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.ReadObject, Occurrence: Math.Min(2, fetches)),
                _ => new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.ReadObject, Occurrence: fetches),
            };
            FaultArm arm = cluster.Faults.Arm(match, FaultAction.Block());
            var buffer = new byte[lenA];
            Task<int> read = s.ReadAsync(buffer.AsMemory(), Ct).AsTask();
            await arm.WaitTriggeredAsync(Bounded);
            Assert.False(read.IsCompleted, "the live read is held mid-call");

            CommitToken tokenB = await ReplaceAsync(wv, path, kind, b);
            Assert.False(tokenB.IsNone, "the replacement committed");
            BytesEqual(b, await wv.Files.ReadAllBytesAsync(path, Ct), "the replacement is the current revision");
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct); // GC (250 ms) runs while the read is still held
            Assert.False(read.IsCompleted, "the read is still held while maintenance runs");

            arm.Release();
            int n = await read.Within(cluster, "the held live read completes after release");
            cluster.Faults.AssertReached(arm);
            Assert.InRange(n, 1, lenA);
            bool isA = buffer.AsSpan(0, n).SequenceEqual(a.AsSpan(0, n));
            bool isB = n <= b.Length && buffer.AsSpan(0, n).SequenceEqual(b.AsSpan(0, n));
            Assert.True(isA || isB,
                $"One read call must return a single revision; got {n} bytes matching neither A (first difference at {FirstDifference(buffer.AsSpan(0, n), a)}) " +
                $"nor B (first difference at {FirstDifference(buffer.AsSpan(0, n), b)}) — mixed extents (point {point}, {kind}, {fetches} fetches).");
            Assert.Equal(n, s.Position);
            Log($"STR-014 {point}/{kind}: {fetches} fetches per read; the held read returned {n} bytes of revision {(isA && !isB ? "A" : isB && !isA ? "B" : "A=B prefix")}.");

            BytesEqual(b, await ReadWholeAsync(s), "a live call that starts after the replacement committed observes it");
            await s.DisposeAsync();
        }, Ct);
    }

    private static byte[] BuildReplacement(ReplacementKind kind, byte[] a)
    {
        switch (kind)
        {
            case ReplacementKind.WriteAllSameLength:
                return NonZero(0x5014_000B, a.Length);
            case ReplacementKind.WriteAllShorter:
                return NonZero(0x5014_000C, Chunk + 77);
            case ReplacementKind.StreamOverwriteEveryChunk:
            {
                byte[] b = (byte[])a.Clone();
                for (int k = 0; (long)k * Chunk + 110 <= a.Length; k++)
                    NonZero(0x5014_0100UL + (ulong)k, 100).CopyTo(b, k * Chunk + 10);
                return b;
            }

            case ReplacementKind.TruncateViaSetLength:
                return a.AsSpan(0, Chunk + 5).ToArray();
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    private static async Task<CommitToken> ReplaceAsync(DfsVolume volume, string path, ReplacementKind kind, byte[] b)
    {
        switch (kind)
        {
            case ReplacementKind.WriteAllSameLength:
            case ReplacementKind.WriteAllShorter:
                return await volume.Files.WriteAllBytesAsync(path, b, Ct);
            case ReplacementKind.StreamOverwriteEveryChunk:
            {
                await using DfsStream w = await volume.Files.OpenAsync(path, Writer(), Ct);
                for (int k = 0; (long)k * Chunk + 110 <= b.Length; k++)
                {
                    w.Seek(k * Chunk + 10, SeekOrigin.Begin);
                    await w.WriteAsync(b.AsMemory(k * Chunk + 10, 100), Ct);
                }

                return await w.CommitAsync(Ct);
            }

            case ReplacementKind.TruncateViaSetLength:
            {
                await using DfsStream w = await volume.Files.OpenAsync(path, Writer(), Ct);
                await w.SetLengthAsync(b.Length, Ct);
                return await w.CommitAsync(Ct);
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    // ------------------------------------------------------------------ STR-015

    /// <summary>
    /// A live stream reads A; another permitted writer commits B and then C through each publication path; each new live
    /// call (and Length/GetInfoAsync) of the still-open stream observes the current revision, never a silently pinned
    /// opening view, while a VersionPinned stream opened at the same time keeps A. The recorded history is linearizable.
    /// </summary>
    [Theory]
    [InlineData(CommitPath.WriteAllBytes)]
    [InlineData(CommitPath.StreamCommit)]
    [InlineData(CommitPath.WriteThroughWrite)]
    [InlineData(CommitPath.AppendAllBytes)]
    [InlineData(CommitPath.TransactionCommit)]
    [InlineData(CommitPath.CopyOverwrite)]
    [InlineData(CommitPath.StreamTruncate)]
    [Catalog("STR-015", "S", "INV-03", "INV-07")]
    public async Task STR_015_LiveCallsMayObserveNewRevisions(CommitPath commitPath)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient reader = await ConnectAsync(cluster, "r");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume rv = reader.GetVolume(Vol);
            const string path = "/str015.bin";
            byte[] current = NonZero(0x5015_0000, 2 * Chunk + 10);
            await cluster.History.RecordAsync("w", "write", path, current, () => wv.Files.WriteAllBytesAsync(path, current, Ct));

            DfsStream live = await rv.Files.OpenAsync(path, Reader(DfsReadView.Live), Ct);
            DfsStream pinned = await rv.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
            byte[] original = current;
            BytesEqual(original.AsMemory(0, 1000), await ReadUpToAsync(live, 1000), "first live call reads A");
            BytesEqual(original, await cluster.History.RecordAsync("r", "read", path, null, () => ReadWholeAsync(live)), "live view of A");

            for (int round = 1; round <= 2; round++)
            {
                (byte[] next, CommitToken? token) = await CommitThroughAsync(cluster, wv, path, commitPath, current, round);
                current = next;
                DfsFileInfo writerView = await wv.Files.GetInfoAsync(path, Ct);
                Assert.Equal(next.Length, writerView.Length);

                byte[] observed = await cluster.History.RecordAsync("r", "read", path, null, () => ReadWholeAsync(live));
                BytesEqual(next, observed, $"round {round} ({commitPath}): a new live call observes the revision committed before it started");
                DfsFileInfo liveInfo = await live.GetInfoAsync(Ct);
                Assert.Equal(next.Length, liveInfo.Length);
                Assert.Equal(next.Length, await live.GetLengthAsync(Ct));
                Assert.Equal(writerView.Revision, liveInfo.Revision);
                Assert.Equal(live.InodeId, liveInfo.InodeId);
                if (token is CommitToken t)
                {
                    Assert.False(t.IsNone, $"{commitPath} returned a commit token");
                    Assert.True(liveInfo.ObservedAt.IsAtOrAfter(t), $"round {round}: the live metadata call observed a point at or after the writer's commit");
                }

                BytesEqual(original, await ReadWholeAsync(pinned), $"round {round}: the VersionPinned stream keeps the opening revision");
                Assert.Equal(original.Length, await pinned.GetLengthAsync(Ct));
            }

            await live.DisposeAsync();
            await pinned.DisposeAsync();
            List<HistoryOp> history = cluster.History.Operations.Where(o => o.Key == path).ToList();
            LinearizabilityChecker.Check(history, new RegisterModel()).AssertValid();
        }, Ct);
    }

    private static async Task<(byte[] Next, CommitToken? Token)> CommitThroughAsync(TestCluster cluster, DfsVolume volume, string path,
        CommitPath commitPath, byte[] current, int round)
    {
        ulong seed = 0x5015_1000UL + (ulong)round * 16 + (ulong)commitPath;
        switch (commitPath)
        {
            case CommitPath.WriteAllBytes:
            {
                byte[] next = NonZero(seed, 2 * Chunk + 10 + round * 1000);
                CommitToken token = await cluster.History.RecordAsync("w", "write", path, next, () => volume.Files.WriteAllBytesAsync(path, next, Ct));
                return (next, token);
            }

            case CommitPath.StreamCommit:
            case CommitPath.WriteThroughWrite:
            {
                byte[] next = (byte[])current.Clone();
                byte[] patch = NonZero(seed, 500);
                patch.CopyTo(next, Chunk - 250); // spans the first extent boundary
                bool through = commitPath == CommitPath.WriteThroughWrite;
                CommitToken? token = await cluster.History.RecordAsync<CommitToken?>("w", "write", path, next, async () =>
                {
                    await using DfsStream w = await volume.Files.OpenAsync(path,
                        Writer(FileMode.Open, FileAccess.Write, through ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
                    w.Seek(Chunk - 250, SeekOrigin.Begin);
                    await w.WriteAsync(patch, Ct);
                    return through ? null : await w.CommitAsync(Ct);
                });
                return (next, token);
            }

            case CommitPath.AppendAllBytes:
            {
                byte[] suffix = NonZero(seed, 777);
                byte[] next = [.. current, .. suffix];
                CommitToken token = await cluster.History.RecordAsync("w", "write", path, next, () => volume.Files.AppendAllBytesAsync(path, suffix, Ct));
                return (next, token);
            }

            case CommitPath.TransactionCommit:
            {
                byte[] next = NonZero(seed, Chunk + 333 * round);
                CommitToken token = await cluster.History.RecordAsync("w", "write", path, next, async () =>
                {
                    await using DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
                    await tx.Files.WriteAllBytesAsync(path, next, Ct);
                    return await tx.CommitAsync(Ct);
                });
                return (next, token);
            }

            case CommitPath.CopyOverwrite:
            {
                byte[] next = NonZero(seed, 3 * Chunk + round);
                string source = $"/str015-src-{round}.bin";
                await volume.Files.WriteAllBytesAsync(source, next, Ct);
                CommitToken token = await cluster.History.RecordAsync("w", "write", path, next, () => volume.Files.CopyAsync(source, path, overwrite: true, Ct));
                return (next, token);
            }

            case CommitPath.StreamTruncate:
            {
                byte[] next = current.AsSpan(0, Math.Max(1, current.Length - Chunk - 17 * round)).ToArray();
                CommitToken token = await cluster.History.RecordAsync("w", "write", path, next, async () =>
                {
                    await using DfsStream w = await volume.Files.OpenAsync(path, Writer(), Ct);
                    await w.SetLengthAsync(next.Length, Ct);
                    return await w.CommitAsync(Ct);
                });
                return (next, token);
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(commitPath), commitPath, null);
        }
    }

    // ------------------------------------------------------------------ STR-016

    /// <summary>
    /// While a VersionPinned stream is open, the file is overwritten, truncated, appended, renamed, deleted, deleted and
    /// recreated, or replaced by a rename-over (and all of these in sequence). After every step, after many GC passes and
    /// beyond the pin duration (renewed by the live client), the pinned stream still returns exactly the opening revision
    /// with the same inode, revision and length, and its objects remain stored.
    /// </summary>
    [Theory]
    [InlineData(PinnedMutation.Overwrite)]
    [InlineData(PinnedMutation.Truncate)]
    [InlineData(PinnedMutation.Append)]
    [InlineData(PinnedMutation.Rename)]
    [InlineData(PinnedMutation.Delete)]
    [InlineData(PinnedMutation.DeleteAndRecreate)]
    [InlineData(PinnedMutation.RenameOver)]
    [InlineData(PinnedMutation.Sequence)]
    [Catalog("STR-016", "S", "INV-03", "INV-07")]
    public async Task STR_016_PinnedStreamRemainsStable(PinnedMutation mutation)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient reader = await ConnectAsync(cluster, "r");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume rv = reader.GetVolume(Vol);
            await wv.Directories.CreateDirectoryAsync("/dir", Ct);
            string path = "/dir/str016.bin";
            byte[] a = NonZero(0x5016_0000, 3 * Chunk + 100);
            await wv.Files.WriteAllBytesAsync(path, a, Ct);
            IReadOnlySet<ObjectId> objectsA = await GetObjectsAsync(cluster, path);

            DfsStream pinned = await rv.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
            InodeId inode = pinned.InodeId;
            ulong revision = pinned.OpenRevision;
            BytesEqual(a.AsMemory(0, 5000), await ReadUpToAsync(pinned, 5000), "pinned read before any change");

            PinnedMutation[] steps = mutation == PinnedMutation.Sequence
                ? [PinnedMutation.Overwrite, PinnedMutation.Truncate, PinnedMutation.Append, PinnedMutation.Rename, PinnedMutation.RenameOver, PinnedMutation.DeleteAndRecreate, PinnedMutation.Delete]
                : [mutation];
            string current = path;
            int index = 0;
            foreach (PinnedMutation step in steps)
            {
                index++;
                current = await MutateAsync(wv, current, step, index);
                await AssertPinnedAsync(pinned, a, inode, revision, $"while open, after {step}");
            }

            // Many GC passes (250 ms) and more than twice the pin duration (2 s); the open client keeps renewing its pin.
            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            await AssertPinnedAsync(pinned, a, inode, revision, "after GC passes and pin renewals");
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            Assert.True(objectsA.All(stored.Contains), "objects of a pinned revision are protected from reclamation");
            await pinned.DisposeAsync();
        }, Ct);
    }

    private static async Task<string> MutateAsync(DfsVolume volume, string path, PinnedMutation step, int index)
    {
        ulong seed = 0x5016_1000UL + (ulong)index;
        switch (step)
        {
            case PinnedMutation.Overwrite:
            {
                byte[] b = NonZero(seed, 2 * Chunk + 3);
                await volume.Files.WriteAllBytesAsync(path, b, Ct);
                BytesEqual(b, await volume.Files.ReadAllBytesAsync(path, Ct), "overwrite is visible to the writer");
                return path;
            }

            case PinnedMutation.Truncate:
            {
                byte[] c = NonZero(seed, 10);
                await using (DfsStream t = await volume.Files.OpenAsync(path, Writer(FileMode.Truncate, FileAccess.Write), Ct))
                    await t.WriteAsync(c, Ct);
                BytesEqual(c, await volume.Files.ReadAllBytesAsync(path, Ct), "truncation is visible to the writer");
                return path;
            }

            case PinnedMutation.Append:
            {
                byte[] before = await volume.Files.ReadAllBytesAsync(path, Ct);
                byte[] suffix = NonZero(seed, 1000);
                await volume.Files.AppendAllBytesAsync(path, suffix, Ct);
                BytesEqual(before.Concat(suffix).ToArray(), await volume.Files.ReadAllBytesAsync(path, Ct), "append is visible to the writer");
                return path;
            }

            case PinnedMutation.Rename:
            {
                string target = $"/dir/renamed-{index}.bin";
                await volume.Files.MoveAsync(path, target, overwrite: false, Ct);
                Assert.False(await volume.Files.ExistsAsync(path, Ct), "the old name is gone after rename");
                Assert.True(await volume.Files.ExistsAsync(target, Ct), "the new name exists after rename");
                return target;
            }

            case PinnedMutation.Delete:
                await volume.Files.DeleteAsync(path, Ct);
                Assert.False(await volume.Files.ExistsAsync(path, Ct), "the name is gone after delete");
                return path;

            case PinnedMutation.DeleteAndRecreate:
            {
                InodeId old = (await volume.Files.GetInfoAsync(path, Ct)).InodeId;
                await volume.Files.DeleteAsync(path, Ct);
                byte[] c = NonZero(seed, 4000);
                await volume.Files.WriteAllBytesAsync(path, c, Ct);
                DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
                Assert.NotEqual(old, info.InodeId);
                BytesEqual(c, await volume.Files.ReadAllBytesAsync(path, Ct), "recreated file");
                return path;
            }

            case PinnedMutation.RenameOver:
            {
                string other = $"/dir/other-{index}.bin";
                byte[] d = NonZero(seed, Chunk + 9);
                await volume.Files.WriteAllBytesAsync(other, d, Ct);
                InodeId otherInode = (await volume.Files.GetInfoAsync(other, Ct)).InodeId;
                await volume.Files.MoveAsync(other, path, overwrite: true, Ct);
                Assert.Equal(otherInode, (await volume.Files.GetInfoAsync(path, Ct)).InodeId);
                BytesEqual(d, await volume.Files.ReadAllBytesAsync(path, Ct), "rename-over replaced the name's target");
                return path;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(step), step, null);
        }
    }

    private static async Task AssertPinnedAsync(DfsStream pinned, byte[] a, InodeId inode, ulong revision, string because)
    {
        Assert.True(pinned.Length == a.Length, $"pinned Length {pinned.Length}, expected {a.Length} ({because}).");
        Assert.Equal(a.Length, await pinned.GetLengthAsync(Ct));
        DfsFileInfo info = await pinned.GetInfoAsync(Ct);
        Assert.True(info.Revision == revision, $"pinned revision {info.Revision}, expected {revision} ({because}).");
        Assert.Equal(inode, info.InodeId);
        Assert.Equal(a.Length, info.Length);
        BytesEqual(a, await ReadWholeAsync(pinned), $"pinned contents ({because})");
        pinned.Seek(2 * Chunk - 7, SeekOrigin.Begin);
        BytesEqual(a.AsMemory(2 * Chunk - 7, 14), await ReadUpToAsync(pinned, 14), $"pinned window across an extent boundary ({because})");
    }

    // ------------------------------------------------------------------ STR-017

    /// <summary>
    /// A pinned reader stops renewing (client isolated, isolated plus a metadata leader change, or a frozen client clock)
    /// after the file was replaced. Once the server expired its handle/pin and GC reclaimed the pinned revision's objects
    /// and new objects reused the space, every further operation on that stream fails with a typed pin-expired/lease-lost
    /// error — during the outage nothing is returned either — and it never returns A, B or reused bytes or silently
    /// switches to the current revision.
    /// </summary>
    [Theory]
    [InlineData(PinExpiryCause.ClientIsolated)]
    [InlineData(PinExpiryCause.ClientIsolatedWithLeaderChange)]
    [InlineData(PinExpiryCause.ClientClockFrozen)]
    [Catalog("STR-017", "S", "INV-03", "INV-07", "INV-16")]
    public async Task STR_017_ExpiredPinFailsExplicitly(PinExpiryCause cause)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            DfsVolume wv = writer.GetVolume(Vol);
            TestClock? readerClock = cause == PinExpiryCause.ClientClockFrozen ? new TestClock(cluster.Time) : null;
            DfsClient reader = await ConnectAsync(cluster, "r", operationTimeout: TimeSpan.FromSeconds(2), clock: readerClock);
            DfsVolume rv = reader.GetVolume(Vol);
            const string path = "/str017.bin";
            byte[] a = NonZero(0x5017_0000, 3 * Chunk + 50);
            await wv.Files.WriteAllBytesAsync(path, a, Ct);
            IReadOnlySet<ObjectId> objectsA = await GetObjectsAsync(cluster, path);

            DfsStream pinned = await rv.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
            InodeId inode = pinned.InodeId;
            BytesEqual(a.AsMemory(0, 1000), await ReadUpToAsync(pinned, 1000), "pinned read before expiry");
            byte[] b = NonZero(0x5017_0001, 3 * Chunk + 50);
            await wv.Files.WriteAllBytesAsync(path, b, Ct); // A's objects are now protected only by the reader's pin

            TestNode? crashedLeader = null;
            switch (cause)
            {
                case PinExpiryCause.ClientIsolated:
                    cluster.Network.Isolate("client:r");
                    break;
                case PinExpiryCause.ClientIsolatedWithLeaderChange:
                    cluster.Network.Isolate("client:r");
                    crashedLeader = await cluster.GetLeaderAsync(Vol, Ct);
                    await crashedLeader.CrashAsync();
                    break;
                case PinExpiryCause.ClientClockFrozen:
                    readerClock!.Freeze(); // the client stops renewing; server time keeps running
                    break;
            }

            if (cause != PinExpiryCause.ClientClockFrozen)
            {
                // Safety during the outage, before the server expired the pin: the pin is still valid, so the cut-off reader
                // may only return verified bytes of the pinned revision A (for example from a client-side cache) or fail
                // explicitly; it never returns B, zeroes or a fabricated EOF, and a failed read leaves Position unchanged.
                pinned.Seek(1000, SeekOrigin.Begin);
                var duringBuffer = new byte[4096];
                int? during = null;
                Exception? duringEx = await Record.ExceptionAsync(async () => during = await pinned.ReadAsync(duringBuffer, Ct));
                if (during is int got)
                {
                    Assert.InRange(got, 1, duringBuffer.Length);
                    BytesEqual(a.AsMemory(1000, got), duringBuffer.AsMemory(0, got), "a read during the outage (pin not yet expired) returns only revision A");
                    Assert.Equal(1000 + got, pinned.Position);
                }
                else
                {
                    Assert.True(duringEx is DfsException or TimeoutException,
                        $"a read during the outage fails explicitly (availability or expiry), got {duringEx?.GetType().FullName}: {duringEx?.Message}");
                    Assert.False(duringEx is EndOfStreamException, "an outage is not EOF");
                    Assert.Equal(1000, pinned.Position);
                }
            }

            await cluster.WaitUntilAsync(async () =>
                    (await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles.All(h => h.Inode != inode || h.Expired),
                Bounded, "the pinned reader's handle and pin expire once it stops renewing (lease/pin 2 s)", Ct);
            if (crashedLeader is not null)
            {
                await crashedLeader.RestartAsync();
                await cluster.WaitForStableAsync(Bounded, Ct);
            }

            await cluster.WaitUntilAsync(async () =>
                    !(await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Overlaps(objectsA),
                Bounded, "objects of the expired pinned revision are reclaimed by GC (250 ms interval)", Ct);
            for (int k = 0; k < 4; k++)
                await wv.Files.WriteAllBytesAsync($"/str017-reuse-{k}.bin", NonZero(0x5017_1000UL + (ulong)k, 3 * Chunk), Ct);

            if (cause == PinExpiryCause.ClientClockFrozen)
                readerClock!.Unfreeze();
            else
                cluster.Network.Heal();

            for (int attempt = 0; attempt < 2; attempt++)
            {
                pinned.Seek(1000, SeekOrigin.Begin);
                await AssertExpiredAsync(() => pinned.ReadAsync(new byte[4096], Ct).AsTask(), $"read past the consumed prefix, attempt {attempt}");
                Assert.Equal(1000, pinned.Position);
                pinned.Seek(0, SeekOrigin.Begin);
                await AssertExpiredAsync(() => pinned.ReadAsync(new byte[16], Ct).AsTask(), $"re-read from the start, attempt {attempt}");
                await AssertExpiredAsync(() => pinned.ReadAsync(new byte[1], Ct).AsTask(), $"single-byte read, attempt {attempt}");
                await AssertExpiredAsync(() => pinned.GetInfoAsync(Ct).AsTask(), $"metadata of the expired view, attempt {attempt}");
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct); // the failure is stable, not a transient that later switches revisions
            }

            Exception? disposal = await Record.ExceptionAsync(() => pinned.DisposeAsync().AsTask().Within(cluster, "disposing an expired read handle completes"));
            Assert.False(disposal is TimeoutException, "disposal of the expired handle completes within the bound");
            await reader.DisposeAsync();

            await using DfsClient fresh = await ConnectAsync(cluster, "after");
            await using DfsStream current = await fresh.GetVolume(Vol).Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
            BytesEqual(b, await ReadWholeAsync(current), "a new pinned open sees the current revision");
        }, Ct);
    }

    private static async Task AssertExpiredAsync(Func<Task> action, string because)
    {
        bool returned = false;
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await action();
            returned = true;
        });
        Assert.False(returned, $"An operation on an expired pinned handle must fail rather than return data ({because}).");
        Assert.True(ex is DfsPinExpiredException { ErrorCode: DfsErrorCode.PinExpired } or DfsLeaseLostException { ErrorCode: DfsErrorCode.LeaseLost },
            $"Expected DfsPinExpiredException/DfsLeaseLostException ({because}) but got {ex?.GetType().FullName}{(ex is DfsException d ? $" ({d.ErrorCode})" : string.Empty)}: {ex?.Message}");
    }
}
