using System.Runtime.CompilerServices;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Pub.PubHelpers;

namespace Tedd.Tdfs.Tests.Unit.Pub;

/// <summary>Scenario selector for PUB-007.</summary>
public enum NoOpScenario
{
    /// <summary>A writable handle that never wrote anything.</summary>
    FreshHandleNoWrites,
    /// <summary>A writable handle after a real write that was already flushed.</summary>
    AfterFlushedWrite,
    /// <summary>An append handle after an appended suffix was already flushed.</summary>
    AppendAfterFlushedWrite,
    /// <summary>A WriteThrough handle after a (self-committing) write.</summary>
    WriteThroughAfterWrite,
    /// <summary>A read-only handle (flush is a documented no-op).</summary>
    ReadOnlyHandle,
}

/// <summary>Flush API used by PUB-007.</summary>
public enum FlushApi
{
    /// <summary><see cref="Stream.Flush()"/>.</summary>
    FlushSync,
    /// <summary><see cref="Stream.FlushAsync(CancellationToken)"/>.</summary>
    FlushAsync,
    /// <summary><see cref="DfsStream.CommitAsync(CancellationToken)"/>.</summary>
    CommitAsync,
}

/// <summary>How PUB-016 disposes the dirty stream.</summary>
public enum DisposeKind
{
    /// <summary>Explicit <see cref="Stream.Dispose()"/>.</summary>
    DisposeSync,
    /// <summary>Explicit <see cref="Stream.DisposeAsync"/>.</summary>
    DisposeAsync,
    /// <summary>A <c>using</c> block with synchronous open and write.</summary>
    UsingBlock,
    /// <summary>An <c>await using</c> block.</summary>
    AwaitUsingBlock,
}

/// <summary>What the discarded staging "B" in PUB-018 contains besides a write.</summary>
public enum StagedChange
{
    /// <summary>Data only.</summary>
    WriteOnly,
    /// <summary>Data plus a shrinking SetLength.</summary>
    WriteAndShrink,
    /// <summary>Data plus an extending SetLength.</summary>
    WriteAndExtend,
}

/// <summary>How PUB-020 opened the abandoned stream.</summary>
public enum AbandonKind
{
    /// <summary><see cref="DfsFileClient.OpenAsync(string, DfsOpenOptions, CancellationToken)"/> with Mode Open.</summary>
    OpenWithOptions,
    /// <summary><see cref="DfsFileClient.OpenWriteAsync"/> (no truncation).</summary>
    OpenWriteHelper,
    /// <summary><see cref="DfsFileClient.CreateAsync"/> (truncation committed at open).</summary>
    CreateHelper,
}

/// <summary>Where PUB-022 makes the buffered write fail partway through.</summary>
public enum StagedWriteFailure
{
    /// <summary>The client's first chunk upload of the call fails (and keeps failing).</summary>
    ClientChunkFirst,
    /// <summary>The second chunk upload fails (first chunk of the call already staged).</summary>
    ClientChunkSecond,
    /// <summary>The fourth chunk upload fails (three chunks of the call already staged).</summary>
    ClientChunkFourth,
    /// <summary>Storage nodes fail object writes with EIO after a few successful ones.</summary>
    ServerObjectWriteIoError,
    /// <summary>Storage nodes fail the durability primitive (flush) after a few successful ones.</summary>
    ServerFlushIoError,
    /// <summary>Storage nodes report ENOSPC after a few successful object writes.</summary>
    ServerDiskFull,
}

/// <summary>Limit scenario for PUB-024.</summary>
public enum LimitCase
{
    /// <summary>Stream staging of MaxPublishBytes + 1 bytes, then flush.</summary>
    StreamOverPublishLimit,
    /// <summary>Control: stream staging of exactly MaxPublishBytes.</summary>
    StreamAtPublishLimit,
    /// <summary>WriteAllBytes of MaxPublishBytes + 1 bytes.</summary>
    WriteAllOverPublishLimit,
    /// <summary>Control: WriteAllBytes of exactly MaxPublishBytes.</summary>
    WriteAllAtPublishLimit,
    /// <summary>AppendAllBytes of MaxPublishBytes + 1 bytes.</summary>
    AppendOverPublishLimit,
    /// <summary>WriteThrough write that would make the file MaxFileLength + 1 long.</summary>
    WriteThroughPastMaxFileLength,
    /// <summary>Control: WriteThrough write ending exactly at MaxFileLength.</summary>
    WriteThroughAtMaxFileLength,
    /// <summary>OnFlush SetLength(MaxFileLength + 1), then flush.</summary>
    SetLengthPastMaxFileLength,
    /// <summary>Transaction staging MaxTransactionStagedBytes + 1 bytes over two files, then commit.</summary>
    TransactionStagedOverLimit,
}

/// <summary>
/// PUB — buffering, flush, commit and disposal (test plan §7.6; FSD §§6.3–6.4, 10.3, 13). Unit-level SDK contract rows
/// run against an in-memory three-zone cluster (target 3 / ack 2 copies in 2 zones, three voters).
/// </summary>
public sealed class PubUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // PUB-001
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(initial length, write offset, write length, overload) around inline/chunk/buffer boundaries.</summary>
    public static TheoryData<int, long, int, WriteKind> Pub001Cases => new()
    {
        { 0, 0, 1, WriteKind.ByteByByte },
        { 100, 50, 4096, WriteKind.ArraySync },          // inline file grows across the inline threshold
        { 4095, 4095, 2, WriteKind.SpanSync },           // threshold - 1 -> threshold + 1
        { 4097, 0, 4097, WriteKind.ArrayAsync },         // extent file, overwrite
        { 70_000, 65_535, 2, WriteKind.MemoryAsync },    // straddles the first chunk boundary
        { 70_000, 100_000, 10, WriteKind.MemoryAsync },  // beyond EOF: staged hole + extension
        { 0, 0, Chunk - 1, WriteKind.ArraySync },        // just below the buffer: stays in client memory
        { 200_000, 1_000, 150_000, WriteKind.ArrayAsync }, // above the buffer: staged upload, still private
    };

    [Theory]
    [MemberData(nameof(Pub001Cases))]
    [Catalog("PUB-001", "U", "INV-02", "INV-03")]
    public Task PUB_001_OnFlushKeepsWritesPrivate(int initialLength, long offset, int writeLength, WriteKind kind) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub001/f";
            byte[] initial = TestData.Bytes(0x0001_0001, initialLength);
            byte[] data = TestData.Bytes(0x0001_0002, writeLength);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await ov.Files.GetInfoAsync(path, Ct);

            await using DfsStream liveReader = await ov.Files.OpenAsync(path, LiveReaderOptions(), Ct);
            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
            Assert.Equal(DfsWriteMode.OnFlush, writer.WriteMode);

            var model = new ReferenceFileModel(initial);
            writer.Seek(offset, SeekOrigin.Begin);
            await WriteAsync(writer, data, kind, Ct);
            model.Write(offset, data);

            // The owning writer reads its own staged version.
            Assert.Equal(model.StagedLength, await writer.GetLengthAsync(Ct));
            Assert.Equal(offset + writeLength, writer.Position);
            await AssertBytesAsync(model.Staged, ReadFromStartAsync(writer, Ct), "the owning writer must read its own staged modifications");
            Assert.True(writer.PendingBytes > 0, "an OnFlush write must leave staged, unpublished bytes");

            // Every other observer still sees the preceding committed revision.
            await AssertCommittedAsync(ov, path, initial, "another client must see the last committed version before flush", before.Revision);
            await AssertBytesAsync(initial, ReadFromStartAsync(liveReader, Ct), "a live reader handle opened before the write must see committed bytes only");
            await AssertBytesAsync(initial, wv.Files.ReadAllBytesAsync(path, Ct), "a different handle of the writing client must not see another handle's staging");
            FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, path);
            Assert.NotNull(inspected);
            Assert.Equal(before.Revision, inspected.Revision);
            Assert.Equal(initial.LongLength, inspected.Length);

            // Discarding the staging leaves the committed revision untouched.
            await writer.AbortAsync(Ct);
            await AssertCommittedAsync(ov, path, initial, "abort of an unflushed OnFlush write publishes nothing", before.Revision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-007
    // ------------------------------------------------------------------------------------------------------------

    private const int RepeatedFlushes = 5;

    /// <summary>(initial length, scenario, flush API).</summary>
    public static TheoryData<int, NoOpScenario, FlushApi> Pub007Cases => new()
    {
        { 0, NoOpScenario.FreshHandleNoWrites, FlushApi.FlushSync },
        { 100, NoOpScenario.FreshHandleNoWrites, FlushApi.FlushAsync },
        { 200_000, NoOpScenario.FreshHandleNoWrites, FlushApi.CommitAsync },
        { 100, NoOpScenario.AfterFlushedWrite, FlushApi.FlushAsync },
        { 4096, NoOpScenario.AfterFlushedWrite, FlushApi.FlushSync },
        { 200_000, NoOpScenario.AfterFlushedWrite, FlushApi.CommitAsync },
        { 100, NoOpScenario.AppendAfterFlushedWrite, FlushApi.FlushAsync },
        { 200_000, NoOpScenario.AppendAfterFlushedWrite, FlushApi.CommitAsync },
        { 70_000, NoOpScenario.AppendAfterFlushedWrite, FlushApi.FlushSync },
        { 5_000, NoOpScenario.WriteThroughAfterWrite, FlushApi.FlushAsync },
        { 5_000, NoOpScenario.WriteThroughAfterWrite, FlushApi.CommitAsync },
        { 5_000, NoOpScenario.ReadOnlyHandle, FlushApi.FlushSync },
        { 200_000, NoOpScenario.ReadOnlyHandle, FlushApi.FlushAsync },
    };

    [Theory]
    [MemberData(nameof(Pub007Cases))]
    [Catalog("PUB-007", "U", "INV-02", "INV-07")]
    public Task PUB_007_NoOpFlushDoesNotDuplicateData(int initialLength, NoOpScenario scenario, FlushApi api) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub007/f";
            byte[] initial = TestData.Bytes(0x0007_0001, initialLength);
            byte[] change = TestData.Bytes(0x0007_0002, 3_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);

            DfsOpenOptions open = scenario switch
            {
                NoOpScenario.ReadOnlyHandle => LiveReaderOptions(),
                NoOpScenario.AppendAfterFlushedWrite => AppendOptions(),
                NoOpScenario.WriteThroughAfterWrite => WriterOptions(FileMode.Open, DfsWriteMode.WriteThrough),
                _ => WriterOptions(FileMode.Open),
            };
            var model = new ReferenceFileModel(initial);
            await using DfsStream stream = await wv.Files.OpenAsync(path, open, Ct);
            switch (scenario)
            {
                case NoOpScenario.AfterFlushedWrite:
                    stream.Seek(1, SeekOrigin.Begin);
                    await stream.WriteAsync(change, Ct);
                    await stream.FlushAsync(Ct);
                    model.Write(1, change);
                    model.Flush();
                    break;
                case NoOpScenario.AppendAfterFlushedWrite:
                    await stream.WriteAsync(change, Ct);
                    await stream.FlushAsync(Ct);
                    model.Write(model.CommittedLength, change);
                    model.Flush();
                    break;
                case NoOpScenario.WriteThroughAfterWrite:
                    stream.Seek(1, SeekOrigin.Begin);
                    await stream.WriteAsync(change, Ct);
                    model.Write(1, change);
                    model.Flush();
                    break;
            }

            DfsFileInfo baseline = await AssertCommittedAsync(ov, path, model.Committed, "baseline after the last real publication");
            HashSet<ObjectId> baselineExtents = await ReferencedObjectsAsync(cluster, path);
            IReadOnlySet<ObjectId> baselineStored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);

            for (int i = 0; i < RepeatedFlushes; i++)
            {
                switch (api)
                {
                    case FlushApi.FlushSync:
                        stream.Flush();
                        break;
                    case FlushApi.FlushAsync:
                        await stream.FlushAsync(Ct);
                        break;
                    case FlushApi.CommitAsync:
                        CommitToken token = await stream.CommitAsync(Ct);
                        if (!token.IsNone)
                        {
                            CommitToken last = (await ov.GetInfoAsync(Ct)).LastCommit;
                            Assert.True(token.IsComparableWith(last), $"No-op commit token {token} is not comparable with the volume's {last}.");
                            Assert.True(last.IsAtOrAfter(token), $"No-op commit token {token} claims a publication after the volume's last commit {last}.");
                        }
                        break;
                }

                Assert.Equal(0, stream.PendingBytes);
                DfsFileInfo now = await ov.Files.GetInfoAsync(path, Ct);
                Assert.True(baseline.Revision == now.Revision, $"No-op flush #{i + 1} created revision {now.Revision} (baseline {baseline.Revision}).");
                Assert.Equal(baseline.Length, now.Length);
            }

            await AssertCommittedAsync(ov, path, model.Committed, "repeated no-op flushes must neither mutate, duplicate nor truncate", baseline.Revision);
            Assert.True(baselineExtents.SetEquals(await ReferencedObjectsAsync(cluster, path)), "no-op flushes must not remap any extent");
            IReadOnlySet<ObjectId> storedAfter = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            Assert.True(storedAfter.IsSubsetOf(baselineStored), "no-op flushes must not upload new objects");

            // Disposal is one more (no-op) flush: still no duplicate append or truncation.
            await stream.DisposeAsync();
            await AssertCommittedAsync(ov, path, model.Committed, "disposing after no-op flushes must not change the file", baseline.Revision);
            if (scenario == NoOpScenario.AppendAfterFlushedWrite)
                Assert.Equal(initial.LongLength + change.LongLength, (await ov.Files.GetInfoAsync(path, Ct)).Length);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-016
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(initial length, staged length, dispose style, append handle).</summary>
    public static TheoryData<int, int, DisposeKind, bool> Pub016Cases => new()
    {
        { 1_000, 100, DisposeKind.DisposeAsync, false },       // inline result
        { 1_000, 4_097, DisposeKind.DisposeSync, false },      // crosses the inline threshold
        { 50_000, 200_000, DisposeKind.DisposeAsync, false },  // partially uploaded before dispose
        { 50_000, 200_000, DisposeKind.AwaitUsingBlock, false },
        { 3_000, 3_000, DisposeKind.UsingBlock, true },        // append handle, sync path
        { 70_000, 150_000, DisposeKind.DisposeAsync, true },
    };

    [Theory]
    [MemberData(nameof(Pub016Cases))]
    [Catalog("PUB-016", "U", "INV-01", "INV-16")]
    public Task PUB_016_SuccessfulDisposeFlushes(int initialLength, int stagedLength, DisposeKind kind, bool append) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub016/f";
            const long writeOffset = 500;
            byte[] initial = TestData.Bytes(0x0016_0001, initialLength);
            byte[] data = TestData.Bytes(0x0016_0002, stagedLength);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await ov.Files.GetInfoAsync(path, Ct);

            DfsOpenOptions open = append ? AppendOptions() : WriterOptions(FileMode.Open);
            var model = new ReferenceFileModel(initial);
            model.Write(append ? initial.Length : writeOffset, data);
            model.Flush();

            DfsStream? disposed = null;
            switch (kind)
            {
                case DisposeKind.DisposeSync:
                {
                    DfsStream s = await wv.Files.OpenAsync(path, open, Ct);
                    if (!append)
                        s.Seek(writeOffset, SeekOrigin.Begin);
                    await s.WriteAsync(data, Ct);
                    Assert.True(s.PendingBytes > 0);
                    s.Dispose();
                    disposed = s;
                    break;
                }
                case DisposeKind.DisposeAsync:
                {
                    DfsStream s = await wv.Files.OpenAsync(path, open, Ct);
                    if (!append)
                        s.Seek(writeOffset, SeekOrigin.Begin);
                    await s.WriteAsync(data, Ct);
                    Assert.True(s.PendingBytes > 0);
                    await s.DisposeAsync();
                    disposed = s;
                    break;
                }
                case DisposeKind.UsingBlock:
                    using (DfsStream s = wv.Files.Open(path, open))
                    {
                        if (!append)
                            s.Seek(writeOffset, SeekOrigin.Begin);
                        s.Write(data, 0, data.Length);
                        Assert.True(s.PendingBytes > 0);
                        disposed = s;
                    }
                    break;
                case DisposeKind.AwaitUsingBlock:
                    await using (DfsStream s = await wv.Files.OpenAsync(path, open, Ct))
                    {
                        if (!append)
                            s.Seek(writeOffset, SeekOrigin.Begin);
                        await s.WriteAsync(data, Ct);
                        Assert.True(s.PendingBytes > 0);
                        disposed = s;
                    }
                    break;
            }

            Assert.NotNull(disposed);
            InodeId inode = disposed.InodeId;

            // Success means the pending bytes are published ...
            DfsFileInfo after = await AssertCommittedAsync(ov, path, model.Committed, "successful dispose must have flushed every pending byte");
            Assert.True(after.Revision > before.Revision, "dispose must publish a new revision");
            Assert.Equal(inode, after.InodeId);
            // ... meet the live durability policy ...
            await AssertDurableAsync(cluster, path, ThreeZonePolicy, "dispose returned success");
            // ... and the reservation was released before Dispose returned.
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.DoesNotContain(state.Handles, h => h.Inode == inode && !h.Expired);
            Assert.True(await TryOpenWriterAsync(ov, path), "the writer reservation must be released when Dispose succeeds");

            // A disposed stream rejects further I/O.
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await disposed.WriteAsync(new byte[] { 1 }, Ct));

            // Durable means durable images, not server memory.
            await CrashAndRestartAllAsync(cluster);
            await AssertCommittedAsync(ov, path, model.Committed, "the disposed revision must survive a cold restart of every node", after.Revision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-018
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(initial length, A length, B length, extra change in B).</summary>
    public static TheoryData<int, int, int, StagedChange> Pub018Cases => new()
    {
        { 0, 100, 100, StagedChange.WriteOnly },              // inline A, inline B
        { 10_000, 3_000, 200_000, StagedChange.WriteOnly },   // B exceeds the buffer: uploaded before abort
        { 200_000, 70_000, 5_000, StagedChange.WriteAndShrink },
        { 5_000, 100, 1, StagedChange.WriteAndExtend },
        { 100_000, 5_000, 300_000, StagedChange.WriteAndExtend },
    };

    [Theory]
    [MemberData(nameof(Pub018Cases))]
    [Catalog("PUB-018", "U", "INV-02", "INV-09")]
    public Task PUB_018_AbortDiscardsOnlyUncommittedChanges(int initialLength, int aLength, int bLength, StagedChange bChange) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub018/f";
            byte[] initial = TestData.Bytes(0x0018_0001, initialLength);
            byte[] a = TestData.Bytes(0x0018_0002, aLength);
            byte[] b = TestData.Bytes(0x0018_0003, bLength);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);

            var model = new ReferenceFileModel(initial);
            DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);

            // A: staged and flushed.
            writer.Seek(0, SeekOrigin.Begin);
            await writer.WriteAsync(a, Ct);
            await writer.FlushAsync(Ct);
            model.Write(0, a);
            model.Flush();
            DfsFileInfo infoA = await AssertCommittedAsync(ov, path, model.Committed, "A is committed by its flush");
            HashSet<ObjectId> refsA = await ReferencedObjectsAsync(cluster, path);
            IReadOnlySet<ObjectId> storedBeforeB = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);

            // B: staged only (possibly partially uploaded), then aborted.
            long bOffset = aLength / 2;
            writer.Seek(bOffset, SeekOrigin.Begin);
            await writer.WriteAsync(b, Ct);
            model.Write(bOffset, b);
            switch (bChange)
            {
                case StagedChange.WriteAndShrink:
                    long shrunk = model.StagedLength / 2;
                    await writer.SetLengthAsync(shrunk, Ct);
                    model.SetLength(shrunk);
                    break;
                case StagedChange.WriteAndExtend:
                    long extended = model.StagedLength + 100_000;
                    await writer.SetLengthAsync(extended, Ct);
                    model.SetLength(extended);
                    break;
            }

            Assert.True(writer.PendingBytes > 0, "B must be staged before the abort");
            await AssertBytesAsync(model.Staged, ReadFromStartAsync(writer, Ct), "the writer sees B before the abort");
            HashSet<ObjectId> bObjects = (await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Except(storedBeforeB).ToHashSet();

            await writer.AbortAsync(Ct);
            model.Abort();

            Assert.Equal(0, writer.PendingBytes);
            await AssertCommittedAsync(ov, path, model.Committed, "abort keeps the flushed A and discards the staged B", infoA.Revision);
            Assert.True(await TryOpenWriterAsync(ov, path), "abort must release the writer reservation");

            // Disposing the aborted stream must not resurrect B.
            Exception? disposeError = await Record.ExceptionAsync(async () => await writer.DisposeAsync());
            Assert.Null(disposeError);
            await AssertCommittedAsync(ov, path, model.Committed, "disposal after abort publishes nothing", infoA.Revision);

            // B is never published; A's objects remain referenced and durable.
            IReadOnlySet<ObjectId> reachable = (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects();
            Assert.Empty(bObjects.Intersect(reachable));
            Assert.True(refsA.SetEquals(await ReferencedObjectsAsync(cluster, path)), "the committed extent map of A must be unchanged by the abort");
            Assert.True(refsA.IsSubsetOf(reachable), "A's objects must stay reachable");
            await AssertDurableAsync(cluster, path, ThreeZonePolicy, "A remains durably committed");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-019
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(mode, access, file exists before the open, staged bytes that are then aborted).</summary>
    public static TheoryData<FileMode, FileAccess, bool, int> Pub019Cases => new()
    {
        { FileMode.CreateNew, FileAccess.ReadWrite, false, 100 },
        { FileMode.CreateNew, FileAccess.Write, false, 200_000 },
        { FileMode.Create, FileAccess.ReadWrite, false, 100 },
        { FileMode.Create, FileAccess.ReadWrite, true, 100 },
        { FileMode.Create, FileAccess.Write, true, 200_000 },
        { FileMode.Truncate, FileAccess.Write, true, 100 },
        { FileMode.Truncate, FileAccess.Write, true, 200_000 },
        { FileMode.OpenOrCreate, FileAccess.ReadWrite, false, 100 },
        { FileMode.OpenOrCreate, FileAccess.ReadWrite, true, 100 },   // control: no creating effect
        { FileMode.Append, FileAccess.Write, false, 100 },
        { FileMode.Append, FileAccess.Write, true, 200_000 },         // control: no creating effect
    };

    [Theory]
    [MemberData(nameof(Pub019Cases))]
    [Catalog("PUB-019", "U", "INV-02", "INV-07")]
    public Task PUB_019_AbortDoesNotUndoCreatingOpen(FileMode mode, FileAccess access, bool exists, int stagedLength) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub019/f";
            byte[] initial = TestData.Bytes(0x0019_0001, 50_000);
            byte[] staged = TestData.Bytes(0x0019_0002, stagedLength);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await wv.Directories.CreateDirectoryAsync("/pub019", Ct);
            InodeId? inodeBefore = null;
            if (exists)
            {
                await wv.Files.WriteAllBytesAsync(path, initial, Ct);
                inodeBefore = (await ov.Files.GetInfoAsync(path, Ct)).InodeId;
            }
            else
            {
                Assert.False(await ov.Files.ExistsAsync(path, Ct));
            }

            bool createsOrTruncates = mode is FileMode.Create or FileMode.CreateNew or FileMode.Truncate
                || (!exists && mode is FileMode.OpenOrCreate or FileMode.Append);
            byte[] afterOpen = createsOrTruncates ? Array.Empty<byte>() : initial;

            DfsStream stream = await wv.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = access, Share = FileShare.Read }, Ct);
            InodeId inode = stream.InodeId;
            ulong openRevision = stream.OpenRevision;
            if (inodeBefore is { } existing)
                Assert.Equal(existing, inode);

            // The creating/truncating open is one committed operation, visible to others at once.
            Assert.True(await ov.Files.ExistsAsync(path, Ct), "the open's namespace effect is committed when the open succeeds");
            DfsFileInfo opened = await AssertCommittedAsync(ov, path, afterOpen, "the open's create/truncate effect is committed", openRevision);
            Assert.Equal(inode, opened.InodeId);

            // Later staging is aborted.
            if (mode != FileMode.Append)
                stream.Seek(0, SeekOrigin.Begin);
            await stream.WriteAsync(staged, Ct);
            Assert.True(stream.PendingBytes > 0);
            await stream.AbortAsync(Ct);

            Assert.True(await ov.Files.ExistsAsync(path, Ct), "abort must not undo a committed create");
            DfsFileInfo afterAbort = await AssertCommittedAsync(ov, path, afterOpen, "abort must not undo the committed create/truncate, nor publish staging", openRevision);
            Assert.Equal(inode, afterAbort.InodeId);
            Assert.True(await TryOpenWriterAsync(ov, path), "abort must release the reservation taken by the open");

            await stream.DisposeAsync();
            DfsFileInfo final = await AssertCommittedAsync(ov, path, afterOpen, "disposal after abort neither publishes staging nor reverts the open", openRevision);
            Assert.Equal(inode, final.InodeId);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-020
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(staged length, how the stream was opened).</summary>
    public static TheoryData<int, AbandonKind> Pub020Cases => new()
    {
        { 100, AbandonKind.OpenWithOptions },
        { 4_097, AbandonKind.OpenWriteHelper },
        { 200_000, AbandonKind.OpenWithOptions },   // partially uploaded staging
        { 100, AbandonKind.CreateHelper },
        { 200_000, AbandonKind.CreateHelper },
    };

    [Theory]
    [MemberData(nameof(Pub020Cases))]
    [Catalog("PUB-020", "U", "INV-02", "INV-16")]
    public Task PUB_020_FinalizerDoesNotCommit(int stagedLength, AbandonKind kind) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub020/f";
            byte[] initial = TestData.Bytes(0x0020_0001, 30_000);
            byte[] staged = TestData.Bytes(0x0020_0002, stagedLength);
            DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);

            byte[] committedAfterOpen = kind == AbandonKind.CreateHelper ? Array.Empty<byte>() : initial;
            long publishesBefore = PublishRequests(cluster, "c0");
            ulong revisionAfterOpen;
            WeakReference weak;
            (weak, revisionAfterOpen) = await AbandonDirtyStreamAsync(wv, path, kind, staged);
            long publishesAfterOpen = PublishRequests(cluster, "c0");

            ForceFullCollection();
            Assert.False(weak.IsAlive,
                "the abandoned DfsStream must be collectable so its finalizer can run (otherwise this test proves nothing and its staged buffers leak)");

            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(publishesAfterOpen, PublishRequests(cluster, "c0"));
            await AssertCommittedAsync(ov, path, committedAfterOpen, "the finalizer must not publish staged bytes", revisionAfterOpen);

            // Client disposal is not a substitute for flushing abandoned streams either; the reservation is released only
            // by server-authoritative expiry/close.
            await writerClient.DisposeAsync();
            await cluster.WaitUntilAsync(() => TryOpenWriterAsync(ov, path), TimeSpan.FromSeconds(30),
                "the abandoned writer's reservation must be released by the server after its lease/session ends", Ct);
            Assert.Equal(publishesAfterOpen, PublishRequests(cluster, "c0"));
            Assert.True(publishesAfterOpen >= publishesBefore);
            await AssertCommittedAsync(ov, path, committedAfterOpen, "no publication after collection, client disposal or lease expiry", revisionAfterOpen);
        }, Ct);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<(WeakReference Stream, ulong OpenRevision)> AbandonDirtyStreamAsync(DfsVolume volume, string path, AbandonKind kind, byte[] staged)
    {
        DfsStream stream = kind switch
        {
            AbandonKind.OpenWithOptions => await volume.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct),
            AbandonKind.OpenWriteHelper => await volume.Files.OpenWriteAsync(path, Ct),
            AbandonKind.CreateHelper => await volume.Files.CreateAsync(path, Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
        await stream.WriteAsync(staged, Ct);
        Assert.True(stream.PendingBytes > 0, "the abandoned stream must be dirty");
        ulong revision = stream.OpenRevision;
        return (new WeakReference(stream), revision);
    }

    private static void ForceFullCollection()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
        GC.Collect();
    }

    // ------------------------------------------------------------------------------------------------------------
    // PUB-022
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(failure point, write overload). ByteByByte is excluded: a one-byte call cannot fail "partway".</summary>
    public static TheoryData<StagedWriteFailure, WriteKind> Pub022Cases => new()
    {
        { StagedWriteFailure.ClientChunkFirst, WriteKind.MemoryAsync },
        { StagedWriteFailure.ClientChunkSecond, WriteKind.MemoryAsync },
        { StagedWriteFailure.ClientChunkSecond, WriteKind.ArraySync },
        { StagedWriteFailure.ClientChunkFourth, WriteKind.ArrayAsync },
        { StagedWriteFailure.ClientChunkFourth, WriteKind.SpanSync },
        { StagedWriteFailure.ServerObjectWriteIoError, WriteKind.MemoryAsync },
        { StagedWriteFailure.ServerFlushIoError, WriteKind.ArrayAsync },
        { StagedWriteFailure.ServerDiskFull, WriteKind.ArraySync },
    };

    [Theory]
    [MemberData(nameof(Pub022Cases))]
    [Catalog("PUB-022", "U", "INV-02", "INV-07")]
    public Task PUB_022_FailedStagedWriteCannotLeakPartialCall(StagedWriteFailure failure, WriteKind kind) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            const string path = "/pub022/f";
            const long w2Offset = 20_000;
            byte[] initial = TestData.Bytes(0x0022_0001, 300_000);
            byte[] w1 = TestData.Bytes(0x0022_0002, 1_000);
            byte[] w2 = TestData.Bytes(0x0022_0003, 4 * Chunk + 1_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", bufferSize: DefaultBuffer);
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await ov.Files.GetInfoAsync(path, Ct);

            var model = new ReferenceFileModel(initial);
            DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: DefaultBuffer), Ct);
            writer.Seek(0, SeekOrigin.Begin);
            await writer.WriteAsync(w1, Ct);
            model.Write(0, w1);
            byte[] preCall = model.Staged;
            model.Write(w2Offset, w2);
            byte[] fullStaged = model.Staged;

            FaultArm arm = failure switch
            {
                StagedWriteFailure.ClientChunkFirst => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 1, Count: 10_000), FaultAction.IoError()),
                StagedWriteFailure.ClientChunkSecond => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 2, Count: 10_000), FaultAction.IoError()),
                StagedWriteFailure.ClientChunkFourth => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 4, Count: 10_000), FaultAction.IoError()),
                StagedWriteFailure.ServerObjectWriteIoError => cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Operation: DfsOperationKind.PutObject, Occurrence: 4, Count: 10_000), FaultAction.IoError()),
                StagedWriteFailure.ServerFlushIoError => cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Operation: DfsOperationKind.PutObject, Occurrence: 4, Count: 10_000), FaultAction.IoError()),
                StagedWriteFailure.ServerDiskFull => cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Operation: DfsOperationKind.PutObject, Occurrence: 4, Count: 10_000), FaultAction.DiskFull()),
                _ => throw new ArgumentOutOfRangeException(nameof(failure)),
            };

            writer.Seek(w2Offset, SeekOrigin.Begin);
            Exception? writeError = await Record.ExceptionAsync(() => WriteAsync(writer, w2, kind, Ct));
            byte[] expected;
            if (writeError is not null)
            {
                // The call failed partway through: it must be rolled back privately or fault the stream.
                cluster.Faults.AssertReached(arm);
                arm.Disarm();
                Assert.False(writeError is ArgumentException, $"A storage failure must not surface as an argument error: {writeError}");
                if (writer.IsFaulted)
                {
                    Assert.NotNull(writer.Fault);
                    Assert.NotNull(await Record.ExceptionAsync(() => writer.FlushAsync(Ct)));
                    Assert.NotNull(await Record.ExceptionAsync(async () => await writer.WriteAsync(new byte[] { 0x5A }, Ct)));
                    await Record.ExceptionAsync(async () => await writer.DisposeAsync());
                    expected = initial;
                }
                else
                {
                    await AssertBytesAsync(preCall, ReadFromStartAsync(writer, Ct), "a failed write that does not fault the stream must restore the preceding private state");
                    await writer.FlushAsync(Ct);
                    await writer.DisposeAsync();
                    expected = preCall;
                }
            }
            else
            {
                // The SDK accepted the call and deferred its uploads; the persistent fault must surface at publication.
                Exception? flushError = await Record.ExceptionAsync(() => writer.FlushAsync(Ct));
                cluster.Faults.AssertReached(arm);
                Assert.NotNull(flushError);
                arm.Disarm();
                Exception? disposeError = await Record.ExceptionAsync(async () => await writer.DisposeAsync());
                byte[] committed = await ov.Files.ReadAllBytesAsync(path, Ct);
                bool isInitial = committed.AsSpan().SequenceEqual(initial);
                bool isFull = committed.AsSpan().SequenceEqual(fullStaged);
                Assert.True(isInitial || isFull, "after a failed publication the file must be the old revision or the complete accepted staging, never a partial call");
                if (disposeError is null)
                    Assert.True(isFull, "a successful dispose must not silently drop an accepted write");
                expected = isFull ? fullStaged : initial;
            }

            // Exact equality with the old revision or the pre-call staging proves no byte of the failed call leaked.
            DfsFileInfo final = await AssertCommittedAsync(ov, path, expected, "no partial failed call may be published by flush or dispose");
            if (ReferenceEquals(expected, initial))
                Assert.Equal(before.Revision, final.Revision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-024
    // ------------------------------------------------------------------------------------------------------------

    private const long PublishLimit = 256 * 1024;
    private const long FileLengthLimit = 1024 * 1024;
    private const long TxStagedLimit = 256 * 1024;

    /// <summary>
    /// A second volume that limits only the file length, so a sparse extension to exactly MaxFileLength is not also
    /// counted against the (separately tested) publish-bytes limit.
    /// </summary>
    private const string LengthVol = "lenvol";

    /// <summary>All limit scenarios (over-by-one and exact-limit controls).</summary>
    public static TheoryData<LimitCase> Pub024Cases
    {
        get
        {
            var data = new TheoryData<LimitCase>();
            foreach (LimitCase value in Enum.GetValues<LimitCase>())
                data.Add(value);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Pub024Cases))]
    [Catalog("PUB-024", "U", "INV-02", "INV-16")]
    public Task PUB_024_PublicationLimitIsAtomic(LimitCase limitCase) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with
        {
            Volumes =
            [
                new TestVolumeSpec(Vol)
                {
                    Limits = new DfsVolumeLimits
                    {
                        MaxPublishBytes = PublishLimit,
                        MaxTransactionStagedBytes = TxStagedLimit,
                    },
                },
                new TestVolumeSpec(LengthVol)
                {
                    Limits = new DfsVolumeLimits { MaxFileLength = FileLengthLimit },
                },
            ],
        }, async cluster =>
        {
            const string path = "/pub024/f";
            const string txA = "/pub024/a";
            const string txB = "/pub024/b";
            byte[] initial = TestData.Bytes(0x0024_0001, 1_000);
            bool lengthCase = limitCase is LimitCase.WriteThroughPastMaxFileLength or LimitCase.WriteThroughAtMaxFileLength
                or LimitCase.SetLengthPastMaxFileLength;
            string volumeName = lengthCase ? LengthVol : Vol;
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient otherClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(volumeName);
            DfsVolume ov = otherClient.GetVolume(volumeName);

            // The limits are advertised.
            DfsVolumeLimits publishLimits = (await writerClient.GetVolume(Vol).GetInfoAsync(Ct)).Limits;
            Assert.Equal(PublishLimit, publishLimits.MaxPublishBytes);
            Assert.Equal(TxStagedLimit, publishLimits.MaxTransactionStagedBytes);
            DfsVolumeLimits lengthLimits = (await writerClient.GetVolume(LengthVol).GetInfoAsync(Ct)).Limits;
            Assert.Equal(FileLengthLimit, lengthLimits.MaxFileLength);

            await SeedFileAsync(wv, path, initial);
            await wv.Files.WriteAllBytesAsync(txA, initial, Ct);
            await wv.Files.WriteAllBytesAsync(txB, initial, Ct);
            DfsFileInfo before = await ov.Files.GetInfoAsync(path, Ct);
            HashSet<ObjectId> refsBefore = await ReferencedObjectsAsync(cluster, path, volumeName);

            byte[] overPublish = TestData.Bytes(0x0024_0002, (int)PublishLimit + 1);
            byte[] atPublish = TestData.Bytes(0x0024_0003, (int)PublishLimit);

            Exception? error = null;
            byte[] expected = initial;
            long expectedRequested = -1;
            long expectedLimit = PublishLimit;
            switch (limitCase)
            {
                case LimitCase.StreamOverPublishLimit:
                case LimitCase.StreamAtPublishLimit:
                {
                    byte[] data = limitCase == LimitCase.StreamOverPublishLimit ? overPublish : atPublish;
                    DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
                    error = await Record.ExceptionAsync(async () =>
                    {
                        writer.Seek(0, SeekOrigin.Begin);
                        await writer.WriteAsync(data, Ct);
                        await writer.FlushAsync(Ct);
                    });
                    if (error is null)
                    {
                        await writer.DisposeAsync();
                        expected = data;
                    }
                    else
                    {
                        await Record.ExceptionAsync(() => writer.AbortAsync(Ct));
                        await Record.ExceptionAsync(async () => await writer.DisposeAsync());
                    }
                    break;
                }
                case LimitCase.WriteAllOverPublishLimit:
                    error = await Record.ExceptionAsync(() => wv.Files.WriteAllBytesAsync(path, overPublish, Ct));
                    expectedRequested = PublishLimit + 1;
                    break;
                case LimitCase.WriteAllAtPublishLimit:
                    error = await Record.ExceptionAsync(() => wv.Files.WriteAllBytesAsync(path, atPublish, Ct));
                    expected = atPublish;
                    break;
                case LimitCase.AppendOverPublishLimit:
                    error = await Record.ExceptionAsync(() => wv.Files.AppendAllBytesAsync(path, overPublish, Ct));
                    expectedRequested = PublishLimit + 1;
                    break;
                case LimitCase.WriteThroughPastMaxFileLength:
                case LimitCase.WriteThroughAtMaxFileLength:
                {
                    long offset = limitCase == LimitCase.WriteThroughPastMaxFileLength ? FileLengthLimit : FileLengthLimit - 1;
                    await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, DfsWriteMode.WriteThrough), Ct);
                    writer.Seek(offset, SeekOrigin.Begin);
                    error = await Record.ExceptionAsync(async () => await writer.WriteAsync(new byte[] { 0xEE }, Ct));
                    expectedLimit = FileLengthLimit;
                    if (limitCase == LimitCase.WriteThroughPastMaxFileLength)
                    {
                        expectedRequested = FileLengthLimit + 1;
                    }
                    else
                    {
                        var model = new ReferenceFileModel(initial);
                        model.Write(offset, new byte[] { 0xEE });
                        expected = model.Staged;
                    }
                    break;
                }
                case LimitCase.SetLengthPastMaxFileLength:
                {
                    DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
                    error = await Record.ExceptionAsync(async () =>
                    {
                        await writer.SetLengthAsync(FileLengthLimit + 1, Ct);
                        await writer.FlushAsync(Ct);
                    });
                    expectedLimit = FileLengthLimit;
                    expectedRequested = FileLengthLimit + 1;
                    await Record.ExceptionAsync(() => writer.AbortAsync(Ct));
                    await Record.ExceptionAsync(async () => await writer.DisposeAsync());
                    break;
                }
                case LimitCase.TransactionStagedOverLimit:
                {
                    DfsTransaction tx = await wv.Transactions.BeginAsync(null, Ct);
                    int half = (int)(TxStagedLimit / 2);
                    error = await Record.ExceptionAsync(async () =>
                    {
                        await tx.Files.WriteAllBytesAsync(txA, TestData.Bytes(0x0024_0004, half), Ct);
                        await tx.Files.WriteAllBytesAsync(txB, TestData.Bytes(0x0024_0005, (int)TxStagedLimit - half + 1), Ct);
                        await tx.CommitAsync(Ct);
                    });
                    expectedLimit = TxStagedLimit;
                    await Record.ExceptionAsync(async () => await tx.DisposeAsync());
                    Assert.NotEqual(DfsTransactionState.Committed, tx.State);
                    await AssertCommittedAsync(ov, txA, initial, "an over-limit transaction must not publish any of its files");
                    await AssertCommittedAsync(ov, txB, initial, "an over-limit transaction must not publish any of its files");
                    break;
                }
            }

            bool isControl = limitCase is LimitCase.StreamAtPublishLimit or LimitCase.WriteAllAtPublishLimit or LimitCase.WriteThroughAtMaxFileLength;
            if (isControl)
            {
                Assert.Null(error);
                DfsFileInfo published = await AssertCommittedAsync(ov, path, expected, "a publication exactly at the advertised limit must succeed");
                Assert.True(published.Revision > before.Revision);
                return;
            }

            DfsLimitExceededException limit = Assert.IsAssignableFrom<DfsLimitExceededException>(error);
            Assert.Equal(DfsErrorCode.LimitExceeded, limit.ErrorCode);
            Assert.False(string.IsNullOrEmpty(limit.LimitName), "the typed limit failure must name the limit");
            Assert.Equal(expectedLimit, limit.Limit);
            Assert.True(limit.Requested > limit.Limit, $"Requested {limit.Requested} must exceed the limit {limit.Limit}.");
            if (expectedRequested >= 0)
                Assert.Equal(expectedRequested, limit.Requested);

            // No partial committed file revision.
            await AssertCommittedAsync(ov, path, initial, "exceeding a publication limit must not publish any part of the update", before.Revision);
            Assert.True(refsBefore.SetEquals(await ReferencedObjectsAsync(cluster, path, volumeName)), "the committed extent map must be unchanged");
            FileInspection? inspected = await cluster.Inspect.GetFileAsync(volumeName, path);
            Assert.NotNull(inspected);
            Assert.Equal(before.Revision, inspected.Revision);
            Assert.Equal(initial.LongLength, inspected.Length);
        }, Ct);
}
