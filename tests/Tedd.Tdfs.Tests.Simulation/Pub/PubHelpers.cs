using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Pub;

/// <summary>Publication path exercised by a PUB simulation scenario.</summary>
public enum PublishPath
{
    /// <summary>Staged OnFlush stream changes published by <see cref="Stream.FlushAsync(CancellationToken)"/> (SDK-assigned operation id).</summary>
    StreamFlush,
    /// <summary>Staged OnFlush stream changes published by <see cref="DfsStream.CommitAsync(DfsMutationOptions, CancellationToken)"/> with a reserved operation id.</summary>
    StreamCommit,
    /// <summary>One <see cref="DfsWriteMode.WriteThrough"/> write call (SDK-assigned operation id).</summary>
    WriteThroughWrite,
    /// <summary><see cref="DfsFileClient.WriteAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/> with a reserved id.</summary>
    WriteAllHelper,
    /// <summary><see cref="DfsFileClient.AppendAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/> with a reserved id.</summary>
    AppendHelper,
}

/// <summary>Which <see cref="Stream"/> write overload a PUB test drives.</summary>
public enum WriteKind
{
    /// <summary><see cref="Stream.Write(byte[], int, int)"/> (needs realtime mode: synchronous network I/O).</summary>
    ArraySync,
    /// <summary><see cref="Stream.Write(ReadOnlySpan{byte})"/> (needs realtime mode).</summary>
    SpanSync,
    /// <summary><see cref="Stream.WriteAsync(byte[], int, int, CancellationToken)"/>.</summary>
    ArrayAsync,
    /// <summary><see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    MemoryAsync,
    /// <summary><see cref="Stream.WriteByte(byte)"/> per byte (needs realtime mode).</summary>
    ByteByByte,
}

/// <summary>
/// A prepared publication: the file holds <see cref="Initial"/> (committed), any stream staging is already done, and
/// <see cref="RunAsync"/> performs exactly the publishing call. <see cref="Expected"/> is the complete new revision.
/// </summary>
internal sealed class Publication
{
    public required PublishPath Kind { get; init; }
    public required string Path { get; init; }
    public required byte[] Initial { get; init; }
    public required byte[] Expected { get; init; }
    public DfsStream? Writer { get; init; }
    public DfsOperationId? ReservedId { get; init; }
    public required Func<CancellationToken, Task<CommitToken>> RunAsync { get; init; }

    /// <summary>The publish request type this path sends to the volume leader.</summary>
    public MessageType RequestType => PubHelpers.RequestTypeOf(Kind);

    /// <summary>The logical operation kind of the publish request.</summary>
    public DfsOperationKind OperationKind => PubHelpers.OperationKindOf(Kind);
}

/// <summary>Shared helpers for the PUB simulation tests (FSD §§6.3–6.4, 10.3, 13).</summary>
internal static class PubHelpers
{
    /// <summary>The harness volume.</summary>
    public const string Vol = "vol";

    /// <summary>NodeTimings.Fast logical update unit (chunk) size.</summary>
    public const int Chunk = 64 * 1024;

    /// <summary>A stream buffer large enough that staged test data stays in client memory until publication.</summary>
    public const int LargeBuffer = 4 << 20;

    /// <summary>NodeTimings.Fast handle lease.</summary>
    public static readonly TimeSpan Lease = TimeSpan.FromSeconds(2);

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Target 3 / ack 3 copies in 3 zones: one unavailable zone blocks success.</summary>
    public static DfsStoragePolicy StrongPolicy => new()
    {
        TargetCopies = 3,
        TargetDistinctZones = 3,
        DurableCopiesBeforeAck = 3,
        DurableZonesBeforeAck = 3,
    };

    /// <summary>The three-zone default policy (target 3 / ack 2 copies in 2 zones).</summary>
    public static DfsStoragePolicy ThreeZonePolicy => TestClusterOptions.ThreeZones.GetEffectiveDefaultPolicy();

    /// <summary>Deterministic simulation of <paramref name="options"/> (default: three zones).</summary>
    public static TestClusterOptions Deterministic(TestClusterOptions? options = null) =>
        (options ?? TestClusterOptions.ThreeZones) with { Mode = ClusterMode.Deterministic };

    /// <summary>
    /// Deterministic unless the scenario drives synchronous network-bound SDK members (which would block the single
    /// simulation thread); those run in realtime mode against the same in-memory transport and simulated storage.
    /// </summary>
    public static TestClusterOptions SimOrRealtime(bool usesSynchronousIo, TestClusterOptions? options = null) =>
        (options ?? TestClusterOptions.ThreeZones) with { Mode = usesSynchronousIo ? ClusterMode.Realtime : ClusterMode.Deterministic };

    /// <summary>True for write overloads that perform synchronous I/O.</summary>
    public static bool IsSynchronous(WriteKind kind) => kind is WriteKind.ArraySync or WriteKind.SpanSync or WriteKind.ByteByByte;

    /// <summary>Connects a named client.</summary>
    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, int? bufferSize = null, TimeSpan? operationTimeout = null,
        TestClock? clock = null) =>
        cluster.ConnectAsync(new TestClientOptions
        {
            Name = name,
            BufferSize = bufferSize,
            OperationTimeout = operationTimeout,
            Clock = clock,
        }, Ct);

    /// <summary>A writer open with <c>Share.Read</c>.</summary>
    public static DfsOpenOptions WriterOptions(FileMode mode = FileMode.Open, DfsWriteMode writeMode = DfsWriteMode.OnFlush,
        FileAccess access = FileAccess.ReadWrite, int bufferSize = 64 * 1024) => new()
        {
            Mode = mode,
            Access = access,
            Share = FileShare.Read,
            WriteMode = writeMode,
            BufferSize = bufferSize,
        };

    /// <summary>A live reader that tolerates a concurrent writer.</summary>
    public static DfsOpenOptions LiveReaderOptions() => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite,
        ReadView = DfsReadView.Live,
    };

    /// <summary>A version-pinned reader that tolerates a concurrent writer.</summary>
    public static DfsOpenOptions PinnedReaderOptions() => LiveReaderOptions() with { ReadView = DfsReadView.VersionPinned };

    /// <summary>Creates the parent directory (if needed) and commits <paramref name="content"/>.</summary>
    public static async Task<CommitToken> SeedFileAsync(DfsVolume volume, string path, byte[] content)
    {
        string parent = DfsPath.GetParent(path);
        if (parent != DfsPath.Root)
            await volume.Directories.CreateDirectoryAsync(parent, Ct);
        return await volume.Files.WriteAllBytesAsync(path, content, Ct);
    }

    /// <summary>Seeks and writes asynchronously.</summary>
    public static async Task WriteAtAsync(DfsStream stream, long offset, byte[] data, CancellationToken ct = default)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        await stream.WriteAsync(data.AsMemory(), ct == default ? Ct : ct);
    }

    /// <summary>Drives one write overload.</summary>
    public static async Task WriteAsync(DfsStream stream, byte[] data, WriteKind kind, CancellationToken ct)
    {
        switch (kind)
        {
            case WriteKind.ArraySync:
                stream.Write(data, 0, data.Length);
                break;
            case WriteKind.SpanSync:
                stream.Write(data.AsSpan());
                break;
            case WriteKind.ArrayAsync:
                await stream.WriteAsync(data, 0, data.Length, ct);
                break;
            case WriteKind.MemoryAsync:
                await stream.WriteAsync(data.AsMemory(), ct);
                break;
            case WriteKind.ByteByByte:
                foreach (byte b in data)
                    stream.WriteByte(b);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    /// <summary>Reads the stream's whole view from offset 0.</summary>
    public static async Task<byte[]> ReadFromStartAsync(DfsStream stream)
    {
        stream.Seek(0, SeekOrigin.Begin);
        using var ms = new MemoryStream();
        var buffer = new byte[64 * 1024];
        int n;
        while ((n = await stream.ReadAsync(buffer.AsMemory(), Ct)) > 0)
            ms.Write(buffer, 0, n);
        return ms.ToArray();
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Asserts the committed bytes/length (and optionally revision) seen by an independent observer.</summary>
    public static async Task<DfsFileInfo> AssertCommittedAsync(DfsVolume observer, string path, byte[] expected, string because,
        ulong? expectedRevision = null)
    {
        byte[] actual = await observer.Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, because);
        DfsFileInfo info = await observer.Files.GetInfoAsync(path, Ct);
        Assert.True(expected.LongLength == info.Length, $"Expected length {expected.LongLength} but observed {info.Length}: {because}");
        if (expectedRevision is { } revision)
            Assert.True(revision == info.Revision, $"Expected revision {revision} but observed {info.Revision}: {because}");
        return info;
    }

    /// <summary>Nodes whose replicated state can be inspected right now.</summary>
    public static IEnumerable<TestNode> RunningNodes(TestCluster cluster) =>
        cluster.Nodes.Where(n => n.State == TestNodeState.Running);

    /// <summary>
    /// Asserts every running replica's applied view of <paramref name="path"/> has <paramref name="revision"/> and
    /// <paramref name="length"/> (used to prove that nothing was published anywhere, not only at the leader).
    /// </summary>
    public static async Task AssertReplicasAtRevisionAsync(TestCluster cluster, string path, ulong revision, long length, string because)
    {
        foreach (TestNode node in RunningNodes(cluster))
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, node.Name);
            FileInspection? file = state.GetFile(path);
            Assert.True(file is not null, $"{node.Name} cannot resolve {path}: {because}");
            Assert.True(file.Revision == revision, $"{node.Name} applied revision {file.Revision}, expected {revision}: {because}");
            Assert.True(file.Length == length, $"{node.Name} applied length {file.Length}, expected {length}: {because}");
        }
    }

    /// <summary>Asserts none of <paramref name="objects"/> is reachable from any running replica's state.</summary>
    public static async Task AssertUnreachableAsync(TestCluster cluster, IEnumerable<ObjectId> objects, string because)
    {
        var set = objects.ToHashSet();
        if (set.Count == 0)
            return;
        foreach (TestNode node in RunningNodes(cluster))
        {
            IReadOnlySet<ObjectId> reachable = (await cluster.Inspect.GetVolumeStateAsync(Vol, node.Name)).ReachableObjects();
            ObjectId[] leaked = set.Where(reachable.Contains).ToArray();
            Assert.True(leaked.Length == 0, $"{node.Name} references unpublished object(s) {string.Join(", ", leaked)}: {because}");
        }
    }

    /// <summary>Objects referenced by the current revision of <paramref name="path"/> (empty for inline files).</summary>
    public static async Task<HashSet<ObjectId>> ReferencedObjectsAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.True(file is not null, $"Inspector cannot resolve {path}.");
        return file.Extents.Select(e => e.Object).ToHashSet();
    }

    /// <summary>Objects stored by any node that were not stored in <paramref name="before"/>.</summary>
    public static async Task<HashSet<ObjectId>> NewObjectsSinceAsync(TestCluster cluster, IReadOnlySet<ObjectId> before) =>
        (await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Where(o => !before.Contains(o)).ToHashSet();

    /// <summary>Asserts every object of the current revision of <paramref name="path"/> is durable on at least the policy's ack domains.</summary>
    public static async Task AssertDurableAsync(TestCluster cluster, string path, DfsStoragePolicy policy, string because)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.True(file is not null, $"Inspector cannot resolve {path}: {because}");
        foreach (ExtentInspection extent in file.Extents)
        {
            (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
            Assert.True(copies >= policy.DurableCopiesBeforeAck,
                $"Object {extent.Object} (offset {extent.Offset}) has {copies} durable copies; policy requires {policy.DurableCopiesBeforeAck}: {because}");
            Assert.True(zones >= policy.DurableZonesBeforeAck,
                $"Object {extent.Object} (offset {extent.Offset}) spans {zones} zones; policy requires {policy.DurableZonesBeforeAck}: {because}");
        }
    }

    /// <summary>Tries to take the writer reservation; false on a typed sharing/writer conflict.</summary>
    public static async Task<bool> TryOpenWriterAsync(DfsVolume volume, string path)
    {
        try
        {
            DfsStream probe = await volume.Files.OpenAsync(path, WriterOptions(FileMode.Open, access: FileAccess.Write), Ct);
            await probe.DisposeAsync();
            return true;
        }
        catch (DfsSharingViolationException)
        {
            return false;
        }
    }

    /// <summary>The publish request message type of a path.</summary>
    public static MessageType RequestTypeOf(PublishPath kind) => kind switch
    {
        PublishPath.WriteAllHelper => MessageType.WriteAll,
        PublishPath.AppendHelper => MessageType.Append,
        _ => MessageType.CommitFile,
    };

    /// <summary>The logical operation kind of a path's publish request.</summary>
    public static DfsOperationKind OperationKindOf(PublishPath kind) => kind switch
    {
        PublishPath.WriteAllHelper => DfsOperationKind.WriteAll,
        PublishPath.AppendHelper => DfsOperationKind.Append,
        _ => DfsOperationKind.CommitFile,
    };

    /// <summary>Publication requests (CommitFile/WriteAll/Append) sent by a client so far.</summary>
    public static long PublishRequests(TestCluster cluster, string clientName)
    {
        string from = "client:" + clientName;
        return cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.CommitFile))
            + cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.WriteAll))
            + cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.Append));
    }

    /// <summary>PutObject (data upload) requests sent by a client so far.</summary>
    public static long PutObjectsFrom(TestCluster cluster, string clientName, string? toNode = null) =>
        cluster.Network.Log.CountMessages(new MessageFilter(From: "client:" + clientName, To: toNode, Type: MessageType.PutObject));

    /// <summary>True when a published/durable backup boundary covers <paramref name="token"/> (same lineage, at or after it).</summary>
    public static bool Covers(CommitToken boundary, CommitToken token) =>
        !boundary.IsNone && boundary.IsComparableWith(token) && boundary.IsAtOrAfter(token);

    /// <summary>
    /// Waits (on the harness clock) for <paramref name="task"/> and returns its exception (null on success). Fails the
    /// test when it does not terminate within <paramref name="bound"/> — liveness waits are always bounded.
    /// </summary>
    public static async Task<Exception?> CaptureWithinAsync(TestCluster cluster, Task task, TimeSpan bound, string because)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task delay = cluster.DelayAsync(bound, cts.Token);
        Task first = await Task.WhenAny(task, delay);
        if (!ReferenceEquals(first, task))
            Assert.Fail($"Operation did not terminate within {bound}: {because}");
        cts.Cancel();
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>Asserts that <paramref name="task"/> is still pending after <paramref name="hold"/> of harness time.</summary>
    public static async Task AssertStillPendingAsync(TestCluster cluster, Task task, TimeSpan hold, string because)
    {
        await cluster.DelayAsync(hold, Ct);
        if (task.IsCompleted)
        {
            string detail = task.IsFaulted ? $"faulted: {task.Exception?.GetBaseException()}" : task.IsCanceled ? "cancelled" : "succeeded";
            Assert.Fail($"Operation completed ({detail}) while it must still be waiting: {because}");
        }
    }

    /// <summary>Crashes every live node and restarts all from their durable images.</summary>
    public static async Task CrashAndRestartAllAsync(TestCluster cluster, CrashMode mode = CrashMode.LoseUnflushed)
    {
        foreach (TestNode node in cluster.Nodes)
            await node.CrashAsync(mode);
        foreach (TestNode node in cluster.Nodes)
            await node.RestartAsync();
        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
    }

    /// <summary>Concatenates byte arrays.</summary>
    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }
        return result;
    }

    /// <summary>
    /// Seeds <paramref name="path"/> with <paramref name="initial"/> and prepares a publication of the given kind.
    /// Stream paths stage two distant edits plus a length extension (a multi-extent revision) unless
    /// <paramref name="aligned"/> is set, in which case the payload is written at offset 0 (chunk aligned).
    /// The stream buffer is <see cref="LargeBuffer"/>, so every upload happens inside the publishing call.
    /// </summary>
    public static async Task<Publication> PreparePublicationAsync(DfsVolume volume, string path, PublishPath kind, byte[] initial,
        byte[] payload, bool aligned = false)
    {
        await SeedFileAsync(volume, path, initial);
        DfsOperationId? reserved = kind is PublishPath.StreamCommit or PublishPath.WriteAllHelper or PublishPath.AppendHelper
            ? await volume.NewOperationIdAsync(Ct)
            : null;
        switch (kind)
        {
            case PublishPath.StreamFlush:
            case PublishPath.StreamCommit:
            {
                DfsStream writer = await volume.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: LargeBuffer), Ct);
                var model = new ReferenceFileModel(initial);
                if (aligned)
                {
                    await WriteAtAsync(writer, 0, payload);
                    model.Write(0, payload);
                }
                else
                {
                    int half = payload.Length / 2;
                    byte[] first = payload[..half];
                    byte[] second = payload[half..];
                    long secondOffset = initial.Length + 70_000;
                    await WriteAtAsync(writer, 0, first);
                    model.Write(0, first);
                    await WriteAtAsync(writer, secondOffset, second);
                    model.Write(secondOffset, second);
                    long newLength = model.StagedLength + 12_345;
                    await writer.SetLengthAsync(newLength, Ct);
                    model.SetLength(newLength);
                }

                return new Publication
                {
                    Kind = kind,
                    Path = path,
                    Initial = initial,
                    Expected = model.Staged,
                    Writer = writer,
                    ReservedId = reserved,
                    RunAsync = kind == PublishPath.StreamFlush
                        ? async ct =>
                        {
                            await writer.FlushAsync(ct);
                            return CommitToken.None;
                        }
                        : ct => writer.CommitAsync(new DfsMutationOptions { OperationId = reserved }, ct),
                };
            }
            case PublishPath.WriteThroughWrite:
            {
                DfsStream writer = await volume.Files.OpenAsync(path,
                    WriterOptions(FileMode.Open, DfsWriteMode.WriteThrough, bufferSize: LargeBuffer), Ct);
                var model = new ReferenceFileModel(initial);
                model.Write(0, payload);
                return new Publication
                {
                    Kind = kind,
                    Path = path,
                    Initial = initial,
                    Expected = model.Staged,
                    Writer = writer,
                    RunAsync = async ct =>
                    {
                        writer.Seek(0, SeekOrigin.Begin);
                        await writer.WriteAsync(payload.AsMemory(), ct);
                        return CommitToken.None;
                    },
                };
            }
            case PublishPath.WriteAllHelper:
                return new Publication
                {
                    Kind = kind,
                    Path = path,
                    Initial = initial,
                    Expected = payload,
                    ReservedId = reserved,
                    RunAsync = ct => volume.Files.WriteAllBytesAsync(path, payload, new DfsMutationOptions { OperationId = reserved }, ct),
                };
            case PublishPath.AppendHelper:
                return new Publication
                {
                    Kind = kind,
                    Path = path,
                    Initial = initial,
                    Expected = Concat(initial, payload),
                    ReservedId = reserved,
                    RunAsync = ct => volume.Files.AppendAllBytesAsync(path, payload, new DfsMutationOptions { OperationId = reserved }, ct),
                };
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    /// <summary>The operation id to query after an uncertain publication: the reserved one, else the one carried by the error.</summary>
    public static DfsOperationId OperationIdFor(Publication publication, DfsCommitOutcomeUnknownException unknown)
    {
        Assert.False(unknown.OperationId.IsEmpty, "an unknown-outcome error must carry the operation id");
        if (publication.ReservedId is { } reserved)
            Assert.Equal(reserved, unknown.OperationId);
        return unknown.OperationId;
    }

    /// <summary>
    /// Resolves an operation authoritatively (fencing it if still undecided) from an independent client and asserts the
    /// committed state matches the decision: Committed → <see cref="Publication.Expected"/> at the token's revision,
    /// Aborted → <see cref="Publication.Initial"/>. Returns the outcome.
    /// </summary>
    public static async Task<DfsOperationOutcome> ResolveAndVerifyAsync(TestCluster cluster, DfsVolume observer, Publication publication,
        DfsOperationId id, DfsOutcomeStatus? required = null)
    {
        DfsOperationOutcome outcome = await observer.QueryOperationAsync(id, resolve: true, Ct);
        Assert.Equal(id, outcome.Id);
        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
            $"resolve:true must yield a definite outcome, got {outcome.Status} ({outcome.Detail})");
        if (required is { } status)
            Assert.Equal(status, outcome.Status);
        if (outcome.Status == DfsOutcomeStatus.Committed)
        {
            Assert.False(outcome.Token.IsNone);
            DfsFileInfo info = await AssertCommittedAsync(observer, publication.Path, publication.Expected, "the resolved committed outcome must be fully visible");
            Assert.True(outcome.Token.Sequence == info.Revision,
                $"the file revision {info.Revision} must be the committed decision's sequence {outcome.Token.Sequence}");
        }
        else
        {
            await AssertCommittedAsync(observer, publication.Path, publication.Initial, "an aborted (fenced) publication must leave the old revision");
        }

        // A definite decision is final: a later query (and time) cannot flip it.
        await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
        DfsOperationOutcome again = await observer.QueryOperationAsync(id, resolve: false, Ct);
        Assert.Equal(outcome.Status, again.Status);
        Assert.Equal(outcome.Token, again.Token);
        await AssertCommittedAsync(observer, publication.Path,
            outcome.Status == DfsOutcomeStatus.Committed ? publication.Expected : publication.Initial,
            "a definite outcome must not change later");
        return outcome;
    }
}
