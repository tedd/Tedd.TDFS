using System.Globalization;
using System.Security.Cryptography;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Con.ConHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Net;

/// <summary>
/// NET — connection faults and transport behaviour (level S).
/// FSD §§5, 6, 10, 12, 13; invariants INV-01, INV-02, INV-03, INV-06, INV-08, INV-17.
/// </summary>
public sealed class NetSimulationTests
{
    public static TheoryData<string, int> DropRequestCases()
    {
        var data = new TheoryData<string, int>();
        foreach (string mutation in NetHelpers.MutationNames)
        {
            data.Add(mutation, 1);
            data.Add(mutation, 3);
        }

        return data;
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-001 RequestDroppedBeforeDelivery
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-001", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(DropRequestCases))]
    public async Task NET_001_RequestDroppedBeforeDelivery(string mutation, int droppedCopies)
    {
        MutationCase m = NetHelpers.CreateMutation(mutation);
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await m.SetupAsync(volume);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);

            // The request (and its first retries) vanish before any server sees them.
            MessageRule drop = cluster.Network.DropNext(new MessageFilter(From: ClientNet("c0"), Type: m.RequestType), droppedCopies);
            (CommitToken token, Exception? error) = await CaptureValueAsync(() => m.InvokeAsync(volume, id, Ct));
            Assert.True(drop.Matched >= 1, "the request drop must have been reached");

            if (error is null)
            {
                // Success is only reported for an executed request: every dropped copy was consumed before the delivered
                // retry, and the operation took effect exactly once with the reported token.
                Assert.Equal(droppedCopies, drop.Matched);
                await NetHelpers.AssertExactlyOnceAsync(m, volume, id, token, null);
            }
            else
            {
                // The SDK transmitted the request and cannot know that it was lost in the network: it must not report a
                // definite failure, only that the commit may have happened.
                NetHelpers.AssertUnknownWithIdentity(m, error, id, "a transmitted request without any reply");
                drop.Cancel();
                DfsOperationOutcome seen = await m.QueryAsync(volume, id, false);
                if (m.SupportsRetry && seen.Status == DfsOutcomeStatus.NotFound)
                {
                    // Never executed and not fenced: the caller's retry with the same identity executes exactly once.
                    CommitToken retried = await m.InvokeAsync(volume, id, Ct);
                    await NetHelpers.AssertExactlyOnceAsync(m, volume, id, retried, null);
                }
                else
                {
                    DfsOperationOutcome outcome = await NetHelpers.AssertExactlyOnceAsync(m, volume, id, null, error);
                    if (m.SupportsRetry && outcome.Status == DfsOutcomeStatus.Committed)
                    {
                        CommitToken retried = await m.InvokeAsync(volume, id, Ct);
                        if (m.ReturnsCommitToken)
                            Assert.Equal(outcome.Token, retried);
                        await m.AssertAppliedOnceAsync(volume);
                    }
                }
            }

            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-002 RequestDeliveredReplyDropped
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> ReplyDropCases()
    {
        var data = new TheoryData<string, string>();
        foreach (string mutation in NetHelpers.MutationNames)
        {
            data.Add(mutation, "one-reply-lost");
            data.Add(mutation, "all-replies-lost");
            data.Add(mutation, "client-crash-after-send");
        }

        return data;
    }

    [Theory, Catalog("NET-002", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(ReplyDropCases))]
    public async Task NET_002_RequestDeliveredReplyDropped(string mutation, string mode)
    {
        MutationCase m = NetHelpers.CreateMutation(mutation);
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await m.SetupAsync(volume);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);

            // The request is delivered and executed; its reply (and, depending on the mode, every retry's reply) is lost.
            MessageRule drop = cluster.Network.DropReplies(new MessageFilter(From: ClientNet("c0"), Type: m.RequestType), mode == "one-reply-lost" ? 1 : 10_000);

            if (mode == "one-reply-lost")
            {
                (CommitToken token, Exception? error) = await CaptureValueAsync(() => m.InvokeAsync(volume, id, Ct));
                Assert.True(drop.Matched >= 1, "the reply drop must have been reached");

                // The SDK's retry with the same identity is answered from the outcome record (success), or the caller is
                // told that the commit may have happened; never a definite failure.
                if (error is not null)
                    NetHelpers.AssertUnknownWithIdentity(m, error, id, "a delivered request whose reply was lost");
                drop.Cancel();
                DfsOperationOutcome outcome = await NetHelpers.AssertExactlyOnceAsync(m, volume, id, error is null ? token : null, error);
                if (error is not null)
                    Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);   // it was executed; only the reply was lost
                await client.DisposeAsync();
                await RecoverAndConvergeAsync(cluster);
                return;
            }

            DfsVolume resolver = volume;
            if (mode == "client-crash-after-send")
            {
                // The process that sent the request dies while the request is in flight (test plan §9.2: drop every
                // response to the caller and crash its process); a new process resolves and retries with the persisted
                // identity. The delivered request executes although its sender is gone.
                Task<(CommitToken Value, Exception? Error)> abandoned = CaptureValueAsync(() => m.InvokeAsync(volume, id, Ct));
                await cluster.WaitUntilAsync(() => Task.FromResult(drop.Matched >= 1), ElectionBound, "the request must be delivered", Ct);
                cluster.KillClient(client);
                drop.Cancel();
                _ = abandoned;   // belongs to the dead process; its result is never observed
                DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "c1", OperationTimeout = ElectionBound }, Ct);
                resolver = other.GetVolume(Vol);
                await cluster.WaitUntilAsync(async () => (await m.QueryAsync(resolver, id, false)).Status == DfsOutcomeStatus.Committed, ElectionBound,
                    $"{m}: the delivered request must commit although its sender died", Ct);
            }
            else
            {
                (CommitToken _, Exception? error) = await CaptureValueAsync(() => m.InvokeAsync(volume, id, Ct));
                Assert.True(drop.Matched >= 1, "the reply drop must have been reached");

                // Every reply was lost: the caller must learn that the commit may have happened, carrying the identity.
                NetHelpers.AssertUnknownWithIdentity(m, error, id, "no reply was ever delivered");
                drop.Cancel();
            }

            DfsOperationOutcome committed = await m.QueryAsync(resolver, id, false);
            Assert.Equal(DfsOutcomeStatus.Committed, committed.Status);
            await m.AssertAppliedOnceAsync(resolver);

            if (m.SupportsRetry)
            {
                CommitToken retried = await m.InvokeAsync(resolver, id, Ct);
                if (m.ReturnsCommitToken)
                    Assert.Equal(committed.Token, retried);
                await m.AssertAppliedOnceAsync(resolver);
                if (m.InvokeChangedAsync is not null)
                {
                    await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(() => m.InvokeChangedAsync(resolver, id), DfsErrorCode.OperationIdMismatch);
                    await m.AssertAppliedOnceAsync(resolver);
                }
            }

            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-003 HalfOpenConnection
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-003", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("client-to-leader-requests-vanish")]
    [InlineData("leader-to-client-replies-vanish")]
    [InlineData("client-to-storage-node-vanishes")]
    [InlineData("leader-follower-link-vanishes")]
    public async Task NET_003_HalfOpenConnection(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            TimeSpan deadline = TimeSpan.FromSeconds(3);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = deadline }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] old = TestData.Bytes(0x3001, 50_000);
            await volume.Files.WriteAllBytesAsync("/h", old, Ct);
            await AssertContentAsync(volume, "/h", old, "warm-up: connections to the leader and storage nodes are pooled");
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);
            byte[] updated = TestData.Bytes(0x3002, 200_000);

            switch (variant)
            {
                case "client-to-leader-requests-vanish":
                    cluster.Network.BlackHole(ClientNet("c0"), leader.Name);
                    break;
                case "leader-to-client-replies-vanish":
                    cluster.Network.BlackHole(leader.Name, ClientNet("c0"));
                    break;
                case "client-to-storage-node-vanishes":
                    cluster.Network.BlackHole(ClientNet("c0"), followers[0].Name);
                    break;
                default:
                    cluster.Network.BlackHole(leader.Name, followers[0].Name);
                    cluster.Network.BlackHole(followers[0].Name, leader.Name);
                    break;
            }

            long start = cluster.Time.GetTimestamp();
            (CommitToken token, Exception? error) = await CaptureValueAsync(() => volume.Files.WriteAllBytesAsync("/h", updated, WithId(id), Ct));
            TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
            Assert.True(elapsed <= deadline + TimeSpan.FromSeconds(3), $"{variant}: the operation hung for {elapsed} on a half-open connection");

            switch (variant)
            {
                case "client-to-leader-requests-vanish":
                    AssertNoCommitClaimed(error, "requests to the leader never arrive");
                    break;
                case "leader-to-client-replies-vanish":
                    // No reply from the leader can arrive, so success cannot be observed. Whether the WriteAll itself was
                    // ever transmitted depends on earlier steps (e.g. a staging registration whose reply also vanished), so
                    // the outcome is checked against the authoritative resolution below.
                    AssertNoCommitClaimed(error, "replies from the leader never arrive");
                    break;
                default:
                    // Rerouted to the next placement target / committed with the other follower within the deadline.
                    Assert.True(error is null, $"{variant}: a healthy alternative path existed, but the write failed: {error}");
                    break;
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ElectionBound, Ct);
            if (error is null)
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"an acknowledged write resolved as {outcome.Status}");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    Assert.Equal(outcome.Token, token);
                Assert.Equal(token.Sequence, (await volume.Files.GetInfoAsync("/h", Ct)).Revision);
                await AssertContentAsync(volume, "/h", updated, "committed write");
                await NetHelpers.AssertDurablyPlacedAsync(cluster, "/h", "rerouted upload");
            }
            else if (variant == "client-to-leader-requests-vanish")
            {
                // Nothing the client sent to the leader arrived: the request can never have executed.
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
                await AssertContentAsync(volume, "/h", old, "a request that never arrived has no effect");
            }
            else
            {
                // The request may have executed: the resolution is definite, consistent with the report, and exactly once.
                await AssertResolvedConsistentlyAsync(volume, id, error,
                    () => AssertContentAsync(volume, "/h", updated, "the executed request took effect once"),
                    () => AssertContentAsync(volume, "/h", old, "an aborted request has no effect"),
                    $"{variant} write");
            }

            // The client recovers promptly after the network does (no stuck connection in its pool).
            long again = cluster.Time.GetTimestamp();
            await volume.Files.WriteAllBytesAsync("/h2", TestData.Bytes(0x3003, 1000), Ct);
            Assert.True(cluster.Time.GetElapsedTime(again) <= deadline, "the first operation after healing must not wait on a dead connection");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-004 ResetDuringObjectUpload
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string, int> UploadBoundaries => new()
    {
        // boundary, site, occurrence (for client.upload.chunk: the chunk's position in the upload order, -1 = last chunk)
        { "request-header", FaultSites.ClientRequestSend, 1 },
        { "payload-chunk-1-first", FaultSites.ClientUploadChunk, 0 },
        { "payload-chunk-2", FaultSites.ClientUploadChunk, 1 },
        { "payload-chunk-3-interior", FaultSites.ClientUploadChunk, 2 },
        { "payload-chunk-4", FaultSites.ClientUploadChunk, 3 },
        { "payload-chunk-5-last", FaultSites.ClientUploadChunk, -1 },
        { "server-payload-write", FaultSites.ObjectWrite, 1 },
        { "checksum-before-flush", FaultSites.ObjectBeforeFlush, 1 },
        { "finalization-after-flush", FaultSites.ObjectAfterFlushBeforeReceipt, 1 },
    };

    /// <summary>
    /// Resets the client's connections at one upload boundary of a five-chunk publication (four full 64 KiB logical update
    /// units plus a 100-byte tail). Chunk boundaries are located from a fault-free reference run of the same deterministic
    /// scenario: the hit number of the first upload of the selected chunk (so every chunk, including the short last one,
    /// is covered regardless of how many targets each chunk is sent to).
    /// </summary>
    [Theory, Catalog("NET-004", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(UploadBoundaries))]
    public async Task NET_004_ResetDuringObjectUpload(string boundary, string site, int occurrence)
    {
        int hit = occurrence;
        if (site == FaultSites.ClientUploadChunk)
        {
            IReadOnlyList<FaultContext> reference = await RunUploadResetScenarioAsync(boundary, site, occurrence: null);
            int[] chunks = reference.Select(c => c.Occurrence).Distinct().ToArray();
            Assert.True(chunks.Length >= 5, $"the publication must upload five chunks; the reference run uploaded {chunks.Length}");
            int chunk = occurrence < 0 ? chunks[^1] : chunks[occurrence];
            hit = 1 + reference.ToList().FindIndex(c => c.Occurrence == chunk);
            Log($"NET-004 {boundary}: chunk index {chunk} is upload hit {hit} of {reference.Count}");
        }

        await RunUploadResetScenarioAsync(boundary, site, hit);
    }

    private static async Task<IReadOnlyList<FaultContext>> RunUploadResetScenarioAsync(string boundary, string site, int? occurrence)
    {
        return await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] old = TestData.Bytes(0x4001, 3000);
            await volume.Files.WriteAllBytesAsync("/up", old, Ct);
            byte[] data = TestData.Bytes(0x4002, (4 * 64 * 1024) + 100);

            // A null occurrence is the reference run: an observer arm records every hit without changing behaviour.
            DfsOperationKind? operation = site == FaultSites.ClientUploadChunk ? null : DfsOperationKind.PutObject;
            FaultArm arm = occurrence is null
                ? cluster.Faults.Arm(new FaultMatch(site, Operation: operation, Count: 100_000), FaultAction.Delay(TimeSpan.Zero))
                : cluster.Faults.Arm(new FaultMatch(site, Operation: operation, Occurrence: occurrence.Value),
                    FaultAction.Custom(_ =>
                    {
                        cluster.Network.ResetConnections(ClientNet("c0"));
                        return Task.CompletedTask;
                    }));
            Exception? error = await CaptureAsync(async () =>
            {
                await using DfsStream stream = await volume.Files.OpenAsync("/up", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                await stream.WriteAsync(data, Ct);
                await stream.SetLengthAsync(data.Length, Ct);
                await stream.FlushAsync(Ct);
            });
            cluster.Faults.AssertReached(arm);
            IReadOnlyList<FaultContext> contexts = arm.Contexts.ToArray();
            Log($"NET-004 {boundary}{(occurrence is null ? " (reference)" : "")}: {(error is null ? "flush succeeded" : error.GetType().Name)}");
            if (occurrence is null)
            {
                Assert.True(error is null, $"the fault-free reference run must succeed: {error}");
                return contexts;
            }

            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, Ct);
            DfsVolume observed = observer.GetVolume(Vol);
            byte[] now = await observed.Files.ReadAllBytesAsync("/up", Ct);
            if (error is null)
                AssertEx.BytesEqual(data, now, "a successful flush publishes the complete revision");
            else
            {
                NetHelpers.AssertTypedFailure(error, boundary);
                Assert.True(now.AsSpan().SequenceEqual(old) || now.AsSpan().SequenceEqual(data), "a flush interrupted by a reset publishes all or nothing");
            }

            if (now.AsSpan().SequenceEqual(data))
            {
                // Every object of the published revision is complete and durably held per policy: no receipt that the
                // commit counted can belong to an incomplete object.
                await NetHelpers.AssertDurablyPlacedAsync(cluster, "/up", boundary);
                foreach (ExtentInfo extent in await NetHelpers.ExtentsAsync(cluster, "/up"))
                {
                    foreach (string holder in await cluster.Inspect.GetObjectHoldersAsync(extent.Object))
                    {
                        ObjectRecordInfo record = (await cluster.Node(holder).Inspect.ListObjectsAsync(Vol)).Single(r => r.Id == extent.Object);
                        Assert.Equal(extent.ObjectLength, record.Length);
                    }
                }
            }

            // No stored record anywhere (including orphans of the interrupted upload) fails verification.
            await cluster.Admin.StartScrubAsync(null, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            Assert.Equal(0, (await cluster.Inspect.GetTotalCountersAsync()).ChecksumFailures);

            // Liveness: the same data can be written once the connection is back.
            if (error is not null)
            {
                await volume.Files.WriteAllBytesAsync("/up", data, Ct);
                await AssertContentAsync(observed, "/up", data, "retry after the reset");
            }

            await RecoverAndConvergeAsync(cluster);
            return contexts;
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-005 ResetDuringRead
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string, DfsOperationKind, int> ReadBoundaries => new()
    {
        { "after-metadata-resolution", FaultSites.ClientReplyReceived, DfsOperationKind.Read, 1 },
        { "before-first-object", FaultSites.ClientRequestSend, DfsOperationKind.ReadObject, 1 },
        { "between-objects", FaultSites.ClientRequestSend, DfsOperationKind.ReadObject, 2 },
        { "before-last-object", FaultSites.ClientRequestSend, DfsOperationKind.ReadObject, 5 },
    };

    [Theory, Catalog("NET-005", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(ReadBoundaries))]
    public async Task NET_005_ResetDuringRead(string boundary, string site, DfsOperationKind operation, int occurrence)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader", OperationTimeout = ElectionBound }, Ct);
            DfsVolume writerVolume = writer.GetVolume(Vol);
            DfsVolume readerVolume = reader.GetVolume(Vol);

            // Five separately published extents (one object each).
            byte[][] chunks = Enumerable.Range(0, 5).Select(i => TestData.Bytes(0x5100 + (ulong)i, 64 * 1024)).ToArray();
            await using (DfsStream stream = await writerVolume.Files.CreateAsync("/r", Ct))
            {
                foreach (byte[] chunk in chunks)
                {
                    await stream.WriteAsync(chunk, Ct);
                    await stream.FlushAsync(Ct);
                }
            }

            byte[] oldContent = Concat(chunks);
            byte[] newContent = TestData.Bytes(0x5200, oldContent.Length);
            Assert.True((await NetHelpers.ExtentsAsync(cluster, "/r")).Select(e => e.Object).Distinct().Count() >= 5, "the file must span five objects");

            await using DfsStream handle = await readerVolume.Files.OpenAsync("/r", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct);
            bool overwritten = false;
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Operation: operation, Occurrence: occurrence), FaultAction.Custom(async _ =>
            {
                // A newer revision is committed while the read call is in progress, then the reader's connections reset.
                await writerVolume.Files.WriteAllBytesAsync("/r", newContent, Ct);
                overwritten = true;
                cluster.Network.ResetConnections(ClientNet("reader"));
            }));

            (byte[] all, List<(long Offset, byte[] Bytes)> calls) = await NetHelpers.ReadAllCallsAsync(handle, oldContent.Length, Ct);
            cluster.Faults.AssertReached(arm);
            Assert.True(overwritten);

            // The call in progress stays on its selected revision; every later call returns bytes of one revision only.
            Assert.NotEmpty(calls);
            AssertEx.BytesEqual(oldContent.AsSpan((int)calls[0].Offset, calls[0].Bytes.Length), calls[0].Bytes, $"{boundary}: the interrupted read call must stay on its revision");
            foreach ((long offset, byte[] bytes) in calls.Skip(1))
            {
                bool isOld = oldContent.AsSpan((int)offset, bytes.Length).SequenceEqual(bytes);
                bool isNew = newContent.AsSpan((int)offset, bytes.Length).SequenceEqual(bytes);
                Assert.True(isOld || isNew, $"{boundary}: read call at offset {offset} mixed revisions");
            }

            Assert.Equal(oldContent.Length, all.Length);

            // A new read observes the newer revision completely.
            handle.Seek(0, SeekOrigin.Begin);
            (byte[] again, _) = await NetHelpers.ReadAllCallsAsync(handle, newContent.Length, Ct);
            AssertEx.BytesEqual(newContent, again, "a later live read selects the newer revision");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-006 PartialProtocolFrame
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-006", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("error-reply-small")]
    [InlineData("error-reply-large")]
    public void NET_006_PartialProtocolFrame_Codec(string variant)
    {
        ErrorReplyMessage message = variant == "error-reply-small"
            ? new ErrorReplyMessage(DfsErrorCode.NotLeader, "not leader", new DfsOperationId(7, 9), true, "mem://n1", new DfsTransactionId(new DfsOperationId(7, 3)))
            : new ErrorReplyMessage(DfsErrorCode.Integrity, new string('x', 60_000) + "æ中", new DfsOperationId(1, 2), false);
        byte[] frame = MessageCodec.Encode(message);

        // The complete frame reconstructs exactly (decode + re-encode is the identity).
        IWireMessage decoded = MessageCodec.Decode(frame);
        Assert.Equal(message, Assert.IsType<ErrorReplyMessage>(decoded));
        AssertEx.BytesEqual(frame, MessageCodec.Encode(decoded), "re-encoding a decoded frame");

        // Every (or, for large frames, a sampled set of) strict prefix is rejected, never parsed as a shorter message.
        foreach (int length in PrefixLengths(frame.Length, exhaustive: variant == "error-reply-small", seed: 0x0601))
            Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(frame.AsSpan(0, length)));

        // The mirror image of a split: a frame followed by bytes of the next frame (coalesced delivery) is never parsed as
        // the first message with the rest silently ignored; the frame boundary belongs to the transport envelope.
        foreach (int extra in new[] { 1, 2, frame.Length / 2 })
        {
            byte[] coalesced = Concat(frame, frame.AsSpan(0, Math.Max(1, extra)).ToArray());
            Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(coalesced));
        }
    }

    [Fact, Catalog("NET-006", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    public async Task NET_006_PartialProtocolFrame()
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/cap-small", TestData.Bytes(0x6001, 50), Ct);
            byte[] big = TestData.Bytes(0x6002, 200_000);

            // Real frames captured from SDK traffic: a small metadata request and a large object upload.
            CapturedMessage small = await CaptureFirstAsync(cluster, new MessageFilter(From: ClientNet("c0"), Type: MessageType.GetInfo),
                () => volume.Files.GetInfoAsync("/cap-small", Ct));
            CapturedMessage large = await CaptureFirstAsync(cluster, new MessageFilter(From: ClientNet("c0"), Type: MessageType.PutObject),
                () => volume.Files.WriteAllBytesAsync("/cap-big", big, Ct));
            Assert.True(large.Frame.Length > 60 * 1024, "the upload frame must exceed one transport fragment");

            foreach (CapturedMessage captured in new[] { small, large })
            {
                byte[] frame = captured.Frame.ToArray();
                IWireMessage decoded = MessageCodec.Decode(frame);
                AssertEx.BytesEqual(frame, MessageCodec.Encode(decoded), $"{captured.Type}: decode/re-encode reconstructs the frame exactly");
                foreach (int length in PrefixLengths(frame.Length, exhaustive: frame.Length <= 512, seed: 0x0603))
                    Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(frame.AsSpan(0, length)));
            }

            // Through the transport: every split of the small frame and sampled splits of the large frame produce exactly
            // the same server result as the unsplit frame.
            await using (RawProtocolClient raw = await cluster.ConnectRawAsync(small.To, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                DfsErrorCode baseline = await raw.SendFrameAsync(small.Frame);
                for (int split = 1; split < small.Frame.Length; split++)
                    Assert.Equal(baseline, await raw.SendFrameInPiecesAsync(small.Frame, [split]));
                Assert.Equal(baseline, await raw.SendFrameInPiecesAsync(small.Frame, Enumerable.Range(1, small.Frame.Length - 1).ToArray()));
                Assert.True(raw.IsConnected, "valid split delivery must not break the connection");
            }

            await using (RawProtocolClient raw = await cluster.ConnectRawAsync(large.To, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                DfsErrorCode baseline = await raw.SendFrameAsync(large.Frame);
                var random = new SeededRandom(0x0604);
                int length = large.Frame.Length;
                var splitSets = new List<int[]>
                {
                    new[] { 1 }, new[] { 2 }, new[] { length - 1 },
                    new[] { (60 * 1024) - 1 }, new[] { 60 * 1024 }, new[] { (60 * 1024) + 1 },
                    new[] { 1, 2, 3, length - 2, length - 1 },
                };
                for (int s = 0; s < 24; s++)
                {
                    int count = 1 + random.Next(6);
                    splitSets.Add(Enumerable.Range(0, count).Select(_ => 1 + random.Next(length - 1)).Distinct().Order().ToArray());
                }

                foreach (int[] splits in splitSets)
                    Assert.Equal(baseline, await raw.SendFrameInPiecesAsync(large.Frame, splits));
                Assert.True(raw.IsConnected);
            }

            await AssertContentAsync(volume, "/cap-big", big, "the captured upload's file");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    private static async Task<CapturedMessage> CaptureFirstAsync(TestCluster cluster, MessageFilter filter, Func<Task> action)
    {
        MessageHold hold = cluster.Network.Hold(filter);
        Task running = action();
        await hold.WaitForAsync(1, ElectionBound);
        CapturedMessage captured = hold.Captured[0];
        hold.ReleaseAll();
        await running;
        hold.Dispose();
        return captured;
    }

    private static IEnumerable<int> PrefixLengths(int frameLength, bool exhaustive, ulong seed)
    {
        if (exhaustive)
            return Enumerable.Range(0, frameLength);
        var lengths = new SortedSet<int> { 0, 1, 2, 3, frameLength - 2, frameLength - 1 };
        foreach (int pivot in new[] { 60 * 1024, frameLength / 2 })
        {
            for (int delta = -1; delta <= 1; delta++)
            {
                if (pivot + delta > 0 && pivot + delta < frameLength)
                    lengths.Add(pivot + delta);
            }
        }

        var random = new SeededRandom(seed);
        for (int i = 0; i < 64; i++)
            lengths.Add(random.Next(frameLength));
        return lengths.Where(l => l >= 0 && l < frameLength);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-007 TruncatedFrameAtEof
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-007", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("request-truncated-then-eof")]
    [InlineData("object-reply-truncated-in-header")]
    [InlineData("object-reply-truncated-in-payload")]
    [InlineData("metadata-reply-truncated")]
    [InlineData("stream-object-reply-truncated")]
    public async Task NET_007_TruncatedFrameAtEof(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] big = TestData.Bytes(0x7001, 300_000);
            await volume.Files.WriteAllBytesAsync("/big", big, Ct);

            if (variant == "request-truncated-then-eof")
            {
                // Capture a real WriteAll request for a new file and never deliver it.
                DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                MessageHold hold = cluster.Network.Hold(new MessageFilter(From: ClientNet("c0"), Type: MessageType.WriteAll));
                Task<Exception?> pending = CaptureAsync(() => volume.Files.WriteAllBytesAsync("/trunc", TestData.Bytes(0x7002, 2000), WithId(id), Ct));
                await hold.WaitForAsync(1, ElectionBound);
                CapturedMessage captured = hold.Captured[0];
                hold.DropAll();
                Exception? error = await pending;
                hold.DropAll();   // retries captured meanwhile are discarded too, never released by Dispose
                hold.Dispose();

                // The SDK transmitted the request and never heard back: it cannot know the request was lost.
                AssertUnknownOutcome(error, id, "a transmitted WriteAll that never arrived");

                // The identity stays unresolved (not fenced) while truncated copies of its frame are delivered, so any
                // truncated frame wrongly accepted as a complete request would create the file.
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                byte[] frame = captured.Frame.ToArray();
                foreach (int delivered in new[] { 0, 1, 2, 3, frame.Length / 2, frame.Length - 1 })
                {
                    // End-of-stream inside the frame: transport/protocol failure, no effect.
                    await using (RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct))
                    {
                        Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                        DfsErrorCode code = await raw.SendPartialFrameAndCloseAsync(frame, delivered);
                        Assert.NotEqual(DfsErrorCode.None, code);
                        Assert.False(raw.IsConnected);
                    }

                    // A complete transport message carrying a truncated application frame is a protocol error.
                    if (delivered > 0)
                    {
                        await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
                        Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                        Assert.Equal(DfsErrorCode.ProtocolError, await raw.SendFrameAsync(frame.AsMemory(0, delivered)));
                    }

                    Assert.False(await volume.Files.ExistsAsync("/trunc", Ct), $"a truncated request ({delivered} of {frame.Length} bytes) must never create a file");
                }

                // No complete copy of the request was ever delivered: resolving the identity yields a definite abort.
                Assert.Equal(DfsOutcomeStatus.Aborted, (await volume.QueryOperationAsync(id, resolve: true, Ct)).Status);
                Assert.False(await volume.Files.ExistsAsync("/trunc", Ct));
                await AssertContentAsync(volume, "/big", big, "the cluster keeps serving");
                await RecoverAndConvergeAsync(cluster);
                return;
            }

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume readerVolume = reader.GetVolume(Vol);
            (MessageType type, int keep) = variant switch
            {
                "object-reply-truncated-in-header" => (MessageType.GetObjectResult, 1),
                "object-reply-truncated-in-payload" => (MessageType.GetObjectResult, 4096),
                "metadata-reply-truncated" => (MessageType.ReadRangeResult, 3),
                _ => (MessageType.GetObjectResult, 3000),
            };
            MessageRule truncate = cluster.Network.TruncateNext(new MessageFilter(To: ClientNet("reader"), Type: type), keep);

            // ReadRange carries live-read metadata only for live stream reads (whole-file helpers pin a revision through
            // their handle), so the metadata variant reads through a live stream like the stream variant.
            if (variant is "stream-object-reply-truncated" or "metadata-reply-truncated")
            {
                await using DfsStream stream = await readerVolume.Files.OpenAsync("/big", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
                (byte[] all, Exception? streamError) = (Array.Empty<byte>(), null);
                try
                {
                    (all, _) = await NetHelpers.ReadAllCallsAsync(stream, 32 * 1024, Ct);
                }
                catch (Exception ex)
                {
                    streamError = ex;
                }

                Assert.True(truncate.Matched >= 1, "the truncation must have been reached");
                if (streamError is null)
                    AssertEx.BytesEqual(big, all, "a stream that reached EOF must have returned every byte (never a premature EOF)");
                else
                    Assert.True(streamError is DfsProtocolException or IOException || IsUnavailability(streamError), $"truncation must surface as a transport/protocol failure: {streamError}");
            }
            else
            {
                (byte[]? bytes, Exception? error) = await CaptureValueAsync(() => readerVolume.Files.ReadAllBytesAsync("/big", Ct));
                Assert.True(truncate.Matched >= 1, "the truncation must have been reached");
                if (error is null)
                    AssertEx.BytesEqual(big, bytes!, "a successful read after a truncated reply must be complete (retried on a new connection)");
                else
                    Assert.True(error is DfsProtocolException or IOException || IsUnavailability(error), $"truncation must surface as a transport/protocol failure: {error}");
            }

            await AssertContentAsync(readerVolume, "/big", big, "subsequent reads are complete");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-008 DuplicateMutationRpc
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> DuplicateCases()
    {
        var data = new TheoryData<string, string>();
        foreach (string mutation in new[] { "append", "move", "copy", "create-snapshot", "write-all", "create-directory", "delete", "stream-commit", "tx-commit" })
        {
            data.Add(mutation, "duplicate-in-flight");
            data.Add(mutation, "replay-after-completion");
            if (NetHelpers.CreateMutation(mutation).SupportsRetry)
                data.Add(mutation, "two-clients-same-identity");
        }

        return data;
    }

    [Theory, Catalog("NET-008", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(DuplicateCases))]
    public async Task NET_008_DuplicateMutationRpc(string mutation, string mode)
    {
        MutationCase m = NetHelpers.CreateMutation(mutation);
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await m.SetupAsync(volume);
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);
            CommitToken token;

            switch (mode)
            {
                case "duplicate-in-flight":
                {
                    MessageRule duplicate = cluster.Network.DuplicateNext(new MessageFilter(From: ClientNet("c0"), Type: m.RequestType));
                    token = await m.InvokeAsync(volume, id, Ct);
                    Assert.Equal(1, duplicate.Matched);
                    break;
                }

                case "replay-after-completion":
                {
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(From: ClientNet("c0"), Type: m.RequestType));
                    Task<CommitToken> running = m.InvokeAsync(volume, id, Ct);
                    await hold.WaitForAsync(1, ElectionBound);
                    CapturedMessage captured = hold.Captured[0];
                    hold.ReleaseAll();
                    token = await running;
                    hold.Dispose();

                    // The identical request arrives again over a separate connection, sequentially and concurrently.
                    DfsErrorCode replay = await cluster.Network.ReplayAsync(captured);
                    Assert.True(replay is DfsErrorCode.None or DfsErrorCode.OperationExpired, $"a replay must return the stored outcome or an explicit expiry, got {replay}");
                    DfsErrorCode[] concurrent = await Task.WhenAll(cluster.Network.ReplayAsync(captured), cluster.Network.ReplayAsync(captured));
                    Assert.All(concurrent, code => Assert.True(code is DfsErrorCode.None or DfsErrorCode.OperationExpired, $"concurrent replay returned {code}"));
                    break;
                }

                default:
                {
                    // Two processes of the same principal submit the same identity and payload at the same time.
                    await using DfsClient second = await cluster.ConnectAsync(new TestClientOptions { Name = "c1", OperationTimeout = ElectionBound }, Ct);
                    DfsVolume secondVolume = second.GetVolume(Vol);
                    CommitToken[] tokens = await Task.WhenAll(m.InvokeAsync(volume, id, Ct), m.InvokeAsync(secondVolume, id, Ct));
                    if (m.ReturnsCommitToken)
                        Assert.Equal(tokens[0], tokens[1]);
                    token = tokens[0];
                    break;
                }
            }

            await NetHelpers.AssertExactlyOnceAsync(m, volume, id, token, error: null);
            await RecoverAndConvergeAsync(cluster);
            await m.AssertAppliedOnceAsync(volume);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-009 DelayedOldReply
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every reply of a first call is held until the caller's 1 s deadline has expired; a second call is then issued and,
    /// while it is outstanding, the stale replies of the abandoned call are delivered first (in capture order), followed by
    /// the new call's own reply. Correlation must route the stale replies nowhere: the outstanding call completes with its
    /// own result, and later calls are unaffected.
    /// </summary>
    [Theory, Catalog("NET-009", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("get-info")]
    [InlineData("read-range")]
    [InlineData("write-all")]
    public async Task NET_009_DelayedOldReply(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsVolume setupVolume = setup.GetVolume(Vol);
            await setupVolume.Files.WriteAllBytesAsync("/a", TestData.Bytes(0x9001, 100), Ct);
            await setupVolume.Files.WriteAllBytesAsync("/b", TestData.Bytes(0x9002, 200), Ct);
            byte[] big = TestData.Bytes(0x9003, 2 * 64 * 1024);
            await setupVolume.Files.WriteAllBytesAsync("/big", big, Ct);
            DfsFileInfo infoA = await setupVolume.Files.GetInfoAsync("/a", Ct);
            DfsFileInfo infoB = await setupVolume.Files.GetInfoAsync("/b", Ct);

            // A short deadline so the first call is abandoned while its replies are still held.
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(1) }, Ct);
            DfsVolume volume = client.GetVolume(Vol);

            switch (variant)
            {
                case "get-info":
                {
                    await volume.Files.GetInfoAsync("/b", Ct);   // warm the pooled connection to the leader
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.GetInfoResult));
                    (DfsFileInfo? first, Exception? firstError) = await CaptureValueAsync(() => volume.Files.GetInfoAsync("/a", Ct));
                    Assert.True(hold.Count >= 1, "the first call's reply must have been captured");
                    Assert.True(firstError is not null, $"no reply reached the caller, yet it returned {first?.Path}");
                    Assert.True(IsUnavailability(firstError!), $"an expired read must fail explicitly, got {firstError}");

                    int stale = hold.Count;
                    Task<DfsFileInfo> second = volume.Files.GetInfoAsync("/b", Ct);
                    await hold.WaitForAsync(stale + 1, ElectionBound);
                    hold.ReleaseAll();   // stale /a replies arrive first, while the /b call is outstanding
                    hold.Dispose();
                    DfsFileInfo secondInfo = await second;
                    Assert.Equal("/b", secondInfo.Path);
                    Assert.Equal(infoB.InodeId, secondInfo.InodeId);
                    Assert.Equal(200, secondInfo.Length);

                    DfsFileInfo third = await volume.Files.GetInfoAsync("/a", Ct);
                    Assert.Equal(infoA.InodeId, third.InodeId);
                    Assert.Equal(100, third.Length);
                    break;
                }

                case "read-range":
                {
                    await using DfsStream stream = await volume.Files.OpenAsync("/big", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.ReadRangeResult));
                    var firstBuffer = new byte[1000];
                    (int firstCount, Exception? firstError) = await CaptureValueAsync(() => stream.ReadAsync(firstBuffer, Ct).AsTask());
                    Assert.True(hold.Count >= 1, "the first read's reply must have been captured");
                    Assert.True(firstError is not null, $"no reply reached the caller, yet the read returned {firstCount} bytes");
                    Assert.True(IsUnavailability(firstError!), $"an expired read must fail explicitly, got {firstError}");
                    Assert.Equal(0, stream.Position);

                    int stale = hold.Count;
                    stream.Seek(70_000, SeekOrigin.Begin);
                    var secondBuffer = new byte[1000];
                    Task<int> second = stream.ReadAsync(secondBuffer, Ct).AsTask();
                    await hold.WaitForAsync(stale + 1, ElectionBound);
                    hold.ReleaseAll();   // the stale offset-0 replies arrive first, while the offset-70000 read is outstanding
                    hold.Dispose();
                    int secondCount = await second;
                    Assert.True(secondCount > 0);
                    AssertEx.BytesEqual(big.AsSpan(70_000, secondCount), secondBuffer.AsSpan(0, secondCount), "a later read must not be completed by the stale reply of an earlier one");
                    Assert.Equal(70_000 + secondCount, stream.Position);

                    stream.Seek(100, SeekOrigin.Begin);
                    int thirdCount = await stream.ReadAsync(secondBuffer, Ct);
                    AssertEx.BytesEqual(big.AsSpan(100, thirdCount), secondBuffer.AsSpan(0, thirdCount), "read after the stale replies arrived");
                    break;
                }

                default:
                {
                    DfsOperationId k1 = await volume.NewOperationIdAsync(Ct);
                    DfsOperationId k2 = await volume.NewOperationIdAsync(Ct);
                    byte[] v1 = TestData.Bytes(0x9011, 3000);
                    byte[] v2 = TestData.Bytes(0x9012, 3000);
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.WriteAllResult));
                    (CommitToken _, Exception? e1) = await CaptureValueAsync(() => volume.Files.WriteAllBytesAsync("/w", v1, WithId(k1), Ct));
                    Assert.True(hold.Count >= 1, "the first write's reply must have been captured");

                    // The first write was delivered and executed, but no reply reached the caller.
                    AssertUnknownOutcome(e1, k1, "a WriteAll whose replies were all held past its deadline");

                    int stale = hold.Count;
                    Task<CommitToken> second = volume.Files.WriteAllBytesAsync("/w", v2, WithId(k2), Ct);
                    await hold.WaitForAsync(stale + 1, ElectionBound);
                    hold.ReleaseAll();   // the stale k1 results arrive first, while the k2 write is outstanding
                    hold.Dispose();
                    CommitToken t2 = await second;

                    // t2 is the second write's own token: it names the revision that wrote v2 (an oracle independent of
                    // outcome retention) and differs from the first write's still-unacknowledged committed outcome.
                    Assert.Equal(t2.Sequence, (await volume.Files.GetInfoAsync("/w", Ct)).Revision);
                    DfsOperationOutcome o1 = await volume.QueryOperationAsync(k1, resolve: true, Ct);
                    Assert.Equal(DfsOutcomeStatus.Committed, o1.Status);
                    Assert.NotEqual(o1.Token, t2);
                    Assert.True(o1.Token.CompareTo(t2) < 0, "the first write committed before the second");
                    await AssertContentAsync(volume, "/w", v2, "the later write is the final state");
                    break;
                }
            }

            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-010 OutOfOrderResponses
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-010", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("get-info")]
    [InlineData("write-all")]
    [InlineData("stream-reads")]
    [InlineData("mixed")]
    public async Task NET_010_OutOfOrderResponses(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            const int n = 6;
            var contents = new byte[n][];
            for (int i = 0; i < n; i++)
            {
                contents[i] = TestData.Bytes(0xA000 + (ulong)i, 100 + (i * 37));
                await volume.Files.WriteAllBytesAsync("/f" + i.ToString(CultureInfo.InvariantCulture), contents[i], Ct);
            }

            var infos = new DfsFileInfo[n];
            for (int i = 0; i < n; i++)
                infos[i] = await volume.Files.GetInfoAsync("/f" + i.ToString(CultureInfo.InvariantCulture), Ct);

            switch (variant)
            {
                case "get-info":
                {
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.GetInfoResult));
                    Task<DfsFileInfo>[] calls = Enumerable.Range(0, n).Select(i => volume.Files.GetInfoAsync("/f" + i.ToString(CultureInfo.InvariantCulture), Ct)).ToArray();
                    await hold.WaitForAsync(n, ElectionBound);
                    hold.ReleaseAll(reverseOrder: true);
                    hold.Dispose();   // stop capturing before awaiting, so any further round trip is not held forever
                    DfsFileInfo[] results = await Task.WhenAll(calls);
                    for (int i = 0; i < n; i++)
                    {
                        Assert.Equal("/f" + i.ToString(CultureInfo.InvariantCulture), results[i].Path);
                        Assert.Equal(infos[i].InodeId, results[i].InodeId);
                        Assert.Equal(contents[i].Length, results[i].Length);
                    }

                    break;
                }

                case "write-all":
                {
                    var ids = new DfsOperationId[n];
                    var updated = new byte[n][];
                    for (int i = 0; i < n; i++)
                    {
                        ids[i] = await volume.NewOperationIdAsync(Ct);
                        updated[i] = TestData.Bytes(0xA100 + (ulong)i, 300 + i);
                    }

                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.WriteAllResult));
                    Task<CommitToken>[] calls = Enumerable.Range(0, n)
                        .Select(i => volume.Files.WriteAllBytesAsync("/f" + i.ToString(CultureInfo.InvariantCulture), updated[i], WithId(ids[i]), Ct)).ToArray();
                    await hold.WaitForAsync(n, ElectionBound);
                    hold.ReleaseAll(reverseOrder: true);
                    hold.Dispose();
                    CommitToken[] tokens = await Task.WhenAll(calls);
                    Assert.Equal(n, tokens.Distinct().Count());
                    for (int i = 0; i < n; i++)
                    {
                        // Each caller received its own token: the one naming the revision of its own file (independent of
                        // outcome retention), and the one its outcome record holds while retained.
                        string path = "/f" + i.ToString(CultureInfo.InvariantCulture);
                        Assert.Equal(tokens[i].Sequence, (await volume.Files.GetInfoAsync(path, Ct)).Revision);
                        DfsOperationOutcome outcome = await volume.QueryOperationAsync(ids[i], resolve: false, Ct);
                        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"{ids[i]}: {outcome.Status}");
                        if (outcome.Status == DfsOutcomeStatus.Committed)
                            Assert.Equal(outcome.Token, tokens[i]);
                        await AssertContentAsync(volume, path, updated[i], "each caller's own write");
                    }

                    break;
                }

                case "stream-reads":
                {
                    byte[] big = TestData.Bytes(0xA200, n * 8192);
                    await volume.Files.WriteAllBytesAsync("/big", big, Ct);
                    var streams = new DfsStream[n];
                    for (int i = 0; i < n; i++)
                    {
                        streams[i] = await volume.Files.OpenAsync("/big", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
                        streams[i].Seek(i * 8192, SeekOrigin.Begin);
                    }

                    var buffers = Enumerable.Range(0, n).Select(_ => new byte[1000]).ToArray();
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.ReadRangeResult));
                    Task<int>[] reads = Enumerable.Range(0, n).Select(i => streams[i].ReadAsync(buffers[i], Ct).AsTask()).ToArray();
                    await hold.WaitForAsync(n, ElectionBound);
                    hold.ReleaseAll(reverseOrder: true);
                    hold.Dispose();
                    int[] counts = await Task.WhenAll(reads);
                    for (int i = 0; i < n; i++)
                    {
                        Assert.True(counts[i] > 0);
                        AssertEx.BytesEqual(big.AsSpan(i * 8192, counts[i]), buffers[i].AsSpan(0, counts[i]), $"stream {i} must receive its own offset's bytes");
                        Assert.Equal((i * 8192) + counts[i], streams[i].Position);
                        await streams[i].DisposeAsync();
                    }

                    break;
                }

                default:
                {
                    DfsOperationId writeId = await volume.NewOperationIdAsync(Ct);
                    byte[] written = TestData.Bytes(0xA300, 500);
                    MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Direction: LinkDirection.Reply));
                    Task<DfsFileInfo> info = volume.Files.GetInfoAsync("/f0", Ct);
                    Task<bool> exists = volume.Files.ExistsAsync("/f1", Ct);
                    Task<bool> missing = volume.Files.ExistsAsync("/absent", Ct);
                    Task<CommitToken> write = volume.Files.WriteAllBytesAsync("/f2", written, WithId(writeId), Ct);
                    Task<DfsDirectoryInfo> mkdir = volume.Directories.CreateDirectoryAsync("/newdir", Ct);
                    await hold.WaitForAsync(5, ElectionBound);
                    hold.ReleaseAll(reverseOrder: true);
                    hold.Dispose();   // a call needing a further round trip (e.g. an identity reservation) is not held forever
                    await Task.WhenAll(info, exists, missing, write, mkdir);
                    Assert.Equal("/f0", info.Result.Path);
                    Assert.Equal(infos[0].InodeId, info.Result.InodeId);
                    Assert.True(exists.Result);
                    Assert.False(missing.Result);
                    Assert.Equal(write.Result.Sequence, (await volume.Files.GetInfoAsync("/f2", Ct)).Revision);
                    DfsOperationOutcome writeOutcome = await volume.QueryOperationAsync(writeId, resolve: false, Ct);
                    if (writeOutcome.Status == DfsOutcomeStatus.Committed)
                        Assert.Equal(writeOutcome.Token, write.Result);
                    Assert.Equal("/newdir", mkdir.Result.Path);
                    await AssertContentAsync(volume, "/f2", written, "mixed write");
                    break;
                }
            }

            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-011 AsymmetricPartition
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-011", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("leader-to-one-follower-cut")]
    [InlineData("one-follower-to-leader-cut")]
    [InlineData("leader-outbound-cut")]
    [InlineData("leader-inbound-cut")]
    [InlineData("client-to-leader-cut")]
    [InlineData("storage-receipts-cut")]
    public async Task NET_011_AsymmetricPartition(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            bool clientCut = variant == "client-to-leader-cut";
            await using DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer", OperationTimeout = clientCut ? ShortOperationTimeout : ElectionBound }, Ct);
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume writerVolume = writer.GetVolume(Vol);
            DfsVolume readerVolume = reader.GetVolume(Vol);
            byte[] v0 = TestData.Bytes(0xB000, 300);
            await writerVolume.Files.WriteAllBytesAsync("/reg", v0, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            TestNode cutFollower = followers[0];
            TestNode healthyFollower = followers[1];

            // Identities and values are prepared before the cut (a cut can make reserving identities impossible).
            bool large = variant == "storage-receipts-cut";
            var planned = new List<(DfsOperationId Id, byte[] Value)>();
            for (int i = 1; i <= 6; i++)
                planned.Add((await writerVolume.NewOperationIdAsync(Ct), TestData.Bytes(0xB000 + (ulong)i, large ? 70_000 : 300)));

            switch (variant)
            {
                case "leader-to-one-follower-cut":
                    cluster.Network.Cut(leader.Name, cutFollower.Name);
                    break;
                case "one-follower-to-leader-cut":
                    cluster.Network.Cut(cutFollower.Name, leader.Name);
                    break;
                case "leader-outbound-cut":
                    cluster.Network.Cut(leader.Name, followers[0].Name);
                    cluster.Network.Cut(leader.Name, followers[1].Name);
                    break;
                case "leader-inbound-cut":
                    cluster.Network.Cut(followers[0].Name, leader.Name);
                    cluster.Network.Cut(followers[1].Name, leader.Name);
                    break;
                case "client-to-leader-cut":
                    cluster.Network.Cut(ClientNet("writer"), leader.Name);
                    break;
                default:
                    cluster.Network.Cut(cutFollower.Name, ClientNet("writer"));
                    break;
            }

            var writes = new List<(long HistoryId, DfsOperationId OperationId, byte[] Value, CommitToken? Token)>();

            async Task WriterAsync()
            {
                for (int i = 0; i < planned.Count; i++)
                {
                    (DfsOperationId id, byte[] value) = planned[i];
                    long historyId = cluster.History.Invoke("writer", "write", "/reg", value, id);
                    (CommitToken written, Exception? error) = await CaptureValueAsync(() => writerVolume.Files.WriteAllBytesAsync("/reg", value, WithId(id), Ct));
                    CommitToken? token = error is null ? written : null;
                    if (error is null)
                    {
                        cluster.History.Complete(historyId, OpOutcome.Ok, written);

                        // Assertions run outside the operation's try/catch, so a violation fails the test rather than being
                        // recorded as a failed write.
                        if (variant is "leader-to-one-follower-cut" or "one-follower-to-leader-cut")
                        {
                            // The acknowledgement must come from a completed exchange: the healthy follower holds the entry.
                            Assert.True((await GroupOnAsync(healthyFollower)).LastIndex >= written.Sequence,
                                $"write {i} was acknowledged without the follower whose exchange could complete");
                            if (variant == "leader-to-one-follower-cut")
                                Assert.True((await GroupOnAsync(cutFollower)).LastIndex < written.Sequence, "the cut follower cannot have received the entry");
                        }
                    }
                    else
                    {
                        cluster.History.Complete(historyId, HistoryRecorder.Classify(error), error);
                    }

                    writes.Add((historyId, id, value, token));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(150), Ct);
                }
            }

            async Task ReaderAsync()
            {
                for (int i = 0; i < 8; i++)
                {
                    await CaptureAsync(() => cluster.History.RecordAsync<byte[]>("reader", "read", "/reg", null,
                        () => readerVolume.Files.ReadAllBytesAsync("/reg", Ct)));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(120), Ct);
                }
            }

            await Task.WhenAll(WriterAsync(), ReaderAsync());
            await cluster.Inspect.AssertNoSplitBrainAsync();

            int succeeded = writes.Count(w => w.Token is not null);
            switch (variant)
            {
                case "leader-to-one-follower-cut":
                case "one-follower-to-leader-cut":
                case "storage-receipts-cut":
                    Assert.Equal(writes.Count, succeeded);
                    break;
                case "client-to-leader-cut":
                    Assert.Equal(0, succeeded);
                    break;
                default:
                    Assert.True(succeeded >= 1, "after the asymmetric cut the followers must elect a leader and accept writes");
                    break;
            }

            if (variant == "storage-receipts-cut")
                await NetHelpers.AssertDurablyPlacedAsync(cluster, "/reg", "receipts are counted only from completed exchanges");

            if (variant is "leader-outbound-cut" or "leader-inbound-cut")
            {
                // A leader that can only send (or only receive) completes no protocol exchange with any follower: one-way
                // reachability must not keep it acting as leader.
                await cluster.WaitUntilAsync(async () => await LeaderAmongAsync([leader]) is null, ElectionBound,
                    $"[{variant}] the old leader must stop leading once its exchanges cannot complete", Ct);
                TestNode newLeader = await WaitForLeaderAmongAsync(cluster, followers, $"[{variant}] the followers must elect a leader");
                Assert.NotEqual(leader.Name, newLeader.Name);
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ElectionBound, Ct);
            foreach ((long historyId, DfsOperationId operationId, byte[] _, CommitToken? token) in writes)
            {
                HistoryOp op = cluster.History.Get(historyId);
                if (op.Outcome != OpOutcome.Unknown)
                    continue;
                DfsOperationOutcome outcome = await writerVolume.QueryOperationAsync(operationId, resolve: true, Ct);
                cluster.History.Resolve(historyId, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed, outcome.Token);
                if (clientCut)
                    Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
            }

            await cluster.History.RecordAsync<byte[]>("reader", "read", "/reg", null, () => readerVolume.Files.ReadAllBytesAsync("/reg", Ct));
            LinearizabilityChecker.Check(cluster.History.Operations.Where(op => op.Key == "/reg").ToList(), new RegisterModel(v0)).AssertValid();
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-012 IntermittentFlapping
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-012", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    public async Task NET_012_IntermittentFlapping(ulong seed)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { Seed = 0x1200 + seed, ScenarioTimeout = TimeSpan.FromSeconds(300) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await cluster.History.RecordAsync("c0", "append", "/flap", Array.Empty<byte>(), () => volume.Files.WriteAllBytesAsync("/flap", Array.Empty<byte>(), Ct));
            var random = new SeededRandom(seed);
            bool flapping = true;
            var attempts = new List<(long HistoryId, DfsOperationId OperationId, byte[] Chunk)>();

            async Task FlapAsync()
            {
                for (int i = 0; i < 30; i++)
                {
                    TestNode node = cluster.Nodes[random.Next(cluster.Nodes.Count)];
                    TestNode other = cluster.Nodes[(node.Index + 1 + random.Next(cluster.Nodes.Count - 1)) % cluster.Nodes.Count];
                    switch (random.Next(4))
                    {
                        case 0:
                            cluster.Network.Isolate(ClientNet("c0"));
                            break;
                        case 1:
                            cluster.Network.CutBoth(node.Name, other.Name);
                            break;
                        case 2:
                            cluster.Network.ResetConnections(random.Next(2) == 0 ? ClientNet("c0") : node.Name);
                            break;
                        default:
                            cluster.Network.BlackHole(ClientNet("c0"), node.Name);
                            break;
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50 + random.Next(400)), Ct);
                    cluster.Network.Heal();
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(300)), Ct);
                }

                flapping = false;
            }

            async Task AppendAsync()
            {
                for (int i = 0; i < 40; i++)
                {
                    byte[] chunk = TestData.Bytes(0x1210 + (ulong)i, 64);
                    DfsOperationId id;
                    try
                    {
                        id = await volume.NewOperationIdAsync(Ct);
                    }
                    catch (Exception ex) when (IsUnavailability(ex) || ex is IOException)
                    {
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
                        i--;
                        continue;
                    }

                    long historyId = cluster.History.Invoke("c0", "append", "/flap", chunk, id);
                    try
                    {
                        CommitToken token = await volume.Files.AppendAllBytesAsync("/flap", chunk, WithId(id), Ct);
                        cluster.History.Complete(historyId, OpOutcome.Ok, token);
                    }
                    catch (Exception ex)
                    {
                        cluster.History.Complete(historyId, HistoryRecorder.Classify(ex), ex);
                    }

                    attempts.Add((historyId, id, chunk));
                }
            }

            Task flapper = FlapAsync();
            Task appender = AppendAsync();
            await flapper;
            Assert.False(flapping);
            cluster.Network.Heal();

            // Once the network is stable, all outstanding work completes within a bound (no unbounded retry queue).
            await cluster.WaitUntilAsync(() => Task.FromResult(appender.IsCompleted), TimeSpan.FromSeconds(60), "appends must complete once the network is stable", Ct);
            await appender;
            await cluster.WaitForStableAsync(ElectionBound, Ct);

            // Resolve every uncertain append authoritatively before checking the history. Acknowledged appends carry their
            // token in the history (their outcome records may already be retired by the acknowledgement watermark);
            // unacknowledged ones are resolved; a definite failure must resolve as aborted.
            var committed = new List<(CommitToken Token, byte[] Chunk)>();
            foreach ((long historyId, DfsOperationId id, byte[] chunk) in attempts)
            {
                HistoryOp op = cluster.History.Get(historyId);
                if (op.Outcome == OpOutcome.Ok)
                {
                    CommitToken acknowledged = Assert.IsType<CommitToken>(op.Output);
                    DfsOperationOutcome retained = await volume.QueryOperationAsync(id, resolve: false, Ct);
                    Assert.True(retained.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"acknowledged append {id} resolved as {retained.Status}");
                    if (retained.Status == DfsOutcomeStatus.Committed)
                        Assert.Equal(acknowledged, retained.Token);
                    committed.Add((acknowledged, chunk));
                    continue;
                }

                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"append {id} must resolve definitely, got {outcome.Status}");
                if (op.Outcome == OpOutcome.Unknown)
                    cluster.History.Resolve(historyId, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed, outcome.Token);
                else
                    Assert.True(outcome.Status == DfsOutcomeStatus.Aborted, $"append {id} was reported as a definite failure ({op.Output}) but committed");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    committed.Add((outcome.Token, chunk));
            }

            byte[] final = await cluster.History.RecordAsync<byte[]>("c0", "read", "/flap", null, () => volume.Files.ReadAllBytesAsync("/flap", Ct));
            LinearizabilityChecker.Check(cluster.History.Operations.Where(op => op.Key == "/flap").ToList(), new AppendLogModel()).AssertValid();

            // Exactly once, in commit order.
            byte[] expected = Concat([.. committed.OrderBy(c => c.Token.Sequence).Select(c => c.Chunk)]);
            AssertEx.BytesEqual(expected, final, "every committed append appears exactly once, in commit order");

            // An idle client with nothing pending has no retry queue left: no mutation, upload or outcome query is sent any
            // more (keep-alives and routing refreshes are not retries).
            cluster.Network.Log.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            MessageType[] retryable = [MessageType.Append, MessageType.WriteAll, MessageType.PutObject, MessageType.CreateStaging, MessageType.CommitFile, MessageType.QueryOperation];
            foreach (MessageType type in retryable)
                Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: ClientNet("c0"), Type: type)));
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-013 LatencyAndBandwidthAsymmetry
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-013", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("slow-follower")]
    [InlineData("slow-leader-node")]
    public async Task NET_013_LatencyAndBandwidthAsymmetry(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(60) }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/warm", TestData.Bytes(0xD000, 100), Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode slow = variant == "slow-follower" ? Others(cluster, leader)[0] : leader;
            TestNode fast = Others(cluster, slow)[0];
            cluster.Network.SetLatency(ClientNet("c0"), slow.Name, TimeSpan.FromMilliseconds(400));
            cluster.Network.SetLatency(slow.Name, ClientNet("c0"), TimeSpan.FromMilliseconds(400));
            cluster.Network.SetBandwidth(ClientNet("c0"), slow.Name, 256 * 1024);
            cluster.Network.SetLatency(ClientNet("c0"), fast.Name, TimeSpan.FromMilliseconds(1));

            async Task<int> WriteObservingBacklogAsync(string path, byte[] data)
            {
                // Uploads to the slow replica are held; their number in flight shows whether the client applies backpressure.
                MessageHold hold = cluster.Network.Hold(new MessageFilter(From: ClientNet("c0"), To: slow.Name, Type: MessageType.PutObject));
                int maxHeld = 0;
                Task write = WriteStreamAsync(volume, path, data);
                while (!write.IsCompleted)
                {
                    maxHeld = Math.Max(maxHeld, hold.Count);
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                }

                maxHeld = Math.Max(maxHeld, hold.Count);
                await write;

                // Acknowledged although the slow replica's uploads never arrived: the acknowledgement was backed by
                // actual durable copies in the required domains (no false acknowledgement).
                await NetHelpers.AssertDurablyPlacedAsync(cluster, path, "acknowledged with a slow replica");
                hold.ReleaseAll();
                hold.Dispose();
                return maxHeld;
            }

            byte[] smallFile = TestData.Bytes(0xD001, 12 * 64 * 1024);
            byte[] largeFile = TestData.Bytes(0xD002, 48 * 64 * 1024);
            int heldSmall = await WriteObservingBacklogAsync("/small", smallFile);
            int heldLarge = await WriteObservingBacklogAsync("/large", largeFile);
            Log($"NET-013 {variant}: max outstanding uploads to the slow replica: {heldSmall} (12 chunks), {heldLarge} (48 chunks)");

            // Outstanding requests to one replica are bounded by the client's limits, not by the file size.
            Assert.True(heldLarge < 24, $"{heldLarge} uploads to one slow replica were outstanding for a 48-chunk file");
            Assert.True(heldLarge <= Math.Max(heldSmall, 4) + 4, $"outstanding uploads grew with file size ({heldSmall} -> {heldLarge})");

            await AssertContentAsync(volume, "/small", smallFile, "small file");
            await AssertContentAsync(volume, "/large", largeFile, "large file");

            // Once the slow replica catches up, the target copy count is restored.
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(120), Ct);
            DfsStoragePolicy policy = cluster.Options.GetEffectiveDefaultPolicy();
            foreach (ExtentInfo extent in await NetHelpers.ExtentsAsync(cluster, "/large"))
                Assert.True((await cluster.Inspect.CountDurableDomainsAsync(extent.Object)).Copies >= policy.TargetCopies, "repair must restore the target copy count");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    private static async Task WriteStreamAsync(DfsVolume volume, string path, byte[] data)
    {
        await using DfsStream stream = await volume.Files.CreateAsync(path, Ct);
        for (int offset = 0; offset < data.Length; offset += 64 * 1024)
            await stream.WriteAsync(data.AsMemory(offset, Math.Min(64 * 1024, data.Length - offset)), Ct);
        await stream.FlushAsync(Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-014 DnsAndEndpointChange
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-014", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("endpoint-taken-by-foreign-cluster")]
    [InlineData("node-moves-to-new-endpoint")]
    public async Task NET_014_DnsAndEndpointChange(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] before = TestData.Bytes(0xE001, 5000);
            await volume.Files.WriteAllBytesAsync("/e", before, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            if (variant == "endpoint-taken-by-foreign-cluster")
            {
                // The leader's address now leads to a different cluster (with the same principals).
                string endpoint = leader.Endpoint;
                await leader.CrashAsync(CrashMode.LoseUnflushed);
                NodeHost foreign = await cluster.StartForeignClusterNodeAsync(endpoint, Ct);
                await WaitForLeaderAmongAsync(cluster, Others(cluster, leader), "the remaining voters must elect a leader");

                // The pooled client reconnects, detects the foreign identity, and keeps working with its own cluster.
                byte[] after = TestData.Bytes(0xE002, 5000);
                await volume.Files.WriteAllBytesAsync("/e", after, Ct);
                Assert.Equal(cluster.ClusterId, client.Cluster.ClusterId);
                await AssertContentAsync(volume, "/e", after, "write routed to the right cluster");
                VolumeStateInspection? foreignState = await foreign.Inspect.GetVolumeStateAsync(Vol);
                Assert.True(foreignState is null || foreignState.GetFile("/e") is null, "no data may be routed to the foreign cluster");

                // A new client whose only seed is the reused address refuses the foreign cluster explicitly.
                await AssertEx.ThrowsDfsAsync<DfsClusterIdentityException>(
                    () => cluster.ConnectAsync(new TestClientOptions { Name = "seeded-wrong", SeedNodes = [leader.Name], ExpectedClusterId = cluster.ClusterId }, Ct),
                    DfsErrorCode.ClusterMismatch);

                await foreign.DisposeAsync();
                await leader.RestartAsync();
            }
            else
            {
                // A follower restarts on a different endpoint; the client must find it there after validating identity.
                TestNode moving = Others(cluster, leader)[0];
                string oldEndpoint = moving.Endpoint;
                await AssertContentAsync(volume, "/e", before, "warm connections");
                await moving.StopAsync();
                await moving.RestartAsync(options => options with { ListenEndpoint = oldEndpoint + "-moved" });
                Assert.NotEqual(oldEndpoint, moving.Endpoint);
                await cluster.WaitForStableAsync(ElectionBound, Ct);
                await cluster.TransferLeadershipAsync(Vol, moving.Name, Ct);
                byte[] after = TestData.Bytes(0xE003, 5000);
                long start = cluster.Time.GetTimestamp();
                await volume.Files.WriteAllBytesAsync("/e", after, Ct);
                Assert.True(cluster.Time.GetElapsedTime(start) <= ElectionBound, "rediscovery of the moved leader must be bounded");
                await AssertContentAsync(volume, "/e", after, "write through the moved leader");
                Assert.Equal(moving.Name, (await cluster.GetLeaderAsync(Vol, Ct)).Name);
                DfsNodeInfo info = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == moving.Name);
                Assert.Contains(moving.Endpoint, info.Endpoints);
            }

            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-015 TlsFailureMidHandshake
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-015", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("quic-wrong-spki-pin")]
    [InlineData("quic-certificate-callback-rejects")]
    [InlineData("quic-certificate-callback-throws")]
    [InlineData("wrong-secret")]
    [InlineData("handshake-result-dropped")]
    [InlineData("unauthenticated-raw-requests")]
    public async Task NET_015_TlsFailureMidHandshake(string variant)
    {
        bool quic = variant.StartsWith("quic-", StringComparison.Ordinal);
        TestClusterOptions options = quic
            ? TestClusterOptions.ThreeZones with { Transport = TransportKind.Quic, Mode = ClusterMode.Realtime }
            : Det(TestClusterOptions.ThreeZones);
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume volume = admin.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/existing", TestData.Bytes(0xF001, 100), Ct);
            int hitsBefore = cluster.Faults.Coverage.Hits.Count;

            if (quic)
            {
                DfsClientOptions baseOptions = cluster.CreateClientOptions(new TestClientOptions { Name = "badtls", ConnectTimeout = TimeSpan.FromSeconds(5) });
                DfsTransportSecurity security = variant switch
                {
                    "quic-wrong-spki-pin" => new DfsTransportSecurity { PinnedSpkiSha256 = [SHA256.HashData(TestData.Bytes(0xF002, 64))] },
                    "quic-certificate-callback-rejects" => new DfsTransportSecurity { ValidateServerCertificate = _ => false },
                    _ => new DfsTransportSecurity { ValidateServerCertificate = _ => throw new InvalidOperationException("certificate validation failed") },
                };
                Exception? error = await CaptureAsync(() => DfsClient.ConnectAsync(baseOptions with { Security = security }, Ct));
                Assert.True(error is not null, "a client must not connect when the server certificate is not trusted");
                Assert.True(error is System.Security.Authentication.AuthenticationException or DfsException or IOException or UnauthorizedAccessException,
                    $"certificate failure must be explicit: {error}");

                // No application request (not even the application-level hello) from the failed client reached any server.
                Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: ClientNet("badtls"))));
                Assert.DoesNotContain(cluster.Faults.Coverage.Hits.Skip(hitsBefore), h => h.Site == FaultSites.RpcRequestAccepted
                    && h.Operation is DfsOperationKind.Authenticate or DfsOperationKind.Discover or DfsOperationKind.OpenSession);
            }
            else if (variant == "wrong-secret")
            {
                Exception? error = await CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions { Name = "wrong", Credentials = new DfsCredentials("admin", "not-the-secret") }, Ct));
                Assert.IsType<DfsAuthenticationException>(error);
                AssertOnlyHandshakeTraffic(cluster, "client:wrong");

                // Retrying does not bypass authentication.
                Exception? retry = await CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions { Name = "wrong", Credentials = new DfsCredentials("admin", "not-the-secret") }, Ct));
                Assert.IsType<DfsAuthenticationException>(retry);
                AssertOnlyHandshakeTraffic(cluster, "client:wrong");
            }
            else if (variant == "handshake-result-dropped")
            {
                MessageRule drop = cluster.Network.DropNext(new MessageFilter(To: ClientNet("hs"), Type: MessageType.HelloResult), 1000);
                long start = cluster.Time.GetTimestamp();
                Exception? error = await CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions { Name = "hs", ConnectTimeout = TimeSpan.FromSeconds(3) }, Ct));
                Assert.True(drop.Matched >= 1, "the handshake result drop must have been reached");
                Assert.True(error is not null, "a client whose handshake never completed must not be connected");
                Assert.True(IsUnavailability(error!) || error is DfsException or IOException, $"handshake failure must be explicit: {error}");
                Assert.True(cluster.Time.GetElapsedTime(start) <= TimeSpan.FromSeconds(8), "the connect deadline must bound the handshake");
                AssertOnlyHandshakeTraffic(cluster, ClientNet("hs"));
                drop.Cancel();
            }
            else
            {
                // A raw connection that never authenticated (or failed to) cannot perform any operation.
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
                DfsErrorCode unauthenticated = await raw.SendPathOperationAsync(Vol, DfsOperationKind.WriteAll, "/unauth");
                Assert.True(unauthenticated is DfsErrorCode.AuthenticationFailed or DfsErrorCode.AccessDenied, $"unauthenticated request returned {unauthenticated}");
                Assert.Equal(DfsErrorCode.AuthenticationFailed, await raw.AuthenticateAsync(new DfsCredentials("admin", "not-the-secret")));
                DfsErrorCode afterFailure = await raw.SendPathOperationAsync(Vol, DfsOperationKind.CreateDirectory, "/unauth-dir");
                Assert.True(afterFailure is DfsErrorCode.AuthenticationFailed or DfsErrorCode.AccessDenied, $"request after failed authentication returned {afterFailure}");
                Assert.False(await volume.Files.ExistsAsync("/unauth", Ct));
                Assert.False(await volume.Directories.ExistsAsync("/unauth-dir", Ct));
            }

            await AssertContentAsync(volume, "/existing", TestData.Bytes(0xF001, 100), "authenticated clients are unaffected");
        }, Ct);
    }

    private static void AssertOnlyHandshakeTraffic(TestCluster cluster, string client)
    {
        long all = cluster.Network.Log.CountMessages(new MessageFilter(From: client));
        long handshake = cluster.Network.Log.CountMessages(new MessageFilter(From: client, Operation: DfsOperationKind.Authenticate));
        Assert.True(all == handshake, $"{client} sent {all - handshake} non-handshake messages without completing authentication");
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-016 IdlePoolConnectionDies
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-016", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("idle-connection-silently-dead")]
    [InlineData("connection-dies-with-reply-in-flight")]
    [InlineData("idle-connection-reset")]
    public async Task NET_016_IdlePoolConnectionDies(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            TimeSpan deadline = TimeSpan.FromSeconds(5);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = deadline }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(0x1601, 4000);
            byte[] v2 = TestData.Bytes(0x1602, 4000);
            DfsOperationId k1 = await volume.NewOperationIdAsync(Ct);
            DfsOperationId k2 = await volume.NewOperationIdAsync(Ct);
            CommitToken? t1 = null;
            Exception? e1 = null;

            if (variant == "connection-dies-with-reply-in-flight")
            {
                MessageHold hold = cluster.Network.Hold(new MessageFilter(To: ClientNet("c0"), Type: MessageType.WriteAllResult));
                Task<(CommitToken Value, Exception? Error)> first = CaptureValueAsync(() => volume.Files.WriteAllBytesAsync("/p", v1, WithId(k1), Ct));
                await hold.WaitForAsync(1, ElectionBound);
                cluster.Network.KillConnectionsSilently(ClientNet("c0"));
                hold.ReleaseAll();   // delivered onto the dead connection: it vanishes
                (CommitToken value, Exception? error) = await first;
                hold.Dispose();
                t1 = error is null ? value : null;
                e1 = error;

                // The request was delivered and executed; a reply lost with its connection leaves the caller uncertain
                // (unless a retry over a new connection fetched the stored outcome).
                if (error is not null)
                    AssertUnknownOutcome(error, k1, "a WriteAll whose reply was lost with its connection");
            }
            else
            {
                t1 = await volume.Files.WriteAllBytesAsync("/p", v1, WithId(k1), Ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);   // the pooled connections go idle
                if (variant == "idle-connection-silently-dead")
                    cluster.Network.KillConnectionsSilently(ClientNet("c0"));
                else
                    cluster.Network.ResetConnections(ClientNet("c0"));
            }

            // The next operation reuses the pool, detects the dead connection and completes within its deadline with
            // its own result (never the stale outcome of the previous request).
            long start = cluster.Time.GetTimestamp();
            CommitToken t2 = await volume.Files.WriteAllBytesAsync("/p", v2, WithId(k2), Ct);
            Assert.True(cluster.Time.GetElapsedTime(start) <= deadline + TimeSpan.FromSeconds(1), "recovery from a dead pooled connection must respect the deadline");

            // t2 is the new request's own result (it names the revision that wrote v2), never the stale outcome of k1.
            Assert.Equal(t2.Sequence, (await volume.Files.GetInfoAsync("/p", Ct)).Revision);
            DfsOperationOutcome o2 = await volume.QueryOperationAsync(k2, resolve: false, Ct);
            if (o2.Status == DfsOutcomeStatus.Committed)
                Assert.Equal(o2.Token, t2);
            DfsOperationOutcome o1 = await volume.QueryOperationAsync(k1, resolve: true, Ct);
            if (t1 is { } token1)
            {
                // Acknowledged: its outcome record may already be retired by the client's acknowledgement watermark.
                Assert.True(o1.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"acknowledged k1 resolved as {o1.Status}");
                if (o1.Status == DfsOutcomeStatus.Committed)
                    Assert.Equal(o1.Token, token1);
                Assert.True(token1.CompareTo(t2) < 0, "the earlier request committed first");
                Assert.NotEqual(token1, t2);
            }
            else
            {
                // Never acknowledged: the executed request stays resolvable as committed.
                Assert.NotNull(e1);
                Assert.Equal(DfsOutcomeStatus.Committed, o1.Status);
                Assert.True(o1.Token.CompareTo(t2) < 0, "the earlier request committed first");
                Assert.NotEqual(o1.Token, t2);
            }

            await AssertContentAsync(volume, "/p", v2, "the latest write");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-017 CancellationRaceWithReply
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> CancellationPoints => new()
    {
        { "before-invocation", FaultSites.ClientOperationStart },
        { "after-validation", FaultSites.ClientValidated },
        { "during-request-send", FaultSites.ClientRequestSend },
        { "server-accepted", FaultSites.RpcRequestAccepted },
        { "before-propose", FaultSites.RaftBeforePropose },
        { "after-local-append", FaultSites.RaftAfterLocalAppend },
        { "after-commit-before-apply", FaultSites.RaftAfterCommitBeforeApply },
        { "after-apply-before-reply", FaultSites.ApplyAfterBeforeReply },
        { "during-reply-send", FaultSites.RpcReplySend },
        { "after-reply-received", FaultSites.ClientReplyReceived },
    };

    [Theory, Catalog("NET-017", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(CancellationPoints))]
    public async Task NET_017_CancellationRaceWithReply(string point, string site)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] prefix = TestData.Bytes(0x1701, 500);
            byte[] suffix = TestData.Bytes(0x1702, 300);
            await cluster.History.RecordAsync("c0", "append", "/c", prefix, () => volume.Files.WriteAllBytesAsync("/c", prefix, Ct));
            DfsOperationId id = await volume.NewOperationIdAsync(Ct);

            // Hold the SDK to its contract: a cancellation it reports must mean "not executed".
            cluster.History.TrustCancellationAsAbort = true;
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            FaultStage stage = FaultSites.StageOf(site);
            bool clientSite = stage is FaultStage.F00 or FaultStage.F01 or FaultStage.F02 or FaultStage.F03 or FaultStage.F14;
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Operation: DfsOperationKind.Append),
                clientSite ? FaultAction.CancelCaller() : FaultAction.Custom(_ =>
                {
                    caller.Cancel();
                    return Task.CompletedTask;
                }));

            long historyId = cluster.History.Invoke("c0", "append", "/c", suffix, id);
            Exception? error = null;
            try
            {
                CommitToken token = await volume.Files.AppendAllBytesAsync("/c", suffix, WithId(id), caller.Token);
                cluster.History.Complete(historyId, OpOutcome.Ok, token);
            }
            catch (Exception ex)
            {
                error = ex;
                cluster.History.Complete(historyId, HistoryRecorder.Classify(ex, requestMayHaveBeenSent: true, trustCancellation: true), ex);
            }

            cluster.Faults.AssertReached(arm);
            cluster.Faults.DisarmAll();
            await cluster.WaitForStableAsync(ElectionBound, Ct);

            DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
            bool committed = outcome.Status == DfsOutcomeStatus.Committed;
            switch (error)
            {
                case null:
                    // Acknowledged: committed (its record may already be retired by the acknowledgement watermark).
                    Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"{point}: success was reported but the outcome is {outcome.Status}");
                    committed = true;
                    break;
                case OperationCanceledException:
                    // Cancellation may only be reported when the SDK can establish that nothing was published.
                    Assert.True(stage != FaultStage.F14, "a reply proving the commit was already received; cancellation would misreport it");
                    Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
                    break;
                default:
                    Assert.True(IsUnknownOutcome(error), $"{point}: expected success, a truthful cancellation or an unknown outcome, got {error}");
                    if (error is DfsCommitOutcomeUnknownException unknown)
                        Assert.Equal(id, unknown.OperationId);
                    Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{point}: an unknown outcome must resolve definitely, got {outcome.Status}");
                    cluster.History.Resolve(historyId, committed ? OpOutcome.Ok : OpOutcome.Failed, outcome.Token);
                    break;
            }

            if (stage is FaultStage.F00 or FaultStage.F01)
                Assert.IsAssignableFrom<OperationCanceledException>(error);

            await AssertContentAsync(volume, "/c", committed ? Concat(prefix, suffix) : prefix, $"{point}: content matches the authoritative outcome");
            await cluster.History.RecordAsync<byte[]>("c0", "read", "/c", null, () => volume.Files.ReadAllBytesAsync("/c", Ct));
            LinearizabilityChecker.Check(cluster.History.Operations.Where(op => op.Key == "/c").ToList(), new AppendLogModel()).AssertValid();
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-018 ReconnectAfterSessionExpiry
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-018", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("client-isolated")]
    [InlineData("client-clock-frozen-and-isolated")]
    public async Task NET_018_ReconnectAfterSessionExpiry(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) }, async cluster =>
        {
            var clock = new TestClock(cluster.Time);
            await using DfsClient stale = await cluster.ConnectAsync(new TestClientOptions { Name = "stale", Clock = clock, OperationTimeout = ShortOperationTimeout }, Ct);
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "other", OperationTimeout = ElectionBound }, Ct);
            DfsVolume staleVolume = stale.GetVolume(Vol);
            DfsVolume otherVolume = other.GetVolume(Vol);
            byte[] v0 = TestData.Bytes(0x1801, 4000);
            byte[] logPrefix = TestData.Bytes(0x1802, 400);
            byte[] logSuffix = TestData.Bytes(0x1803, 200);
            await staleVolume.Files.WriteAllBytesAsync("/f", v0, Ct);
            await staleVolume.Files.WriteAllBytesAsync("/log", logPrefix, Ct);

            // Stale authority: a writer reservation with private staged bytes, an unused reserved identity, and an
            // executed identity whose reply was lost.
            DfsStream writer = await staleVolume.Files.OpenAsync("/f", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            ulong staleGeneration = writer.WriterGeneration;
            await writer.WriteAsync(TestData.Bytes(0x1804, 4000), Ct);
            DfsOperationId reserved = await staleVolume.NewOperationIdAsync(Ct);
            DfsOperationId executed = await staleVolume.NewOperationIdAsync(Ct);
            MessageRule lostReply = cluster.Network.DropReplies(new MessageFilter(From: ClientNet("stale"), Type: MessageType.Append), 10_000);
            Exception? executedError = await CaptureAsync(() => staleVolume.Files.AppendAllBytesAsync("/log", logSuffix, WithId(executed), Ct));
            AssertUnknownOutcome(executedError, executed, "an executed append whose every reply was lost");
            lostReply.Cancel();

            // The client disappears long enough for its leases, session and retained outcomes to expire
            // (Fast timings: lease 2 s, session 10 s, outcome retention 30 s).
            if (variant == "client-clock-frozen-and-isolated")
                clock.Freeze();
            cluster.Network.Isolate(ClientNet("stale"));

            // Meanwhile another writer takes over once the stale reservation expired (server-authoritative expiry).
            byte[] v1 = TestData.Bytes(0x1805, 4500);
            ulong takeoverGeneration = 0;
            await cluster.WaitUntilAsync(async () =>
            {
                Exception? openError = await CaptureAsync(async () =>
                {
                    await using DfsStream takeover = await otherVolume.Files.OpenAsync("/f", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                    takeoverGeneration = takeover.WriterGeneration;
                    await takeover.WriteAsync(v1, Ct);
                    await takeover.SetLengthAsync(v1.Length, Ct);
                    await takeover.FlushAsync(Ct);
                });
                return openError is null;
            }, Lease * 5, "the stale reservation must expire so another writer can take over", Ct);
            Assert.True(takeoverGeneration > staleGeneration, $"the new writer must hold a newer fencing generation ({takeoverGeneration} vs {staleGeneration})");
            await cluster.DelayAsync(TimeSpan.FromSeconds(45), Ct);
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.DoesNotContain(state.Handles, h => h.IsWriter && h.Generation == staleGeneration && !h.Expired);

            clock.Unfreeze();
            cluster.Network.Heal();

            // The resumed client cannot publish with its expired authority; its stream faults.
            await CaptureAsync(async () => await writer.WriteAsync(TestData.Bytes(0x1806, 10), Ct));
            Exception? flushError = await CaptureAsync(() => writer.FlushAsync(Ct));
            Assert.True(flushError is DfsLeaseLostException or DfsPinExpiredException, $"publication with an expired reservation must fail with LeaseLost, got {flushError}");
            Assert.True(writer.IsFaulted, "lease loss must fault the stream");
            await CaptureAsync(async () => await writer.DisposeAsync());
            await AssertContentAsync(otherVolume, "/f", v1, "the replacement writer's revision stays authoritative");

            // Expired identities are rejected explicitly, never replayed as new operations.
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => staleVolume.Files.WriteAllBytesAsync("/reserved", TestData.Bytes(0x1807, 10), WithId(reserved), Ct), DfsErrorCode.OperationExpired);
            Assert.False(await otherVolume.Files.ExistsAsync("/reserved", Ct));
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => staleVolume.Files.AppendAllBytesAsync("/log", logSuffix, WithId(executed), Ct), DfsErrorCode.OperationExpired);
            Assert.Equal(DfsOutcomeStatus.Expired, (await otherVolume.QueryOperationAsync(executed, resolve: false, Ct)).Status);
            await AssertContentAsync(otherVolume, "/log", Concat(logPrefix, logSuffix), "the executed append happened exactly once");

            // Fresh work from the same client works under a new session.
            byte[] fresh = TestData.Bytes(0x1808, 300);
            await staleVolume.Files.WriteAllBytesAsync("/fresh", fresh, Ct);
            await AssertContentAsync(otherVolume, "/fresh", fresh, "new operations after reconnecting");
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-019 BatchPartiallyAcknowledged
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("NET-019", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [InlineData("client-cut-after-receipts", 1)]
    [InlineData("client-cut-after-receipts", 2)]
    [InlineData("client-cut-after-receipts", 5)]
    [InlineData("receipt-issuer-restarted-before-commit", 0)]
    [InlineData("receipt-issuer-crashed-before-commit", 0)]
    public async Task NET_019_BatchPartiallyAcknowledged(string variant, int receiptsBeforeCut)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] old = TestData.Bytes(0x1901, 3000);
            await volume.Files.WriteAllBytesAsync("/batch", old, Ct);
            byte[] data = TestData.Bytes(0x1902, 6 * 64 * 1024);
            DfsStream stream = await volume.Files.OpenAsync("/batch", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            Exception? flushError;

            if (variant == "client-cut-after-receipts")
            {
                // After k receipts the client loses every connection: the remaining receipts never arrive.
                FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientReplyReceived, Operation: DfsOperationKind.PutObject, Occurrence: receiptsBeforeCut),
                    FaultAction.Custom(_ =>
                    {
                        cluster.Network.Isolate(ClientNet("c0"));
                        return Task.CompletedTask;
                    }));
                // Chunks are uploaded while writing (the buffer is smaller than the data), so the cut can already fail
                // the write; the failure of any step of the publication is what is checked.
                flushError = await CaptureAsync(async () =>
                {
                    await stream.WriteAsync(data, Ct);
                    await stream.SetLengthAsync(data.Length, Ct);
                    await stream.FlushAsync(Ct);
                });
                cluster.Faults.AssertReached(arm);
                Assert.True(flushError is not null, "a publication whose receipts are incomplete cannot succeed while the client is cut off");
                Assert.True(IsUnavailability(flushError!) || IsUnknownOutcome(flushError!) || flushError is IOException, $"explicit failure expected: {flushError}");
                cluster.Network.Heal();
            }
            else
            {
                // All receipts arrive, but the commit request is held while a receipt issuer restarts (new incarnation)
                // or crashes; the coordinator must only count receipts that are still valid and durable.
                MessageHold commitHold = cluster.Network.Hold(new MessageFilter(From: ClientNet("c0"), Type: MessageType.CommitFile));
                await stream.WriteAsync(data, Ct);
                await stream.SetLengthAsync(data.Length, Ct);
                Task<Exception?> flush = CaptureAsync(() => stream.FlushAsync(Ct));
                await commitHold.WaitForAsync(1, ElectionBound);
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                TestNode issuer = Others(cluster, leader)[0];
                if (variant == "receipt-issuer-restarted-before-commit")
                {
                    await issuer.StopAsync();
                    await issuer.RestartAsync();
                }
                else
                {
                    await issuer.CrashAsync(CrashMode.LoseUnflushed);
                }

                commitHold.ReleaseAll();
                flushError = await flush;
                commitHold.Dispose();
                if (issuer.State != TestNodeState.Running)
                    await issuer.RestartAsync();
            }

            // Retry the publication through the same handle if the stream is not faulted; it publishes all or nothing.
            if (flushError is not null && !stream.IsFaulted)
                flushError = await CaptureAsync(() => stream.FlushAsync(Ct));
            await CaptureAsync(async () => await stream.DisposeAsync());

            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer", OperationTimeout = ElectionBound }, Ct);
            DfsVolume observed = observer.GetVolume(Vol);
            byte[] now = await observed.Files.ReadAllBytesAsync("/batch", Ct);
            bool published = now.AsSpan().SequenceEqual(data);
            Assert.True(published || now.AsSpan().SequenceEqual(old), "a partially acknowledged batch must never publish a partial revision");
            if (flushError is null)
                Assert.True(published, "a successful flush must publish the complete revision");
            FileInspection? inspection = await cluster.Inspect.GetFileAsync(Vol, "/batch");
            Assert.True(inspection is not null && (inspection.Length == old.Length || inspection.Length == data.Length));
            if (published)
                await NetHelpers.AssertDurablyPlacedAsync(cluster, "/batch", "only valid durable receipts may be counted");
            Assert.Equal(0, (await cluster.Inspect.GetReplicaHealthAsync(Vol)).MissingObjects);
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NET-020 EveryOperationEveryReachableStage
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<DfsOperationKind> CampaignOperations()
    {
        var data = new TheoryData<DfsOperationKind>();
        foreach (DfsOperationKind operation in NetFaultMatrix.Drivers.Keys.Order())
            data.Add(operation);
        return data;
    }

    /// <summary>
    /// Runs every applicable (stage site × fault) case of one operation family of the matrix and requires the
    /// campaign slice to be fully accounted: generated = applicable + excluded, reached = completed = passed = applicable.
    /// </summary>
    [Theory, Catalog("NET-020", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    [MemberData(nameof(CampaignOperations))]
    public async Task NET_020_EveryOperationEveryReachableStage(DfsOperationKind operation)
    {
        OperationDriver driver = NetFaultMatrix.Drivers[operation];
        List<MatrixCase> slice = NetFaultMatrix.Generate().Where(c => c.Operation == operation).ToList();
        List<MatrixCase> applicable = slice.Where(c => c.Applicable).ToList();
        var results = new List<MatrixCaseResult>();
        foreach (MatrixCase matrixCase in applicable)
            results.Add(await NetFaultMatrix.RunCaseAsync(matrixCase, driver));

        var notes = new List<string>
        {
            $"bounds: {operation} x {FaultSites.All.Count} registered sites x {Enum.GetValues<MatrixFault>().Length} faults; one fault per case at the first matching occurrence; " +
            "deterministic three-zone cluster (three voters, target 3 / ack 2 copies in 2 zones); one client; fresh cluster per case; " +
            "later occurrences of multi-chunk boundaries are covered by NET-004/NET-019 and CON-013",
        };
        notes.AddRange(slice.Where(c => !c.Applicable).Select(c => $"excluded {c.Id}: {c.ExclusionReason}"));
        notes.AddRange(results.Where(r => !r.Passed).Select(r => $"FAILED {r.Case.Id}: {r.Failure}"));
        var report = new CampaignReport(
            "NET-020/" + operation,
            Generated: slice.Count,
            Applicable: applicable.Count,
            Reached: results.Count(r => r.Reached),
            Completed: results.Count(r => r.Completed),
            Passed: results.Count(r => r.Passed),
            Failed: results.Count(r => !r.Passed),
            Excluded: slice.Count - applicable.Count,
            Notes: notes);
        Log($"{report.Campaign}: generated={report.Generated} applicable={report.Applicable} excluded={report.Excluded} reached={report.Reached} completed={report.Completed} passed={report.Passed} failed={report.Failed} ({driver.Description})");
        foreach (string note in notes.Where(n => n.StartsWith("FAILED", StringComparison.Ordinal)))
            Log("  " + note);

        // Every generated case is accounted for exactly once: executed (with a result) or excluded (with a reason).
        string[] generatedIds = slice.Select(c => c.Id).ToArray();
        string[] resultIds = results.Select(r => r.Case.Id).ToArray();
        string[] excludedIds = slice.Where(c => !c.Applicable).Select(c => c.Id).ToArray();
        Assert.Equal(generatedIds.Length, generatedIds.Distinct(StringComparer.Ordinal).Count());
        Assert.Empty(resultIds.Intersect(excludedIds, StringComparer.Ordinal));
        Assert.Equal(generatedIds.Order(StringComparer.Ordinal), resultIds.Concat(excludedIds).Order(StringComparer.Ordinal));
        Assert.All(slice.Where(c => !c.Applicable), c => Assert.False(string.IsNullOrWhiteSpace(c.ExclusionReason), $"{c.Id} is excluded without a reason"));
        Assert.Equal(report.Generated, report.Applicable + report.Excluded);
        Assert.Equal(report.Applicable, report.Passed + report.Failed);
        Assert.Equal(1 + report.Excluded + report.Failed, report.Notes.Count);
        Assert.True(report.Applicable > 0, $"{operation} has a driver but no applicable case");
        Assert.True(report.Reached == report.Applicable,
            $"unreached fault points (never counted as exercised): {string.Join(", ", results.Where(r => !r.Reached).Select(r => r.Case.Id))}");
        Assert.Equal(report.Applicable, report.Completed);
        Assert.True(report.Failed == 0, string.Join(Environment.NewLine, notes.Where(n => n.StartsWith("FAILED", StringComparison.Ordinal))));
        Assert.True(report.IsComplete);
        Assert.Equal(0, report.Unreached);
    }

    /// <summary>
    /// The generated matrix itself is fully accounted: every operation × site × fault combination is either applicable or
    /// carries a specific exclusion reason, every stage F00-F21 has applicable cases, and the only operations and sites
    /// without applicable cases are the reviewed ones.
    /// </summary>
    [Fact, Catalog("NET-020", "S", "INV-01", "INV-02", "INV-03", "INV-06", "INV-08", "INV-17")]
    public void NET_020_EveryOperationEveryReachableStage_MatrixAccounting()
    {
        IReadOnlyList<MatrixCase> all = NetFaultMatrix.Generate();
        int operations = Enum.GetValues<DfsOperationKind>().Length;
        int faults = Enum.GetValues<MatrixFault>().Length;
        Assert.Equal(operations * FaultSites.All.Count * faults, all.Count);
        Assert.Equal(all.Count, all.Select(c => c.Id).Distinct(StringComparer.Ordinal).Count());

        int applicable = all.Count(c => c.Applicable);
        int excluded = all.Count(c => !c.Applicable);
        Assert.Equal(all.Count, applicable + excluded);
        Assert.All(all.Where(c => !c.Applicable), c => Assert.False(string.IsNullOrWhiteSpace(c.ExclusionReason), $"{c.Id} is excluded without a reason"));
        Assert.DoesNotContain(all, c => c.ExclusionReason is not null && c.ExclusionReason.StartsWith("no driver registered", StringComparison.Ordinal));

        // The per-operation campaign slices (one NET_020 theory row per driven operation) and the wholly excluded operations
        // partition the matrix, so the slice reports together account for every generated case.
        int drivenCases = NetFaultMatrix.Drivers.Keys.Sum(op => all.Count(c => c.Operation == op));
        int wholeExcludedCases = NetFaultMatrix.OperationExclusions.Keys.Sum(op => all.Count(c => c.Operation == op));
        Assert.Equal(all.Count, drivenCases + wholeExcludedCases);
        Assert.All(all.Where(c => NetFaultMatrix.OperationExclusions.ContainsKey(c.Operation)), c => Assert.False(c.Applicable));
        Assert.Equal(NetFaultMatrix.Drivers.Count, CampaignOperations().Count);

        // Every operation kind is either driven or excluded as a whole with a reason, never both.
        foreach (DfsOperationKind operation in Enum.GetValues<DfsOperationKind>())
        {
            bool driven = NetFaultMatrix.Drivers.ContainsKey(operation);
            bool wholeExcluded = NetFaultMatrix.OperationExclusions.ContainsKey(operation);
            Assert.True(driven ^ wholeExcluded, $"{operation} must be either driven or excluded with a reason");
            if (driven)
            {
                Assert.True(all.Any(c => c.Operation == operation && c.Applicable), $"{operation} is driven but has no applicable case");
                Assert.All(NetFaultMatrix.Drivers[operation].Sites, site => Assert.Contains(site, FaultSites.All));
            }
        }

        // Every stage is exercised; the only sites without an applicable case are the reviewed ones.
        foreach (FaultStage stage in Enum.GetValues<FaultStage>())
            Assert.True(all.Any(c => c.Stage == stage && c.Applicable), $"stage {stage} has no applicable case");
        string[] unexercisedSites = FaultSites.All.Where(site => !all.Any(c => c.Site == site && c.Applicable)).ToArray();
        Assert.Equal(NetFaultMatrix.SitesWithoutApplicableCase.Order(StringComparer.Ordinal), unexercisedSites.Order(StringComparer.Ordinal));

        // Every fault kind is applicable somewhere.
        foreach (MatrixFault fault in Enum.GetValues<MatrixFault>())
            Assert.True(all.Any(c => c.Fault == fault && c.Applicable), $"fault {fault} is never applicable");

        ITestOutputHelper? output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"NET-020 matrix: generated={all.Count} applicable={applicable} excluded={excluded}");
        foreach (IGrouping<FaultStage, MatrixCase> group in all.Where(c => c.Applicable).GroupBy(c => c.Stage).OrderBy(g => g.Key))
            output?.WriteLine(string.Create(CultureInfo.InvariantCulture, $"  {group.Key}: {group.Count()} applicable cases"));
        foreach (IGrouping<DfsOperationKind, MatrixCase> group in all.Where(c => c.Applicable).GroupBy(c => c.Operation).OrderBy(g => g.Key))
            output?.WriteLine(string.Create(CultureInfo.InvariantCulture, $"  {group.Key}: {group.Count()} applicable cases"));
    }
}
