using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Client;

/// <summary>
/// Adversarial SDK tests from the client review against <see cref="FakeCluster"/>: requests never migrate to a re-created
/// volume, one handle's lease loss does not tear down the session, rejections after several transmissions stay unknown,
/// transaction ids stay protected, readable writers keep their staged bytes after a failed publication, helpers recover
/// from a closed staging session, and transaction-bound I/O follows the transaction's lifecycle.
/// </summary>
public sealed class ClientReviewTests
{
    private static readonly string Vol = FakeCluster.VolumeName;

    private static byte[] Bytes(int seed, int length)
    {
        var data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    private static async Task<(FakeCluster Cluster, DfsClient Client)> StartAsync(TimeSpan? operationTimeout = null, ScriptedLinkPolicy? policy = null)
    {
        var cluster = new FakeCluster(policy);
        await cluster.StartAsync();
        DfsClient client = await cluster.ConnectAsync(operationTimeout);
        return (cluster, client);
    }

    private static int Count(FakeCluster cluster, MessageType type) => cluster.Received.GetValueOrDefault(type);

    // ------------------------------------------------------------------------------------------------ volume binding

    [Fact]
    public async Task RecreatedVolume_RetryOfALostReplyNeverRunsAgainstTheNewVolume()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(4), policy);
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/victim", Bytes(1, 10));

            // The delete executes on the old volume, its reply is lost, and the name is re-created (new volume, session
            // numbering restarting) holding an unrelated file at the same path.
            byte[] unrelated = Bytes(2, 10);
            bool recreated = false;
            cluster.AfterDispatch = (request, _) =>
            {
                if (!recreated && request is DeleteMessage)
                {
                    recreated = true;
                    cluster.RecreateVolume();
                    cluster.SeedFile("/victim", unrelated);
                }
            };
            policy.Drop(MessageType.DeleteResult, LinkDirection.Reply);

            await Assert.ThrowsAsync<DfsCommitOutcomeUnknownException>(() => files.DeleteAsync("/victim"));
            Assert.True(recreated);
            Assert.Equal(unrelated, cluster.Content("/victim"));
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.Delete));
        }
    }

    [Fact]
    public async Task RecreatedVolume_InFlightHandleReadNeverReadsTheNewVolumesHandle()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(4));
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] original = Enumerable.Repeat((byte)'A', 100).ToArray();
            byte[] foreign = Enumerable.Repeat((byte)'B', 100).ToArray();
            await files.WriteAllBytesAsync("/a", original);
            DfsStream reader = await files.OpenReadAsync("/a");

            // While the read is in flight the volume is re-created; the client's new session then owns a handle with the
            // same number on another file of the new volume.
            bool recreated = false;
            cluster.Intercept = request =>
            {
                if (recreated || request is not ReadRangeMessage)
                    return null;
                recreated = true;
                cluster.RecreateVolume();
                cluster.SeedFile("/b", foreign);
                return new ErrorReplyMessage(DfsErrorCode.NotLeader, "leadership moved", null, true);
            };
            cluster.AfterDispatch = (request, reply) =>
            {
                if (recreated && request is OpenSessionMessage && reply is OpenSessionResultMessage opened)
                    cluster.OpenHandleDirect("/b", opened.Session);
            };

            byte[] buffer = new byte[100];
            Exception? failure = await Record.ExceptionAsync(async () =>
            {
                int read = await reader.ReadAsync(buffer);
                Assert.True(buffer.AsSpan(0, read).SequenceEqual(original.AsSpan(0, read)), "a read through the old volume's handle returned another file's bytes");
            });
            Assert.True(recreated);
            Assert.True(failure is DfsException, $"the old volume's handle must fail, got {failure?.GetType().Name ?? "success"}");
            Assert.True(reader.IsFaulted);
            await reader.DisposeAsync();
        }
    }

    [Fact]
    public async Task RecreatedVolume_KeepAliveFaultsTheOldVolumesStreamsPromptly()
    {
        var cluster = new FakeCluster { LeaseDuration = TimeSpan.FromMilliseconds(300) };
        await cluster.StartAsync();
        DfsClient client = await cluster.ConnectAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/a", Bytes(16, 100));
            DfsStream writer = await files.OpenAsync("/a", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            await writer.WriteAsync(Bytes(17, 10));

            // Nothing else is sent by the application: only the keep-alive can tell the writer its volume is gone.
            cluster.RecreateVolume();
            long started = TimeProvider.System.GetTimestamp();
            while (!writer.IsFaulted && TimeProvider.System.GetElapsedTime(started) < TimeSpan.FromSeconds(10))
                await Task.Delay(20);
            Assert.True(writer.IsFaulted, "the keep-alive must learn that the volume was re-created and fault the old volume's streams");
            await Assert.ThrowsAnyAsync<DfsException>(() => writer.FlushAsync());
            Assert.Null(cluster.Content("/a"));
            await Record.ExceptionAsync(() => writer.DisposeAsync().AsTask());
        }
    }

    // ------------------------------------------------------------------------------------------------ leases

    [Fact]
    public async Task ExpiredHandle_DoesNotFaultTheSessionsOtherStreams()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/a", Bytes(1, 100));
            await files.WriteAllBytesAsync("/b", Bytes(2, 100));
            DfsStream a = await files.OpenReadAsync("/a");
            ulong handleA = cluster.HandleIds.Single();
            DfsStream b = await files.OpenAsync("/b", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            int sessionsBefore = Count(cluster, MessageType.OpenSession);

            cluster.ExpireHandle(handleA);
            await Assert.ThrowsAsync<DfsLeaseLostException>(async () => await a.ReadExactlyAsync(new byte[10]));
            Assert.True(a.IsFaulted);

            // The other stream of the same session is unaffected: its lease is alive on the server.
            Assert.False(b.IsFaulted, "a single handle's lease loss must not fault the session's other streams");
            byte[] update = Bytes(3, 50);
            await b.WriteAsync(update);
            await b.FlushAsync();
            Assert.Equal(update.Concat(Bytes(2, 100).Skip(50)).ToArray(), cluster.Content("/b"));
            Assert.Equal(sessionsBefore, Count(cluster, MessageType.OpenSession));
            await b.DisposeAsync();
            await a.DisposeAsync();
        }
    }

    // ------------------------------------------------------------------------------------------------ outcomes

    [Fact]
    public async Task ExpiredAfterASecondTransmission_IsUnknownNotADefiniteExpiry()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(4), policy);
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsOperationId id = await volume.NewOperationIdAsync();

            // The first transmission commits, its reply is lost, then the session (and the retained outcome) expires.
            bool expired = false;
            cluster.AfterDispatch = (request, _) =>
            {
                if (!expired && request is WriteAllMessage)
                {
                    expired = true;
                    cluster.ExpireSession(id.Session);
                }
            };
            policy.Drop(MessageType.WriteAllResult, LinkDirection.Reply);

            DfsCommitOutcomeUnknownException unknown = await Assert.ThrowsAsync<DfsCommitOutcomeUnknownException>(() =>
                volume.Files.WriteAllBytesAsync("/once", Bytes(4, 10), new DfsMutationOptions { OperationId = id }));
            Assert.Equal(id, unknown.OperationId);
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.WriteAll));
            Assert.NotNull(cluster.Content("/once"));
        }
    }

    [Fact]
    public async Task UnexpectedReplyToAMutation_IsUnknownNotAProtocolFailure()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/x", Bytes(5, 10));
            cluster.Intercept = request => request is DeleteMessage ? new GetInfoResultMessage(false, null, CommitToken.None) : null;
            DfsCommitOutcomeUnknownException unknown = await Assert.ThrowsAsync<DfsCommitOutcomeUnknownException>(() => volume.Files.DeleteAsync("/x"));

            // The id stays protected: later requests never acknowledge it.
            cluster.Intercept = null;
            await volume.Files.WriteAllBytesAsync("/y", Bytes(6, 10));
            Assert.True(cluster.MaxWatermark(unknown.OperationId.Session) <= unknown.OperationId.Sequence);
        }
    }

    // ------------------------------------------------------------------------------------------------ transactions

    [Fact]
    public async Task TransactionIds_AreNeverAcknowledged()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/before", Bytes(7, 10));
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            for (int i = 0; i < 4; i++)
                await volume.Files.WriteAllBytesAsync($"/after{i}", Bytes(8 + i, 10));
            DfsOperationId id = tx.Id.Operation;
            Assert.True(cluster.MaxWatermark(id.Session) <= id.Sequence,
                $"the watermark {cluster.MaxWatermark(id.Session)} acknowledged transaction id {id}, whose outcome must stay queryable");
            await tx.DisposeAsync();
        }
    }

    [Fact]
    public async Task UnknownCommit_DisposalRequestsAbortAndLearnsTheDecision()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(2), policy);
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            policy.Drop(MessageType.TxCommitResult, LinkDirection.Reply, 1000);
            policy.Drop(MessageType.QueryOperationResult, LinkDirection.Reply, 1000);
            await Assert.ThrowsAsync<DfsCommitOutcomeUnknownException>(() => tx.CommitAsync());
            Assert.Equal(DfsTransactionState.Unknown, tx.State);
            Assert.Equal("Committed", cluster.TransactionState(tx.Id));

            // An abort request whose reply is lost leaves the outcome unknown (never reported as aborted).
            policy.Clear();
            policy.Drop(MessageType.TxAbortResult, LinkDirection.Reply, 1000);
            await tx.DisposeAsync();
            Assert.Equal(DfsTransactionState.Unknown, tx.State);

            // Disposal asks for an abort, which never reverses the commit and reports it.
            policy.Clear();
            int aborts = Count(cluster, MessageType.TxAbort);
            await tx.DisposeAsync();
            Assert.True(Count(cluster, MessageType.TxAbort) > aborts, "disposing an unresolved transaction must request an abort");
            Assert.Equal(DfsTransactionState.Committed, tx.State);
            Assert.Equal("Committed", cluster.TransactionState(tx.Id));
        }
    }

    [Fact]
    public async Task TransactionBoundEnumeration_RejectsPagesAfterCommit()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Directories.CreateDirectoryAsync("/d");
            foreach (string name in new[] { "a", "b", "c" })
                await volume.Files.WriteAllBytesAsync("/d/" + name, Bytes(name[0], 10));

            DfsTransaction tx = await volume.Transactions.BeginAsync();
            IAsyncEnumerator<DfsEntryInfo> entries = tx.Directories.EnumerateAsync("/d", new DfsEnumerationOptions { PageSize = 1 }).GetAsyncEnumerator();
            try
            {
                Assert.True(await entries.MoveNextAsync());
                await tx.CommitAsync();
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await entries.MoveNextAsync());
            }
            finally
            {
                await entries.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task TransactionBoundLineReader_RejectsReadsAfterCommit()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            string text = string.Join('\n', Enumerable.Range(0, 30_000).Select(i => $"line {i}"));
            await volume.Files.WriteAllTextAsync("/lines.txt", text);
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            IAsyncEnumerator<string> lines = tx.Files.ReadLinesAsync("/lines.txt").GetAsyncEnumerator();
            try
            {
                Assert.True(await lines.MoveNextAsync());
                await tx.CommitAsync();
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    while (await lines.MoveNextAsync())
                    {
                    }
                });
            }
            finally
            {
                await lines.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task TransactionBoundStream_DisposesQuietlyAfterTheTransactionAborted()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            DfsStream stream = await tx.Files.OpenAsync("/t", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read });
            await stream.WriteAsync(Bytes(9, 100));
            await tx.AbortAsync();
            Assert.Equal(DfsTransactionState.Aborted, tx.State);

            // The abort discarded the transaction's changes: disposing its stream is the documented cleanup, not a failure.
            Exception? failure = await Record.ExceptionAsync(() => stream.DisposeAsync().AsTask());
            Assert.Null(failure);
            Assert.Equal(0, cluster.Applied.GetValueOrDefault(MessageType.CommitFile));
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }
    }

    [Fact]
    public async Task TransactionBoundStream_DisposalOverlappingTheCommitIsRejectedAndChangesNothing()
    {
        var faults = new GateFaults();
        var cluster = new FakeCluster();
        await cluster.StartAsync();
        DfsClient client = await DfsClient.ConnectAsync(cluster.ClientOptions(),
            new DfsClientOptionsInternal { TransportFactory = cluster.Network.Factory, Faults = faults }, CancellationToken.None);
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            DfsStream stream = await tx.Files.OpenAsync("/t", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            byte[] data = Bytes(15, 300);
            await stream.WriteAsync(data);

            // The commit publishes the stream's staged view into the transaction; hold that publication on the wire.
            faults.Arm(c => c.Site == FaultSites.ClientRequestSend && c.Operation == DfsOperationKind.TxOp);
            Task<CommitToken> commit = tx.CommitAsync();
            await faults.Reached.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(DfsTransactionState.Committing, tx.State);

            Exception? overlap = await Record.ExceptionAsync(() => stream.DisposeAsync().AsTask());
            Assert.IsType<InvalidOperationException>(overlap);
            Exception? abortOverlap = await Record.ExceptionAsync(() => stream.AbortAsync());
            Assert.IsType<InvalidOperationException>(abortOverlap);

            faults.Release();
            await commit;
            Assert.Equal(DfsTransactionState.Committed, tx.State);
            Assert.Equal(data, cluster.Content("/t"));
            await stream.DisposeAsync();
            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    // ------------------------------------------------------------------------------------------------ publication

    [Fact]
    public async Task ReadableWriter_KeepsReadingItsStagedBytesAfterAFailedFlush()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            // The buffer holds all written bytes, so nothing is uploaded before the flush (a writer uploads early once its
            // buffered bytes exceed BufferSize, PUB-002).
            DfsStream stream = await files.OpenAsync("/rw",
                new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read, BufferSize = 1 << 20 });
            byte[] data = Bytes(10, 200_000);
            await stream.WriteAsync(data);

            // The first object is stored, the next one finds the node full: nothing is published.
            cluster.PutsBeforeFailure = 1;
            Exception? flush = await Record.ExceptionAsync(() => stream.FlushAsync());
            Assert.IsType<DfsPlacementUnavailableException>(flush);
            Assert.False(stream.IsFaulted);
            Assert.Empty(cluster.Content("/rw")!);

            // The writer still reads exactly its staged bytes ...
            stream.Position = 0;
            byte[] back = new byte[data.Length];
            await stream.ReadExactlyAsync(back);
            Assert.Equal(data, back);

            // ... and a later flush publishes them.
            cluster.PutsBeforeFailure = null;
            await stream.FlushAsync();
            Assert.Equal(data, cluster.Content("/rw"));
            await stream.DisposeAsync();
        }
    }

    [Fact]
    public async Task WholeFileHelpers_RecoverFromAClosedHelperStagingSession()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/big1", Bytes(11, 100_000));

            // The leader expired the session (missed keep-alives): its helper staging session is closed.
            cluster.ExpireSession(cluster.SessionIds.Last());
            int sessionsBefore = Count(cluster, MessageType.OpenSession);

            byte[] second = Bytes(12, 100_000);
            await files.WriteAllBytesAsync("/big2", second);
            Assert.Equal(second, cluster.Content("/big2"));
            byte[] suffix = Bytes(13, 80_000);
            await files.AppendAllBytesAsync("/big2", suffix);
            Assert.Equal(second.Concat(suffix).ToArray(), cluster.Content("/big2"));
            Assert.Equal(sessionsBefore + 1, Count(cluster, MessageType.OpenSession));

            // Appends recover the same way after another expiry.
            cluster.ExpireSession(cluster.SessionIds.Last());
            byte[] more = Bytes(14, 70_000);
            await files.AppendAllBytesAsync("/big2", more);
            Assert.Equal(second.Concat(suffix).Concat(more).ToArray(), cluster.Content("/big2"));
            Assert.Equal(sessionsBefore + 2, Count(cluster, MessageType.OpenSession));
        }
    }

    /// <summary>A fault injector holding the first client site hit matching an armed predicate until released.</summary>
    private sealed class GateFaults : IFaultInjector
    {
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Func<FaultContext, bool>? _match;

        public bool IsEnabled => true;

        public Task Reached => _reached.Task;

        public void Arm(Func<FaultContext, bool> match) => Volatile.Write(ref _match, match);

        public void Release() => _released.TrySetResult();

        public async ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
        {
            Func<FaultContext, bool>? match = Volatile.Read(ref _match);
            if (match is null || !match(context) || Interlocked.CompareExchange(ref _match, null, match) != match)
                return;
            _reached.TrySetResult();
            await _released.Task.WaitAsync(ct);
        }
    }
}
