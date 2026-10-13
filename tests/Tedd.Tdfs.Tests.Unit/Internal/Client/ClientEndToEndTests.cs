using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Client;

/// <summary>
/// The SDK against <see cref="FakeCluster"/> (real handshake, real wire protocol over the in-memory transport): connection
/// and identity checks, whole-file helpers and their representation rules, streams (private staging, read-your-writes,
/// truncate-then-extend, write-through, overlap rejection, abort, disposal), outcome discipline under lost replies,
/// integrity and short reads, validation without traffic, and disposal.
/// </summary>
public sealed class ClientEndToEndTests
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

    // ------------------------------------------------------------------------------------------------ connect

    [Fact]
    public async Task Connect_AuthenticatesAndBindsTheCluster()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            Assert.Equal(cluster.ClusterId, client.Cluster.ClusterId);
            Assert.Equal(FakeCluster.Principal, client.Principal);
            Assert.Equal(FakeCluster.InlineThreshold, client.Capabilities.InlineThreshold);
            Assert.False(client.IsDisposed);
        }
    }

    [Fact]
    public async Task Connect_RejectsAForeignClusterBeforeAuthenticating()
    {
        await using var cluster = new FakeCluster();
        await cluster.StartAsync();
        DfsClientOptions options = cluster.ClientOptions() with { ExpectedClusterId = Guid.NewGuid() };
        await Assert.ThrowsAsync<DfsClusterIdentityException>(() => cluster.ConnectAsync(options: options));
        Assert.False(cluster.Received.ContainsKey(MessageType.HelloProof), "no proof may be sent to a foreign cluster");
    }

    [Fact]
    public async Task Connect_WrongSecretFailsAuthentication()
    {
        await using var cluster = new FakeCluster();
        await cluster.StartAsync();
        DfsClientOptions options = cluster.ClientOptions() with { Credentials = new DfsCredentials(FakeCluster.Principal, "wrong") };
        await Assert.ThrowsAsync<DfsAuthenticationException>(() => cluster.ConnectAsync(options: options));
    }

    [Fact]
    public async Task Connect_AllSeedsOfflineFailsWithinTheConnectTimeout()
    {
        var network = new InMemoryNetwork();
        var options = new DfsClientOptions
        {
            SeedEndpoints = ["mem://nobody-1", "mem://nobody-2"],
            Credentials = new DfsCredentials("p", "s"),
            ConnectTimeout = TimeSpan.FromSeconds(1),
        };
        long started = TimeProvider.System.GetTimestamp();
        await Assert.ThrowsAsync<DfsUnavailableException>(() =>
            DfsClient.ConnectAsync(options, new DfsClientOptionsInternal { TransportFactory = network.Factory }, CancellationToken.None));
        Assert.True(TimeProvider.System.GetElapsedTime(started) < TimeSpan.FromSeconds(10));
    }

    // ------------------------------------------------------------------------------------------------ helpers

    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(FakeCluster.InlineThreshold)]
    [InlineData(FakeCluster.InlineThreshold + 1)]
    [InlineData(200_000)]
    public async Task WriteAllBytes_RoundTripsWithTheDocumentedRepresentation(int length)
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            byte[] data = Bytes(length, length);
            CommitToken token = await volume.Files.WriteAllBytesAsync("/f.bin", data);
            Assert.False(token.IsNone);
            Assert.Equal(data, cluster.Content("/f.bin"));
            Assert.Equal(data, await volume.Files.ReadAllBytesAsync("/f.bin"));
            if (length <= FakeCluster.InlineThreshold)
            {
                Assert.All(cluster.LastChanges, c => Assert.Equal(RangeChangeKind.Inline, c.Kind));
            }
            else
            {
                Assert.All(cluster.LastChanges, c => Assert.Equal(RangeChangeKind.Data, c.Kind));
                // Deterministic chunking at ChunkSize boundaries (payload identity).
                Assert.All(cluster.LastChanges, c => Assert.Equal(0, c.Offset % (64 * 1024)));
            }

            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    [Fact]
    public async Task AppendAllBytes_AppendsInlineAndAsObjects()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] a = Bytes(1, 1000);
            byte[] b = Bytes(2, 2000);
            byte[] c = Bytes(3, 100_000);
            await files.AppendAllBytesAsync("/log", a);
            Assert.All(cluster.LastChanges, x => Assert.Equal(RangeChangeKind.Inline, x.Kind));
            await files.AppendAllBytesAsync("/log", b);
            await files.AppendAllBytesAsync("/log", c);
            Assert.All(cluster.LastChanges, x => Assert.Equal(RangeChangeKind.Data, x.Kind));
            Assert.Equal(a.Concat(b).Concat(c).ToArray(), cluster.Content("/log"));
        }
    }

    [Fact]
    public async Task TextHelpers_DefaultToUtf8WithoutBomAndHonourExplicitPreambles()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            const string text = "héllo wörld \U0001F600";
            await files.WriteAllTextAsync("/t.txt", text);
            Assert.Equal(new UTF8Encoding(false).GetBytes(text), cluster.Content("/t.txt"));
            Assert.Equal(text, await files.ReadAllTextAsync("/t.txt"));

            var utf8Bom = new UTF8Encoding(true);
            await files.WriteAllTextAsync("/bom.txt", text, utf8Bom);
            Assert.Equal(utf8Bom.GetPreamble().Concat(utf8Bom.GetBytes(text)).ToArray(), cluster.Content("/bom.txt"));
            Assert.Equal(text, await files.ReadAllTextAsync("/bom.txt"));
            await files.AppendAllTextAsync("/bom.txt", "!", utf8Bom);
            Assert.Equal(text + "!", await files.ReadAllTextAsync("/bom.txt"));
            Assert.Equal(1, CountOccurrences(cluster.Content("/bom.txt")!, utf8Bom.GetPreamble()));

            string[] lines = ["a", "", "bæ", "c"];
            await files.WriteAllLinesAsync("/lines.txt", lines);
            Assert.Equal(lines, await files.ReadAllLinesAsync("/lines.txt"));
            var lazy = new List<string>();
            await foreach (string line in files.ReadLinesAsync("/lines.txt"))
                lazy.Add(line);
            Assert.Equal(lines, lazy);
            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    private static int CountOccurrences(byte[] haystack, byte[] needle)
    {
        int count = 0;
        for (int i = 0; i + needle.Length <= haystack.Length; i++)
        {
            if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle))
                count++;
        }

        return count;
    }

    [Fact]
    public async Task ReadLines_IsLazyAndReleasesItsHandleWhenDisposedEarly()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            string[] lines = Enumerable.Range(0, 20_000).Select(i => $"line {i:D6}").ToArray();
            await files.WriteAllLinesAsync("/big.txt", lines);
            int taken = 0;
            await foreach (string line in files.ReadLinesAsync("/big.txt"))
            {
                Assert.Equal(lines[taken], line);
                if (++taken == 10)
                    break;
            }

            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    [Fact]
    public async Task ReadAll_EnforcesTheMaterializationLimitWithoutLeakingTheHandle()
    {
        var cluster = new FakeCluster();
        await using (cluster)
        {
            await cluster.StartAsync();
            DfsClientOptions options = cluster.ClientOptions() with { MaxMaterializeBytes = 10_000 };
            await using DfsClient client = await cluster.ConnectAsync(options: options);
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/big", Bytes(5, 20_000));
            DfsLimitExceededException ex = await Assert.ThrowsAsync<DfsLimitExceededException>(() => files.ReadAllBytesAsync("/big"));
            Assert.Equal(DfsErrorCode.LimitExceeded, ex.ErrorCode);
            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    // ------------------------------------------------------------------------------------------------ streams

    [Fact]
    public async Task Stream_StagesPrivatelyAndPublishesOneRevisionPerFlush()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] initial = Bytes(10, 10_000);
            await files.WriteAllBytesAsync("/s.bin", initial);
            ulong before = cluster.Revision("/s.bin");

            DfsStream s = await files.OpenAsync("/s.bin", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            byte[] patch = Bytes(11, 3000);
            s.Seek(5000, SeekOrigin.Begin);
            await s.WriteAsync(patch);
            Assert.Equal(initial, cluster.Content("/s.bin"));
            Assert.Equal(before, cluster.Revision("/s.bin"));
            Assert.Equal(3000, s.PendingBytes);

            // Read-your-writes through the overlay (base bytes + staged bytes).
            s.Seek(0, SeekOrigin.Begin);
            byte[] staged = await ReadAllAsync(s);
            byte[] expected = initial.ToArray();
            patch.CopyTo(expected, 5000);
            Assert.Equal(expected, staged);

            int commitsBefore = cluster.Applied.GetValueOrDefault(MessageType.CommitFile);
            await s.FlushAsync();
            Assert.Equal(commitsBefore + 1, cluster.Applied.GetValueOrDefault(MessageType.CommitFile));
            Assert.Equal(expected, cluster.Content("/s.bin"));
            Assert.Equal(0, s.PendingBytes);

            // A no-op flush publishes nothing.
            await s.FlushAsync();
            Assert.Equal(commitsBefore + 1, cluster.Applied.GetValueOrDefault(MessageType.CommitFile));
            await s.DisposeAsync();
            Assert.Equal(0, cluster.OpenHandles);
        }
    }

    [Fact]
    public async Task Stream_TruncateThenExtendNeverRevealsDiscardedBytes()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] initial = Bytes(20, 9000);
            await files.WriteAllBytesAsync("/t.bin", initial);
            await using (DfsStream s = await files.OpenAsync("/t.bin", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }))
            {
                await s.SetLengthAsync(1000);
                await s.SetLengthAsync(9000);
                Assert.Equal(9000, s.Length);
                s.Seek(0, SeekOrigin.Begin);
                byte[] staged = await ReadAllAsync(s);
                Assert.Equal(initial.AsSpan(0, 1000).ToArray(), staged[..1000]);
                Assert.All(staged[1000..], b => Assert.Equal(0, b));
            }

            byte[] committed = cluster.Content("/t.bin")!;
            Assert.Equal(9000, committed.Length);
            Assert.Equal(initial.AsSpan(0, 1000).ToArray(), committed[..1000]);
            Assert.All(committed[1000..], b => Assert.Equal(0, b));
        }
    }

    [Fact]
    public async Task Stream_WriteThroughPublishesEveryCall()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await using DfsStream s = await files.OpenAsync("/wt.bin",
                new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough });
            await s.WriteAsync(Bytes(30, 100));
            Assert.Equal(100, cluster.Content("/wt.bin")!.Length);
            await s.WriteAsync(Bytes(31, 50_000));
            Assert.Equal(50_100, cluster.Content("/wt.bin")!.Length);
            Assert.Equal(0, s.PendingBytes);
        }
    }

    [Fact]
    public async Task Stream_OverlappingCallsAreRejectedImmediately()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(5), policy);
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/o.bin", Bytes(40, 10_000));
            DfsStream s = await files.OpenAsync("/o.bin", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            await s.WriteAsync(Bytes(41, 10));

            // The commit's first reply is lost, so the flush is in flight long enough to overlap.
            policy.Drop(MessageType.CommitFileResult, LinkDirection.Reply);
            Task flush = s.FlushAsync();
            Assert.False(flush.IsCompleted);
            Assert.Throws<InvalidOperationException>(() => s.Seek(0, SeekOrigin.Begin));
            await Assert.ThrowsAsync<InvalidOperationException>(() => s.ReadAsync(new byte[10], 0, 10));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await s.DisposeAsync());
            await flush;
            Assert.True(s.CanRead && s.CanWrite, "a rejected DisposeAsync leaves the stream open");
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.CommitFile));
            await s.DisposeAsync();
            Assert.False(s.CanRead);
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await s.WriteAsync(new byte[1]));
        }
    }

    [Fact]
    public async Task Stream_AbortDiscardsStagedChangesAndReleasesTheHandle()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] initial = Bytes(50, 5000);
            await files.WriteAllBytesAsync("/a.bin", initial);
            DfsStream s = await files.OpenAsync("/a.bin", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read });
            await s.WriteAsync(Bytes(51, 200_000));
            await s.AbortAsync();
            Assert.Equal(0, s.PendingBytes);
            Assert.Equal(0, cluster.OpenHandles);
            await s.DisposeAsync();
            Assert.Equal(initial, cluster.Content("/a.bin"));
        }
    }

    [Fact]
    public async Task Stream_AppendTargetsTheEndAndIsNotSeekable()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] initial = Bytes(60, 100);
            await files.WriteAllBytesAsync("/ap.bin", initial);
            await using (DfsStream s = await files.OpenAsync("/ap.bin", new DfsOpenOptions { Mode = FileMode.Append, Access = FileAccess.Write, Share = FileShare.Read }))
            {
                Assert.False(s.CanSeek);
                Assert.Throws<NotSupportedException>(() => s.Seek(0, SeekOrigin.Begin));
                Assert.Throws<NotSupportedException>(() => s.SetLength(1));
                await s.WriteAsync(Bytes(61, 50));
            }

            Assert.Equal(initial.Concat(Bytes(61, 50)).ToArray(), cluster.Content("/ap.bin"));
        }
    }

    [Fact]
    public async Task Stream_FailedWriteDoesNotMoveThePositionOrStageBytes()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await using DfsStream s = await files.OpenAsync("/w.bin", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read });
            await s.WriteAsync(Bytes(70, 10));
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => s.WriteAsync(Bytes(71, 10), cancelled.Token).AsTask());
            Assert.Equal(10, s.Position);
            Assert.Equal(10, s.PendingBytes);
        }
    }

    [Fact]
    public async Task Stream_ShortBackendResultsYieldShortReadsNeverFalseEof()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] data = Bytes(80, 30_000);
            await files.WriteAllBytesAsync("/short.bin", data);
            cluster.MaxReadResult = 997;
            await using DfsStream s = await files.OpenReadAsync("/short.bin");
            var buffer = new byte[10_000];
            int n = await s.ReadAsync(buffer);
            Assert.InRange(n, 1, 997);
            Assert.Equal(data.AsSpan(0, n).ToArray(), buffer[..n]);
            Assert.Equal(n, s.Position);
            Assert.Equal(data, await files.ReadAllBytesAsync("/short.bin"));
        }
    }

    private static async Task<byte[]> ReadAllAsync(Stream s)
    {
        var output = new MemoryStream();
        var buffer = new byte[8192];
        int n;
        while ((n = await s.ReadAsync(buffer)) > 0)
            output.Write(buffer, 0, n);
        return output.ToArray();
    }

    // ------------------------------------------------------------------------------------------------ outcomes

    [Fact]
    public async Task LostReply_IsRetriedWithTheSameIdAndAppliedOnce()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(6), policy);
        await using (cluster)
        await using (client)
        {
            policy.Drop(MessageType.WriteAllResult, LinkDirection.Reply);
            await client.GetVolume(Vol).Files.WriteAllBytesAsync("/once", Bytes(90, 10));
            Assert.Equal(1, policy.Dropped);
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.WriteAll));
            Assert.True(cluster.Received.GetValueOrDefault(MessageType.WriteAll) >= 2);
        }
    }

    [Fact]
    public async Task LostReplies_AreResolvedThroughTheOutcomeQuery()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(3), policy);
        await using (cluster)
        await using (client)
        {
            policy.Drop(MessageType.WriteAllResult, LinkDirection.Reply, 1000);
            CommitToken token = await client.GetVolume(Vol).Files.WriteAllBytesAsync("/resolved", Bytes(91, 10));
            Assert.False(token.IsNone);
            Assert.True(cluster.Received.GetValueOrDefault(MessageType.QueryOperation) >= 1);
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.WriteAll));
        }
    }

    [Fact]
    public async Task UnresolvableOutcome_IsReportedAsUnknownWithItsOperationId()
    {
        var policy = new ScriptedLinkPolicy();
        (FakeCluster cluster, DfsClient client) = await StartAsync(TimeSpan.FromSeconds(2), policy);
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsOperationId id = await volume.NewOperationIdAsync();
            policy.Drop(MessageType.WriteAllResult, LinkDirection.Reply, 1000);
            policy.Drop(MessageType.QueryOperationResult, LinkDirection.Reply, 1000);
            DfsCommitOutcomeUnknownException unknown = await Assert.ThrowsAsync<DfsCommitOutcomeUnknownException>(() =>
                volume.Files.WriteAllBytesAsync("/unknown", Bytes(92, 10), new DfsMutationOptions { OperationId = id }));
            Assert.Equal(id, unknown.OperationId);
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.WriteAll));

            // Once replies flow again the application resolves the id: it committed exactly once.
            policy.Clear();
            DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
        }
    }

    [Fact]
    public async Task ApplicationOperationIds_ReplayTheStoredOutcome()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsOperationId id = await volume.NewOperationIdAsync();
            var options = new DfsMutationOptions { OperationId = id };
            CommitToken first = await volume.Files.WriteAllBytesAsync("/id", Bytes(93, 10), options);
            CommitToken second = await volume.Files.WriteAllBytesAsync("/id", Bytes(93, 10), options);
            Assert.Equal(first, second);
            Assert.Equal(1, cluster.Applied.GetValueOrDefault(MessageType.WriteAll));
            DfsOperationOutcome outcome = await volume.QueryOperationAsync(id);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
        }
    }

    [Fact]
    public async Task CancelledBeforeTransmission_IsCancellationAndSendsNothing()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.GetVolume(Vol).Files.WriteAllBytesAsync("/c", Bytes(94, 10), cancelled.Token));
            Assert.Equal(0, cluster.Received.GetValueOrDefault(MessageType.WriteAll));
        }
    }

    // ------------------------------------------------------------------------------------------------ integrity

    [Fact]
    public async Task CorruptOrMissingObjects_FailWithIntegrityNeverZeroes()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await files.WriteAllBytesAsync("/i.bin", Bytes(100, 50_000));
            cluster.CorruptReads = 100;
            await Assert.ThrowsAsync<DfsIntegrityException>(() => files.ReadAllBytesAsync("/i.bin"));
            cluster.CorruptReads = 0;
            await using DfsStream s = await files.OpenReadAsync("/i.bin");
            cluster.LoseAllObjects();
            var buffer = new byte[100];
            await Assert.ThrowsAsync<DfsIntegrityException>(() => s.ReadAsync(buffer, 0, buffer.Length));
            Assert.Equal(0, s.Position);
        }
    }

    // ------------------------------------------------------------------------------------------------ validation and lifecycle

    [Theory]
    [InlineData("relative")]
    [InlineData("/a//b")]
    [InlineData("/a/../b")]
    [InlineData("/a\\b")]
    [InlineData("/x:y/z")]
    public async Task InvalidPaths_AreRejectedLocallyWithoutTraffic(string path)
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            int before = cluster.Received.Values.Sum();
            ArgumentException ex = await Assert.ThrowsAnyAsync<ArgumentException>(() => client.GetVolume(Vol).Files.WriteAllBytesAsync(path, new byte[1]));
            Assert.Equal("path", ex.ParamName);
            ex = await Assert.ThrowsAnyAsync<ArgumentException>(() => client.GetVolume(Vol).Files.CopyAsync("/ok", path));
            Assert.Equal("destinationPath", ex.ParamName);
            Assert.Equal(before, cluster.Received.Values.Sum() - cluster.Received.GetValueOrDefault(MessageType.KeepAlive));
        }
    }

    [Fact]
    public async Task InvalidOpenOptions_AreRejectedLocally()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsFileClient files = client.GetVolume(Vol).Files;
            await Assert.ThrowsAnyAsync<ArgumentException>(() => files.OpenAsync("/x", new DfsOpenOptions { Mode = (FileMode)99, Access = FileAccess.Write }));
            await Assert.ThrowsAnyAsync<ArgumentException>(() => files.OpenAsync("/x", new DfsOpenOptions { Mode = FileMode.Truncate, Access = FileAccess.ReadWrite }));
            await Assert.ThrowsAsync<NotSupportedException>(() => files.OpenAsync("/x", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Options = FileOptions.DeleteOnClose }));
            await Assert.ThrowsAnyAsync<ArgumentException>(() => files.OpenAsync("/x", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.Write }));
            Assert.Equal(0, cluster.Received.GetValueOrDefault(MessageType.OpenHandle));
        }
    }

    [Fact]
    public async Task Disposal_IsIdempotentAndRejectsNewWork()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        {
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync("/d", Bytes(110, 10));
            await client.DisposeAsync();
            await client.DisposeAsync();
            client.Dispose();
            Assert.True(client.IsDisposed);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Files.ReadAllBytesAsync("/d"));
            Assert.Throws<ObjectDisposedException>(() => client.GetVolume(Vol));
        }
    }

    [Fact]
    public async Task Directories_CreateEnumerateAndDelete()
    {
        (FakeCluster cluster, DfsClient client) = await StartAsync();
        await using (cluster)
        await using (client)
        {
            DfsVolume volume = client.GetVolume(Vol);
            DfsDirectoryInfo info = await volume.Directories.CreateDirectoryAsync("/a/b/c");
            Assert.Equal("/a/b/c", info.Path);
            Assert.True(await volume.Directories.ExistsAsync("/a/b"));
            Assert.False(await volume.Files.ExistsAsync("/a/b"));
            for (int i = 0; i < 25; i++)
                await volume.Files.WriteAllBytesAsync($"/a/f{i:D2}", Bytes(i, 3));
            var names = new List<string>();
            await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync("/a", new DfsEnumerationOptions { PageSize = 7 }))
                names.Add(entry.Name);
            Assert.Equal(26, names.Count);
            Assert.Equal(names.Order(StringComparer.Ordinal), names);
            await volume.Directories.DeleteAsync("/a", recursive: true);
            Assert.False(await volume.Directories.ExistsAsync("/a"));
            await Assert.ThrowsAsync<DirectoryNotFoundException>(() => volume.Directories.DeleteAsync("/a"));
            await Assert.ThrowsAsync<FileNotFoundException>(() => volume.Files.GetInfoAsync("/a/f01"));
        }
    }
}
