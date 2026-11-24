using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Workloads;

public sealed class ConcurrentAppendTests
{
    private sealed record Append(CommitToken Token, byte[] Payload);

    [Theory]
    [InlineData(4, 1)]
    [InlineData(8, 257)]
    [InlineData(4, 65537)]
    public Task ConcurrentAppendsAndRetriesMatchCommitOrder(int workers, int recordSize)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { Storage = StorageKind.Disk }, async cluster =>
        {
            const string path = "/journal";
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, ct);
            DfsVolume observer = reader.GetVolume("vol");
            await observer.Files.WriteAllBytesAsync(path, Array.Empty<byte>(), ct);
            var history = new List<Append>();
            byte[]? checkpoint = null;
            for (int round = 0; round < 2; round++)
            {
                // Release every client's first request together, after all sessions and operation IDs exist.
                var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                int ready = 0;
                using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
                async Task<List<Append>> RunAsync(int worker)
                {
                    try
                    {
                        await using DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = $"w{round}-{worker}" }, stop.Token);
                        DfsVolume volume = writer.GetVolume("vol");
                        var records = new List<Append>();
                        for (int index = 0; index < 8; index++)
                        {
                            int serial = (round * workers + worker) * 8 + index;
                            byte[] payload = TestData.Bytes((ulong)(serial + 1), recordSize);
                            payload[0] = checked((byte)serial);
                            var options = new DfsMutationOptions { OperationId = await volume.NewOperationIdAsync(stop.Token) };
                            if (index == 0)
                            {
                                if (Interlocked.Increment(ref ready) == workers)
                                    start.TrySetResult();
                                await start.Task.WaitAsync(stop.Token);
                            }

                            CommitToken token = await volume.Files.AppendAllBytesAsync(path, payload, options, stop.Token);
                            Assert.False(token.IsNone);
                            records.Add(new Append(token, payload));
                            Assert.Equal(token, await volume.Files.AppendAllBytesAsync(path, payload, options, stop.Token));
                        }
                        return records;
                    }
                    catch
                    {
                        await stop.CancelAsync();
                        throw;
                    }
                }

                List<Append>[] completed = await Task.WhenAll(Enumerable.Range(0, workers).Select(RunAsync));
                history.AddRange(completed.SelectMany(records => records));
                Assert.Equal(history.Count, history.Select(record => record.Token).Distinct().Count());
                Assert.All(history, record => Assert.True(history[0].Token.IsComparableWith(record.Token)));
                byte[] expected = history.OrderBy(record => record.Token.Sequence).SelectMany(record => record.Payload).ToArray();
                byte[] live = await observer.Files.ReadAllBytesAsync(path, ct);
                AssertEx.BytesEqual(expected, live, $"journal after round {round}");
                if (round == 0)
                {
                    checkpoint = expected;
                    await observer.Snapshots.CreateAsync("halfway", ct: ct);
                }
            }

            await using DfsSnapshotView snapshot = await observer.Snapshots.OpenAsync("halfway", ct);
            byte[] pinned = await snapshot.Files.ReadAllBytesAsync(path, ct);
            AssertEx.BytesEqual(checkpoint!, pinned, "snapshot after subsequent appends");
        }, ct);
    }
}
