using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Prop.PropHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Prop;

public sealed class TransactionRecoveryWorkloadTests
{
    public static TheoryData<ulong, CrashMode> Histories
    {
        get
        {
            var cases = new TheoryData<ulong, CrashMode>();
            foreach (ulong seed in new ulong[] { 17011, 17027, 17041 })
                foreach (CrashMode crash in Enum.GetValues<CrashMode>())
                    cases.Add(seed, crash);
            return cases;
        }
    }

    [Theory, MemberData(nameof(Histories))]
    public Task ColdRestartPreservesCommittedTreesAndHistoricalSnapshots(ulong seed, CrashMode crash) =>
        RunPropertyAsync(nameof(ColdRestartPreservesCommittedTreesAndHistoricalSnapshots), seed, trace =>
            ClusterTest.RunAsync(Sim(seed), async cluster =>
            {
                var random = new SeededRandom(seed);
                int[] sizes = [0, 1, InlineThreshold - 1, InlineThreshold, InlineThreshold + 1, ChunkSize - 1, ChunkSize + 1];
                var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                var snapshots = new Dictionary<string, Dictionary<string, byte[]>>(StringComparer.Ordinal);
                await using (DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct))
                await using (DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct))
                {
                    DfsVolume volume = writer.GetVolume(Vol);
                    DfsVolume observer = reader.GetVolume(Vol);
                    for (int round = 0; round < 12; round++)
                    {
                        // Only replace arrays in a new dictionary, so each saved snapshot stays independent of later rounds.
                        var staged = new Dictionary<string, byte[]>(committed, StringComparer.Ordinal);
                        await using DfsTransaction tx = await volume.Transactions.BeginAsync(ct: Ct);
                        for (int step = 0; step < 5; step++)
                        {
                            string path = $"/f{random.Next(8)}";
                            string target = $"/f{8 + random.Next(4)}";
                            int size = sizes[random.Next(sizes.Length)];
                            byte[] bytes = TestData.Bytes(seed + (ulong)(5 * round + step), size);
                            int operation = random.Next(5);
                            trace.Step($"round={round} step={step} op={operation} path={path} target={target} length={size}");
                            switch (operation)
                            {
                                case 0 when staged.Remove(path):
                                    await tx.Files.DeleteAsync(path, Ct);
                                    break;
                                case 1 when staged.TryGetValue(path, out byte[]? source):
                                    await tx.Files.CopyAsync(path, target, overwrite: true, ct: Ct);
                                    staged[target] = source;
                                    break;
                                case 2 when staged.Remove(path, out byte[]? moved):
                                    await tx.Files.MoveAsync(path, target, overwrite: true, ct: Ct);
                                    staged[target] = moved;
                                    break;
                                case 3:
                                    await tx.Files.AppendAllBytesAsync(path, bytes, Ct);
                                    staged[path] = [.. (staged.GetValueOrDefault(path) ?? []), .. bytes];
                                    break;
                                default:
                                    await tx.Files.WriteAllBytesAsync(path, bytes, Ct);
                                    staged[path] = bytes;
                                    break;
                            }
                        }

                        await VerifyAsync(tx.Files, tx.Directories, staged);
                        await VerifyAsync(observer.Files, observer.Directories, committed);
                        if (round % 3 == 1)
                        {
                            await tx.AbortAsync(Ct);
                        }
                        else
                        {
                            if (round % 3 == 0)
                            {
                                string name = $"s{round}";
                                await tx.CommitWithSnapshotAsync(name, Ct);
                                snapshots.Add(name, staged);
                            }
                            else
                            {
                                await tx.CommitAsync(Ct);
                            }
                            committed = staged;
                        }
                        await VerifyAsync(observer.Files, observer.Directories, committed);
                    }
                }

                trace.Step($"cold restart {crash}");
                foreach (TestNode node in cluster.Nodes)
                    await node.CrashAsync(crash);
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

                await using DfsClient recovered = await cluster.ConnectAsync(new TestClientOptions { Name = "recovered" }, Ct);
                DfsVolume restored = recovered.GetVolume(Vol);
                await VerifyAsync(restored.Files, restored.Directories, committed);
                Assert.Equal(snapshots.Keys.Order(), (await restored.Snapshots.ListAsync(Ct)).Select(info => info.Name).Order());
                foreach ((string name, Dictionary<string, byte[]> tree) in snapshots)
                {
                    await using DfsSnapshotView view = await restored.Snapshots.OpenAsync(name, Ct);
                    await VerifyAsync(view.Files, view.Directories, tree);
                }
            }, Ct));

    private static async Task VerifyAsync(DfsFileClient files, DfsDirectoryClient dirs, Dictionary<string, byte[]> model)
    {
        var paths = new List<string>();
        await foreach (DfsEntryInfo entry in dirs.EnumerateAsync("/", new DfsEnumerationOptions { PageSize = 3 }, Ct))
            paths.Add(entry.Path);
        Assert.Equal(model.Keys.Order(StringComparer.Ordinal), paths.Order(StringComparer.Ordinal));
        foreach ((string path, byte[] expected) in model)
        {
            byte[] actual = await files.ReadAllBytesAsync(path, Ct);
            AssertEx.BytesEqual(expected, actual, path);
        }
    }
}
