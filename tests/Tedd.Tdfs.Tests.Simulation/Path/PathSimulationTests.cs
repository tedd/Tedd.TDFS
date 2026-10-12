using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Simulation.Sdk;

namespace Tedd.Tdfs.Tests.Simulation.Paths;

/// <summary>PATH suite, level S (test plan §7.3, FSD §4 NS-01/NS-02): canonical-name races under deterministic simulation.</summary>
public sealed class PathSimulationTests
{
    private const string Volume = SdkHelpers.Volume;

    /// <summary>Canonical (NFC) names and several input spellings of each (precomposed, decomposed, reordered marks, compatibility-canonical forms).</summary>
    private static readonly (string Nfc, string[] Spellings)[] CanonicalNames =
    [
        ("x\u00E9", ["x\u00E9", "xe\u0301"]),
        ("\u1EAD", ["\u1EAD", "a\u0323\u0302", "a\u0302\u0323", "\u1EA1\u0302"]),
        ("\uAC01", ["\uAC01", "\u1100\u1161\u11A8", "\uAC00\u11A8"]),
        ("\u00C5ngstr\u00F6m", ["\u00C5ngstr\u00F6m", "\u212Bngstro\u0308m", "A\u030Angstr\u00F6m"]),
    ];

    public static TheoryData<ulong> Seeds => new(11UL, 12UL, 13UL, 14UL, 15UL);

    [Theory, MemberData(nameof(Seeds))]
    [Catalog("PATH-011", "S", "INV-07")]
    public async Task PATH_011_CanonicalNameCreateRace(ulong seed)
    {
        // The spellings really are canonically equivalent (independent ICU oracle).
        foreach ((string nfc, string[] spellings) in CanonicalNames)
        {
            foreach (string spelling in spellings)
                Assert.Equal(nfc, spelling.Normalize(NormalizationForm.FormC));
        }

        TestClusterOptions options = SdkHelpers.Deterministic with { Seed = seed };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            const int ClientCount = 4;
            var clients = new List<DfsClient>();
            for (int i = 0; i < ClientCount; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "c" + i }));
            try
            {
                DfsVolume[] volumes = clients.Select(c => c.GetVolume(Volume)).ToArray();
                await volumes[0].Directories.CreateDirectoryAsync("/race");

                for (int n = 0; n < CanonicalNames.Length; n++)
                {
                    (string nfc, string[] spellings) = CanonicalNames[n];

                    // 1. Concurrent CreateNew with different spellings: exactly one entry and one winner.
                    byte[][] contents = Enumerable.Range(0, ClientCount).Select(i => TestData.Bytes(seed * 1000 + (ulong)(n * 10 + i), 64 + i)).ToArray();
                    Task<InodeId>[] creates = Enumerable.Range(0, ClientCount)
                        .Select(i => CreateNewAsync(volumes[i], $"/race/n{n}-{spellings[i % spellings.Length]}", contents[i]))
                        .ToArray();
                    Exception?[] errors = await Task.WhenAll(creates.Select(SettleAsync));
                    int[] winners = Enumerable.Range(0, ClientCount).Where(i => errors[i] is null).ToArray();
                    Assert.True(winners.Length == 1, $"name {n}: {winners.Length} concurrent CreateNew calls succeeded for one canonical name");
                    for (int i = 0; i < ClientCount; i++)
                    {
                        if (i == winners[0])
                            continue;
                        DfsAlreadyExistsException duplicate = Assert.IsAssignableFrom<DfsAlreadyExistsException>(SdkHelpers.Unwrap(errors[i]!));
                        Assert.Equal(DfsErrorCode.AlreadyExists, duplicate.ErrorCode);
                    }

                    InodeId winnerInode = await creates[winners[0]];
                    List<string> entries = await ListNamesAsync(volumes[0].Directories, "/race", $"n{n}-");
                    Assert.Equal(new[] { $"n{n}-{nfc}" }, entries);
                    foreach (string spelling in spellings)
                    {
                        DfsFileInfo info = await volumes[1].Files.GetInfoAsync($"/race/n{n}-{spelling}");
                        Assert.Equal(winnerInode, info.InodeId);
                        Assert.Equal($"/race/n{n}-{nfc}", info.Path);
                    }

                    byte[] stored = await volumes[2].Files.ReadAllBytesAsync($"/race/n{n}-{nfc}");
                    AssertEx.BytesEqual(contents[winners[0]], stored, $"name {n}: content is the winner's");

                    // 2. Concurrent CreateDirectory (idempotent): all succeed with the same inode, one entry.
                    Task<DfsDirectoryInfo>[] dirs = Enumerable.Range(0, ClientCount)
                        .Select(i => volumes[i].Directories.CreateDirectoryAsync($"/race/d{n}-{spellings[i % spellings.Length]}"))
                        .ToArray();
                    DfsDirectoryInfo[] dirInfos = await Task.WhenAll(dirs);
                    Assert.Single(dirInfos.Select(d => d.InodeId).Distinct());
                    Assert.All(dirInfos, d => Assert.Equal($"/race/d{n}-{nfc}", d.Path));
                    Assert.Equal(new[] { $"d{n}-{nfc}" }, await ListNamesAsync(volumes[0].Directories, "/race", $"d{n}-"));

                    // 3. Concurrent create-or-replace through different spellings: one register, linearizable history.
                    // Concurrent WriteAll/ReadAll on one inode may legally be refused with a typed sharing/writer
                    // conflict (no effect, no automatic waiting — LOCK-03); such refusals are retried inside the
                    // recorded operation and are not evidence about canonical-name identity.
                    string key = $"/race/w{n}-{nfc}";
                    InodeId? registerInode = null;
                    for (int round = 0; round < 3; round++)
                    {
                        Task[] ops = Enumerable.Range(0, ClientCount).Select(i =>
                        {
                            DfsVolume v = volumes[i];
                            string spelling = $"/race/w{n}-{spellings[(i + round) % spellings.Length]}";
                            if ((i + round) % 2 == 0)
                            {
                                byte[] value = Encoding.UTF8.GetBytes($"s{seed}-n{n}-r{round}-c{i}");
                                return (Task)cluster.History.RecordAsync("c" + i, "write", key, value,
                                    () => SdkHelpers.RetryOnSharingViolationAsync(cluster, () => v.Files.WriteAllBytesAsync(spelling, value)));
                            }

                            return cluster.History.RecordAsync<byte[]?>("c" + i, "read", key, null, () => SdkHelpers.RetryOnSharingViolationAsync<byte[]?>(cluster, async () =>
                            {
                                try
                                {
                                    return await v.Files.ReadAllBytesAsync(spelling);
                                }
                                catch (FileNotFoundException)
                                {
                                    return null;
                                }
                            }));
                        }).ToArray();
                        await Task.WhenAll(ops);
                        InodeId inode = (await volumes[0].Files.GetInfoAsync(key)).InodeId;
                        registerInode ??= inode;
                        Assert.Equal(registerInode, inode); // replacing through any spelling preserves the inode
                    }

                    Assert.Equal(new[] { $"w{n}-{nfc}" }, await ListNamesAsync(volumes[0].Directories, "/race", $"w{n}-"));
                    List<HistoryOp> history = cluster.History.Operations.Where(o => o.Key == key).ToList();
                    Assert.Equal(3 * ClientCount, history.Count);
                    LinearizabilityChecker.Check(history, new RegisterModel()).AssertValid();
                }

                // Every caught-up replica holds the same canonical namespace.
                await cluster.WaitForReplicationAsync(Volume);
                await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
                for (int n = 0; n < CanonicalNames.Length; n++)
                {
                    string path = $"/race/n{n}-{CanonicalNames[n].Nfc}";
                    InodeId? expected = null;
                    foreach (TestNode node in cluster.Nodes)
                    {
                        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, node.Name);
                        FileInspection? file = state.GetFile(path);
                        Assert.NotNull(file);
                        expected ??= file.Inode;
                        Assert.Equal(expected, file.Inode);
                        // No second entry is stored under a non-NFC spelling. (Whether the inspector normalizes its
                        // argument is unspecified, so a lookup may also resolve to the one canonical entry.)
                        foreach (string spelling in CanonicalNames[n].Spellings.Where(s => s != CanonicalNames[n].Nfc))
                        {
                            FileInspection? alias = state.GetFile($"/race/n{n}-{spelling}");
                            Assert.True(alias is null || alias.Inode == file.Inode, $"{node.Name}: a separate entry exists for spelling {spelling} of name {n}");
                        }
                    }
                }
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, SdkHelpers.Ct);
    }

    private static async Task<Exception?> SettleAsync(Task task)
    {
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

    private static async Task<InodeId> CreateNewAsync(DfsVolume volume, string path, byte[] content)
    {
        await using DfsStream s = await volume.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None });
        await s.WriteAsync(content);
        await s.FlushAsync();
        return s.InodeId;
    }

    private static async Task<List<string>> ListNamesAsync(DfsDirectoryClient directories, string path, string prefix)
    {
        var names = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(path))
        {
            if (entry.Name.StartsWith(prefix, StringComparison.Ordinal))
                names.Add(entry.Name);
        }

        return names;
    }
}
