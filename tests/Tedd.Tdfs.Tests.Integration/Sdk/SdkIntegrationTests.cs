using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Sdk;

/// <summary>
/// SDK suite, level I (test plan §7.2, FSD §§1, 5): real QUIC transport, real disk storage and the public SDK entry
/// points — no mount, synchronous wrappers under a single-threaded synchronization context, and platform parity.
/// </summary>
public sealed class SdkIntegrationTests
{
    private const string Volume = SdkHelpers.Volume;

    private static CancellationToken Ct => SdkHelpers.Ct;

    // =====================================================================================================
    // SDK-002 NoMountRequired
    // =====================================================================================================

    private static readonly string[] MountComponentMarkers = ["fuse", "dokan", "winfsp", "projfs"];

    [Fact]
    [Catalog("SDK-002", "I", "INV-08", "INV-17")]
    public async Task SDK_002_NoMountRequired()
    {
        string marker = "sdk002-" + Convert.ToHexString(TestData.Bytes(2002, 6)).ToLowerInvariant();
        Assert.Empty(LoadedMountAssemblies());
        await ClusterTest.RunAsync(SdkHelpers.QuicOnDisk, async cluster =>
        {
            // The public entry point with network seeds only: no local path is configured anywhere.
            DfsClientOptions options = cluster.CreateClientOptions();
            Assert.All(options.SeedEndpoints, s => Assert.StartsWith("quic://", s, StringComparison.Ordinal));
            await using DfsClient client = await DfsClient.ConnectAsync(options, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            string dir = "/" + marker;
            await volume.Directories.CreateDirectoryAsync(dir, Ct);

            // File helpers.
            const string Settings = "{\"enabled\":true}";
            await volume.Files.WriteAllTextAsync(dir + "/settings.json", Settings, null, Ct);
            Assert.Equal(Settings, await volume.Files.ReadAllTextAsync(dir + "/settings.json", null, Ct));

            // StreamWriter over a returned stream, which is a Stream but not a FileStream (no OS handle).
            string[] lines = Enumerable.Range(0, 200).Select(i => $"line {i}: {TestData.Utf8Text((ulong)i, 40)}").ToArray();
            await using (DfsStream output = await volume.Files.CreateAsync(dir + "/log.txt", Ct))
            {
                Assert.IsNotType<FileStream>(output);
                Assert.IsAssignableFrom<Stream>(output);
                await using (var writer = new StreamWriter(output, new UTF8Encoding(false), 1024, leaveOpen: true))
                {
                    foreach (string line in lines)
                        await writer.WriteLineAsync(line);
                    await writer.FlushAsync(Ct);
                }

                await output.FlushAsync(Ct);
            }

            // StreamReader over a returned read stream.
            await using (Stream input = await volume.Files.OpenReadAsync(dir + "/log.txt", Ct))
            {
                using var reader = new StreamReader(input);
                string text = await reader.ReadToEndAsync(Ct);
                Assert.Equal(string.Concat(lines.Select(l => l + Environment.NewLine)), text);
            }

            Assert.Equal(lines, await volume.Files.ReadAllLinesAsync(dir + "/log.txt", null, Ct));

            // CopyToAsync in both directions (multi-chunk payload).
            byte[] blob = TestData.Bytes(2003, 300_000);
            await using (DfsStream output = await volume.Files.CreateAsync(dir + "/blob.bin", Ct))
                await new MemoryStream(blob).CopyToAsync(output, 81920, Ct);
            var copy = new MemoryStream();
            await using (Stream input = await volume.Files.OpenReadAsync(dir + "/blob.bin", Ct))
                await input.CopyToAsync(copy, Ct);
            AssertEx.BytesEqual(blob, copy.ToArray());

            // A seek-requiring consumer: ZipArchive in read mode needs Seek/Length/Position.
            byte[][] entries = Enumerable.Range(0, 3).Select(i => TestData.Bytes(2010 + (ulong)i, 10_000 * (i + 1))).ToArray();
            await using (DfsStream zipOut = await volume.Files.CreateAsync(dir + "/archive.zip", Ct))
            {
                using (var zip = new ZipArchive(zipOut, ZipArchiveMode.Create, leaveOpen: true))
                {
                    for (int i = 0; i < entries.Length; i++)
                    {
                        await using Stream entryStream = zip.CreateEntry($"entry{i}.bin").Open();
                        await entryStream.WriteAsync(entries[i], Ct);
                    }
                }

                await zipOut.FlushAsync(Ct);
            }

            await using (DfsStream zipIn = await volume.Files.OpenReadAsync(dir + "/archive.zip", Ct))
            {
                Assert.True(zipIn.CanSeek);
                using var zip = new ZipArchive(zipIn, ZipArchiveMode.Read, leaveOpen: true);
                Assert.Equal(entries.Length, zip.Entries.Count);
                for (int i = 0; i < entries.Length; i++)
                {
                    await using Stream entryStream = zip.GetEntry($"entry{i}.bin")!.Open();
                    var extracted = new MemoryStream();
                    await entryStream.CopyToAsync(extracted, Ct);
                    AssertEx.BytesEqual(entries[i], extracted.ToArray(), $"zip entry {i}");
                }
            }

            // Layered consumers: compression and buffering wrappers.
            byte[] compressible = TestData.Compressible(200_000);
            await using (DfsStream output = await volume.Files.CreateAsync(dir + "/data.gz", Ct))
            {
                await using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
                    await gzip.WriteAsync(compressible, Ct);
                await output.FlushAsync(Ct);
            }

            Assert.True((await volume.Files.GetInfoAsync(dir + "/data.gz", Ct)).Length < compressible.Length / 10);
            await using (Stream input = await volume.Files.OpenReadAsync(dir + "/data.gz", Ct))
            await using (var gunzip = new GZipStream(new BufferedStream(input, 4096), CompressionMode.Decompress))
            {
                var inflated = new MemoryStream();
                await gunzip.CopyToAsync(inflated, Ct);
                AssertEx.BytesEqual(compressible, inflated.ToArray());
            }

            // No hidden local-path dependency: nothing named after the volume paths exists on the host.
            string localEquivalent = System.IO.Path.GetFullPath(dir);
            Assert.False(Directory.Exists(localEquivalent), $"{localEquivalent} must not exist");
            Assert.False(File.Exists(System.IO.Path.Combine(localEquivalent, "settings.json")));
            Assert.Empty(FindNamed(cluster.DataRoot, marker, recursive: true));
            Assert.Empty(FindNamed(Environment.CurrentDirectory, marker, recursive: false));
            Assert.Empty(FindNamed(System.IO.Path.GetTempPath(), marker, recursive: false));
            Assert.Empty(LoadedMountAssemblies());
        }, Ct);
    }

    private static string[] LoadedMountAssemblies() =>
        AppDomain.CurrentDomain.GetAssemblies()
            .Select(a => a.GetName().Name ?? string.Empty)
            .Where(n => MountComponentMarkers.Any(m => n.Contains(m, StringComparison.OrdinalIgnoreCase)))
            .ToArray();

    private static string[] FindNamed(string root, string marker, bool recursive)
    {
        if (!Directory.Exists(root))
            return [];
        var options = new EnumerationOptions { RecurseSubdirectories = recursive, IgnoreInaccessible = true, AttributesToSkip = 0 };
        return Directory.EnumerateFileSystemEntries(root, "*", options)
            .Where(p => System.IO.Path.GetFileName(p).Contains(marker, StringComparison.OrdinalIgnoreCase))
            .ToArray();
    }

    // =====================================================================================================
    // SDK-010 SyncContextDoesNotDeadlock
    // =====================================================================================================

    private static readonly string[] SyncSteps =
    [
        "DfsClient.Connect", "Directories.CreateDirectory", "Files.WriteAllBytes", "Files.ReadAllBytes", "Files.Exists", "Files.GetInfo",
        "Files.WriteAllLines", "Files.ReadLines", "Files.AppendAllText", "Stream.Write+SetLength+Flush+Dispose", "Stream.Read+Length",
        "Files.Copy", "Files.Move", "Files.Delete", "Directories.Enumerate", "Directories.ListPage", "Transactions.Begin+Commit",
        "Snapshots.Create+List+Open", "Volume.GetInfo", "Volume.NewOperationId+QueryOperation", "DfsClient.Dispose",
    ];

    [Fact]
    [Catalog("SDK-010", "I", "INV-17")]
    public async Task SDK_010_SyncContextDoesNotDeadlock()
    {
        await ClusterTest.RunAsync(SdkHelpers.QuicOnDisk, async cluster =>
        {
            DfsClientOptions options = cluster.CreateClientOptions();

            // Synchronous wrappers called directly on a single-threaded context (the context's only thread blocks).
            Task<List<string>> direct = SingleThreadedSynchronizationContext.Run(
                context => Task.FromResult(RunSyncSurface(context, options with { ClientName = "sdk010-direct" }, "/sync-direct")), "sdk-010-direct");
            List<string> steps = await direct.WaitAsync(TimeSpan.FromSeconds(90), Ct);
            Assert.Equal(SyncSteps, steps);

            // The same wrappers called from an async method that is itself running on the single-threaded context.
            Task<List<string>> nested = SingleThreadedSynchronizationContext.Run(async context =>
            {
                await Task.Yield();
                Assert.Same(context, SynchronizationContext.Current);
                Assert.Equal(context.ThreadId, Environment.CurrentManagedThreadId);
                List<string> inner = RunSyncSurface(context, options with { ClientName = "sdk010-nested" }, "/sync-nested");
                await Task.Yield();
                Assert.Equal(context.ThreadId, Environment.CurrentManagedThreadId);
                return inner;
            }, "sdk-010-nested");
            Assert.Equal(SyncSteps, await nested.WaitAsync(TimeSpan.FromSeconds(90), Ct));
        }, Ct);
    }

    private static List<string> RunSyncSurface(SingleThreadedSynchronizationContext context, DfsClientOptions options, string root)
    {
        var steps = new List<string>();
        void Step(string name, Action action)
        {
            int postsBefore = context.PostCount;
            action();
            Assert.Same(context, SynchronizationContext.Current);
            Assert.True(context.PostCount == postsBefore,
                $"{name} scheduled {context.PostCount - postsBefore} continuation(s) onto the blocked synchronization context");
            Assert.Equal(0, context.SendCount);
            steps.Add(name);
        }

        byte[] data = TestData.Bytes(1010, 100_000);
        string[] lines = ["alpha", "beta", "\u00E6\u00F8\u00E5", "\U0001F600"];
        string snapshot = "sdk010" + root.Replace('/', '-');
        DfsClient? client = null;
        Step("DfsClient.Connect", () => client = DfsClient.Connect(options));
        DfsVolume volume = client!.GetVolume(Volume);
        Step("Directories.CreateDirectory", () => Assert.Equal(root, volume.Directories.CreateDirectory(root).Path));
        Step("Files.WriteAllBytes", () => Assert.False(volume.Files.WriteAllBytes(root + "/a.bin", data).IsNone));
        Step("Files.ReadAllBytes", () => AssertEx.BytesEqual(data, volume.Files.ReadAllBytes(root + "/a.bin")));
        Step("Files.Exists", () => Assert.True(volume.Files.Exists(root + "/a.bin")));
        Step("Files.GetInfo", () => Assert.Equal(data.Length, volume.Files.GetInfo(root + "/a.bin").Length));
        Step("Files.WriteAllLines", () => volume.Files.WriteAllLines(root + "/lines.txt", lines));
        Step("Files.ReadLines", () => Assert.Equal(lines, volume.Files.ReadLines(root + "/lines.txt").ToArray()));
        Step("Files.AppendAllText", () =>
        {
            volume.Files.AppendAllText(root + "/lines.txt", "tail");
            Assert.EndsWith("tail", volume.Files.ReadAllText(root + "/lines.txt"), StringComparison.Ordinal);
        });
        Step("Stream.Write+SetLength+Flush+Dispose", () =>
        {
            using DfsStream s = volume.Files.Create(root + "/s.bin");
            s.Write(data);
            s.Flush();
            s.SetLength(50_000);
        });
        Step("Stream.Read+Length", () =>
        {
            using DfsStream s = volume.Files.OpenRead(root + "/s.bin");
            Assert.Equal(50_000, s.Length);
            byte[] buffer = new byte[50_000];
            s.ReadExactly(buffer);
            AssertEx.BytesEqual(data.AsSpan(0, 50_000), buffer);
            Assert.Equal(-1, s.ReadByte());
        });
        Step("Files.Copy", () => volume.Files.Copy(root + "/a.bin", root + "/copy.bin"));
        Step("Files.Move", () => volume.Files.Move(root + "/copy.bin", root + "/moved.bin"));
        Step("Files.Delete", () =>
        {
            volume.Files.Delete(root + "/moved.bin");
            Assert.False(volume.Files.Exists(root + "/moved.bin"));
        });
        Step("Directories.Enumerate", () =>
            Assert.Equal(new[] { "a.bin", "lines.txt", "s.bin" }, volume.Directories.Enumerate(root).Select(e => e.Name).Order(StringComparer.Ordinal).ToArray()));
        Step("Directories.ListPage", () => Assert.Equal(3, volume.Directories.ListPage(root).Entries.Count));
        Step("Transactions.Begin+Commit", () =>
        {
            using DfsTransaction tx = volume.Transactions.Begin();
            tx.Files.WriteAllBytes(root + "/tx.bin", data.AsMemory(0, 10));
            Assert.False(tx.Commit().IsNone);
            Assert.Equal(DfsTransactionState.Committed, tx.State);
        });
        Step("Snapshots.Create+List+Open", () =>
        {
            DfsSnapshotInfo info = volume.Snapshots.Create(snapshot);
            Assert.Equal(snapshot, info.Name);
            Assert.Contains(volume.Snapshots.List(), s => s.Name == snapshot);
            using DfsSnapshotView view = volume.Snapshots.Open(snapshot);
            AssertEx.BytesEqual(data, view.Files.ReadAllBytes(root + "/a.bin"));
        });
        Step("Volume.GetInfo", () => Assert.Equal(Volume, volume.GetInfo().Name));
        Step("Volume.NewOperationId+QueryOperation", () =>
        {
            DfsOperationId id = volume.NewOperationId();
            CommitToken token = volume.Files.AppendAllBytes(root + "/a.bin", new byte[] { 1, 2, 3 }, new DfsMutationOptions { OperationId = id });
            DfsOperationOutcome outcome = volume.QueryOperation(id);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(token, outcome.Token);
        });
        Step("DfsClient.Dispose", () =>
        {
            client.Dispose();
            Assert.True(client.IsDisposed);
        });
        return steps;
    }

    // =====================================================================================================
    // SDK-015 PlatformContractParity
    // =====================================================================================================

    /// <summary>
    /// Names that are reserved, rewritten, case-folded, normalized or otherwise special on at least one host OS
    /// (Windows device names, NTFS stream syntax, trailing dots/spaces, HFS+/APFS normalization, case-insensitive
    /// file systems). In the volume namespace each is an ordinary, case-sensitive, NFC-identified name.
    /// </summary>
    private static readonly string[] HostHostileNames =
    [
        "CON", "con", "NUL", "aux.txt", "COM1", "LPT1.log", "PRN", "a:b", "file.txt:stream", "a*b?", "<>|\"", "trailing.", "trailing ",
        " leading", "Readme", "README", "caf\u00E9", "cafe\u0301", "\u00C5ngstr\u00F6m", "\u212Bngstr\u00F6m", "tab\tname", "new\nline",
        new string('x', 255), "...", ".hidden", "~$lock", "desktop.ini", ".DS_Store", "Thumbs.db", "\u0130", "i", "back`tick", "semi;colon",
        "%TEMP%", "$HOME", "\U0001F600",
    ];

    [Fact]
    [Catalog("SDK-015", "I", "INV-08")]
    public async Task SDK_015_PlatformContractParity()
    {
        string platform = OperatingSystem.IsWindows() ? "windows" : OperatingSystem.IsLinux() ? "linux" : OperatingSystem.IsMacOS() ? "macos" : "unsupported";
        if (platform == "unsupported")
            Assert.Skip($"SDK-015 covers the supported SDK platforms (Windows, Linux, macOS); this host is {RuntimeInformation.OSDescription}: status not run");

        // Expected transcript, computed from the plan alone (independent of the host OS): later writes to an
        // NFC-equivalent name replace earlier ones; every other name is its own entry.
        var expected = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        for (int i = 0; i < HostHostileNames.Length; i++)
            expected[HostHostileNames[i].Normalize(NormalizationForm.FormC)] = ContentFor(i);
        string deepPath = "/sdk015/deep" + string.Concat(Enumerable.Range(0, 15).Select(i => "/" + new string((char)('a' + i), 250)));
        Assert.True(Encoding.UTF8.GetByteCount(deepPath) is > 3700 and <= DfsPath.MaxPathBytes);
        byte[] deepContent = TestData.Bytes(1599, 1000);

        await ClusterTest.RunAsync(SdkHelpers.QuicOnDisk, async cluster =>
        {
            await using DfsClient client = await DfsClient.ConnectAsync(cluster.CreateClientOptions(), Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/sdk015/names", Ct);
            for (int i = 0; i < HostHostileNames.Length; i++)
                await volume.Files.WriteAllBytesAsync("/sdk015/names/" + HostHostileNames[i], ContentFor(i), Ct);
            await volume.Directories.CreateDirectoryAsync(DfsPath.GetParent(deepPath), Ct);
            await volume.Files.WriteAllBytesAsync(deepPath, deepContent, Ct);

            // Actual transcript from the volume.
            var actual = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
            await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync("/sdk015/names", null, Ct))
                actual[entry.Name] = await volume.Files.ReadAllBytesAsync("/sdk015/names/" + entry.Name, Ct);

            string expectedTranscript = Transcript(expected);
            string actualTranscript = Transcript(actual);
            SdkHelpers.Log($"SDK-015 on {platform} ({RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}): transcript sha256 {Hash(actualTranscript)}; expected {Hash(expectedTranscript)}");
            Assert.Equal(expectedTranscript, actualTranscript);

            // Host rules did not leak into the namespace: no stripping, folding, stream splitting or device mapping.
            Assert.False(await volume.Files.ExistsAsync("/sdk015/names/trailing", Ct));
            Assert.False(await volume.Files.ExistsAsync("/sdk015/names/file.txt", Ct));
            Assert.False(await volume.Files.ExistsAsync("/sdk015/names/readme", Ct));
            Assert.False(await volume.Files.ExistsAsync("/sdk015/names/leading", Ct));
            Assert.NotEqual((await volume.Files.GetInfoAsync("/sdk015/names/CON", Ct)).InodeId, (await volume.Files.GetInfoAsync("/sdk015/names/con", Ct)).InodeId);
            Assert.Equal((await volume.Files.GetInfoAsync("/sdk015/names/caf\u00E9", Ct)).InodeId, (await volume.Files.GetInfoAsync("/sdk015/names/cafe\u0301", Ct)).InodeId);
            byte[] deepRead = await volume.Files.ReadAllBytesAsync(deepPath, Ct);
            AssertEx.BytesEqual(deepContent, deepRead, "a path far beyond host MAX_PATH limits");
            Assert.Equal(deepPath, (await volume.Files.GetInfoAsync(deepPath, Ct)).Path);
        }, Ct);

        static byte[] ContentFor(int index) => TestData.Bytes(1500 + (ulong)index, 16 + index);

        static string Transcript(SortedDictionary<string, byte[]> entries) =>
            string.Join("\n", entries.Select(kv =>
                $"{Convert.ToHexString(Encoding.UTF8.GetBytes(kv.Key))}|{kv.Value.Length}|{Convert.ToHexString(SHA256.HashData(kv.Value))}"));

        static string Hash(string transcript) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(transcript)));
    }
}
