using System.Globalization;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Prop.PropHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Prop;

/// <summary>
/// PROP — property-based and model-based operation histories (test plan §7.28). Every test generates seeded histories
/// (sizes and offsets biased to boundary values), runs them against deterministic simulated clusters and compares the
/// results with independent oracles (ReferenceFileModel, ReferenceNamespaceModel, TreeModel, LinearizabilityChecker,
/// SerializabilityChecker). Failures are reported with the seed and the generated step trace.
/// </summary>
public sealed class PropSimulationTests
{
    // =====================================================================================================
    // PROP-001 GeneratedFileByteModel
    // =====================================================================================================

    /// <summary>(seed, initial file length) — initial lengths cover 0, 1, inline ±1, chunk and 2×chunk+1.</summary>
    public static TheoryData<ulong, int> FileByteModelCases => new()
    {
        { 101, 0 }, { 102, 1 }, { 103, InlineThreshold - 1 }, { 104, InlineThreshold + 1 }, { 105, 64 * 1024 }, { 106, 2 * 64 * 1024 + 1 },
    };

    [Theory, MemberData(nameof(FileByteModelCases)), Catalog("PROP-001", "S", "INV-02", "INV-03", "INV-07", "INV-14")]
    public Task PROP_001_GeneratedFileByteModel(ulong seed, int initialLength) =>
        RunPropertyAsync("PROP-001", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            const string dir = "/prop001";
            const string path = "/prop001/file.bin";
            const int operations = 48;
            long maxLength = 2L * ChunkSize + InlineThreshold + 3;
            var random = new SeededRandom(Derive(seed, "prop-001"));
            var sizes = new BoundaryBiasedSizes(random.Fork("sizes"), InlineThreshold, ChunkSize, BufferSize);

            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);

            byte[] initial = Content(seed, 0, initialLength);
            await wv.Directories.CreateDirectoryAsync(dir, Ct);
            CommitToken lastToken = await wv.Files.WriteAllBytesAsync(path, initial, Ct);
            Assert.False(lastToken.IsNone);
            var model = new ReferenceFileModel(initial);
            DfsFileInfo baseline = await rv.Files.GetInfoAsync(path, Ct);
            Assert.Equal(initial.LongLength, baseline.Length);
            InodeId inode = baseline.InodeId;

            DfsStream writer = await OpenWriterAsync(wv, path);
            Assert.Equal(inode, writer.InodeId);
            // A writable open may itself advance the inode record (writer generation, ARCHITECTURE §5.3), so the baseline
            // revision for "publication creates a new revision" / "pins see the latest revision" is taken after the open.
            ulong revision = await RevisionAfterOpenAsync(rv, path, baseline.Revision, initial.LongLength);
            DfsStream live = await rv.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct);
            var pins = new List<(DfsStream Stream, byte[] Image)>();
            int dataId = 1;
            try
            {
                for (int step = 0; step < operations; step++)
                {
                    int roll = random.Next(100);
                    if (roll < 30)
                    {
                        long offset = sizes.NextOffset(model.StagedLength, maxLength - 1);
                        int length = (int)sizes.NextLength(Math.Min(maxLength - offset, 2L * ChunkSize));
                        byte[] data = Content(seed, dataId++, length);
                        trace.Step($"write offset={offset} length={length}");
                        writer.Seek(offset, SeekOrigin.Begin);
                        await WriteWithRandomOverloadAsync(writer, data, random);
                        model.Write(offset, data);
                        Array.Fill(data, (byte)0xEE); // caller reuses its buffer: the accepted bytes must not change (FSD §6.4)
                        Assert.Equal(offset + length, writer.Position);
                        Assert.Equal(model.StagedLength, await writer.GetLengthAsync(Ct));
                    }
                    else if (roll < 37)
                    {
                        long target = sizes.NextOffset(model.StagedLength, maxLength + 7);
                        long before = writer.Position;
                        (long arg, SeekOrigin origin) = random.Next(3) switch
                        {
                            0 => (target, SeekOrigin.Begin),
                            1 => (target - before, SeekOrigin.Current),
                            _ => (target - model.StagedLength, SeekOrigin.End),
                        };
                        trace.Step($"seek {origin} {arg} (target {target})");
                        Assert.Equal(target, writer.Seek(arg, origin));
                        Assert.Equal(target, writer.Position);
                        Assert.Equal(model.StagedLength, await writer.GetLengthAsync(Ct)); // seeking beyond EOF never extends
                    }
                    else if (roll < 45)
                    {
                        long newLength = sizes.NextLength(maxLength);
                        long position = writer.Position;
                        trace.Step($"setlength {newLength}");
                        await writer.SetLengthAsync(newLength, Ct);
                        model.SetLength(newLength);
                        Assert.Equal(position, writer.Position); // SetLength never moves Position (FSD §6.2)
                        Assert.Equal(newLength, await writer.GetLengthAsync(Ct));
                    }
                    else if (roll < 57)
                    {
                        bool pending = model.HasPendingChanges;
                        bool useCommit = random.Next(2) == 0;
                        trace.Step((useCommit ? "commit" : "flush") + (pending ? " (pending changes)" : " (no byte changes)"));
                        if (useCommit)
                        {
                            CommitToken token = await writer.CommitAsync(Ct);
                            if (pending)
                            {
                                Assert.False(token.IsNone);
                                Assert.True(token.IsComparableWith(lastToken) && token.CompareTo(lastToken) > 0,
                                    $"commit token {token} must follow {lastToken}");
                                lastToken = token;
                            }
                        }
                        else
                        {
                            await writer.FlushAsync(Ct);
                        }

                        model.Flush();
                        DfsFileInfo info = await rv.Files.GetInfoAsync(path, Ct);
                        Assert.Equal(inode, info.InodeId);
                        Assert.Equal(model.CommittedLength, info.Length);
                        if (pending)
                            Assert.True(info.Revision > revision, $"publication did not create a new revision ({info.Revision} <= {revision})");
                        else
                            Assert.True(info.Revision >= revision);
                        revision = info.Revision;
                        await BytesEqualAsync(model.Committed, rv.Files.ReadAllBytesAsync(path, Ct), "committed view after publication");
                    }
                    else if (roll < 61)
                    {
                        trace.Step("abort staged changes, reopen writer");
                        await writer.AbortAsync(Ct);
                        model.Abort();
                        DfsFileInfo info = await rv.Files.GetInfoAsync(path, Ct);
                        Assert.Equal(revision, info.Revision); // abort publishes nothing
                        Assert.Equal(model.CommittedLength, info.Length);
                        await writer.DisposeAsync();
                        writer = await OpenWriterAsync(wv, path);
                        Assert.Equal(inode, writer.InodeId);
                        Assert.Equal(0, writer.Position);
                        Assert.Equal(model.StagedLength, await writer.GetLengthAsync(Ct));
                        revision = await RevisionAfterOpenAsync(rv, path, revision, model.CommittedLength);
                    }
                    else if (roll < 73)
                    {
                        long offset = sizes.NextOffset(model.StagedLength, maxLength);
                        int count = (int)sizes.NextLength(2L * ChunkSize);
                        trace.Step($"read staged offset={offset} count={count}");
                        byte[] got = await ReadRangeAsync(writer, offset, count);
                        AssertEx.BytesEqual(model.Read(offset, count, staged: true), got, $"writer's own staged view at {offset}+{count}");
                    }
                    else if (roll < 85)
                    {
                        long offset = sizes.NextOffset(model.CommittedLength, maxLength);
                        int count = (int)sizes.NextLength(2L * ChunkSize);
                        trace.Step($"read live offset={offset} count={count}");
                        byte[] got = await ReadRangeAsync(live, offset, count);
                        AssertEx.BytesEqual(model.Read(offset, count, staged: false), got, $"live committed view at {offset}+{count}");
                        Assert.Equal(model.CommittedLength, await live.GetLengthAsync(Ct));
                    }
                    else if (roll < 90)
                    {
                        trace.Step("open version-pinned reader");
                        if (pins.Count == 2)
                        {
                            await pins[0].Stream.DisposeAsync();
                            pins.RemoveAt(0);
                        }

                        DfsStream pinned = await rv.Files.OpenAsync(path,
                            new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned }, Ct);
                        Assert.Equal(DfsReadView.VersionPinned, pinned.ReadView);
                        Assert.Equal(inode, pinned.InodeId);
                        Assert.Equal(revision, pinned.OpenRevision);
                        pins.Add((pinned, model.Committed));
                    }
                    else if (pins.Count > 0)
                    {
                        (DfsStream pinned, byte[] image) = pins[random.Next(pins.Count)];
                        long offset = sizes.NextOffset(image.LongLength, maxLength);
                        int count = (int)sizes.NextLength(2L * ChunkSize);
                        trace.Step($"read pinned (revision {pinned.OpenRevision}) offset={offset} count={count}");
                        byte[] got = await ReadRangeAsync(pinned, offset, count);
                        byte[] expected = offset >= image.Length ? [] : image.AsSpan((int)offset, (int)Math.Min(count, image.Length - offset)).ToArray();
                        AssertEx.BytesEqual(expected, got, $"pinned view at {offset}+{count}");
                        Assert.Equal(image.LongLength, await pinned.GetLengthAsync(Ct));
                    }
                }

                bool finalPending = model.HasPendingChanges;
                trace.Step("dispose writer (publishes pending changes)");
                await writer.DisposeAsync();
                model.Flush();
                await BytesEqualAsync(model.Committed, rv.Files.ReadAllBytesAsync(path, Ct), "final committed bytes");
                DfsFileInfo end = await rv.Files.GetInfoAsync(path, Ct);
                Assert.Equal(inode, end.InodeId);
                Assert.Equal(model.CommittedLength, end.Length);
                if (finalPending)
                    Assert.True(end.Revision > revision);
                foreach ((DfsStream pinned, byte[] image) in pins)
                    await BytesEqualAsync(image, ReadRangeAsync(pinned, 0, image.Length + 1), "pinned view after later publications");
            }
            finally
            {
                foreach ((DfsStream pinned, _) in pins)
                    await SafeDisposeAsync(pinned);
                await SafeDisposeAsync(live);
                await SafeDisposeAsync(writer);
            }
        }));

    private static Task<DfsStream> OpenWriterAsync(DfsVolume volume, string path) =>
        volume.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);

    /// <summary>
    /// The committed revision right after a (non-truncating) writable open: never older than <paramref name="previous"/>,
    /// and the open published no bytes (same committed length).
    /// </summary>
    private static async Task<ulong> RevisionAfterOpenAsync(DfsVolume reader, string path, ulong previous, long committedLength)
    {
        DfsFileInfo info = await reader.Files.GetInfoAsync(path, Ct);
        Assert.True(info.Revision >= previous, $"revision went backwards after a writable open ({info.Revision} < {previous})");
        Assert.Equal(committedLength, info.Length);
        return info.Revision;
    }

    /// <summary>Writes through one of the asynchronous overloads (whole array, memory, or many fragments).</summary>
    private static async Task WriteWithRandomOverloadAsync(Stream stream, byte[] data, IRandomSource random)
    {
        switch (random.Next(3))
        {
            case 0:
                await stream.WriteAsync(data, 0, data.Length, Ct);
                break;
            case 1:
                await stream.WriteAsync(data.AsMemory(), Ct);
                break;
            default:
                for (int pos = 0; pos < data.Length;)
                {
                    int n = Math.Min(data.Length - pos, 1 + random.Next(9_000));
                    await stream.WriteAsync(data.AsMemory(pos, n), Ct);
                    pos += n;
                }

                break;
        }
    }

    /// <summary>Cleanup-only disposal: a disposal failure must not mask the assertion that already failed the test.</summary>
    private static async Task SafeDisposeAsync(IAsyncDisposable? disposable)
    {
        if (disposable is null)
            return;
        try
        {
            await disposable.DisposeAsync();
        }
        catch (Exception ex) when (ex is not NotImplementedException)
        {
            Log("cleanup disposal failed: " + ex.Message);
        }
    }

    // =====================================================================================================
    // PROP-002 GeneratedNamespaceModel
    // =====================================================================================================

    public static TheoryData<ulong> NamespaceSeeds => new() { 201, 202, 203, 204, 205 };

    private enum NsKind { CreateFile, CreateDirectory, MoveFile, MoveDirectory, DeleteFile, DeleteDirectory }

    private sealed record NsOp(NsKind Kind, string Path, string? Target = null, bool Flag = false)
    {
        public override string ToString() => $"{Kind} {Path}{(Target is null ? "" : " -> " + Target)}{(Flag ? " (overwrite/recursive)" : "")}";
    }

    [Theory, MemberData(nameof(NamespaceSeeds)), Catalog("PROP-002", "S", "INV-02", "INV-07")]
    public Task PROP_002_GeneratedNamespaceModel(ulong seed) =>
        RunPropertyAsync("PROP-002", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            var random = new SeededRandom(Derive(seed, "prop-002"));
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "ns" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            var model = new ReferenceNamespaceModel();
            var identities = new IdentityMap();
            identities.Bind(RootModelInode, (await volume.Directories.GetInfoAsync("/", Ct)).InodeId, "/");
            Assert.Equal(InodeId.Root, (await volume.Directories.GetInfoAsync("/", Ct)).InodeId);
            var contents = new Dictionary<long, byte[]>();
            var deletedPaths = new List<string>();
            int contentId = 0;

            for (int step = 0; step < 36; step++)
            {
                NsOp op = GenerateNamespaceOp(random, model, deletedPaths);
                trace.Step(op.ToString());
                string? error;
                bool ok;
                switch (op.Kind)
                {
                    case NsKind.CreateFile:
                    {
                        ok = model.CreateFile(op.Path, out error);
                        byte[] content = Content(seed, ++contentId, random.Next(3) == 0 ? InlineThreshold + 1 : random.Next(300));
                        async Task Create()
                        {
                            DfsStream s = await volume.Files.OpenAsync(op.Path, FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct);
                            await s.WriteAsync(content, Ct);
                            await s.DisposeAsync();
                        }

                        if (ok)
                        {
                            await Create();
                            contents[model.InodeOf(op.Path)] = content;
                        }
                        else
                        {
                            await ExpectFailureAsync(Create, $"CreateNew {op.Path}", AllowedCodes(error!, op.Kind));
                        }

                        break;
                    }

                    case NsKind.CreateDirectory:
                        ok = model.CreateDirectory(op.Path, recursive: true, out error);
                        if (ok)
                        {
                            DfsDirectoryInfo info = await volume.Directories.CreateDirectoryAsync(op.Path, Ct);
                            identities.Bind(model.InodeOf(op.Path), info.InodeId, op.Path);
                        }
                        else
                        {
                            await ExpectFailureAsync(() => volume.Directories.CreateDirectoryAsync(op.Path, Ct), $"CreateDirectory {op.Path}", AllowedCodes(error!, op.Kind));
                        }

                        break;
                    case NsKind.MoveFile:
                        ok = model.Move(op.Path, op.Target!, op.Flag, out error);
                        if (ok)
                            await volume.Files.MoveAsync(op.Path, op.Target!, op.Flag, Ct);
                        else
                            await ExpectFailureAsync(() => volume.Files.MoveAsync(op.Path, op.Target!, op.Flag, Ct), $"Move {op}", AllowedCodes(error!, op.Kind));
                        break;
                    case NsKind.MoveDirectory:
                        ok = model.Move(op.Path, op.Target!, overwrite: false, out error);
                        if (ok)
                            await volume.Directories.MoveAsync(op.Path, op.Target!, Ct);
                        else
                            await ExpectFailureAsync(() => volume.Directories.MoveAsync(op.Path, op.Target!, Ct), $"MoveDirectory {op}", AllowedCodes(error!, op.Kind));
                        break;
                    case NsKind.DeleteFile:
                        if (model.IsFile(op.Path))
                        {
                            deletedPaths.Add(op.Path);
                            Assert.True(model.Delete(op.Path, recursive: false, out _));
                            await volume.Files.DeleteAsync(op.Path, Ct);
                        }
                        else if (model.IsDirectory(op.Path))
                        {
                            // The file API on an existing directory must fail (IsADirectory; .NET-style access denial is
                            // tolerated) and must not remove it: "success" here would be a false success (INV-08).
                            await ExpectFailureAsync(() => volume.Files.DeleteAsync(op.Path, Ct), $"file delete of the directory {op.Path}",
                                DfsErrorCode.IsADirectory, DfsErrorCode.AccessDenied);
                        }
                        else if (!model.Exists(op.Path) && model.IsDirectory(DfsPath.GetParent(op.Path)))
                        {
                            await volume.Files.DeleteAsync(op.Path, Ct); // absent file: success, no change (FSD §5.4)
                        }
                        else
                        {
                            // Parent missing or not a directory: either success-without-change or DirectoryNotFound is legal.
                            try
                            {
                                await volume.Files.DeleteAsync(op.Path, Ct);
                            }
                            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                            {
                                Assert.Contains(DfsErrors.ToCode(ex), new[] { DfsErrorCode.DirectoryNotFound, DfsErrorCode.NotADirectory });
                            }
                        }

                        break;
                    case NsKind.DeleteDirectory:
                        if (model.IsDirectory(op.Path))
                        {
                            List<string> subtree = model.AllPaths().Where(p => DfsPath.IsAncestorOrSelf(op.Path, p)).ToList();
                            ok = model.Delete(op.Path, op.Flag, out error);
                            if (ok)
                            {
                                deletedPaths.AddRange(subtree);
                                await volume.Directories.DeleteAsync(op.Path, op.Flag, Ct);
                            }
                            else
                            {
                                await ExpectFailureAsync(() => volume.Directories.DeleteAsync(op.Path, op.Flag, Ct), $"DeleteDirectory {op}", AllowedCodes(error!, op.Kind));
                            }
                        }
                        else
                        {
                            await ExpectFailureAsync(() => volume.Directories.DeleteAsync(op.Path, op.Flag, Ct), $"DeleteDirectory absent {op.Path}",
                                DfsErrorCode.DirectoryNotFound, DfsErrorCode.NotADirectory);
                        }

                        break;
                }

                // Compare namespace identity and topology after every operation (committed or rejected).
                await AssertNamespaceMatchesAsync(volume, model, identities, contents, verifyContents: step % 6 == 5);
            }

            await AssertNamespaceMatchesAsync(volume, model, identities, contents, verifyContents: true);
        }));

    private static readonly string[] NamespaceNames = ["a", "b", "é", "日本", "x y", "n0"];

    private static NsOp GenerateNamespaceOp(IRandomSource random, ReferenceNamespaceModel model, List<string> deletedPaths)
    {
        string[] dirs = model.AllPaths().Where(model.IsDirectory).ToArray();
        string[] files = model.AllPaths().Where(model.IsFile).ToArray();
        string[] nonRootDirs = dirs.Where(d => d != "/").ToArray();

        string NewPath()
        {
            if (deletedPaths.Count > 0 && random.Next(4) == 0)
                return deletedPaths[random.Next(deletedPaths.Count)]; // name reuse
            string[] parents = dirs.Where(d => DfsPath.Split(d).Count < 3).ToArray();
            string parent = parents[random.Next(parents.Length)];
            if (random.Next(10) == 0)
                parent = DfsPath.Combine(parent, "missing"); // parent that usually does not exist
            return DfsPath.Combine(parent, NamespaceNames[random.Next(NamespaceNames.Length)]);
        }

        switch (random.Next(10))
        {
            case 0:
            case 1:
            case 2:
                return new NsOp(NsKind.CreateFile, files.Length > 0 && random.Next(8) == 0 ? files[random.Next(files.Length)] : NewPath());
            case 3:
            case 4:
                return new NsOp(NsKind.CreateDirectory, random.Next(6) == 0 ? DfsPath.Combine(NewPath(), "deep") : NewPath());
            case 5:
                return files.Length == 0
                    ? new NsOp(NsKind.CreateFile, NewPath())
                    : new NsOp(NsKind.MoveFile, files[random.Next(files.Length)], random.Next(3) == 0 && files.Length > 1 ? files[random.Next(files.Length)] : NewPath(), random.Next(2) == 0);
            case 6:
                if (nonRootDirs.Length == 0)
                    return new NsOp(NsKind.CreateDirectory, NewPath());
                string from = nonRootDirs[random.Next(nonRootDirs.Length)];
                string to = random.Next(5) == 0 ? DfsPath.Combine(from, "inside") : NewPath(); // moving into itself must fail
                if (to == from)
                    to = DfsPath.Combine(from, "inside"); // a directory move onto itself has no specified outcome; use the descendant case
                return new NsOp(NsKind.MoveDirectory, from, to);
            case 7:
                return new NsOp(NsKind.DeleteFile, files.Length > 0 && random.Next(4) != 0 ? files[random.Next(files.Length)] : NewPath());
            default:
                return nonRootDirs.Length == 0
                    ? new NsOp(NsKind.DeleteDirectory, NewPath(), Flag: false)
                    : new NsOp(NsKind.DeleteDirectory, nonRootDirs[random.Next(nonRootDirs.Length)], Flag: random.Next(2) == 0);
        }
    }

    /// <summary>Maps a ReferenceNamespaceModel error to the canonical codes the SDK may legally report for it.</summary>
    private static DfsErrorCode[] AllowedCodes(string modelError, NsKind kind) => modelError switch
    {
        nameof(DfsErrorCode.AlreadyExists) => kind == NsKind.CreateFile
            ? [DfsErrorCode.AlreadyExists, DfsErrorCode.IsADirectory]
            : kind == NsKind.CreateDirectory ? [DfsErrorCode.AlreadyExists, DfsErrorCode.NotADirectory] : [DfsErrorCode.AlreadyExists],
        nameof(DfsErrorCode.DirectoryNotFound) => [DfsErrorCode.DirectoryNotFound],
        nameof(DfsErrorCode.NotADirectory) => [DfsErrorCode.NotADirectory, DfsErrorCode.DirectoryNotFound],
        nameof(DfsErrorCode.FileNotFound) => kind is NsKind.MoveDirectory or NsKind.DeleteDirectory ? [DfsErrorCode.DirectoryNotFound] : [DfsErrorCode.FileNotFound],
        nameof(DfsErrorCode.IsADirectory) => [DfsErrorCode.IsADirectory, DfsErrorCode.AlreadyExists],
        nameof(DfsErrorCode.DirectoryNotEmpty) => [DfsErrorCode.DirectoryNotEmpty],
        nameof(DfsErrorCode.InvalidArgument) or nameof(DfsErrorCode.InvalidPath) => [DfsErrorCode.InvalidArgument, DfsErrorCode.InvalidPath],
        _ => throw new InvalidOperationException($"Unexpected model error '{modelError}'."),
    };

    /// <summary>Bijection between model inode numbers and real inode ids; real ids are never reused.</summary>
    private sealed class IdentityMap
    {
        private readonly Dictionary<long, InodeId> _toReal = [];
        private readonly Dictionary<InodeId, long> _toModel = [];

        public void Bind(long modelInode, InodeId real, string path)
        {
            if (_toReal.TryGetValue(modelInode, out InodeId known))
            {
                Assert.True(known == real, $"{path}: inode identity changed from {known.Value} to {real.Value} (model inode {modelInode})");
                return;
            }

            Assert.False(_toModel.TryGetValue(real, out long other),
                $"{path}: inode {real.Value} reused for a new namespace object (it belonged to model inode {other}); inodes must never be reused");
            _toReal[modelInode] = real;
            _toModel[real] = modelInode;
        }
    }

    private static async Task AssertNamespaceMatchesAsync(DfsVolume volume, ReferenceNamespaceModel model, IdentityMap identities,
        IReadOnlyDictionary<long, byte[]> contents, bool verifyContents)
    {
        var actual = new SortedDictionary<string, DfsEntryInfo>(StringComparer.Ordinal);
        var pending = new Queue<string>();
        pending.Enqueue("/");
        while (pending.Count > 0)
        {
            string dir = pending.Dequeue();
            var names = new List<string>();
            await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(dir, null, Ct))
            {
                names.Add(entry.Name);
                actual[entry.Path] = entry;
                if (entry.Kind == DfsEntryKind.Directory)
                    pending.Enqueue(entry.Path);
            }

            names.Sort(StringComparer.Ordinal);
            Assert.Equal(model.List(dir), names);
        }

        string[] expectedPaths = model.AllPaths().Where(p => p != "/").ToArray();
        Assert.Equal(expectedPaths, actual.Keys.ToArray());
        foreach ((string path, DfsEntryInfo entry) in actual)
        {
            Assert.Equal(model.IsDirectory(path) ? DfsEntryKind.Directory : DfsEntryKind.File, entry.Kind);
            long modelInode = model.InodeOf(path);
            identities.Bind(modelInode, entry.InodeId, path);
            if (entry.Kind == DfsEntryKind.File)
            {
                byte[] expected = contents[modelInode];
                Assert.Equal(expected.LongLength, entry.Length);
                if (verifyContents)
                    await BytesEqualAsync(expected, volume.Files.ReadAllBytesAsync(path, Ct), $"content follows inode identity at {path}");
            }
        }

        DfsDirectoryInfo root = await volume.Directories.GetInfoAsync("/", Ct);
        Assert.Equal(InodeId.Root, root.InodeId);
        Assert.Equal(model.List("/").Count, root.EntryCount);
    }

    // =====================================================================================================
    // PROP-003 GeneratedTransactionHistories
    // =====================================================================================================

    public static TheoryData<ulong, bool> TransactionCases => new() { { 301, false }, { 302, false }, { 303, true }, { 304, true } };

    private sealed record TxPlan(string[] Reads, bool Predicate, (string Key, string? Value)[] Writes);

    [Theory, MemberData(nameof(TransactionCases)), Catalog("PROP-003", "S", "INV-02", "INV-04", "INV-06", "INV-19")]
    public Task PROP_003_GeneratedTransactionHistories(ulong seed, bool leaderFailover) =>
        RunPropertyAsync("PROP-003", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            const int processes = 3;
            const int transactionsPerProcess = 5;
            string[] existing = ["/t/k0", "/t/k1", "/t/k2", "/t/k3"];
            string[] creatable = ["/t/n0", "/t/n1"];
            string[] all = [.. existing, .. creatable];
            var clock = new LogicalClock();
            var records = new List<TxRecord>();
            var gate = new object();
            void Add(TxRecord r)
            {
                lock (gate)
                    records.Add(r);
            }

            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            var initial = new Dictionary<string, string?>(StringComparer.Ordinal);
            await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/t", Ct);
            foreach (string key in existing)
            {
                await setup.GetVolume(Vol).Files.WriteAllTextAsync(key, "init:" + key, null, Ct);
                initial[key] = "init:" + key;
            }

            foreach (string key in creatable)
                initial[key] = null;

            var clients = new List<DfsClient>();
            for (int p = 0; p < processes + 2; p++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "p" + p.ToString(CultureInfo.InvariantCulture) }, Ct));
            try
            {
                // Round 1: forced write skew + phantom. All three read k0 and k1 before any of them writes.
                var barrier = new AsyncBarrier(processes);
                TxPlan[] skew =
                [
                    new(["/t/k0", "/t/k1"], true, [("/t/k0", "skew:p0")]),
                    new(["/t/k0", "/t/k1"], false, [("/t/k1", "skew:p1")]),
                    new(["/t/k0", "/t/k1"], true, [("/t/n0", "skew:p2")]),
                ];
                trace.Step("write-skew round");
                TxRecord[] skewResults = await Task.WhenAll(Enumerable.Range(0, processes).Select(p =>
                    RunTransactionAsync(cluster, clients[p].GetVolume(Vol), $"skew-p{p}", skew[p], clock, barrier.SignalAndWaitAsync, Add)));
                Assert.False(skewResults[0].Outcome == OpOutcome.Ok && skewResults[1].Outcome == OpOutcome.Ok,
                    "both write-skew transactions committed although each read the key the other one wrote");
                int skewCommitted = skewResults.Count(r => r.Outcome == OpOutcome.Ok);
                Assert.InRange(skewCommitted, 1, processes - 1);

                // Random concurrent rounds with ordinary mutations and snapshots interleaved.
                var random = new SeededRandom(Derive(seed, "prop-003"));
                var plans = Enumerable.Range(0, processes).Select(p => Enumerable.Range(0, transactionsPerProcess)
                    .Select(_ => RandomTxPlan(random, all, p)).ToArray()).ToArray();
                foreach (TxPlan[] processPlans in plans)
                {
                    foreach (TxPlan plan in processPlans)
                        trace.Step($"plan reads=[{string.Join(",", plan.Reads)}] predicate={plan.Predicate} writes=[{string.Join(",", plan.Writes.Select(w => $"{w.Key}={w.Value ?? "<delete>"}"))}]");
                }

                var work = new List<Task>();
                for (int p = 0; p < processes; p++)
                    work.Add(RunProcessAsync(clients[p].GetVolume(Vol), p, plans[p]));

                work.Add(OrdinaryMutationsAsync(clients[processes].GetVolume(Vol), new SeededRandom(Derive(seed, "ordinary"))));
                work.Add(SnapshotReadersAsync(clients[processes + 1].GetVolume(Vol)));
                if (leaderFailover)
                    work.Add(FailoverAsync());
                await Task.WhenAll(work);

                // Final state as a committed read-only observation.
                long finalInvoke = clock.Tick();
                var finalReads = new Dictionary<string, string?>(StringComparer.Ordinal);
                foreach (string key in all)
                    finalReads[key] = await ReadTextOrNullAsync(setup.GetVolume(Vol).Files, key);
                Add(new TxRecord("final", finalInvoke, clock.Tick(), OpOutcome.Ok, finalReads, new Dictionary<string, string?>(), []));

                int committed = records.Count(r => r.Outcome == OpOutcome.Ok && r.Writes.Count > 0);
                Log($"PROP-003 seed {seed}: {records.Count} records, {committed} committed writers, skew round committed {skewCommitted}");
                Assert.True(committed >= processes, "too few committed transactions for a meaningful history");
                CheckResult check = SerializabilityChecker.CheckStrict(records, initial);
                Log(check.ToString());
                check.AssertValid();
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await SafeDisposeAsync(c);
            }

            async Task RunProcessAsync(DfsVolume volume, int process, TxPlan[] processPlans)
            {
                for (int i = 0; i < processPlans.Length; i++)
                    await RunTransactionAsync(cluster, volume, $"p{process}-t{i}", processPlans[i], clock, null, Add);
            }

            async Task OrdinaryMutationsAsync(DfsVolume volume, SeededRandom random)
            {
                for (int i = 0; i < 5; i++)
                {
                    string key = all[random.Next(all.Length)];
                    string? value = random.Next(5) == 0 ? null : $"ordinary{i}:{key}";
                    DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                    var options = new DfsMutationOptions { OperationId = id };
                    long invoke = clock.Tick();
                    OpOutcome outcome;
                    long? resolveTime = null;
                    try
                    {
                        if (value is null)
                            await volume.Files.DeleteAsync(key, options, Ct);
                        else
                            await volume.Files.WriteAllBytesAsync(key, Encoding.UTF8.GetBytes(value), options, Ct);
                        outcome = OpOutcome.Ok;
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
                    {
                        outcome = HistoryRecorder.Classify(ex);
                        if (outcome == OpOutcome.Unknown)
                        {
                            DfsOperationOutcome resolved = await ResolveOperationAsync(cluster, volume, id);
                            outcome = resolved.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed;
                            resolveTime = clock.Tick();
                        }
                    }

                    long complete = clock.Tick();
                    Add(new TxRecord($"ordinary-{i}", invoke, complete, outcome, new Dictionary<string, string?>(),
                        new Dictionary<string, string?> { [key] = value }, [])
                    { ResolveTime = outcome == OpOutcome.Ok ? resolveTime : null });
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(30), Ct);
                }
            }

            async Task SnapshotReadersAsync(DfsVolume volume)
            {
                // A committed transaction must appear wholly before or after every snapshot (FSD §8.3): each snapshot's
                // contents are recorded as a read-only transaction whose serialization point lies in the creation interval.
                for (int i = 0; i < 3; i++)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(60), Ct);
                    long invoke = clock.Tick();
                    DfsSnapshotInfo info;
                    try
                    {
                        info = await volume.Snapshots.CreateAsync($"prop003-{i}", null, Ct);
                    }
                    catch (Exception ex) when (ex is DfsUnavailableException or DfsCommitOutcomeUnknownException)
                    {
                        continue; // availability failure during failover; the snapshot is not used
                    }

                    long complete = clock.Tick();
                    await using DfsSnapshotView view = await volume.Snapshots.OpenAsync(info.Name, Ct);
                    var reads = new Dictionary<string, string?>(StringComparer.Ordinal);
                    foreach (string key in all)
                        reads[key] = await ReadTextOrNullAsync(view.Files, key);
                    Add(new TxRecord($"snapshot-{i}", invoke, complete, OpOutcome.Ok, reads, new Dictionary<string, string?>(),
                        [("/t", await ListFileNamesAsync(view.Directories, "/t"))]));
                }
            }

            async Task FailoverAsync()
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(80), Ct);
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                trace.Step($"crash volume leader {leader.Name}");
                await leader.CrashAsync();
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                await leader.RestartAsync();
                trace.Step($"restarted {leader.Name}");
            }
        }));

    private static TxPlan RandomTxPlan(IRandomSource random, string[] keys, int process)
    {
        string[] reads = keys.OrderBy(_ => random.NextUInt64()).Take(1 + random.Next(3)).ToArray();
        var writes = new List<(string, string?)>();
        foreach (string key in keys.OrderBy(_ => random.NextUInt64()).Take(1 + random.Next(2)))
            writes.Add((key, random.Next(7) == 0 ? null : $"p{process}-{random.NextUInt64() % 100_000}:{key}"));
        return new TxPlan(reads, random.Next(10) < 3, writes.ToArray());
    }

    /// <summary>Runs one transaction and records it (committed, definitely aborted, or resolved from its durable id).</summary>
    private static async Task<TxRecord> RunTransactionAsync(TestCluster cluster, DfsVolume volume, string id, TxPlan plan, LogicalClock clock, Func<Task>? barrier, Action<TxRecord> add)
    {
        long invoke = clock.Tick();
        var reads = new Dictionary<string, string?>(StringComparer.Ordinal);
        var writes = new Dictionary<string, string?>(StringComparer.Ordinal);
        var predicates = new List<(string Directory, IReadOnlySet<string> Names)>();
        DfsTransaction? tx = null;
        bool reachedCommit = false;
        try
        {
            tx = await volume.Transactions.BeginAsync(null, Ct);
            Assert.Equal(DfsTransactionState.Active, tx.State);
            foreach (string key in plan.Reads)
            {
                string? value = await ReadTextOrNullAsync(tx.Files, key);
                reads.TryAdd(key, value);
            }

            if (plan.Predicate)
                predicates.Add(("/t", await ListFileNamesAsync(tx.Directories, "/t")));
            if (barrier is not null)
                await barrier();
            foreach ((string key, string? value) in plan.Writes)
            {
                if (value is null)
                    await tx.Files.DeleteAsync(key, Ct);
                else
                    await tx.Files.WriteAllTextAsync(key, value, null, Ct);
                writes[key] = value;
                // Reads see the transaction's own writes.
                Assert.Equal(value, await ReadTextOrNullAsync(tx.Files, key));
            }

            reachedCommit = true;
            await tx.CommitAsync(Ct);
            Assert.Equal(DfsTransactionState.Committed, tx.State);
            var committed = new TxRecord(id, invoke, clock.Tick(), OpOutcome.Ok, reads, writes, predicates);
            add(committed);
            return committed;
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
        {
            long complete = clock.Tick();
            OpOutcome outcome = ex is DfsTransactionConflictException or DfsSharingViolationException
                ? OpOutcome.Failed
                : reachedCommit ? HistoryRecorder.Classify(ex) : OpOutcome.Failed;
            long? resolveTime = null;
            if (outcome == OpOutcome.Unknown && tx is not null)
            {
                if (ex is DfsCommitOutcomeUnknownException unknown)
                    Assert.Equal(tx.Id, unknown.TransactionId); // the durable transaction id is carried for resolution
                DfsOperationOutcome resolved = await ResolveTransactionAsync(cluster, volume, tx.Id);
                outcome = resolved.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed;
                resolveTime = clock.Tick();
            }

            var record = new TxRecord(id, invoke, complete, outcome, reads, writes, predicates) { ResolveTime = outcome == OpOutcome.Ok ? resolveTime : null };
            add(record);
            // An aborted transaction's reads still came from one complete snapshot: check them as a read-only view.
            if (outcome == OpOutcome.Failed && (reads.Count > 0 || predicates.Count > 0))
                add(new TxRecord(id + "/view", invoke, complete, OpOutcome.Ok, reads, new Dictionary<string, string?>(), predicates));
            return record;
        }
        finally
        {
            if (tx is not null)
                await SafeDisposeAsync(tx);
        }
    }

    /// <summary>Resolves a transaction from its durable id; retries only while the cluster is unavailable (bounded).</summary>
    private static async Task<DfsOperationOutcome> ResolveTransactionAsync(TestCluster cluster, DfsVolume volume, DfsTransactionId id)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                DfsOperationOutcome outcome = await volume.Transactions.QueryOutcomeAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                    $"resolve:true must produce a definite outcome for {id}, got {outcome.Status}");
                return outcome;
            }
            catch (DfsUnavailableException) when (attempt < 20)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }
        }
    }

    /// <summary>Resolves an operation from its durable id (resolve:true fences it); retries only while unavailable (bounded).</summary>
    private static async Task<DfsOperationOutcome> ResolveOperationAsync(TestCluster cluster, DfsVolume volume, DfsOperationId id)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
                Assert.Equal(id, outcome.Id);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                    $"resolve:true must produce a definite outcome for {id}, got {outcome.Status}");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    Assert.False(outcome.Token.IsNone);
                return outcome;
            }
            catch (DfsUnavailableException) when (attempt < 20)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }
        }
    }

    private static async Task<string?> ReadTextOrNullAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllTextAsync(path, null, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    private static async Task<IReadOnlySet<string>> ListFileNamesAsync(DfsDirectoryClient directories, string path)
    {
        var names = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(path, new DfsEnumerationOptions { Kinds = DfsEntryKinds.Files }, Ct))
            names.Add(entry.Name);
        return names;
    }

    /// <summary>Simulation-compatible barrier (completes waiters synchronously on the caller's context).</summary>
    private sealed class AsyncBarrier(int participants)
    {
        private readonly TaskCompletionSource _all = new();
        private int _arrived;

        public Task SignalAndWaitAsync()
        {
            if (Interlocked.Increment(ref _arrived) == participants)
                _all.TrySetResult();
            return _all.Task;
        }
    }

    // =====================================================================================================
    // PROP-004 GeneratedLiveReadHistories
    // =====================================================================================================

    public static TheoryData<ulong, bool> LiveReadCases => new() { { 401, false }, { 402, true }, { 403, true } };

    [Theory, MemberData(nameof(LiveReadCases)), Catalog("PROP-004", "S", "INV-03", "INV-06", "INV-08")]
    public Task PROP_004_GeneratedLiveReadHistories(ulong seed, bool crashLeader) =>
        RunPropertyAsync("PROP-004", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            string[] keys = ["/r/k0", "/r/k1", "/r/k2"];
            HistoryRecorder history = cluster.History;
            var unknown = new List<(long HistoryId, DfsOperationId OperationId)>();
            var gate = new object();
            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/r", Ct);

            var clients = new List<DfsClient>();
            for (int p = 0; p < 4; p++)
            {
                clients.Add(await cluster.ConnectAsync(new TestClientOptions
                {
                    Name = "c" + p.ToString(CultureInfo.InvariantCulture),
                    OperationTimeout = p == 3 ? TimeSpan.FromSeconds(2) : null,
                }, Ct));
            }

            try
            {
                // Client c3's mutation replies are lost for a window: the server executes, the client times out (Unknown).
                MessageRule dropWrites = cluster.Network.DropReplies(new MessageFilter(From: "client:c3", Operation: DfsOperationKind.WriteAll), count: 1_000);
                MessageRule dropDeletes = cluster.Network.DropReplies(new MessageFilter(From: "client:c3", Operation: DfsOperationKind.Delete), count: 1_000);
                var tasks = new List<Task>();
                for (int p = 0; p < clients.Count; p++)
                    tasks.Add(ProcessAsync(p, clients[p].GetVolume(Vol), new SeededRandom(Derive(seed, "proc" + p.ToString(CultureInfo.InvariantCulture)))));
                tasks.Add(FaultsAsync(dropWrites, dropDeletes));
                await Task.WhenAll(tasks);

                Assert.True(dropWrites.Matched + dropDeletes.Matched > 0, "the reply-loss window was never reached (fault not exercised)");
                Assert.NotEmpty(unknown); // the history must contain genuinely uncertain operations

                // Narrow half of the unknown operations through their durable ids; leave the rest unknown.
                foreach ((long hid, DfsOperationId opId) in unknown.Where((_, i) => i % 2 == 0))
                {
                    DfsOperationOutcome outcome = await ResolveOperationAsync(cluster, setup.GetVolume(Vol), opId);
                    history.Resolve(hid, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
                    trace.Step($"resolved #{hid} {opId} -> {outcome.Status}");
                }

                foreach (string key in keys)
                {
                    long id = history.Invoke("final", "read", key, null);
                    history.Complete(id, OpOutcome.Ok, await TryReadAllBytesAsync(setup.GetVolume(Vol).Files, key));
                }

                IReadOnlyList<HistoryOp> ops = history.Operations;
                Log($"PROP-004 seed {seed}: {ops.Count} operations, {ops.Count(o => o.Outcome == OpOutcome.Unknown)} still unknown, {ops.Count(o => o.WasResolved)} resolved");
                Assert.Contains(ops, o => o.Outcome == OpOutcome.Unknown);
                Assert.Contains(ops, o => o.WasResolved);
                CheckResult check = LinearizabilityChecker.Check(ops, new RegisterModel());
                Log(check.ToString());
                check.AssertValid();
            }
            finally
            {
                cluster.Network.Heal();
                foreach (DfsClient c in clients)
                    await SafeDisposeAsync(c);
            }

            async Task ProcessAsync(int p, DfsVolume volume, SeededRandom random)
            {
                string process = "c" + p.ToString(CultureInfo.InvariantCulture);
                for (int i = 0; i < 14; i++)
                {
                    string key = keys[random.Next(keys.Length)];
                    int roll = random.Next(100);
                    if (roll < 45)
                    {
                        long hid = history.Invoke(process, "read", key, ViewChecker.Live);
                        try
                        {
                            history.Complete(hid, OpOutcome.Ok, await TryReadAllBytesAsync(volume.Files, key));
                        }
                        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
                        {
                            history.Complete(hid, OpOutcome.Failed, ex); // a failed read has no effect; it must never fabricate absence
                            Assert.IsNotType<FileNotFoundException>(ex);
                        }
                    }
                    else
                    {
                        bool delete = roll >= 85;
                        byte[] data = Content(seed, p * 1000 + i, 1 + random.Next(2 * InlineThreshold));
                        DfsOperationId opId = await volume.NewOperationIdAsync(Ct);
                        long hid = history.Invoke(process, delete ? "delete" : "write", key, delete ? null : data, opId);
                        try
                        {
                            var options = new DfsMutationOptions { OperationId = opId };
                            if (delete)
                                await volume.Files.DeleteAsync(key, options, Ct);
                            else
                                await volume.Files.WriteAllBytesAsync(key, data, options, Ct);
                            history.Complete(hid, OpOutcome.Ok);
                        }
                        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
                        {
                            OpOutcome outcome = HistoryRecorder.Classify(ex);
                            history.Complete(hid, outcome, ex);
                            if (ex is DfsCommitOutcomeUnknownException u)
                                Assert.Equal(opId, u.OperationId); // uncertainty carries the caller's operation id
                            if (outcome == OpOutcome.Unknown)
                            {
                                lock (gate)
                                    unknown.Add((hid, opId));
                            }
                        }
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(25)), Ct);
                }
            }

            async Task FaultsAsync(MessageRule dropWrites, MessageRule dropDeletes)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(400), Ct);
                if (crashLeader)
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    trace.Step($"crash leader {leader.Name}");
                    await leader.CrashAsync();
                    await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                    await leader.RestartAsync();
                }

                await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
                dropWrites.Cancel();
                dropDeletes.Cancel();
                trace.Step("reply-loss window closed");
            }
        }));

    // =====================================================================================================
    // PROP-005 GeneratedSnapshotHistories
    // =====================================================================================================

    public static TheoryData<ulong> SnapshotHistorySeeds => new() { 501, 502, 503, 504 };

    /// <summary>Model of one retained snapshot: its exact image, boundary contract and retention.</summary>
    private sealed record ExpectedSnapshot(string Name, TreeModel Image, CommitToken? ExactBoundary, CommitToken MinimumBoundary, DfsRetention Retention);

    [Theory, MemberData(nameof(SnapshotHistorySeeds)), Catalog("PROP-005", "S", "INV-02", "INV-04", "INV-09", "INV-13", "INV-19")]
    public Task PROP_005_GeneratedSnapshotHistories(ulong seed) =>
        RunPropertyAsync("PROP-005", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            var random = new SeededRandom(Derive(seed, "prop-005"));
            var sizes = new BoundaryBiasedSizes(random.Fork("sizes"), InlineThreshold, ChunkSize, BufferSize);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            var model = new TreeModel();
            var retained = new Dictionary<string, ExpectedSnapshot>(StringComparer.Ordinal);
            var gone = new HashSet<string>(StringComparer.Ordinal);          // deleted, or removed after a valid expiry
            string[] paths = ["/s/a", "/s/b", "/s/d/c", "/s/d/e", "/s/f"];
            CommitToken last = CommitToken.None;
            int dataId = 0;
            int snapshotNumber = 0;
            int exactCount = 0;
            int ordinaryCount = 0;
            await volume.Directories.CreateDirectoryAsync("/s/d", Ct);
            model.MkDirs("/s/d");

            for (int step = 0; step < 44; step++)
            {
                int roll = random.Next(100);
                string path = paths[random.Next(paths.Length)];
                if (roll < 34)
                {
                    // Overwrite/create, optionally as an exact commit-and-snapshot through one of the three APIs.
                    await EnsureParentAsync(path);
                    byte[] data = Content(seed, ++dataId, (int)sizes.NextLength(2L * ChunkSize + 1));
                    int mechanism = random.Next(5); // 0,1: plain; 2: helper + snapshot; 3: stream commit + snapshot; 4: transaction + snapshot
                    string? snapshot = mechanism >= 2 ? $"exact{++snapshotNumber}" : null;
                    trace.Step($"write {path} len={data.Length}{(snapshot is null ? "" : $" + exact snapshot {snapshot} via mechanism {mechanism}")}");
                    CommitToken token;
                    switch (mechanism)
                    {
                        case 3:
                        {
                            await using DfsStream stream = await volume.Files.CreateAsync(path, Ct);
                            await stream.WriteAsync(data, Ct);
                            token = await stream.CommitAsync(new DfsMutationOptions { CommitSnapshotName = snapshot }, Ct);
                            model.Write(path, data);
                            break;
                        }

                        case 4:
                        {
                            await using DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
                            await tx.Files.WriteAllBytesAsync(path, data, Ct);
                            model.Write(path, data);
                            string? other = paths.FirstOrDefault(p => p != path && model.Files.ContainsKey(p) && DfsPath.GetParent(p) == DfsPath.GetParent(path));
                            if (other is not null)
                            {
                                await tx.Files.DeleteAsync(other, Ct); // a multi-file transaction: both changes or neither are in the snapshot
                                model.DeleteFile(other);
                            }

                            DfsSnapshotInfo created = await tx.CommitWithSnapshotAsync(snapshot!, Ct);
                            Assert.Equal(DfsTransactionState.Committed, tx.State);
                            Assert.Equal(snapshot, created.Name);
                            token = created.Boundary;
                            break;
                        }

                        default:
                            token = await volume.Files.WriteAllBytesAsync(path, data, new DfsMutationOptions { CommitSnapshotName = snapshot }, Ct);
                            model.Write(path, data);
                            break;
                    }

                    Assert.False(token.IsNone);
                    if (!last.IsNone)
                        Assert.True(token.CompareTo(last) > 0, $"commit token {token} does not follow {last}");
                    last = token;
                    if (snapshot is not null)
                    {
                        exactCount++;
                        retained[snapshot] = new ExpectedSnapshot(snapshot, model.Clone(), token, token, new DfsRetention());
                        DfsSnapshotInfo info = await volume.Snapshots.GetAsync(snapshot, Ct);
                        Assert.Equal(token, info.Boundary); // exact: the snapshot boundary is the commit itself, not a later checkpoint
                    }
                }
                else if (roll < 46)
                {
                    trace.Step($"delete {path}");
                    CommitToken token = await volume.Files.DeleteAsync(path, Ct);
                    if (model.DeleteFile(path))
                        last = token;
                }
                else if (roll < 51)
                {
                    trace.Step("delete directory /s/d recursively");
                    if (model.Directories.Contains("/s/d"))
                    {
                        last = await volume.Directories.DeleteAsync("/s/d", recursive: true, Ct);
                        model.DeleteTree("/s/d");
                    }
                }
                else if (roll < 56)
                {
                    string[] files = model.Files.Keys.ToArray();
                    if (files.Length > 0)
                    {
                        string from = files[random.Next(files.Length)];
                        string to = DfsPath.GetParent(from) == "/s" ? "/s/f" : DfsPath.Combine(DfsPath.GetParent(from), "moved");
                        if (to != from)
                        {
                            trace.Step($"move {from} -> {to} (overwrite)");
                            last = await volume.Files.MoveAsync(from, to, overwrite: true, Ct);
                            model.MoveFile(from, to);
                        }
                    }
                }
                else if (roll < 72)
                {
                    string name = $"snap{++snapshotNumber}";
                    DfsRetention retention = random.Next(4) switch
                    {
                        0 => new DfsRetention(cluster.Time.GetUtcNow() + TimeSpan.FromMilliseconds(500 + random.Next(2500)), Hold: false),
                        1 => new DfsRetention(cluster.Time.GetUtcNow() + TimeSpan.FromMilliseconds(500 + random.Next(1500)), Hold: true),
                        _ => new DfsRetention(),
                    };
                    bool useMinimum = !last.IsNone && random.Next(2) == 0;
                    trace.Step($"snapshot {name} (retention {retention}, minimumCommit={(useMinimum ? last.ToString() : "none")})");
                    DfsSnapshotInfo info = await volume.Snapshots.CreateAsync(name,
                        new DfsSnapshotOptions { MinimumCommit = useMinimum ? last : null, Retention = retention }, Ct);
                    Assert.Equal(name, info.Name);
                    Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
                    Assert.Empty(info.Adapters);
                    if (!last.IsNone)
                        Assert.True(info.Boundary.IsAtOrAfter(last), $"snapshot {name} boundary {info.Boundary} precedes the last completed commit {last}");
                    ordinaryCount++;
                    retained[name] = new ExpectedSnapshot(name, model.Clone(), null, last, retention);
                }
                else if (roll < 80)
                {
                    // Only snapshots without any retention promise are deleted explicitly.
                    ExpectedSnapshot? victim = retained.Values
                        .Where(s => !s.Retention.Hold && s.Retention.ExpiresUtc is null)
                        .OrderBy(s => s.Name, StringComparer.Ordinal)
                        .FirstOrDefault();
                    if (victim is not null)
                    {
                        trace.Step($"delete snapshot {victim.Name}");
                        await volume.Snapshots.DeleteAsync(victim.Name, Ct);
                        retained.Remove(victim.Name);
                        gone.Add(victim.Name);
                        await ExpectFailureAsync(() => volume.Snapshots.GetAsync(victim.Name, Ct), $"GetAsync of deleted snapshot {victim.Name}", DfsErrorCode.SnapshotNotFound);
                    }
                }
                else if (roll < 86)
                {
                    ExpectedSnapshot? target = retained.Values.OrderBy(s => s.Name, StringComparer.Ordinal)
                        .Skip(random.Next(Math.Max(1, retained.Count))).FirstOrDefault();
                    if (target is not null)
                    {
                        DfsRetention retention = random.Next(2) == 0
                            ? new DfsRetention(null, Hold: true)
                            : new DfsRetention(cluster.Time.GetUtcNow() + TimeSpan.FromSeconds(1 + random.Next(3)), Hold: false);
                        trace.Step($"set retention of {target.Name} to {retention}");
                        await volume.Snapshots.SetRetentionAsync(target.Name, retention, Ct);
                        retained[target.Name] = target with { Retention = retention };
                    }
                }
                else if (roll < 93)
                {
                    trace.Step("let time pass (GC, retention expiry)");
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(1500), Ct);
                }
                else
                {
                    trace.Step("verify every retained snapshot");
                    await VerifySnapshotsAsync($"step {step}");
                }

                if (step % 6 == 5)
                    await VerifySnapshotsAsync($"after step {step}");
            }

            AssertImage(VolumeImage.FromModel(model), await CaptureAsync(volume), compareInodes: false, "live image at the end of the history");
            await VerifySnapshotsAsync("end of history");
            // "Forever": many GC intervals later, with the live namespace emptied, every snapshot that has not validly expired is intact.
            await volume.Directories.DeleteAsync("/s", recursive: true, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(6), Ct);
            await VerifySnapshotsAsync("after deleting the live tree and running GC");
            Log($"PROP-005 seed {seed}: {exactCount} exact and {ordinaryCount} ordinary snapshots, {retained.Count} still retained, {gone.Count} deleted/expired");
            Assert.True(exactCount > 0 && ordinaryCount > 0, "the generated history must contain both exact commit-and-snapshot and ordinary snapshots");

            async Task EnsureParentAsync(string file)
            {
                string parent = DfsPath.GetParent(file);
                if (!model.Directories.Contains(parent))
                {
                    await volume.Directories.CreateDirectoryAsync(parent, Ct);
                    model.MkDirs(parent);
                }
            }

            async Task VerifySnapshotsAsync(string when)
            {
                IReadOnlyList<DfsSnapshotInfo> listed = await volume.Snapshots.ListAsync(Ct);
                DateTimeOffset observedBy = cluster.Time.GetUtcNow(); // taken after the observation: an expiry before this instant may explain an absence
                var byName = listed.ToDictionary(s => s.Name, StringComparer.Ordinal);
                foreach (string name in gone)
                    Assert.False(byName.ContainsKey(name), $"{when}: deleted/expired snapshot {name} reappeared");
                foreach (ExpectedSnapshot expected in retained.Values.OrderBy(s => s.Name, StringComparer.Ordinal).ToArray())
                {
                    if (!byName.TryGetValue(expected.Name, out DfsSnapshotInfo? info))
                    {
                        AssertValidlyExpired(expected, observedBy, when);
                        retained.Remove(expected.Name);
                        gone.Add(expected.Name);
                        continue;
                    }

                    Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
                    Assert.Equal(expected.Retention.Hold, info.Retention.Hold);
                    Assert.Equal(expected.Retention.ExpiresUtc is null, info.Retention.ExpiresUtc is null);
                    if (expected.Retention.ExpiresUtc is DateTimeOffset promised)
                        Assert.True(Math.Abs((info.Retention.ExpiresUtc!.Value - promised).TotalMilliseconds) < 1, $"{expected.Name}: retention expiry {info.Retention.ExpiresUtc} differs from {promised}");
                    if (expected.ExactBoundary is CommitToken exactBoundary)
                        Assert.Equal(exactBoundary, info.Boundary);
                    else if (!expected.MinimumBoundary.IsNone)
                        Assert.True(info.Boundary.IsAtOrAfter(expected.MinimumBoundary));

                    DfsSnapshotView view;
                    try
                    {
                        view = await volume.Snapshots.OpenAsync(expected.Name, Ct);
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException && DfsErrors.ToCode(ex) == DfsErrorCode.SnapshotNotFound)
                    {
                        AssertValidlyExpired(expected, cluster.Time.GetUtcNow(), when);
                        retained.Remove(expected.Name);
                        gone.Add(expected.Name);
                        continue;
                    }

                    await using (view)
                    {
                        Assert.True(view.Files.IsReadOnly);
                        Assert.Equal(info.Boundary, view.Info.Boundary);
                        VolumeImage image = await VolumeImage.CaptureAsync(view.Files, view.Directories);
                        AssertImage(VolumeImage.FromModel(expected.Image), image, compareInodes: false, $"{when}: snapshot {expected.Name} differs from its model boundary");
                    }
                }
            }

            static void AssertValidlyExpired(ExpectedSnapshot expected, DateTimeOffset observedBy, string when)
            {
                bool expired = !expected.Retention.Hold && expected.Retention.ExpiresUtc is DateTimeOffset e && observedBy >= e;
                Assert.True(expired, $"{when}: snapshot {expected.Name} disappeared before a valid expiry (retention {expected.Retention})");
            }
        }));

    // =====================================================================================================
    // PROP-006 GeneratedBackupHistories
    // =====================================================================================================

    public static TheoryData<ulong> BackupHistorySeeds => new() { 601, 602, 603 };

    private static readonly string[] BackupInterruptionSites =
    [
        FaultSites.BackupObjectReceived, FaultSites.BackupBeforeClosureVerify, FaultSites.BackupBeforePointer, FaultSites.BackupAfterPointer,
    ];

    [Theory, MemberData(nameof(BackupHistorySeeds)), Catalog("PROP-006", "S", "INV-10", "INV-13", "INV-15", "INV-19")]
    public Task PROP_006_GeneratedBackupHistories(ulong seed) =>
        RunPropertyAsync("PROP-006", seed, trace => ClusterTest.RunAsync(Sim(seed) with { BackupNodeCount = 1, ScenarioTimeout = TimeSpan.FromMinutes(15) }, async cluster =>
        {
            var random = new SeededRandom(Derive(seed, "prop-006"));
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            var budgets = new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(4), MaxPinnedBytes: 64L << 20);
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(), budgets);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "src" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            Assert.Contains(target, await volume.Backups.ListTargetsAsync(Ct));

            var model = new TreeModel();
            var images = new Dictionary<string, (TreeModel Image, CommitToken Boundary)>(StringComparer.Ordinal);
            var acknowledged = new List<string>();
            var held = new HashSet<string>(StringComparer.Ordinal);
            var kinds = new SortedDictionary<string, int>(StringComparer.Ordinal);
            int dataId = 0;
            int snapshotNumber = 0;
            bool reseeded = false;
            await volume.Directories.CreateDirectoryAsync("/b", Ct);
            model.MkDirs("/b");
            await WriteLargeAsync(); // the first snapshot always carries data objects
            await SnapshotAndAcknowledgeAsync("initial");

            for (int step = 0; step < 18; step++)
            {
                int roll = random.Next(100);
                string kind = roll switch
                {
                    < 34 => "mutate",
                    < 56 => "sync",
                    < 72 => "interrupted-transfer",
                    < 80 => "source-leader-loss",
                    < 88 => "retention-hold",
                    _ => reseeded ? "sync" : "offline-beyond-budget",
                };
                await RunStepAsync(kind, $"step {step}");
            }

            // Every ingredient the row requires (transfer interruption, retention, reseed, source loss) must really be part
            // of each generated history; a seed whose random steps missed one gets it appended deterministically.
            string[] requiredKinds = ["interrupted-transfer", "source-leader-loss", "retention-hold", "offline-beyond-budget"];
            foreach (string kind in requiredKinds.Where(k => kinds.GetValueOrDefault(k) == 0).ToArray())
            {
                trace.Step($"coverage: the generated steps contained no {kind}; appending one");
                await RunStepAsync(kind, "coverage");
            }

            foreach (string kind in requiredKinds)
                Assert.True(kinds.GetValueOrDefault(kind) > 0, $"the generated backup history never performed '{kind}'");

            // Source loss: every published snapshot stays complete and readable with no source service reachable.
            trace.Step("crash every source node");
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync();
            await VerifyBackupAsync("with every source node down");
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            Log($"PROP-006 seed {seed}: {acknowledged.Count} acknowledged snapshots, {held.Count} held, steps {string.Join(", ", kinds.Select(k => $"{k.Key}={k.Value}"))}");
            Assert.True(acknowledged.Count >= 3, "too few acknowledged backup snapshots for a meaningful history");

            async Task RunStepAsync(string kind, string label)
            {
                bool performed = true;
                switch (kind)
                {
                    case "mutate":
                        for (int i = 1 + random.Next(4); i > 0; i--)
                            await MutateAsync();
                        break;
                    case "sync":
                        await MutateAsync();
                        await SnapshotAndAcknowledgeAsync("sync");
                        break;
                    case "interrupted-transfer":
                    {
                        await WriteLargeAsync(); // guarantees at least one object transfer for the object-receipt site
                        string name = await CreateSnapshotAsync();
                        string site = BackupInterruptionSites[random.Next(BackupInterruptionSites.Length)];
                        trace.Step($"interrupt transfer of {name}: crash b0 at {site}");
                        FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: backup.Name), FaultAction.CrashNode(CrashMode.LoseUnflushed));
                        await volume.Backups.RequestSyncAsync(target, name, Ct);
                        await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
                        cluster.Faults.AssertReached(arm);
                        await cluster.WaitUntilAsync(() => Task.FromResult(backup.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10), "backup node crashed at the armed site", Ct);
                        await backup.RestartAsync();
                        IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
                        if (site == FaultSites.BackupAfterPointer)
                            Assert.Contains(name, published); // the pointer was durably replaced before the crash
                        else
                            Assert.DoesNotContain(name, published); // never a partially received snapshot
                        await VerifyBackupAsync($"after crash at {site}");
                        await RequestAndAwaitAsync(name, "resumed after interruption");
                        break;
                    }

                    case "source-leader-loss":
                    {
                        await WriteLargeAsync();
                        string name = await CreateSnapshotAsync();
                        FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: backup.Name), FaultAction.Block());
                        await volume.Backups.RequestSyncAsync(target, name, Ct);
                        await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
                        cluster.Faults.AssertReached(arm);
                        TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                        trace.Step($"crash source leader {leader.Name} while {name} is in transfer");
                        await leader.CrashAsync();
                        arm.Release();
                        await VerifyBackupAsync("source leader lost mid-transfer");
                        await leader.RestartAsync();
                        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                        await RequestAndAwaitAsync(name, "after source leader loss");
                        break;
                    }

                    case "retention-hold":
                    {
                        string? name = acknowledged.FirstOrDefault(a => !held.Contains(a));
                        if (name is null)
                        {
                            performed = false;
                            break;
                        }

                        trace.Step($"hold {name} on the backup");
                        await using (DfsClient admin = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk-retention", Credentials = backup.RetentionAdminCredentials }, Ct))
                            await admin.GetVolume(Vol).Snapshots.SetRetentionAsync(name, new DfsRetention(null, Hold: true), Ct);
                        held.Add(name);
                        await using (DfsClient exporter = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk-export", Credentials = backup.ExportCredentials }, Ct))
                        {
                            await ExpectFailureAsync(() => exporter.GetVolume(Vol).Snapshots.DeleteAsync(name, Ct), $"export credentials deleting held {name}",
                                DfsErrorCode.AccessDenied, DfsErrorCode.ReadOnly, DfsErrorCode.RetentionProtected);
                        }

                        Assert.Contains(name, await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
                        break;
                    }

                    case "offline-beyond-budget":
                    {
                        DfsBackupStatus before = await volume.Backups.GetStatusAsync(target, Ct);
                        trace.Step("isolate b0 beyond the transfer-pin age budget");
                        cluster.Network.Isolate(backup.Name);
                        await MutateAsync();
                        string pending = await CreateSnapshotAsync();
                        try
                        {
                            await volume.Backups.RequestSyncAsync(target, pending, Ct);
                        }
                        catch (DfsUnavailableException)
                        {
                            // The request is only a hint while the target is unreachable.
                        }

                        await cluster.DelayAsync(budgets.MaxPinAge + TimeSpan.FromSeconds(2), Ct);
                        await cluster.WaitUntilAsync(async () => (await volume.Backups.GetStatusAsync(target, Ct)).ReseedRequired,
                            TimeSpan.FromSeconds(30), "the source abandons the lineage once the pin budget is exceeded", Ct);
                        DfsBackupStatus abandoned = await volume.Backups.GetStatusAsync(target, Ct);
                        Assert.Equal(before.LatestPublished?.SnapshotName, abandoned.LatestPublished?.SnapshotName); // last complete snapshot preserved
                        long pinnedWhileOffline = (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).PinnedBytes;
                        Assert.True(pinnedWhileOffline <= budgets.MaxPinnedBytes, $"source pinned {pinnedWhileOffline} bytes for an offline target (budget {budgets.MaxPinnedBytes})");
                        cluster.Network.Heal();
                        await VerifyBackupAsync("after lineage abandonment (destination keeps its complete snapshots)");
                        DfsBackupCheckpoint reseed = await RequestAndAwaitAsync(pending, "reseed");
                        DfsBackupStatus after = await volume.Backups.GetStatusAsync(target, Ct);
                        Assert.False(after.ReseedRequired);
                        Assert.NotEqual(before.LineageId, after.LineageId); // a reseed starts a new incremental lineage
                        Assert.Equal(pending, reseed.SnapshotName);
                        reseeded = true;
                        break;
                    }
                }

                if (performed)
                    kinds[kind] = kinds.GetValueOrDefault(kind) + 1;
                await VerifyBackupAsync($"after {label} ({kind})");
            }

            async Task MutateAsync()
            {
                string path = $"/b/f{random.Next(5)}";
                int op = random.Next(10);
                if (op < 6 || !model.Files.ContainsKey(path))
                {
                    byte[] data = Content(seed, ++dataId, 1 + random.Next(2 * InlineThreshold));
                    await volume.Files.WriteAllBytesAsync(path, data, Ct);
                    model.Write(path, data);
                }
                else if (op < 8)
                {
                    byte[] data = Content(seed, ++dataId, 1 + random.Next(300));
                    await volume.Files.AppendAllBytesAsync(path, data, Ct);
                    model.Append(path, data);
                }
                else
                {
                    await volume.Files.DeleteAsync(path, Ct);
                    model.DeleteFile(path);
                }
            }

            async Task WriteLargeAsync()
            {
                string path = $"/b/large{random.Next(3)}.bin";
                byte[] data = Content(seed, ++dataId, ChunkSize + 1 + random.Next(ChunkSize));
                await volume.Files.WriteAllBytesAsync(path, data, Ct);
                model.Write(path, data);
            }

            async Task<string> CreateSnapshotAsync()
            {
                string name = $"b{++snapshotNumber}";
                DfsSnapshotInfo info = await volume.Snapshots.CreateAsync(name, null, Ct);
                images[name] = (model.Clone(), info.Boundary);
                trace.Step($"snapshot {name} at {info.Boundary}");
                return name;
            }

            async Task SnapshotAndAcknowledgeAsync(string why) => await RequestAndAwaitAsync(await CreateSnapshotAsync(), why);

            async Task<DfsBackupCheckpoint> RequestAndAwaitAsync(string name, string why)
            {
                await volume.Backups.RequestSyncAsync(target, name, Ct);
                DfsBackupCheckpoint checkpoint = await volume.Backups.AwaitBackupSnapshotAsync(target, name, TimeSpan.FromSeconds(60), Ct);
                trace.Step($"acknowledged {name} ({why})");
                Assert.Equal(target, checkpoint.Target);
                Assert.Equal(name, checkpoint.SnapshotName);
                Assert.True(checkpoint.IsExactSnapshot);
                Assert.Equal(images[name].Boundary, checkpoint.CoveredCommit);
                DfsBackupStatus status = await volume.Backups.GetStatusAsync(target, Ct);
                Assert.False(status.ReseedRequired);
                Assert.NotNull(status.LatestPublished);
                Assert.True(status.PublishedBoundary.IsAtOrAfter(checkpoint.CoveredCommit), "published boundary behind an acknowledged checkpoint");
                Assert.True(status.DurableBoundary.IsAtOrAfter(status.PublishedBoundary), "status publishes beyond its durable boundary");
                acknowledged.Remove(name);
                acknowledged.Add(name);
                await VerifyBackupAsync($"right after acknowledging {name}");
                return checkpoint;
            }

            async Task VerifyBackupAsync(string when)
            {
                await using DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk-reader", Credentials = backup.ReaderCredentials }, Ct);
                DfsVolume backupVolume = reader.GetVolume(Vol);
                Assert.True(backupVolume.Snapshots.IsReadOnly);
                IReadOnlyList<DfsSnapshotInfo> published = await backupVolume.Snapshots.ListAsync(Ct);
                string[] names = published.Select(p => p.Name).Order(StringComparer.Ordinal).ToArray();
                Assert.Equal(names, (await backup.Inspect.GetPublishedSnapshotsAsync(Vol)).Order(StringComparer.Ordinal).ToArray());
                foreach (string h in held)
                    Assert.Contains(h, names);
                if (acknowledged.Count > 0)
                    Assert.Contains(acknowledged[^1], names); // the latest acknowledged recovery point is retained
                foreach (DfsSnapshotInfo info in published)
                {
                    Assert.True(images.TryGetValue(info.Name, out var expected), $"{when}: backup exposes unknown snapshot {info.Name}");
                    Assert.Equal(expected.Boundary, info.Boundary);
                    await using DfsSnapshotView view = await backupVolume.Snapshots.OpenAsync(info.Name, Ct);
                    Assert.True(view.Files.IsReadOnly);
                    await ExpectFailureAsync(() => view.Files.WriteAllBytesAsync("/b/forbidden", new byte[] { 1 }, Ct), "write to a backup snapshot view", DfsErrorCode.ReadOnly);
                    VolumeImage image = await VolumeImage.CaptureAsync(view.Files, view.Directories);
                    AssertImage(VolumeImage.FromModel(expected.Image), image, compareInodes: false, $"{when}: published backup snapshot {info.Name} is not its complete dependency closure");
                }
            }
        }));

    // =====================================================================================================
    // PROP-007 FaultAndWorkloadSeedCrossProduct
    // =====================================================================================================

    public static TheoryData<ulong, ulong> CrossProductSeeds
    {
        get
        {
            var data = new TheoryData<ulong, ulong>();
            foreach (ulong workload in new ulong[] { 701, 702 })
            {
                foreach (ulong faults in new ulong[] { 7001, 7002, 7003 })
                    data.Add(workload, faults);
            }

            return data;
        }
    }

    private enum ChaosKind { CrashRestart, IsolateNode, PauseResume, DiskStall, FlushFailure, LinkLatency, ResetConnections }

    /// <summary>One generated fault event (node index, gap after the previous event, duration).</summary>
    private sealed record ChaosEvent(ChaosKind Kind, int Node, int GapMs, int DurationMs, CrashMode Mode)
    {
        public override string ToString() => $"+{GapMs}ms {Kind} n{Node} for {DurationMs}ms{(Kind == ChaosKind.CrashRestart ? $" ({Mode})" : "")}";
    }

    /// <summary>One generated workload operation of one process.</summary>
    private sealed record WorkloadOp(string Kind, string Key, int DataId, int Length, int GapMs)
    {
        public override string ToString() => $"{Kind} {Key}{(Kind == "write" ? $" d{DataId} len={Length}" : "")} after {GapMs}ms";
    }

    /// <summary>Result of one cross-product execution (compared for replay determinism).</summary>
    private sealed record CrossProductResult(string Digest, IReadOnlyList<string> ClusterTrace, IReadOnlyList<string> History);

    private static readonly string[] CrossProductKeys = ["/w/k0", "/w/k1", "/w/k2"];

    private static IReadOnlyList<ChaosEvent> GenerateFaultSchedule(ulong faultSeed, int nodes)
    {
        var random = new SeededRandom(Derive(faultSeed, "fault-schedule"));
        var events = new List<ChaosEvent>();
        CrashMode[] modes = [CrashMode.LoseUnflushed, CrashMode.KeepUnflushed, CrashMode.RandomSubset, CrashMode.TearLastWrite];
        for (int i = 0; i < 6; i++)
        {
            events.Add(new ChaosEvent((ChaosKind)random.Next(7), random.Next(nodes), 50 + random.Next(250), 200 + random.Next(1200),
                modes[random.Next(modes.Length)]));
        }

        return events;
    }

    private static IReadOnlyList<WorkloadOp>[] GenerateWorkload(ulong workloadSeed, int processes, int opsPerProcess)
    {
        var all = new IReadOnlyList<WorkloadOp>[processes];
        for (int p = 0; p < processes; p++)
        {
            var random = new SeededRandom(Derive(workloadSeed, "workload-p" + p.ToString(CultureInfo.InvariantCulture)));
            var sizes = new BoundaryBiasedSizes(random.Fork("sizes"), InlineThreshold, ChunkSize, BufferSize);
            var ops = new List<WorkloadOp>();
            for (int i = 0; i < opsPerProcess; i++)
            {
                int roll = random.Next(100);
                string key = CrossProductKeys[random.Next(CrossProductKeys.Length)];
                string kind = roll < 45 ? "read" : roll < 88 ? "write" : "delete";
                int length = kind == "write" ? Math.Max(1, (int)sizes.NextLength(ChunkSize + 1L)) : 0;
                ops.Add(new WorkloadOp(kind, key, kind == "write" ? p * 10_000 + i + 1 : 0, length, random.Next(60)));
            }

            all[p] = ops;
        }

        return all;
    }

    [Theory, MemberData(nameof(CrossProductSeeds)), Catalog("PROP-007", "S", "INV-01", "INV-03", "INV-06", "INV-08", "INV-18", "INV-20")]
    public Task PROP_007_FaultAndWorkloadSeedCrossProduct(ulong workloadSeed, ulong faultSeed) =>
        RunPropertyAsync("PROP-007", Combine(workloadSeed, faultSeed), async trace =>
        {
            trace.Step($"workloadSeed={workloadSeed} faultSeed={faultSeed}");
            CrossProductResult result = await RunCrossProductAsync(workloadSeed, faultSeed, trace);
            Assert.NotEmpty(result.ClusterTrace);
            Assert.False(string.IsNullOrEmpty(result.Digest));
        });

    /// <summary>
    /// Replay-artifact check for PROP-007: the same (workload, fault) seed pair must reproduce the identical cluster trace,
    /// history and replicated-state digest, so saved seeds + starting image + trace are sufficient to replay a failure.
    /// </summary>
    [Fact, Catalog("PROP-007", "S", "INV-20")]
    public Task PROP_007_FaultAndWorkloadSeedCrossProduct_ReplayIsDeterministic() =>
        RunPropertyAsync("PROP-007", Combine(701, 7001), async trace =>
        {
            Assert.Equal(GenerateFaultSchedule(7001, 3), GenerateFaultSchedule(7001, 3));
            Assert.Equal(GenerateWorkload(701, 3, 16).SelectMany(o => o), GenerateWorkload(701, 3, 16).SelectMany(o => o));
            Assert.NotEqual(GenerateFaultSchedule(7001, 3), GenerateFaultSchedule(7002, 3)); // fault seeds really vary the schedule
            CrossProductResult first = await RunCrossProductAsync(701, 7001, trace);
            CrossProductResult second = await RunCrossProductAsync(701, 7001, trace);
            Assert.Equal(first.Digest, second.Digest);
            Assert.Equal(first.History, second.History);
            Assert.Equal(first.ClusterTrace.Count, second.ClusterTrace.Count);
            for (int i = 0; i < first.ClusterTrace.Count; i++)
                Assert.True(first.ClusterTrace[i] == second.ClusterTrace[i], $"replay diverged at trace line {i}: '{first.ClusterTrace[i]}' vs '{second.ClusterTrace[i]}'");
        });

    /// <summary>Marker of the deliberately injected oracle violation used by the PROP-007 artifact check.</summary>
    private const string InjectedCrossProductViolation = "injected oracle violation (PROP-007 replay-artifact check)";

    /// <summary>
    /// PROP-007 "save complete starting images and traces for every failure": a cross-product run that fails its oracle
    /// (here a deliberately injected violation after the faulted workload) leaves a replay directory with the seeds, every
    /// node's durable starting image, the fault schedule, the generated workload and steps, the cluster trace, the history
    /// and the failure itself.
    /// </summary>
    [Fact, Catalog("PROP-007", "S", "INV-20")]
    public Task PROP_007_FaultAndWorkloadSeedCrossProduct_FailureArtifactsSaved()
    {
        const ulong workloadSeed = 799;
        const ulong faultSeed = 7999;
        return RunPropertyAsync("PROP-007", Combine(workloadSeed, faultSeed), async trace =>
        {
            string directory = CrossProductFailureDirectory(workloadSeed, faultSeed);
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);

            Exception? failure = await Record.ExceptionAsync(() => RunCrossProductAsync(workloadSeed, faultSeed, trace, injectViolation: true));
            Assert.NotNull(failure);
            if (failure is NotImplementedException)
                throw failure; // the harness is not implemented yet: not a pass
            Assert.Contains(InjectedCrossProductViolation, failure.ToString(), StringComparison.Ordinal);
            Assert.Contains(directory, failure.ToString(), StringComparison.Ordinal);

            Assert.True(Directory.Exists(directory), $"no replay directory {directory}");
            string summary = File.ReadAllText(Path.Combine(directory, "starting-image.txt"));
            Assert.Contains($"workloadSeed={workloadSeed.ToString(CultureInfo.InvariantCulture)}", summary, StringComparison.Ordinal);
            Assert.Contains($"faultSeed={faultSeed.ToString(CultureInfo.InvariantCulture)}", summary, StringComparison.Ordinal);
            Assert.Contains("clusterSeed=", summary, StringComparison.Ordinal);
            Assert.Equal(CrossProductKeys.Length, summary.Split('\n').Count(l => l.StartsWith("initial ", StringComparison.Ordinal)));
            foreach (string node in new[] { "n0", "n1", "n2" })
            {
                string image = Path.Combine(directory, "starting-image", node);
                Assert.True(Directory.Exists(image) && Directory.EnumerateFiles(image, "*", SearchOption.AllDirectories).Any(),
                    $"the starting image of {node} was not saved under {image}");
            }

            Assert.Equal(GenerateFaultSchedule(faultSeed, 3).Select(e => e.ToString()), File.ReadAllLines(Path.Combine(directory, "fault-schedule.txt")));
            Assert.Equal(GenerateWorkload(workloadSeed, 3, 16).Sum(ops => ops.Count), File.ReadAllLines(Path.Combine(directory, "workload.txt")).Length);
            Assert.NotEmpty(File.ReadAllLines(Path.Combine(directory, "generated-steps.txt")));
            Assert.NotEmpty(File.ReadAllLines(Path.Combine(directory, "cluster-trace.txt")));
            string[] history = File.ReadAllLines(Path.Combine(directory, "history.txt"));
            Assert.True(history.Length >= CrossProductKeys.Length * 2, $"history.txt holds only {history.Length} operations");
            Assert.Contains(InjectedCrossProductViolation, File.ReadAllText(Path.Combine(directory, "failure.txt")), StringComparison.Ordinal);
            Assert.Contains(faultSeed.ToString(CultureInfo.InvariantCulture), File.ReadAllText(Path.Combine(directory, "README.txt")), StringComparison.Ordinal);
        });
    }

    private static string CrossProductFailureDirectory(ulong workloadSeed, ulong faultSeed) =>
        Path.Combine(new TestClusterOptions().ResolveDataRoot(), "prop-007-failures",
            $"w{workloadSeed.ToString(CultureInfo.InvariantCulture)}-f{faultSeed.ToString(CultureInfo.InvariantCulture)}");

    private static Task<CrossProductResult> RunCrossProductAsync(ulong workloadSeed, ulong faultSeed, PropertyTrace trace, bool injectViolation = false)
    {
        // Node/network/storage/scheduler randomness all derive from the cluster seed, which depends on the fault seed only,
        // so the same environment seed is combined with every workload seed.
        TestClusterOptions options = Sim(Derive(faultSeed, "cluster")) with { ScenarioTimeout = TimeSpan.FromMinutes(10) };
        return ClusterTest.RunAsync(options, async cluster =>
        {
            IReadOnlyList<ChaosEvent> schedule = GenerateFaultSchedule(faultSeed, cluster.Nodes.Count);
            IReadOnlyList<WorkloadOp>[] workload = GenerateWorkload(workloadSeed, 3, 16);
            foreach (ChaosEvent e in schedule)
                trace.Step("fault " + e);
            HistoryRecorder history = cluster.History;
            var unknown = new List<(long HistoryId, DfsOperationId OperationId)>();
            var gate = new object();
            var clients = new List<DfsClient>();
            StartingImage? start = null;
            try
            {
                await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
                DfsVolume setupVolume = setup.GetVolume(Vol);
                await setupVolume.Directories.CreateDirectoryAsync("/w", Ct);
                var initial = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (string key in CrossProductKeys)
                {
                    byte[] data = Content(workloadSeed, -1 - initial.Count, InlineThreshold + 1);
                    long id = history.Invoke("setup", "write", key, data);
                    await setupVolume.Files.WriteAllBytesAsync(key, data, Ct);
                    history.Complete(id, OpOutcome.Ok);
                    initial[key] = data;
                }

                await cluster.WaitForReplicationAsync(Vol, Ct);
                start = StartingImage.Capture(cluster, workloadSeed, faultSeed, initial);
                Assert.All(start.NodeFiles, n => Assert.True(n.Value.Count > 0, $"starting image of {n.Key} is empty"));

                for (int p = 0; p < workload.Length; p++)
                    clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "w" + p.ToString(CultureInfo.InvariantCulture), OperationTimeout = TimeSpan.FromSeconds(3) }, Ct));
                var tasks = new List<Task>();
                for (int p = 0; p < workload.Length; p++)
                    tasks.Add(ProcessAsync("w" + p.ToString(CultureInfo.InvariantCulture), clients[p].GetVolume(Vol), workload[p]));
                tasks.Add(ChaosAsync());
                await Task.WhenAll(tasks);

                // Restore every prerequisite (connectivity, processes, storage) before any liveness expectation.
                cluster.Network.Heal();
                foreach (TestNode node in cluster.Nodes)
                {
                    node.Disk.Faults.IoDelay = null;
                    node.Disk.Faults.FailNextFlushes = 0;
                    if (node.State == TestNodeState.Paused)
                        await node.ResumeAsync();
                    else if (node.State is TestNodeState.Crashed or TestNodeState.Stopped)
                        await node.RestartAsync();
                }

                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                foreach ((long hid, DfsOperationId opId) in unknown)
                {
                    DfsOperationOutcome outcome = await ResolveOperationAsync(cluster, setupVolume, opId);
                    history.Resolve(hid, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
                }

                foreach (string key in CrossProductKeys)
                {
                    long id = history.Invoke("final", "read", key, null);
                    history.Complete(id, OpOutcome.Ok, await TryReadAllBytesAsync(setupVolume.Files, key));
                }

                CheckResult check = LinearizabilityChecker.Check(history.Operations, new RegisterModel());
                Log($"PROP-007 w{workloadSeed}/f{faultSeed}: {history.Count} ops, {unknown.Count} resolved unknowns; {check}");
                if (injectViolation)
                    throw new InvalidOperationException(InjectedCrossProductViolation);
                check.AssertValid();
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                string digest = await cluster.ComputeDigestAsync(Ct);
                string[] summary = history.Operations
                    .Select(o => $"{o.Process}:{o.Kind}:{o.Key}:{o.Outcome}:{(o.Output is byte[] b ? Hash(b) : o.Output is null ? "-" : o.Output.GetType().Name)}")
                    .ToArray();
                return new CrossProductResult(digest, cluster.Trace.ToArray(), summary);
            }
            catch (Exception ex) when (ex is not NotImplementedException)
            {
                string directory = SaveCrossProductFailure(cluster, workloadSeed, faultSeed, schedule, workload, start, trace, ex);
                throw new Xunit.Sdk.XunitException($"PROP-007 failure for workload seed {workloadSeed} x fault seed {faultSeed}; starting image, seeds and traces saved to {directory}", ex);
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await SafeDisposeAsync(c);
            }

            async Task ProcessAsync(string process, DfsVolume volume, IReadOnlyList<WorkloadOp> ops)
            {
                foreach (WorkloadOp op in ops)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(op.GapMs), Ct);
                    if (op.Kind == "read")
                    {
                        long hid = history.Invoke(process, "read", op.Key, null);
                        try
                        {
                            // Genuine absence is recorded as a null read; the checker rejects fabricated absence.
                            history.Complete(hid, OpOutcome.Ok, await TryReadAllBytesAsync(volume.Files, op.Key));
                        }
                        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
                        {
                            history.Complete(hid, OpOutcome.Failed, ex);
                        }

                        continue;
                    }

                    byte[]? data = op.Kind == "write" ? Content(workloadSeed, op.DataId, op.Length) : null;
                    DfsOperationId opId;
                    try
                    {
                        opId = await volume.NewOperationIdAsync(Ct);
                    }
                    catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException)
                    {
                        continue; // no id was reserved, so nothing was sent
                    }

                    long id = history.Invoke(process, op.Kind, op.Key, data, opId);
                    try
                    {
                        var options = new DfsMutationOptions { OperationId = opId };
                        if (data is null)
                            await volume.Files.DeleteAsync(op.Key, options, Ct);
                        else
                            await volume.Files.WriteAllBytesAsync(op.Key, data, options, Ct);
                        history.Complete(id, OpOutcome.Ok);
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
                    {
                        OpOutcome outcome = HistoryRecorder.Classify(ex);
                        history.Complete(id, outcome, ex);
                        if (outcome == OpOutcome.Unknown)
                        {
                            lock (gate)
                                unknown.Add((id, opId));
                        }
                    }
                }
            }

            async Task ChaosAsync()
            {
                foreach (ChaosEvent e in schedule)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.GapMs), Ct);
                    TestNode node = cluster.Nodes[e.Node];
                    TestNode peer = cluster.Nodes[(e.Node + 1) % cluster.Nodes.Count];
                    switch (e.Kind)
                    {
                        case ChaosKind.CrashRestart:
                            await node.CrashAsync(e.Mode);
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            await node.RestartAsync();
                            break;
                        case ChaosKind.IsolateNode:
                            cluster.Network.Isolate(node.Name);
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            cluster.Network.Heal();
                            break;
                        case ChaosKind.PauseResume:
                            await node.PauseAsync();
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            await node.ResumeAsync();
                            break;
                        case ChaosKind.DiskStall:
                            node.Disk.Faults.IoDelay = TimeSpan.FromMilliseconds(40);
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            node.Disk.Faults.IoDelay = null;
                            break;
                        case ChaosKind.FlushFailure:
                            node.Disk.Faults.FailNextFlushes = 1;
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            node.Disk.Faults.FailNextFlushes = 0;
                            if (node.State == TestNodeState.Crashed)
                                await node.RestartAsync(); // a failed durability primitive may fail-stop the node
                            break;
                        case ChaosKind.LinkLatency:
                            cluster.Network.SetLatency(node.Name, peer.Name, TimeSpan.FromMilliseconds(30));
                            cluster.Network.SetLatency(peer.Name, node.Name, TimeSpan.FromMilliseconds(30));
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(e.DurationMs), Ct);
                            cluster.Network.SetLatency(node.Name, peer.Name, TimeSpan.Zero);
                            cluster.Network.SetLatency(peer.Name, node.Name, TimeSpan.Zero);
                            break;
                        default:
                            cluster.Network.ResetConnections(node.Name);
                            break;
                    }

                    // Safety is checked during the fault history, not only after convergence.
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                }
            }
        });
    }

    private static string SaveCrossProductFailure(TestCluster cluster, ulong workloadSeed, ulong faultSeed, IReadOnlyList<ChaosEvent> schedule,
        IReadOnlyList<WorkloadOp>[] workload, StartingImage? start, PropertyTrace trace, Exception failure)
    {
        string directory = CrossProductFailureDirectory(workloadSeed, faultSeed);
        Directory.CreateDirectory(directory);
        start?.Save(directory);
        File.WriteAllLines(Path.Combine(directory, "fault-schedule.txt"), schedule.Select(e => e.ToString()));
        File.WriteAllLines(Path.Combine(directory, "workload.txt"), workload.SelectMany((ops, p) => ops.Select((o, i) => $"w{p} #{i} {o}")));
        File.WriteAllLines(Path.Combine(directory, "generated-steps.txt"), trace.Steps);
        try
        {
            File.WriteAllLines(Path.Combine(directory, "cluster-trace.txt"), cluster.Trace);
            File.WriteAllLines(Path.Combine(directory, "history.txt"), cluster.History.Operations.Select(o =>
                $"#{o.Id} {o.Process} {o.Kind} {o.Key} invoke={o.InvokeTime} complete={o.CompleteTime} resolve={o.ResolveTime} {o.Outcome} op={o.OperationId}"));
        }
        catch (NotImplementedException)
        {
            // Harness members not implemented yet: the seeds and generator inputs above are still saved.
        }

        File.WriteAllText(Path.Combine(directory, "failure.txt"), failure.ToString());
        File.WriteAllText(Path.Combine(directory, "README.txt"),
            $"PROP-007 replay: workload seed {workloadSeed}, fault seed {faultSeed}, cluster seed {(start?.ClusterSeed.ToString(CultureInfo.InvariantCulture) ?? "n/a")}.{Environment.NewLine}" +
            "Re-run PROP_007_FaultAndWorkloadSeedCrossProduct with these seeds; starting-image/ holds every node's durable files captured before the workload.");
        return directory;
    }

    // =====================================================================================================
    // PROP-008 MetamorphicBatchingInvariant
    // =====================================================================================================

    public static TheoryData<ulong> BatchingSeeds => new() { 801, 802, 803 };

    /// <summary>One byte update: a write of Data at Offset, or (Data null) a length change to NewLength.</summary>
    private sealed record ByteUpdate(long Offset, byte[]? Data, long NewLength)
    {
        public override string ToString() => Data is null ? $"setlength {NewLength}" : $"write {Offset}+{Data.Length}";
    }

    /// <summary>One way of delivering the same logical updates (buffering, fragmentation, chunking, transport).</summary>
    private sealed record BatchingVariant(string Name, Func<TestClusterOptions, TestClusterOptions> Configure, int BufferSize, DfsWriteMode Mode,
        int MaxFragment, bool ByteWiseSmallWrites, long? LinkBytesPerSecond);

    private static BatchingVariant[] BatchingVariants =>
    [
        new("baseline", o => o, 64 * 1024, DfsWriteMode.OnFlush, int.MaxValue, false, null),
        new("small-buffer-fragmented", o => o, 4096, DfsWriteMode.OnFlush, 997, false, null),
        new("tiny-buffer-bytewise", o => o, 513, DfsWriteMode.OnFlush, 61, true, null),
        new("write-through", o => o, 64 * 1024, DfsWriteMode.WriteThrough, 4096, false, null),
        new("small-chunks-throttled-link", o => o with { Timings = NodeTimings.Fast with { ChunkSize = 16 << 10 } }, 7001, DfsWriteMode.OnFlush, 9000, false, 512L << 10),
    ];

    [Theory, MemberData(nameof(BatchingSeeds)), Catalog("PROP-008", "S", "INV-02", "INV-07", "INV-14")]
    public Task PROP_008_MetamorphicBatchingInvariant(ulong seed) =>
        RunPropertyAsync("PROP-008", seed, async trace =>
        {
            (List<List<ByteUpdate>> segments, List<string> expected, List<bool> changes) = GenerateByteProgram(seed, trace);
            var observed = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
            foreach (BatchingVariant variant in BatchingVariants)
            {
                trace.Step("variant " + variant.Name);
                observed[variant.Name] = await ClusterTest.RunAsync(variant.Configure(Sim(seed)),
                    cluster => RunBatchingVariantAsync(cluster, variant, seed, segments, changes));
            }

            foreach ((string name, IReadOnlyList<string> boundaries) in observed)
            {
                Assert.Equal(expected.Count, boundaries.Count);
                for (int i = 0; i < expected.Count; i++)
                    Assert.True(expected[i] == boundaries[i], $"variant {name}: publication boundary {i} is {boundaries[i]}, reference model {expected[i]}");
            }

            // Every delivery strategy yields the same bytes at every equivalent publication boundary.
            Assert.Single(observed.Values.Select(v => string.Join("|", v)).Distinct(StringComparer.Ordinal));
        });

    /// <summary>
    /// PROP-008 transport-fragmentation dimension: the in-memory simulated transport delivers whole messages, so the same
    /// byte program is also delivered over the real QUIC transport (envelope frames of at most 60 KiB, fragmented and
    /// reassembled per correlation id; chunk-sized object uploads and 128 KiB writes span several frames), with whole and
    /// fragmented write calls. Single writer, sequential calls: the boundaries are timing-independent.
    /// </summary>
    [Theory, MemberData(nameof(BatchingSeeds)), Catalog("PROP-008", "S", "INV-02", "INV-07", "INV-14")]
    public Task PROP_008_MetamorphicBatchingInvariant_RealTransportFragmentation(ulong seed) =>
        RunPropertyAsync("PROP-008", seed, async trace =>
        {
            (List<List<ByteUpdate>> segments, List<string> expected, List<bool> changes) = GenerateByteProgram(seed, trace);
            BatchingVariant[] variants =
            [
                new("quic-whole-writes", o => o with { Mode = ClusterMode.Realtime, Transport = TransportKind.Quic }, 64 * 1024, DfsWriteMode.OnFlush, int.MaxValue, false, null),
                new("quic-fragmented-writes", o => o with { Mode = ClusterMode.Realtime, Transport = TransportKind.Quic }, 4096, DfsWriteMode.OnFlush, 997, false, null),
            ];
            foreach (BatchingVariant variant in variants)
            {
                trace.Step("variant " + variant.Name);
                IReadOnlyList<string> boundaries = await ClusterTest.RunAsync(variant.Configure(Sim(seed)),
                    cluster => RunBatchingVariantAsync(cluster, variant, seed, segments, changes));
                Assert.Equal(expected.Count, boundaries.Count);
                for (int i = 0; i < expected.Count; i++)
                    Assert.True(expected[i] == boundaries[i], $"variant {variant.Name}: publication boundary {i} is {boundaries[i]}, reference model {expected[i]}");
            }
        });

    private static (List<List<ByteUpdate>> Segments, List<string> Expected, List<bool> Changes) GenerateByteProgram(ulong seed, PropertyTrace trace)
    {
        var random = new SeededRandom(Derive(seed, "prop-008"));
        var sizes = new BoundaryBiasedSizes(random.Fork("sizes"), InlineThreshold, ChunkSize, BufferSize, [16 << 10, 7001, 4096, 513]);
        long maxLength = 3L * ChunkSize + InlineThreshold;
        var model = new ReferenceFileModel(ReadOnlySpan<byte>.Empty);
        var segments = new List<List<ByteUpdate>>();
        var expected = new List<string>();
        var changes = new List<bool>();
        int dataId = 0;
        for (int s = 0; s < 7; s++)
        {
            var updates = new List<ByteUpdate>();
            for (int u = 1 + random.Next(5); u > 0; u--)
            {
                if (random.Next(100) < 15)
                {
                    long length = sizes.NextLength(maxLength);
                    updates.Add(new ByteUpdate(0, null, length));
                    model.SetLength(length);
                }
                else
                {
                    long offset = sizes.NextOffset(model.StagedLength, maxLength - 1);
                    int length = (int)Math.Max(1, sizes.NextLength(Math.Min(maxLength - offset, 2L * ChunkSize)));
                    byte[] data = Content(seed, ++dataId, length);
                    updates.Add(new ByteUpdate(offset, data, 0));
                    model.Write(offset, data);
                }

                trace.Step($"segment {s}: {updates[^1]}");
            }

            changes.Add(model.HasPendingChanges);
            model.Flush();
            expected.Add(Boundary(model.Committed));
            segments.Add(updates);
        }

        return (segments, expected, changes);
    }

    private static async Task<IReadOnlyList<string>> RunBatchingVariantAsync(TestCluster cluster, BatchingVariant variant, ulong seed,
        IReadOnlyList<List<ByteUpdate>> segments, IReadOnlyList<bool> changes)
    {
        const string path = "/m/f.bin";
        await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "w", BufferSize = variant.BufferSize }, Ct);
        await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
        if (variant.LinkBytesPerSecond is long bytesPerSecond)
        {
            foreach (TestNode node in cluster.Nodes)
            {
                cluster.Network.SetBandwidth("client:w", node.Name, bytesPerSecond);
                cluster.Network.SetBandwidth(node.Name, "client:w", bytesPerSecond);
                cluster.Network.SetLatency("client:w", node.Name, TimeSpan.FromMilliseconds(3));
            }
        }

        DfsVolume wv = writerClient.GetVolume(Vol);
        DfsVolume rv = readerClient.GetVolume(Vol);
        await wv.Directories.CreateDirectoryAsync("/m", Ct);
        var fragments = new SeededRandom(Derive(seed, "fragments:" + variant.Name));
        var boundaries = new List<string>();
        DfsStream writer = await wv.Files.OpenAsync(path, new DfsOpenOptions
        {
            Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read, WriteMode = variant.Mode, BufferSize = variant.BufferSize,
        }, Ct);
        try
        {
            Assert.Equal(variant.Mode, writer.WriteMode);
            ulong revision = (await rv.Files.GetInfoAsync(path, Ct)).Revision;
            for (int s = 0; s < segments.Count; s++)
            {
                foreach (ByteUpdate update in segments[s])
                {
                    if (update.Data is null)
                    {
                        await writer.SetLengthAsync(update.NewLength, Ct);
                        continue;
                    }

                    writer.Seek(update.Offset, SeekOrigin.Begin);
                    for (int pos = 0; pos < update.Data.Length;)
                    {
                        int remaining = update.Data.Length - pos;
                        int n = variant.MaxFragment == int.MaxValue ? remaining
                            : variant.ByteWiseSmallWrites && update.Data.Length <= 64 ? 1
                            : Math.Min(remaining, 1 + fragments.Next(variant.MaxFragment));
                        await writer.WriteAsync(update.Data.AsMemory(pos, n), Ct);
                        pos += n;
                    }

                    Assert.Equal(update.Offset + update.Data.Length, writer.Position);
                }

                await writer.FlushAsync(Ct); // the equivalent publication boundary
                byte[] committed = await rv.Files.ReadAllBytesAsync(path, Ct);
                DfsFileInfo info = await rv.Files.GetInfoAsync(path, Ct);
                Assert.Equal(committed.LongLength, info.Length);
                if (variant.Mode == DfsWriteMode.OnFlush && changes[s])
                    Assert.True(info.Revision > revision, $"{variant.Name}: segment {s} changed bytes but no new revision was published");
                Assert.True(info.Revision >= revision);
                revision = info.Revision;
                boundaries.Add(Boundary(committed));
            }
        }
        finally
        {
            await SafeDisposeAsync(writer);
        }

        return boundaries;
    }

    // =====================================================================================================
    // PROP-009 MetamorphicPlacementInvariant
    // =====================================================================================================

    public static TheoryData<ulong> PlacementSeeds => new() { 901, 902 };

    /// <summary>A legal replica placement / latency / availability profile.</summary>
    private sealed record PlacementVariant(string Name, TestClusterOptions Options, Action<TestCluster>? Setup = null, string? OfflineNode = null, int OfflineFromStep = -1);

    private sealed record PlacementRun(IReadOnlyList<string> Results, string Digest, string Signature);

    private static PlacementVariant[] PlacementVariants =>
    [
        new("three-zones", TestClusterOptions.ThreeZones),
        new("two-plus-one", TestClusterOptions.ThreeZones with { Topology = TestTopology.TwoPlusOne }),
        new("single-zone", TestClusterOptions.ThreeZones with { Topology = TestTopology.SingleZone }),
        new("four-nodes-skewed-latency", TestClusterOptions.ThreeZones with { NodeCount = 4 }, Setup: c =>
        {
            c.Network.SetLatency("n0", "n1", TimeSpan.FromMilliseconds(40));
            c.Network.SetLatency("n1", "n0", TimeSpan.FromMilliseconds(40));
            c.Network.SetLatency("n2", "n3", TimeSpan.FromMilliseconds(15));
            c.Network.SetLatency("n3", "n2", TimeSpan.FromMilliseconds(15));
            c.Network.SetLatency("client:c0", "n0", TimeSpan.FromMilliseconds(25));
        }),
        new("five-nodes", TestClusterOptions.FiveNodes),
        new("five-nodes-one-offline", TestClusterOptions.FiveNodes, OfflineNode: "n4", OfflineFromStep: 8),
    ];

    [Theory, MemberData(nameof(PlacementSeeds)), Catalog("PROP-009", "S", "INV-02", "INV-07", "INV-08", "INV-11", "INV-14")]
    public Task PROP_009_MetamorphicPlacementInvariant(ulong seed) =>
        RunPropertyAsync("PROP-009", seed, async trace =>
        {
            var generator = new ProgramGenerator(seed, "prop-009");
            List<ProgStep> program = generator.Generate(40);
            program.Add(generator.Step(new ProgOp(ProgKind.Mkdir, "/p")));
            program.Add(generator.Step(new ProgOp(ProgKind.Write, "/p/big.bin", DataId: 1_000_000, Length: 2 * ChunkSize + 1)));
            program.Add(generator.Step(new ProgOp(ProgKind.Read, "/p/big.bin")));
            foreach (ProgStep step in program)
                trace.Step($"{step.Op} => {step.Expected}");
            string expectedDigest = VolumeImage.FromModel(generator.Model).Digest();

            var runs = new Dictionary<string, PlacementRun>(StringComparer.Ordinal);
            foreach (PlacementVariant variant in PlacementVariants)
            {
                trace.Step("placement variant " + variant.Name);
                TestClusterOptions options = variant.Options with { Mode = ClusterMode.Deterministic, Seed = Derive(seed, "cluster:" + variant.Name) };
                runs[variant.Name] = await ClusterTest.RunAsync(options, cluster => RunPlacementVariantAsync(cluster, variant, program, seed));
            }

            foreach ((string name, PlacementRun run) in runs)
            {
                Assert.Equal(program.Select(p => p.Expected), run.Results);
                Assert.True(expectedDigest == run.Digest, $"variant {name}: final namespace/content digest {run.Digest} differs from the reference {expectedDigest}");
                Log($"PROP-009 {name}: placement {run.Signature}");
            }

            Assert.True(runs.Values.Select(r => r.Signature).Distinct(StringComparer.Ordinal).Count() >= 3,
                "the variants did not produce different legal placements, so the metamorphic relation was not exercised");
        });

    private static async Task<PlacementRun> RunPlacementVariantAsync(TestCluster cluster, PlacementVariant variant, IReadOnlyList<ProgStep> program, ulong seed)
    {
        variant.Setup?.Invoke(cluster);
        await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
        DfsVolume volume = client.GetVolume(Vol);
        var results = new List<string>();
        TestNode? offline = null;
        try
        {
            for (int i = 0; i < program.Count; i++)
            {
                if (i == variant.OfflineFromStep && variant.OfflineNode is string name)
                {
                    offline = cluster.Node(name);
                    await offline.CrashAsync();
                }

                (string result, _) = await ProgramRunner.ExecuteAsync(volume, program[i].Op, seed);
                Assert.True(result == program[i].Expected, $"{variant.Name} step {i} {program[i].Op}: got {result}, expected {program[i].Expected}");
                results.Add(result);
            }

            VolumeImage image = await CaptureAsync(volume);
            FileInspection file = await cluster.Inspect.GetFileAsync(Vol, "/p/big.bin")
                ?? throw new Xunit.Sdk.XunitException($"{variant.Name}: the inspector does not see /p/big.bin");
            Assert.False(file.IsInline);
            Assert.Equal(2L * ChunkSize + 1, file.Length);
            Assert.NotEmpty(file.Extents);
            DfsStoragePolicy policy = cluster.Options.GetEffectiveDefaultPolicy();
            var signature = new List<string>();
            foreach (ObjectId obj in file.Extents.Select(e => e.Object).Distinct())
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck,
                    $"{variant.Name}: object {obj} has {copies} copies in {zones} zones, below the acknowledged policy {policy}");
                Assert.True(holders.Count >= copies);
                if (offline is not null)
                    Assert.DoesNotContain(offline.Name, holders); // an offline node is never counted as a holder
                signature.Add(string.Join("+", holders.Order(StringComparer.Ordinal).Select(h => h + "@" + cluster.Node(h).Labels.Zone)));
            }

            return new PlacementRun(results, image.Digest(), string.Join("|", signature));
        }
        finally
        {
            if (offline is not null && offline.State == TestNodeState.Crashed)
                await offline.RestartAsync();
        }
    }

    // =====================================================================================================
    // PROP-010 MetamorphicRestartInvariant
    // =====================================================================================================

    public static TheoryData<ulong> RestartSeeds => new() { 1001, 1002, 1003 };

    private enum RestartKind { AllCrashLoseUnflushed, AllCrashTornTail, LeaderCrashRandomSubset, RollingGraceful }

    private sealed record ProgramRun(IReadOnlyList<string> Results, IReadOnlyDictionary<int, string> DigestAt, string FinalDigest);

    [Theory, MemberData(nameof(RestartSeeds)), Catalog("PROP-010", "S", "INV-01", "INV-06", "INV-07", "INV-18")]
    public Task PROP_010_MetamorphicRestartInvariant(ulong seed) =>
        RunPropertyAsync("PROP-010", seed, async trace =>
        {
            var generator = new ProgramGenerator(seed, "prop-010");
            List<ProgStep> program = generator.Generate(36);
            var random = new SeededRandom(Derive(seed, "restart-points"));
            int[] durable = Enumerable.Range(0, program.Count).Where(i => program[i].Op.IsMutation).ToArray();
            RestartKind[] kinds = Enum.GetValues<RestartKind>();
            Assert.True(durable.Length >= kinds.Length, "the generated program has too few durable decisions");
            var points = new SortedDictionary<int, RestartKind>();
            foreach (RestartKind kind in kinds.OrderBy(_ => random.NextUInt64()))
            {
                int at;
                do
                {
                    at = durable[random.Next(durable.Length)];
                }
                while (points.ContainsKey(at));
                points[at] = kind;
            }

            foreach (ProgStep step in program)
                trace.Step($"{step.Op} => {step.Expected}");
            foreach ((int at, RestartKind kind) in points)
                trace.Step($"cold restart {kind} after step {at} ({program[at].Op})");

            ProgramRun reference = await ClusterTest.RunAsync(Sim(seed), c => RunProgramWithRestartsAsync(c, program, seed, points, restart: false));
            ProgramRun restarted = await ClusterTest.RunAsync(Sim(seed), c => RunProgramWithRestartsAsync(c, program, seed, points, restart: true));
            Assert.Equal(reference.Results, restarted.Results);
            foreach ((int at, RestartKind kind) in points)
                Assert.True(reference.DigestAt[at] == restarted.DigestAt[at], $"state after the {kind} restart following step {at} differs from the uninterrupted run");
            Assert.Equal(reference.FinalDigest, restarted.FinalDigest);
            Assert.Equal(VolumeImage.FromModel(generator.Model).Digest(), reference.FinalDigest);
        });

    private static async Task<ProgramRun> RunProgramWithRestartsAsync(TestCluster cluster, IReadOnlyList<ProgStep> program, ulong seed,
        IReadOnlyDictionary<int, RestartKind> points, bool restart)
    {
        DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "prog" }, Ct);
        var results = new List<string>();
        var digests = new Dictionary<int, string>();
        try
        {
            for (int i = 0; i < program.Count; i++)
            {
                (string result, _) = await ProgramRunner.ExecuteAsync(client.GetVolume(Vol), program[i].Op, seed);
                Assert.True(result == program[i].Expected, $"step {i} {program[i].Op}: got {result}, expected {program[i].Expected}");
                results.Add(result);
                if (!points.TryGetValue(i, out RestartKind kind))
                    continue;

                VolumeImage before = await CaptureAsync(client.GetVolume(Vol));
                digests[i] = before.Digest();
                if (!restart)
                    continue;
                await client.DisposeAsync();
                await ColdRestartAsync(cluster, kind);
                client = await cluster.ConnectAsync(new TestClientOptions { Name = "prog" }, Ct);
                VolumeImage after = await CaptureAsync(client.GetVolume(Vol));
                AssertImage(before, after, compareInodes: true, $"committed state (paths, bytes, inode identities) after the {kind} restart following step {i}");
                await cluster.Inspect.AssertNoSplitBrainAsync();
            }

            VolumeImage final = await CaptureAsync(client.GetVolume(Vol));
            if (restart)
            {
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }

            return new ProgramRun(results, digests, final.Digest());
        }
        finally
        {
            await SafeDisposeAsync(client);
        }
    }

    private static async Task ColdRestartAsync(TestCluster cluster, RestartKind kind)
    {
        TestNode[] nodes = cluster.Nodes.ToArray();
        var incarnations = nodes.ToDictionary(n => n.Name, n => n.Incarnation, StringComparer.Ordinal);
        var restarted = new List<TestNode>();
        switch (kind)
        {
            case RestartKind.AllCrashLoseUnflushed:
            case RestartKind.AllCrashTornTail:
            {
                CrashMode mode = kind == RestartKind.AllCrashLoseUnflushed ? CrashMode.LoseUnflushed : CrashMode.TearLastWrite;
                foreach (TestNode node in nodes)
                    await node.CrashAsync(mode);
                foreach (TestNode node in nodes)
                    await node.RestartAsync();
                restarted.AddRange(nodes);
                break;
            }

            case RestartKind.LeaderCrashRandomSubset:
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                await leader.CrashAsync(CrashMode.RandomSubset);
                await leader.RestartAsync();
                restarted.Add(leader);
                break;
            }

            default:
                foreach (TestNode node in nodes)
                {
                    await node.StopAsync();
                    await node.RestartAsync();
                    await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                    restarted.Add(node);
                }

                break;
        }

        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
        foreach (TestNode node in restarted)
            Assert.True(node.Incarnation > incarnations[node.Name], $"{node.Name} restarted without a new incarnation");
    }

    // =====================================================================================================
    // PROP-011 MetamorphicDuplicateRequestInvariant
    // =====================================================================================================

    public static TheoryData<ulong> DuplicateSeeds => new() { 1101, 1102, 1103 };

    private enum DupKind { None, ImmediateRetry, WireDuplicate, LostReplyRetry, DelayedRetry, MismatchedReuse }

    private sealed record DuplicateRun(IReadOnlyList<string> Results, IReadOnlyList<string> DigestAfter, IReadOnlyDictionary<DupKind, int> Applied);

    [Theory, MemberData(nameof(DuplicateSeeds)), Catalog("PROP-011", "S", "INV-02", "INV-06", "INV-07")]
    public Task PROP_011_MetamorphicDuplicateRequestInvariant(ulong seed) =>
        RunPropertyAsync("PROP-011", seed, async trace =>
        {
            var generator = new ProgramGenerator(seed, "prop-011") { Weights = [18, 30, 8, 8, 8, 5, 3, 12, 8] };
            List<ProgStep> program = generator.Generate(40);
            for (int extra = 0; program.Count(s => s.Op.Kind == ProgKind.Append) < 6; extra++)
                program.Add(generator.Step(new ProgOp(ProgKind.Append, "/p/extra.log", DataId: 2_000_000 + extra, Length: 100 + extra)));

            // Assign duplicate kinds round-robin per operation class so every kind is exercised (appends are the
            // non-idempotent case that matters most).
            var random = new SeededRandom(Derive(seed, "duplicates"));
            DupKind[] appendRotation = [DupKind.WireDuplicate, DupKind.LostReplyRetry, DupKind.ImmediateRetry, DupKind.DelayedRetry, DupKind.MismatchedReuse, DupKind.None];
            DupKind[] writeRotation = [DupKind.ImmediateRetry, DupKind.MismatchedReuse, DupKind.DelayedRetry, DupKind.None];
            DupKind[] otherRotation = [DupKind.WireDuplicate, DupKind.ImmediateRetry, DupKind.LostReplyRetry, DupKind.DelayedRetry, DupKind.None];
            int a = random.Next(appendRotation.Length), w = random.Next(writeRotation.Length), o = random.Next(otherRotation.Length);
            DupKind[] kinds = program.Select(s => s.Op.Kind switch
            {
                ProgKind.Read or ProgKind.List => DupKind.None,
                ProgKind.Append => appendRotation[a++ % appendRotation.Length],
                ProgKind.Write => writeRotation[w++ % writeRotation.Length],
                _ => otherRotation[o++ % otherRotation.Length],
            }).ToArray();
            for (int i = 0; i < program.Count; i++)
                trace.Step($"{program[i].Op} => {program[i].Expected}{(kinds[i] == DupKind.None ? "" : $" [duplicate: {kinds[i]}]")}");

            DuplicateRun reference = await ClusterTest.RunAsync(Sim(seed), c => RunDuplicatesAsync(c, program, seed, new DupKind[program.Count]));
            DuplicateRun duplicated = await ClusterTest.RunAsync(Sim(seed), c => RunDuplicatesAsync(c, program, seed, kinds));
            Assert.Equal(reference.Results, duplicated.Results);
            for (int i = 0; i < program.Count; i++)
            {
                Assert.True(reference.DigestAfter[i] == duplicated.DigestAfter[i],
                    $"logical committed state after step {i} ({program[i].Op}, duplicate {kinds[i]}) differs from the duplicate-free run");
            }

            Assert.Equal(VolumeImage.FromModel(generator.Model).Digest(), duplicated.DigestAfter[^1]);
            foreach (DupKind kind in Enum.GetValues<DupKind>().Where(k => k != DupKind.None))
                Assert.True(duplicated.Applied.GetValueOrDefault(kind) > 0, $"duplicate kind {kind} was never applied");
        });

    private static async Task<DuplicateRun> RunDuplicatesAsync(TestCluster cluster, IReadOnlyList<ProgStep> program, ulong seed, IReadOnlyList<DupKind> kinds)
    {
        await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "dup" }, Ct);
        DfsVolume volume = client.GetVolume(Vol);
        var results = new List<string>();
        var digests = new List<string>();
        var applied = new Dictionary<DupKind, int>();
        var delayed = new Queue<(int Step, ProgOp Op, DfsOperationId Id, CommitToken Token, string Result)>();
        for (int i = 0; i < program.Count; i++)
        {
            ProgOp op = program[i].Op;
            DupKind dup = kinds[i];
            string result;
            if (!op.IsMutation)
            {
                (result, _) = await ProgramRunner.ExecuteAsync(volume, op, seed);
            }
            else
            {
                DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                MessageRule? rule = dup switch
                {
                    DupKind.WireDuplicate => cluster.Network.DuplicateNext(new MessageFilter(From: "client:dup", Operation: op.Operation, Direction: LinkDirection.Request)),
                    DupKind.LostReplyRetry => cluster.Network.DropReplies(new MessageFilter(From: "client:dup", Operation: op.Operation)),
                    _ => null,
                };
                CommitToken token;
                try
                {
                    (result, token) = await ProgramRunner.ExecuteAsync(volume, op, seed, id);
                }
                catch (DfsCommitOutcomeUnknownException unknown) when (dup == DupKind.LostReplyRetry)
                {
                    Assert.Equal(id, unknown.OperationId); // uncertainty carries the id the caller retries with
                    (result, token) = await ProgramRunner.ExecuteAsync(volume, op, seed, id);
                }

                if (rule is not null)
                {
                    Assert.True(rule.Matched >= 1, $"step {i}: the {dup} rule never matched a {op.Operation} message");
                    rule.Cancel();
                }

                switch (dup)
                {
                    case DupKind.ImmediateRetry:
                    {
                        string before = (await CaptureAsync(volume)).Digest();
                        (string again, CommitToken againToken) = await ProgramRunner.ExecuteAsync(volume, op, seed, id);
                        Assert.Equal(result, again);
                        Assert.Equal(token, againToken); // repeated delivery of the original result
                        Assert.Equal(before, (await CaptureAsync(volume)).Digest());
                        break;
                    }

                    case DupKind.DelayedRetry:
                        delayed.Enqueue((i, op, id, token, result));
                        break;
                    case DupKind.MismatchedReuse:
                    {
                        string before = (await CaptureAsync(volume)).Digest();
                        // Different content AND length: a different DataId alone leaves a zero-length payload identical
                        // (boundary-biased lengths are often 0), which is a legal retry, not an incompatible reuse.
                        ProgOp changed = op with { DataId = op.DataId + 7_777, Length = op.Length + 1 };
                        Exception ex = await ExpectFailureAsync(() => ProgramRunner.ExecuteAsync(volume, changed, seed, id),
                            $"step {i}: reusing {id} with a different payload", DfsErrorCode.OperationIdMismatch);
                        Assert.IsType<DfsOperationIdMismatchException>(ex);
                        Assert.Equal(before, (await CaptureAsync(volume)).Digest());
                        break;
                    }
                }

                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(id, outcome.Id);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                if (!token.IsNone)
                    Assert.Equal(token, outcome.Token);
                if (dup != DupKind.None)
                    applied[dup] = applied.GetValueOrDefault(dup) + 1;
            }

            // Delayed duplicates of earlier accepted ids arrive after later operations and change nothing.
            while (delayed.Count > 0 && (delayed.Peek().Step <= i - 2 || i == program.Count - 1))
                await RetryDelayedAsync(delayed.Dequeue());
            results.Add(result);
            digests.Add((await CaptureAsync(volume)).Digest());
        }

        return new DuplicateRun(results, digests, applied);

        async Task RetryDelayedAsync((int Step, ProgOp Op, DfsOperationId Id, CommitToken Token, string Result) d)
        {
            string before = (await CaptureAsync(volume)).Digest();
            (string again, CommitToken againToken) = await ProgramRunner.ExecuteAsync(volume, d.Op, seed, d.Id);
            Assert.Equal(d.Result, again);
            Assert.True(d.Token == againToken, $"delayed duplicate of step {d.Step} ({d.Op}) returned {againToken}, original {d.Token}");
            Assert.True(before == (await CaptureAsync(volume)).Digest(), $"delayed duplicate of step {d.Step} ({d.Op}) executed a second logical effect");
        }
    }

    // =====================================================================================================
    // PROP-012 MetamorphicSnapshotReadInvariant
    // =====================================================================================================

    public static TheoryData<ulong> SnapshotReadSeeds => new() { 1201, 1202, 1203 };

    [Theory, MemberData(nameof(SnapshotReadSeeds)), Catalog("PROP-012", "S", "INV-03", "INV-07", "INV-09", "INV-13")]
    public Task PROP_012_MetamorphicSnapshotReadInvariant(ulong seed) =>
        RunPropertyAsync("PROP-012", seed, trace => ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            var generator = new ProgramGenerator(seed, "prop-012");
            List<ProgStep> prefix = generator.Generate(24);
            prefix.Add(generator.Step(new ProgOp(ProgKind.Mkdir, "/p/x")));
            prefix.Add(generator.Step(new ProgOp(ProgKind.Write, "/p/x/big.bin", DataId: 3_000_000, Length: 2 * ChunkSize + 1)));
            TreeModel snapshotModel = generator.Model.Clone();
            List<ProgStep> later = generator.Generate(30);
            var random = new SeededRandom(Derive(seed, "disturbances"));
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "snapread" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            foreach (ProgStep step in prefix)
            {
                trace.Step("prefix " + step.Op);
                (string result, _) = await ProgramRunner.ExecuteAsync(volume, step.Op, seed);
                Assert.Equal(step.Expected, result);
            }

            DfsSnapshotInfo info = await volume.Snapshots.CreateAsync("base", null, Ct);
            VolumeImage liveAtSnapshot = await CaptureAsync(volume);
            await using DfsSnapshotView longLived = await volume.Snapshots.OpenAsync("base", Ct);
            VolumeImage reference = await VolumeImage.CaptureAsync(longLived.Files, longLived.Directories);
            AssertImage(VolumeImage.FromModel(snapshotModel), reference, compareInodes: false, "snapshot contents at creation");
            AssertImage(liveAtSnapshot, reference, compareInodes: true, "snapshot preserves the inode identities of its boundary");
            IReadOnlyList<string> pages = await ListPagesAsync(longLived.Directories, "/p", null);
            DfsDirectoryPage firstPage = await longLived.Directories.ListPageAsync("/p", new DfsEnumerationOptions { PageSize = 1 }, null, Ct);
            string? continuation = firstPage.ContinuationToken;
            byte[] bigAtSnapshot = snapshotModel.Files["/p/x/big.bin"];
            int others = 0;
            bool leaderCrashed = false;

            for (int i = 0; i < later.Count; i++)
            {
                trace.Step("later " + later[i].Op);
                (string result, _) = await ProgramRunner.ExecuteAsync(volume, later[i].Op, seed);
                Assert.Equal(later[i].Expected, result);
                int roll = random.Next(100);
                if (roll < 10)
                {
                    string name = $"other{++others}";
                    trace.Step($"create and delete snapshot {name}");
                    await volume.Snapshots.CreateAsync(name, null, Ct);
                    await volume.Snapshots.DeleteAsync(name, Ct);
                }
                else if (roll < 18)
                {
                    trace.Step("let GC run");
                    await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                }
                else if (roll < 22 && !leaderCrashed)
                {
                    leaderCrashed = true;
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    trace.Step($"crash and restart leader {leader.Name}");
                    await leader.CrashAsync();
                    await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                    await leader.RestartAsync();
                    await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                }

                if (i % 3 == 2)
                    await VerifySnapshotAsync($"after later step {i}");
            }

            // The most extreme later operations: delete everything live, then let GC run repeatedly.
            await volume.Directories.DeleteAsync("/p", recursive: true, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            Assert.NotEqual(reference.Digest(), (await CaptureAsync(volume)).Digest()); // live state really diverged
            await VerifySnapshotAsync("after deleting the live tree");
            DfsSnapshotInfo again = await volume.Snapshots.GetAsync("base", Ct);
            Assert.Equal(info.Id, again.Id);
            Assert.Equal(info.Boundary, again.Boundary);

            // A continuation token taken before the later operations resumes the same pinned listing, or is explicitly expired.
            if (continuation is not null)
            {
                try
                {
                    IReadOnlyList<string> resumed = await ListPagesAsync(longLived.Directories, "/p", continuation);
                    Assert.Equal(pages.Skip(1), resumed);
                }
                catch (DfsPinExpiredException expired)
                {
                    Assert.Equal(DfsErrorCode.ContinuationExpired, expired.ErrorCode); // explicit, never different contents
                }
            }

            async Task VerifySnapshotAsync(string when)
            {
                AssertImage(reference, await VolumeImage.CaptureAsync(longLived.Files, longLived.Directories), compareInodes: true, when + " (view opened before the later operations)");
                await using DfsSnapshotView fresh = await volume.Snapshots.OpenAsync("base", Ct);
                Assert.Equal(info.Boundary, fresh.Info.Boundary);
                AssertImage(reference, await VolumeImage.CaptureAsync(fresh.Files, fresh.Directories), compareInodes: true, when + " (freshly opened view)");
                Assert.Equal(pages, await ListPagesAsync(fresh.Directories, "/p", null));
                // Ranged reads through the snapshot at boundary offsets.
                await using DfsStream stream = await fresh.Files.OpenReadAsync("/p/x/big.bin", Ct);
                foreach (long offset in new long[] { 0, InlineThreshold - 1, ChunkSize - 1, ChunkSize, 2L * ChunkSize })
                {
                    byte[] got = await ReadRangeAsync(stream, offset, 3);
                    AssertEx.BytesEqual(bigAtSnapshot.AsSpan((int)offset, (int)Math.Min(3, bigAtSnapshot.Length - offset)), got, $"{when}: snapshot bytes at {offset}");
                }
            }
        }));

    private static async Task<IReadOnlyList<string>> ListPagesAsync(DfsDirectoryClient directories, string path, string? continuation)
    {
        var pages = new List<string>();
        string? token = continuation;
        do
        {
            DfsDirectoryPage page = await directories.ListPageAsync(path, new DfsEnumerationOptions { PageSize = 1 }, token, Ct);
            pages.Add(string.Join(",", page.Entries.Select(e => $"{e.Name}:{e.Kind}:{e.InodeId.Value}:{e.Length}")));
            token = page.ContinuationToken;
        }
        while (token is not null);
        return pages;
    }

    // =====================================================================================================
    // PROP-013 GeneratorCoversBoundarySizes
    // =====================================================================================================

    /// <summary>
    /// The generator itself: lengths and offsets are biased to 0, 1, inline ±1, chunk ±1, buffer ±1, 2×chunk ±1 and
    /// end-of-file ±1 rather than mostly easy uniform values, deterministically per seed, and the overflow/limit edge
    /// list covers negative, long/int overflow and every configured limit ±1.
    /// </summary>
    [Fact, Catalog("PROP-013", "S", "INV-07", "INV-20")]
    public void PROP_013_GeneratorCoversBoundarySizes()
    {
        foreach ((int inline, int chunk, int buffer) in new[] { (InlineThreshold, ChunkSize, BufferSize), (4096, 16 << 10, 7000) })
        {
            const int draws = 4000;
            long max = 3L * chunk + inline;
            BoundaryBiasedSizes Run(out List<long> values)
            {
                var sizes = new BoundaryBiasedSizes(new SeededRandom(1301), inline, chunk, buffer);
                values = [];
                for (int i = 0; i < draws / 2; i++)
                {
                    values.Add(sizes.NextLength(max));
                    long currentLength = sizes.Boundaries[i % sizes.Boundaries.Count] + (i % 3) - 1;
                    values.Add(sizes.NextOffset(Math.Max(0, currentLength), max));
                }

                return sizes;
            }

            BoundaryBiasedSizes generated = Run(out List<long> first);
            Run(out List<long> second);
            Assert.Equal(first, second); // deterministic per seed (replayable)
            Assert.Equal(draws, generated.Generated);
            Assert.All(first, v => Assert.InRange(v, 0, max));

            var required = new List<string> { "0", "1", "inline-1", "inline", "inline+1", "chunk-1", "chunk", "chunk+1", "2chunk-1", "2chunk", "2chunk+1", "eof-1", "eof", "eof+1" };
            if (buffer != chunk && buffer != inline)
                required.AddRange(["buffer-1", "buffer", "buffer+1"]);
            foreach (string cls in required)
            {
                int count = generated.Classes.GetValueOrDefault(cls);
                Assert.True(count >= draws / 400, $"boundary class '{cls}' drawn only {count} times in {draws} draws (inline={inline}, chunk={chunk}, buffer={buffer})");
            }

            int other = generated.Classes.GetValueOrDefault("other");
            Assert.True(other <= draws * 45 / 100, $"{other} of {draws} values are not boundary values; the generator is not biased");
            // A uniform generator over [0, max] would hit the ~17 boundary values with probability < 0.1%.
            Assert.True(draws - other >= draws / 2);
            TestContext.Current.TestOutputHelper?.WriteLine($"inline={inline} chunk={chunk} buffer={buffer}: " +
                string.Join(", ", generated.Classes.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => $"{c.Key}={c.Value}")));
        }

        var limits = new DfsVolumeLimits { MaxFileLength = 1L << 40, MaxPublishBytes = 1L << 30, MaxTransactionStagedBytes = 1L << 29 };
        var edges = BoundaryBiasedSizes.EdgeCases(limits).ToDictionary(e => e.Name, e => e.Value, StringComparer.Ordinal);
        Assert.Equal(-1, edges["negative"]);
        Assert.Equal(long.MaxValue, edges["long.MaxValue"]);
        Assert.Equal((long)int.MaxValue + 1, edges["int.MaxValue+1"]);
        foreach ((string name, long limit) in new[] { ("maxFileLength", limits.MaxFileLength), ("maxPublishBytes", limits.MaxPublishBytes), ("txStagedBytes", limits.MaxTransactionStagedBytes) })
        {
            Assert.Equal(limit - 1, edges[name + "-1"]);
            Assert.Equal(limit, edges[name]);
            Assert.Equal(limit + 1, edges[name + "+1"]);
        }

        // Extra boundaries (for example configured limits) are biased to as well, with their ±1 neighbours.
        var withLimits = new BoundaryBiasedSizes(new SeededRandom(1302), InlineThreshold, ChunkSize, BufferSize, [100_000]);
        Assert.Contains(99_999L, withLimits.Boundaries);
        Assert.Contains(100_000L, withLimits.Boundaries);
        Assert.Contains(100_001L, withLimits.Boundaries);
    }

    public static TheoryData<ulong> BoundaryClusterSeeds => new() { 1311, 1312 };

    /// <summary>
    /// PROP-013 against a cluster: boundary-biased lengths/offsets (including every boundary value) round-trip exactly;
    /// configured limits succeed at the limit and fail one past it with LimitExceeded and no partial effect; overflow
    /// edges (negative lengths, long.MaxValue positions) are rejected without wrapping or changing the file.
    /// </summary>
    [Theory, MemberData(nameof(BoundaryClusterSeeds)), Catalog("PROP-013", "S", "INV-02", "INV-07", "INV-08", "INV-16")]
    public Task PROP_013_GeneratorCoversBoundarySizes_LimitsAndOverflowEdges(ulong seed)
    {
        var limits = new DfsVolumeLimits { MaxFileLength = 4L * ChunkSize, MaxPublishBytes = 3L * ChunkSize, MaxTransactionStagedBytes = 2L * ChunkSize };
        TestClusterOptions options = Sim(seed) with { Volumes = [new TestVolumeSpec(Vol) { Limits = limits }] };
        return RunPropertyAsync("PROP-013", seed, trace => ClusterTest.RunAsync(options, async cluster =>
        {
            var random = new SeededRandom(Derive(seed, "prop-013"));
            var sizes = new BoundaryBiasedSizes(random.Fork("sizes"), InlineThreshold, ChunkSize, BufferSize,
                [limits.MaxPublishBytes, limits.MaxTransactionStagedBytes]) { BoundaryPercent = 85 };
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "edges" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Directories.CreateDirectoryAsync("/e", Ct);

            // 1. Whole-file publications at every boundary length up to the publish limit plus biased draws.
            var lengths = new SortedSet<long>(sizes.Boundaries.Where(b => b <= limits.MaxPublishBytes));
            for (int i = 0; i < 10; i++)
                lengths.Add(sizes.NextLength(limits.MaxPublishBytes));
            int index = 0;
            foreach (long length in lengths)
            {
                string path = $"/e/len-{length}";
                byte[] data = Content(seed, ++index, (int)length);
                trace.Step($"write {path}");
                await volume.Files.WriteAllBytesAsync(path, data, Ct);
                await BytesEqualAsync(data, volume.Files.ReadAllBytesAsync(path, Ct), $"round trip of {length} bytes");
                DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
                Assert.Equal(length, info.Length);
                Assert.Equal(length <= InlineThreshold, info.IsInline);
            }

            // 2. Partial writes at boundary-biased offsets into a sparse file, checked against the reference model.
            var model = new ReferenceFileModel(ReadOnlySpan<byte>.Empty);
            await using (DfsStream stream = await volume.Files.OpenAsync("/e/sparse", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct))
            {
                for (int i = 0; i < 16; i++)
                {
                    long offset = sizes.NextOffset(model.StagedLength, limits.MaxFileLength - 2);
                    int length = (int)Math.Max(1, sizes.NextLength(Math.Min(ChunkSize + 1L, limits.MaxFileLength - offset)));
                    byte[] data = Content(seed, 500 + i, length);
                    trace.Step($"sparse write {offset}+{length}");
                    stream.Seek(offset, SeekOrigin.Begin);
                    await stream.WriteAsync(data, Ct);
                    model.Write(offset, data);
                    await stream.FlushAsync(Ct); // one publication per write keeps every publication under the publish limit
                    model.Flush();
                    long probe = sizes.NextOffset(model.CommittedLength, model.CommittedLength + 1);
                    await using DfsStream reader = await volume.Files.OpenAsync("/e/sparse", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct);
                    byte[] probed = await ReadRangeAsync(reader, probe, 2);
                    AssertEx.BytesEqual(model.Read(probe, 2, staged: false), probed, $"committed bytes at {probe}");
                }
            }

            await BytesEqualAsync(model.Committed, volume.Files.ReadAllBytesAsync("/e/sparse", Ct), "sparse file after boundary-biased writes");

            // 3. Publish limit: exactly at the limit succeeds; one byte more fails with no partial effect.
            byte[] atLimit = Content(seed, 900, (int)limits.MaxPublishBytes);
            await volume.Files.WriteAllBytesAsync("/e/limit", atLimit, Ct);
            Exception tooBig = await ExpectFailureAsync(() => volume.Files.WriteAllBytesAsync("/e/limit", Content(seed, 901, (int)limits.MaxPublishBytes + 1), Ct),
                "publication one byte above MaxPublishBytes", DfsErrorCode.LimitExceeded);
            var limitException = Assert.IsType<DfsLimitExceededException>(tooBig);
            Assert.Equal(limits.MaxPublishBytes, limitException.Limit);
            Assert.Equal(limits.MaxPublishBytes + 1, limitException.Requested);
            await BytesEqualAsync(atLimit, volume.Files.ReadAllBytesAsync("/e/limit", Ct), "a rejected publication leaves the previous revision");

            // 4. File length limit: extending to exactly MaxFileLength succeeds; one past it is rejected; the length stays.
            await using (DfsStream stream = await volume.Files.OpenAsync("/e/limit", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct))
            {
                await stream.SetLengthAsync(limits.MaxFileLength, Ct);
                await stream.FlushAsync(Ct);
            }

            Assert.Equal(limits.MaxFileLength, (await volume.Files.GetInfoAsync("/e/limit", Ct)).Length);
            await ExpectFailureAsync(() => WithWriterAsync("/e/limit", async stream =>
            {
                await stream.SetLengthAsync(limits.MaxFileLength + 1, Ct);
                await stream.FlushAsync(Ct);
            }), "SetLength one byte beyond MaxFileLength", DfsErrorCode.LimitExceeded);
            await ExpectFailureAsync(() => WithWriterAsync("/e/limit", async stream =>
            {
                stream.Seek(limits.MaxFileLength, SeekOrigin.Begin);
                await stream.WriteAsync(new byte[] { 0x5A }, Ct);
                await stream.FlushAsync(Ct);
            }), "write ending one byte beyond MaxFileLength", DfsErrorCode.LimitExceeded);
            Assert.Equal(limits.MaxFileLength, (await volume.Files.GetInfoAsync("/e/limit", Ct)).Length);

            // 5. Overflow edges: never wrap, never change the file.
            byte[] beforeOverflow = await volume.Files.ReadAllBytesAsync("/e/len-1", Ct);
            await WithWriterAsync("/e/len-1", async stream =>
            {
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => stream.SetLengthAsync(-1, Ct));
                Assert.ThrowsAny<IOException>(() => stream.Seek(-1, SeekOrigin.Begin));
                Assert.Equal(0, stream.Position);
                stream.Seek(long.MaxValue, SeekOrigin.Begin);
                Assert.Equal(long.MaxValue, stream.Position); // seeking past EOF is legal and never extends
                await ExpectFailureAsync(async () =>
                {
                    await stream.WriteAsync(new byte[] { 1 }, Ct);
                    await stream.FlushAsync(Ct);
                }, "a one-byte write at position long.MaxValue", DfsErrorCode.LimitExceeded, DfsErrorCode.InvalidArgument);
            });
            await BytesEqualAsync(beforeOverflow, volume.Files.ReadAllBytesAsync("/e/len-1", Ct), "overflowing writes change nothing");

            // 6. Transaction staged-bytes limit: exactly at the limit commits; one byte more aborts with no partial effect.
            byte[] txAtLimit = Content(seed, 950, (int)limits.MaxTransactionStagedBytes);
            await using (DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/e/tx", txAtLimit, Ct);
                await tx.CommitAsync(Ct);
            }

            await ExpectFailureAsync(async () =>
            {
                DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
                try
                {
                    await tx.Files.WriteAllBytesAsync("/e/tx-other", new byte[] { 7 }, Ct);
                    await tx.Files.WriteAllBytesAsync("/e/tx", Content(seed, 951, (int)limits.MaxTransactionStagedBytes + 1), Ct);
                    await tx.CommitAsync(Ct);
                }
                finally
                {
                    await SafeDisposeAsync(tx);
                }
            }, "transaction staging one byte above MaxTransactionStagedBytes", DfsErrorCode.LimitExceeded);
            await BytesEqualAsync(txAtLimit, volume.Files.ReadAllBytesAsync("/e/tx", Ct), "the over-limit transaction left no partial effect");
            Assert.False(await volume.Files.ExistsAsync("/e/tx-other", Ct));

            Log($"PROP-013 seed {seed}: {string.Join(", ", sizes.Classes.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => $"{c.Key}={c.Value}"))}");
            foreach (long boundary in TestData.BoundaryLengths(InlineThreshold, ChunkSize, BufferSize).Where(b => b <= limits.MaxPublishBytes)
                .Concat([limits.MaxPublishBytes - 1, limits.MaxPublishBytes, limits.MaxTransactionStagedBytes - 1, limits.MaxTransactionStagedBytes, limits.MaxTransactionStagedBytes + 1]))
                Assert.True(lengths.Contains(boundary), $"boundary length {boundary} was never exercised against the cluster");

            async Task WithWriterAsync(string path, Func<DfsStream, Task> body)
            {
                DfsStream stream = await volume.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                try
                {
                    await body(stream);
                }
                finally
                {
                    await SafeDisposeAsync(stream); // a disposal failure must not replace the asserted error
                }
            }
        }));
    }

    // =====================================================================================================
    // PROP-014 LongHistoryCompactionAndEpochs
    // =====================================================================================================

    public static TheoryData<ulong> LongHistorySeeds => new() { 1401, 1402 };

    [Theory, MemberData(nameof(LongHistorySeeds)), Catalog("PROP-014", "S", "INV-06", "INV-07", "INV-09", "INV-13", "INV-18")]
    public Task PROP_014_LongHistoryCompactionAndEpochs(ulong seed) =>
        RunPropertyAsync("PROP-014", seed, trace => ClusterTest.RunAsync(Sim(seed) with { ScenarioTimeout = TimeSpan.FromMinutes(20) }, async cluster =>
        {
            NodeTimings timings = cluster.Options.Timings;
            int total = Math.Max(timings.RaftSnapshotThreshold, timings.MaxOutcomesPerSession) + 64;
            var random = new SeededRandom(Derive(seed, "prop-014"));
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "long" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            DfsVolumeInfo volumeInfo = await volume.GetInfoAsync(Ct);
            var model = new TreeModel();
            await volume.Directories.CreateDirectoryAsync("/h", Ct);
            model.MkDirs("/h");

            // Old history that must survive compaction, retirement, key rotation, repair and restoration.
            byte[] oldLarge = Content(seed, 1, 2 * ChunkSize + 1);
            await volume.Files.WriteAllBytesAsync("/h/old-large.bin", oldLarge, Ct);
            model.Write("/h/old-large.bin", oldLarge);
            for (int i = 0; i < 3; i++)
            {
                byte[] small = Content(seed, 10 + i, 1 + i * 100);
                await volume.Files.WriteAllBytesAsync($"/h/old{i}", small, Ct);
                model.Write($"/h/old{i}", small);
            }

            DfsSnapshotInfo early = await volume.Snapshots.CreateAsync("early", null, Ct);
            TreeModel earlyModel = model.Clone();
            uint oldKeyVersion = (await KeyVersionsAsync(cluster, "/h/old-large.bin")).Max();

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode lagging = cluster.Nodes.First(n => n.Name != leader.Name);
            trace.Step($"take follower {lagging.Name} offline before the long history");
            await lagging.CrashAsync();

            var appended = new List<(DfsOperationId Id, string Path, byte[] Data, CommitToken Token)>();
            CommitToken lastToken = CommitToken.None;
            for (int i = 0; i < total; i++)
            {
                string path = $"/h/log{random.Next(6)}";
                byte[] data = Content(seed, 100 + i, 1 + random.Next(48));
                DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                CommitToken token = await volume.Files.AppendAllBytesAsync(path, data, new DfsMutationOptions { OperationId = id }, Ct);
                Assert.True(lastToken.IsNone || token.CompareTo(lastToken) > 0, $"append {i}: token {token} does not follow {lastToken}");
                lastToken = token;
                model.Append(path, data);
                appended.Add((id, path, data, token));
                if (i == total / 3)
                {
                    trace.Step($"rotate the volume data key after {i + 1} appends");
                    await cluster.Admin.RotateDataKeyAsync(Vol, Ct);
                    byte[] rotated = Content(seed, 2, 2 * ChunkSize + 1);
                    await volume.Files.WriteAllBytesAsync("/h/new-key.bin", rotated, Ct);
                    model.Write("/h/new-key.bin", rotated);
                }

                if (i % 50 == 49)
                    trace.Step($"{i + 1} appends committed");
            }

            // Log compaction happened on every running replica.
            foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            {
                GroupInspection group = await node.Inspect.GetGroupAsync(Vol) ?? throw new Xunit.Sdk.XunitException($"{node.Name} hosts no group {Vol}");
                Assert.True(group.SnapshotIndex > 0, $"{node.Name}: no log compaction after {total} entries (threshold {timings.RaftSnapshotThreshold})");
            }

            // ID retirement: the oldest id is retired and rejected as expired (never replayed); a recent id is answered from its outcome.
            (DfsOperationId firstId, string firstPath, byte[] firstData, _) = appended[0];
            DfsOperationOutcome firstOutcome = await volume.QueryOperationAsync(firstId, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Expired, firstOutcome.Status);
            byte[] beforeReplay = await volume.Files.ReadAllBytesAsync(firstPath, Ct);
            Exception expired = await ExpectFailureAsync(() => volume.Files.AppendAllBytesAsync(firstPath, firstData, new DfsMutationOptions { OperationId = firstId }, Ct),
                "retry of a retired operation id", DfsErrorCode.OperationExpired);
            Assert.IsType<DfsOperationExpiredException>(expired);
            await BytesEqualAsync(beforeReplay, volume.Files.ReadAllBytesAsync(firstPath, Ct), "a retired id must not be replayed as a new append");
            var lastAppend = appended[^1];
            Assert.Equal(lastAppend.Token, await volume.Files.AppendAllBytesAsync(lastAppend.Path, lastAppend.Data, new DfsMutationOptions { OperationId = lastAppend.Id }, Ct));
            VolumeStateInspection leaderState = await cluster.Inspect.GetVolumeStateAsync(Vol);
            SessionInspection session = leaderState.Sessions.Single(s => s.Id == firstId.Session);
            Assert.True(session.RetiredWatermark >= firstId.Sequence, $"session {session.Id}: retired watermark {session.RetiredWatermark} below retired id {firstId}");
            Assert.True(session.RetainedOutcomes <= timings.MaxOutcomesPerSession, $"session {session.Id} retains {session.RetainedOutcomes} outcomes (bound {timings.MaxOutcomesPerSession})");

            // The lagging follower returns after compaction: it installs a checkpoint preserving outcomes, retirement and snapshots.
            trace.Step($"restart {lagging.Name} after compaction");
            await lagging.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);
            GroupInspection lagGroup = await lagging.Inspect.GetGroupAsync(Vol) ?? throw new Xunit.Sdk.XunitException($"{lagging.Name} hosts no group {Vol}");
            Assert.True(lagGroup.SnapshotIndex > 0, $"{lagging.Name} caught up without installing a checkpoint although its history was compacted");
            // Local compaction after a full log replay would also give SnapshotIndex > 0; the leader's log no longer holds
            // the lagging replica's missing entries, so it must have received and installed a checkpoint (F16 site reached).
            Assert.True(cluster.Faults.Coverage.WasReached(FaultSites.RaftSnapshotInstall, lagging.Name),
                $"{lagging.Name} never reached {FaultSites.RaftSnapshotInstall}: it did not install a leader checkpoint after compaction");
            VolumeStateInspection lagState = await cluster.Inspect.GetVolumeStateAsync(Vol, lagging.Name);
            Assert.Contains(lagState.Snapshots, s => s.Name == "early");
            Assert.Equal(session.RetiredWatermark, lagState.Sessions.Single(s => s.Id == session.Id).RetiredWatermark);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

            // Repair: silently lose a replica of an old object; anti-entropy restores the configured copies from a verified holder.
            FileInspection oldFile = await cluster.Inspect.GetFileAsync(Vol, "/h/old-large.bin") ?? throw new Xunit.Sdk.XunitException("old-large.bin not inspectable");
            ObjectId victim = oldFile.Extents[0].Object;
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(victim);
            TestNode loser = cluster.Node(holders[0]);
            trace.Step($"silently delete {victim} from {loser.Name}");
            await loser.Disk.DeleteObjectAsync(victim);
            await cluster.Admin.StartRepairAsync(Vol, Ct);
            DfsStoragePolicy policy = cluster.Options.GetEffectiveDefaultPolicy();
            await cluster.WaitUntilAsync(async () => (await cluster.Inspect.CountDurableDomainsAsync(victim)).Copies >= policy.TargetCopies,
                TimeSpan.FromSeconds(60), "repair restores the target copy count of the lost replica", Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Vol);
            Assert.Equal(0, health.MissingObjects);
            Assert.Equal(0, health.UnderReplicatedObjects);

            // Key rotation: objects written after the rotation use a newer key version; old and new data stay readable.
            uint newKeyVersion = (await KeyVersionsAsync(cluster, "/h/new-key.bin")).Max();
            Assert.True(newKeyVersion > oldKeyVersion, $"objects written after RotateDataKey use key version {newKeyVersion}, not newer than {oldKeyVersion}");
            AssertImage(VolumeImage.FromModel(model), await CaptureAsync(volume), compareInodes: false, "live state after the long history");

            // Old-history retention: the early snapshot is intact after compaction, retirement, rotation and repair.
            await using (DfsSnapshotView view = await volume.Snapshots.OpenAsync("early", Ct))
            {
                AssertImage(VolumeImage.FromModel(earlyModel), await VolumeImage.CaptureAsync(view.Files, view.Directories), compareInodes: false,
                    "early snapshot after the long history");
            }

            // Restoration creates a new volume identity/epoch: its tokens, snapshots and ids never mix with the source's.
            DfsVolumeInfo restoredInfo = await volume.Snapshots.RestoreAsync("early", "restored", Ct);
            Assert.Equal("restored", restoredInfo.Name);
            Assert.NotEqual(volumeInfo.Id, restoredInfo.Id);
            DfsVolume restored = client.GetVolume("restored");
            AssertImage(VolumeImage.FromModel(earlyModel), await CaptureAsync(restored), compareInodes: false, "restored volume equals the early snapshot");
            CommitToken restoredToken = await restored.Files.WriteAllBytesAsync("/h/after-restore", Content(seed, 3, 10), Ct);
            Assert.Equal(restoredInfo.Id, restoredToken.VolumeId);
            Assert.False(restoredToken.IsComparableWith(lastToken));
            Assert.False(restoredToken.IsComparableWith(early.Boundary));
            Assert.Throws<ArgumentException>(() => { _ = restoredToken.CompareTo(lastToken); });
            Assert.Throws<ArgumentException>(() => { _ = restoredToken.IsAtOrAfter(early.Boundary); });
            await ExpectFailureAsync(() => restored.Snapshots.CreateAsync("cross-epoch", new DfsSnapshotOptions { MinimumCommit = lastToken }, Ct),
                "MinimumCommit from another volume/epoch", DfsErrorCode.EpochMismatch, DfsErrorCode.CrossVolume, DfsErrorCode.InvalidArgument);
            byte[]? restoredLog = await TryReadAllBytesAsync(restored.Files, lastAppend.Path);
            await ExpectFailureAsync(() => restored.Files.AppendAllBytesAsync(lastAppend.Path, lastAppend.Data, new DfsMutationOptions { OperationId = lastAppend.Id }, Ct),
                "an operation id of the source volume reused on the restored volume",
                DfsErrorCode.OperationIdMismatch, DfsErrorCode.OperationExpired, DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidArgument);
            byte[]? afterAttempt = await TryReadAllBytesAsync(restored.Files, lastAppend.Path);
            Assert.True(restoredLog is null ? afterAttempt is null : afterAttempt is not null && restoredLog.AsSpan().SequenceEqual(afterAttempt),
                "a foreign operation id changed the restored volume");
            AssertImage(VolumeImage.FromModel(model), await CaptureAsync(volume), compareInodes: false, "the source volume is unaffected by the restore");
        }));

    private static async Task<IReadOnlyList<uint>> KeyVersionsAsync(TestCluster cluster, string path)
    {
        FileInspection file = await cluster.Inspect.GetFileAsync(Vol, path) ?? throw new Xunit.Sdk.XunitException($"{path} not inspectable");
        var ids = file.Extents.Select(e => e.Object).ToHashSet();
        var versions = new List<uint>();
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
            {
                if (ids.Contains(record.Id))
                    versions.Add(record.KeyVersion);
            }
        }

        Assert.NotEmpty(versions);
        return versions;
    }

    // =====================================================================================================
    // PROP-015 FailedHistoriesAreShrunkAndRetained
    // =====================================================================================================

    /// <summary>The shrinker preserves the exact violation signature and returns a 1-minimal history (pure oracle).</summary>
    [Fact, Catalog("PROP-015", "S", "INV-20")]
    public async Task PROP_015_FailedHistoriesAreShrunkAndRetained_ShrinkerIsMinimalAndSignaturePreserving()
    {
        // Violation "A" iff 3 occurs before a 7; violation "B" iff a 5 occurs (a different failure that must not be accepted).
        static Task<string?> Oracle(IReadOnlyList<int> history)
        {
            int three = history.ToList().IndexOf(3);
            int seven = history.ToList().LastIndexOf(7);
            if (three >= 0 && seven > three)
                return Task.FromResult<string?>("A");
            return Task.FromResult<string?>(history.Contains(5) ? "B" : null);
        }

        int[] failing = [1, 5, 3, 9, 2, 5, 8, 7, 4, 6];
        (IReadOnlyList<int> minimal, int evaluations) = await Shrinker.ShrinkAsync(failing, Oracle);
        Assert.Equal(new[] { 3, 7 }, minimal);
        Assert.Equal("A", await Oracle(minimal));
        for (int i = 0; i < minimal.Count; i++)
            Assert.NotEqual("A", await Oracle(minimal.Where((_, k) => k != i).ToArray())); // 1-minimal
        Assert.InRange(evaluations, 2, 400);
        await Assert.ThrowsAsync<ArgumentException>(() => Shrinker.ShrinkAsync<int>([1, 2], Oracle)); // nothing to shrink

        // Retained schedules round-trip through their on-disk format; the checked-in corpus is well formed.
        var schedule = new RegressionSchedule("synthetic", "content-mismatch:f0", [new("append", 0, 1), new("retry", 0, 0)]);
        RegressionSchedule parsed = RegressionSchedule.Parse(schedule.Serialize());
        Assert.Equal(schedule.Name, parsed.Name);
        Assert.Equal(schedule.Violation, parsed.Violation);
        Assert.Equal(schedule.Ops, parsed.Ops);
        Assert.NotEmpty(RegressionCorpus.Retained);
        Assert.Equal(RegressionCorpus.Retained.Count, RegressionCorpus.Retained.Select(r => r.Name).Distinct(StringComparer.Ordinal).Count());
        foreach (RegressionSchedule retained in RegressionCorpus.Retained)
        {
            Assert.NotEmpty(retained.Ops);
            Assert.Equal(retained.Ops, RegressionSchedule.Parse(retained.Serialize()).Ops);
        }
    }

    public static TheoryData<ulong> ShrinkSeeds => new() { 1501, 1502 };

    /// <summary>
    /// A generated history that fails against a negative control (operation dedup removed) is shrunk while preserving its
    /// exact oracle violation, retained on disk as a named regression schedule, still reproduces on the mutated system and
    /// passes on the real system.
    /// </summary>
    [Theory, MemberData(nameof(ShrinkSeeds)), Catalog("PROP-015", "S", "INV-06", "INV-20")]
    public Task PROP_015_FailedHistoriesAreShrunkAndRetained(ulong seed) =>
        RunPropertyAsync("PROP-015", seed, async trace =>
        {
            TestClusterOptions mutated = Sim(seed) with { Mutations = new SafetyMutations { DisableOperationDedup = true } };
            RegressionSchedule found = await ClusterTest.RunAsync(mutated, async cluster =>
            {
                await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "shrink" }, Ct);
                DfsVolume volume = client.GetVolume(Vol);
                int evaluation = 0;
                Task<string?> Evaluate(IReadOnlyList<ScheduleOp> ops) =>
                    ScheduleRunner.RunAsync(volume, "/run" + (++evaluation).ToString(CultureInfo.InvariantCulture), ops, seed);

                var random = new SeededRandom(Derive(seed, "prop-015"));
                List<ScheduleOp>? failing = null;
                string? violation = null;
                for (int attempt = 0; attempt < 8 && failing is null; attempt++)
                {
                    List<ScheduleOp> candidate = Enumerable.Range(0, 12)
                        .Select(_ => new ScheduleOp(ScheduleOp.Kinds[random.Next(ScheduleOp.Kinds.Length)], random.Next(3), 1 + random.Next(9)))
                        .ToList();
                    trace.Step($"attempt {attempt}: {string.Join("; ", candidate)}");
                    violation = await Evaluate(candidate);
                    if (violation is not null)
                        failing = candidate;
                }

                Assert.True(failing is not null, "the negative control (DisableOperationDedup) produced no failing history in 8 generated schedules");
                (IReadOnlyList<ScheduleOp> minimal, int evaluations) = await Shrinker.ShrinkAsync(failing!, Evaluate, maxEvaluations: 150);
                trace.Step($"shrunk {failing!.Count} -> {minimal.Count} steps in {evaluations} evaluations: {string.Join("; ", minimal)} ({violation})");
                Assert.Equal(violation, await Evaluate(minimal)); // the oracle violation is preserved exactly
                Assert.True(minimal.Count <= 3, $"the shrunk history still has {minimal.Count} steps: {string.Join("; ", minimal)}");
                Assert.Contains(minimal, o => o.Kind == "append");
                Assert.Contains(minimal, o => o.Kind == "retry");
                for (int i = 0; i < minimal.Count && minimal.Count > 1; i++)
                {
                    List<ScheduleOp> without = minimal.Where((_, k) => k != i).ToList();
                    Assert.NotEqual(violation, await Evaluate(without)); // 1-minimal: every remaining step is needed
                }

                return new RegressionSchedule($"prop015-seed{seed.ToString(CultureInfo.InvariantCulture)}", violation!, minimal);
            });

            // Retain the minimised failure as a named regression schedule.
            Directory.CreateDirectory(RegressionCorpus.Directory);
            string file = Path.Combine(RegressionCorpus.Directory, found.Name + ".schedule");
            await File.WriteAllTextAsync(file, found.Serialize(), Ct);
            RegressionSchedule reloaded = RegressionSchedule.Parse(await File.ReadAllTextAsync(file, Ct));
            Assert.Equal(found.Name, reloaded.Name);
            Assert.Equal(found.Violation, reloaded.Violation);
            Assert.Equal(found.Ops, reloaded.Ops);
            Log($"PROP-015 seed {seed}: retained {file}{Environment.NewLine}{reloaded.Serialize()}");

            await ReplayRegressionAsync(reloaded, seed);
        });

    public static TheoryData<string> RetainedRegressionNames
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (RegressionSchedule schedule in RegressionCorpus.Retained)
                data.Add(schedule.Name);
            return data;
        }
    }

    /// <summary>Every checked-in regression schedule passes on the real system and still detects the bug it was minimised from.</summary>
    [Theory, MemberData(nameof(RetainedRegressionNames)), Catalog("PROP-015", "S", "INV-06", "INV-20")]
    public Task PROP_015_FailedHistoriesAreShrunkAndRetained_RetainedRegression(string name)
    {
        RegressionSchedule schedule = RegressionCorpus.Retained.Single(r => r.Name == name);
        ulong seed = Derive(1500, name);
        return RunPropertyAsync("PROP-015", seed, _ => ReplayRegressionAsync(schedule, seed));
    }

    private static async Task ReplayRegressionAsync(RegressionSchedule schedule, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "regression" }, Ct);
            string? violation = await ScheduleRunner.RunAsync(client.GetVolume(Vol), "/regression", schedule.Ops, seed);
            Assert.True(violation is null, $"regression '{schedule.Name}' fails on the real system: {violation}");
        });
        await ClusterTest.RunAsync(Sim(seed) with { Mutations = new SafetyMutations { DisableOperationDedup = true } }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "regression" }, Ct);
            string? violation = await ScheduleRunner.RunAsync(client.GetVolume(Vol), "/regression", schedule.Ops, seed);
            Assert.True(schedule.Violation == violation, $"regression '{schedule.Name}' no longer detects its bug: expected {schedule.Violation}, got {violation ?? "no violation"}");
        });
    }

    // =====================================================================================================
    // PROP-016 CheckerDoesNotConfuseLiveAndPinned
    // =====================================================================================================

    /// <summary>The view-labelling checker on hand-built histories (pure): historical values only for pinned/snapshot/backup views.</summary>
    [Fact, Catalog("PROP-016", "S", "INV-03", "INV-20")]
    public void PROP_016_CheckerDoesNotConfuseLiveAndPinned_SyntheticHistories()
    {
        byte[] v1 = [1];
        byte[] v2 = [2];

        // w(v1); pin P; w(v2); read via P = v1 (historical but pinned); live read = v2.
        HistoryRecorder Build(string pinnedReadView, byte[] pinnedValue, bool pinAfterSecondWrite = false, OpOutcome secondWrite = OpOutcome.Ok, byte[]? liveValue = null)
        {
            var h = new HistoryRecorder();
            Done(h, h.Invoke("w", "write", "k", v1));
            if (!pinAfterSecondWrite)
                Done(h, h.Invoke("pins", "pin-open", "k", "pin:P"));
            long second = h.Invoke("w", "write", "k", v2);
            h.Complete(second, secondWrite);
            if (pinAfterSecondWrite)
                Done(h, h.Invoke("pins", "pin-open", "k", "pin:P"));
            Done(h, h.Invoke("pins", "read", "k", pinnedReadView), pinnedValue);
            Done(h, h.Invoke("live", "read", "k", ViewChecker.Live), liveValue ?? v2);
            return h;
        }

        static void Done(HistoryRecorder h, long id, object? output = null) => h.Complete(id, OpOutcome.Ok, output);
        static void Valid(CheckResult r, string because) => Assert.True(r.IsValid && !r.IsInconclusive, $"{because}: {r.Explanation}");
        static void Invalid(CheckResult r, string because) => Assert.True(!r.IsValid && !r.IsInconclusive, $"{because} must be rejected: {r.Explanation}");

        Valid(ViewChecker.Check(Build("pin:P", v1).Operations), "a historical value through a version pin");
        Invalid(ViewChecker.Check(Build(ViewChecker.Live, v1).Operations), "the same historical value labelled as a new live read");
        Invalid(ViewChecker.Check(Build("pin:P", v1, pinAfterSecondWrite: true).Operations), "a pin established after the overwrite returning the old value");
        Valid(ViewChecker.Check(Build("pin:P", v2, pinAfterSecondWrite: true).Operations), "a pin established after the overwrite returning the new value");
        Valid(ViewChecker.Check(Build(ViewChecker.Live, v1, secondWrite: OpOutcome.Unknown, liveValue: v1).Operations),
            "an old live value while the overwrite's outcome is unknown");
        Invalid(ViewChecker.Check(Build("pin:Q", v1).Operations), "a read through a pin that was never established");

        // Two reads of the same pin must agree.
        HistoryRecorder twoReads = Build("pin:P", v1);
        Done(twoReads, twoReads.Invoke("pins", "read", "k", "pin:P"), v2);
        Invalid(ViewChecker.Check(twoReads.Operations), "one pin returning two different versions");

        // Snapshot and backup copies: historical values at the snapshot boundary only.
        HistoryRecorder Snapshot(string view, byte[] value)
        {
            var h = new HistoryRecorder();
            Done(h, h.Invoke("w", "write", "k", v1));
            Done(h, h.Invoke("snap", "snapshot-create", "k", "snapshot:S/k"));
            Done(h, h.Invoke("w", "write", "k", v2));
            Done(h, h.Invoke("r", "read", "k", view), value);
            return h;
        }

        Valid(ViewChecker.Check(Snapshot("snapshot:S/k", v1).Operations), "a snapshot read of the boundary value");
        Valid(ViewChecker.Check(Snapshot("backup:S/k", v1).Operations), "a backup copy of the snapshot");
        Invalid(ViewChecker.Check(Snapshot("backup:S/k", v2).Operations), "a backup copy returning a value from after its snapshot boundary");
        Invalid(ViewChecker.Check(Snapshot(ViewChecker.Live, v1).Operations), "a live read returning the snapshot-era value");

        // Every read must declare its view.
        HistoryRecorder undeclared = Snapshot("snapshot:S/k", v1);
        Done(undeclared, undeclared.Invoke("r", "read", "k", null), v2);
        Invalid(ViewChecker.Check(undeclared.Operations), "a read without a declared view");
        HistoryRecorder unknownView = Snapshot("snapshot:S/k", v1);
        Done(unknownView, unknownView.Invoke("r", "read", "k", "cache:x"), v1);
        Invalid(ViewChecker.Check(unknownView.Operations), "a read with an unknown view kind");

        // A pin covers the key it was established for; another key's historical value cannot borrow it.
        HistoryRecorder otherKey = Snapshot("snapshot:S/k", v1);
        Done(otherKey, otherKey.Invoke("w", "write", "j", v1));
        Done(otherKey, otherKey.Invoke("w", "write", "j", v2));
        Done(otherKey, otherKey.Invoke("r", "read", "j", "snapshot:S/k"), v1);
        Invalid(ViewChecker.Check(otherKey.Operations), "a read of key j through the snapshot view of key k");
    }

    public static TheoryData<ulong, bool> LivePinnedSeeds => new() { { 1601, false }, { 1602, true } };

    /// <summary>
    /// Records a real history with live reads, version-pinned reads, snapshot reads and (optionally) backup reads, each
    /// labelled with its declared view. The labelled history is valid; relabelling the deliberate historical reads as live
    /// makes it invalid, so the checker distinguishes the two.
    /// </summary>
    [Theory, MemberData(nameof(LivePinnedSeeds)), Catalog("PROP-016", "S", "INV-03", "INV-10", "INV-20")]
    public Task PROP_016_CheckerDoesNotConfuseLiveAndPinned(ulong seed, bool withBackup) =>
        RunPropertyAsync("PROP-016", seed, trace => ClusterTest.RunAsync(Sim(seed) with { BackupNodeCount = withBackup ? 1 : 0 }, async cluster =>
        {
            string[] keys = ["/v/k0", "/v/k1"];
            HistoryRecorder history = cluster.History;
            var random = new SeededRandom(Derive(seed, "prop-016"));
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await wv.Directories.CreateDirectoryAsync("/v", Ct);
            DfsClient? backupClient = null;
            string? target = null;
            if (withBackup)
            {
                TestBackupNode backup = cluster.BackupNodes[0];
                await backup.RegisterAsync(Vol);
                target = backup.TargetName;
                backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk", Credentials = backup.ReaderCredentials }, Ct);
            }

            var pins = new List<(string Label, string Key, DfsStream Stream)>();
            var snapshots = new List<string>();
            int dataId = 0;
            int historicalPinned = 0;
            int historicalSnapshot = 0;
            try
            {
                // Scripted prefix guaranteeing a deliberate historical pinned read: w(v1); pin; w(v2); read pin = v1; live = v2.
                await WriteAsync("/v/k0");
                await OpenPinAsync("/v/k0");
                await WriteAsync("/v/k0");
                await ReadPinAsync(pins[0]);
                await LiveReadAsync("/v/k0");

                for (int step = 0; step < 40; step++)
                {
                    string key = keys[random.Next(keys.Length)];
                    int roll = random.Next(100);
                    if (roll < 35)
                    {
                        await WriteAsync(key);
                    }
                    else if (roll < 55)
                    {
                        await LiveReadAsync(key);
                    }
                    else if (roll < 65)
                    {
                        if (await rv.Files.ExistsAsync(key, Ct))
                            await OpenPinAsync(key);
                    }
                    else if (roll < 82)
                    {
                        if (pins.Count > 0)
                            await ReadPinAsync(pins[random.Next(pins.Count)]);
                    }
                    else if (roll < 91)
                    {
                        string name = $"S{snapshots.Count + 1}";
                        trace.Step($"snapshot {name}");
                        long[] ids = keys.Select(k => history.Invoke("snapshotter:" + k, "snapshot-create", k, $"snapshot:{name}/{k}")).ToArray();
                        await wv.Snapshots.CreateAsync(name, null, Ct);
                        foreach (long id in ids)
                            history.Complete(id, OpOutcome.Ok);
                        snapshots.Add(name);
                        if (backupClient is not null)
                        {
                            await wv.Backups.RequestSyncAsync(target!, name, Ct);
                            DfsBackupCheckpoint checkpoint = await wv.Backups.AwaitBackupSnapshotAsync(target!, name, TimeSpan.FromSeconds(60), Ct);
                            Assert.Equal(name, checkpoint.SnapshotName);
                        }
                    }
                    else if (snapshots.Count > 0)
                    {
                        string name = snapshots[random.Next(snapshots.Count)];
                        bool fromBackup = backupClient is not null && random.Next(2) == 0;
                        DfsVolume source = fromBackup ? backupClient!.GetVolume(Vol) : rv;
                        trace.Step($"read snapshot {name} from the {(fromBackup ? "backup" : "source")}");
                        await using DfsSnapshotView view = await source.Snapshots.OpenAsync(name, Ct);
                        foreach (string k in keys)
                        {
                            byte[]? value = await history.RecordAsync<byte[]?>("snapshot-reader", "read", k, $"{(fromBackup ? "backup" : "snapshot")}:{name}/{k}",
                                () => TryReadAllBytesAsync(view.Files, k));
                            byte[]? current = await TryReadAllBytesAsync(wv.Files, k);
                            if (!SameBytes(value, current))
                                historicalSnapshot++;
                        }
                    }
                }

                IReadOnlyList<HistoryOp> ops = history.Operations;
                CheckResult labelled = ViewChecker.Check(ops);
                Log($"PROP-016 seed {seed}: {ops.Count} ops, {historicalPinned} historical pinned reads, {historicalSnapshot} historical snapshot reads; {labelled}");
                labelled.AssertValid();
                Assert.True(historicalPinned > 0, "no pinned read returned a historical value; the live/pinned distinction was not exercised");

                // Negative control: the same observations claimed as new live reads must be rejected.
                HistoryOp[] relabelled = ops.Where(o => o.Kind is not ("pin-open" or "snapshot-create"))
                    .Select(o => o.Kind == "read" ? o with { Input = ViewChecker.Live } : o)
                    .ToArray();
                CheckResult confused = ViewChecker.Check(relabelled);
                Assert.False(confused.IsInconclusive, confused.Explanation);
                Assert.False(confused.IsValid, "historical pinned/snapshot results relabelled as live reads were accepted by the checker");
            }
            finally
            {
                foreach ((_, _, DfsStream stream) in pins)
                    await SafeDisposeAsync(stream);
                await SafeDisposeAsync(backupClient);
            }

            async Task WriteAsync(string key)
            {
                byte[] data = Content(seed, ++dataId, 1 + random.Next(2 * InlineThreshold));
                trace.Step($"write {key} d{dataId}");
                await history.RecordAsync("writer", "write", key, data, () => wv.Files.WriteAllBytesAsync(key, data, Ct));
            }

            async Task LiveReadAsync(string key) =>
                await history.RecordAsync<byte[]?>("reader", "read", key, ViewChecker.Live, () => TryReadAllBytesAsync(rv.Files, key));

            async Task OpenPinAsync(string key)
            {
                string label = "pin:P" + (pins.Count + 1).ToString(CultureInfo.InvariantCulture);
                trace.Step($"open version-pinned reader {label} on {key}");
                long id = history.Invoke("pins", "pin-open", key, label);
                DfsStream stream = await rv.Files.OpenAsync(key, new DfsOpenOptions
                {
                    Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned,
                }, Ct);
                history.Complete(id, OpOutcome.Ok);
                Assert.Equal(DfsReadView.VersionPinned, stream.ReadView);
                pins.Add((label, key, stream));
            }

            async Task ReadPinAsync((string Label, string Key, DfsStream Stream) pin)
            {
                long id = history.Invoke("pins", "read", pin.Key, pin.Label);
                long length = await pin.Stream.GetLengthAsync(Ct);
                byte[] bytes = await ReadRangeAsync(pin.Stream, 0, (int)length + 1);
                history.Complete(id, OpOutcome.Ok, bytes);
                Assert.Equal(length, bytes.LongLength);
                if (!SameBytes(bytes, await TryReadAllBytesAsync(wv.Files, pin.Key)))
                    historicalPinned++;
            }

            static bool SameBytes(byte[]? a, byte[]? b) => a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);
        }));

    // ---- end of PROP tests ----
}
