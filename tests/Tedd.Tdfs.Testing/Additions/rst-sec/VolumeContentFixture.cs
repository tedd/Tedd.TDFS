using System.Text;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// One file of an <see cref="ExpectedVolumeContent"/>: the test's own independently computed expectation (never read
/// back from the system under test).
/// </summary>
/// <param name="Path">Normalized volume path.</param>
/// <param name="Content">Exact expected bytes (holes are zero bytes).</param>
/// <param name="PolicyName">Named storage policy explicitly assigned by the test, or null for the volume default.</param>
/// <param name="Access">Expected inode access flags.</param>
/// <param name="Holes">Ranges that were never written (sparse holes); empty for dense files.</param>
public sealed record ExpectedFile(string Path, byte[] Content, string? PolicyName, DfsInodeAccess Access, IReadOnlyList<(long Offset, long Length)> Holes)
{
    /// <summary>True when the file was created with at least one hole.</summary>
    public bool IsSparse => Holes.Count > 0;
}

/// <summary>Independent manifest of a volume's namespace and bytes (RST-002 oracle).</summary>
public sealed class ExpectedVolumeContent
{
    private readonly SortedDictionary<string, ExpectedFile> _files = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _directories = new(StringComparer.Ordinal);

    /// <summary>Files by path.</summary>
    public IReadOnlyDictionary<string, ExpectedFile> Files => _files;

    /// <summary>Directories (excluding the root).</summary>
    public IReadOnlyCollection<string> Directories => _directories;

    /// <summary>Adds or replaces a file (and records its ancestor directories).</summary>
    public void SetFile(ExpectedFile file)
    {
        ArgumentNullException.ThrowIfNull(file);
        _files[file.Path] = file;
        AddDirectory(DfsPath.GetParent(file.Path));
    }

    /// <summary>Removes a file.</summary>
    public bool RemoveFile(string path) => _files.Remove(path);

    /// <summary>Adds a directory and its ancestors (the root is implicit).</summary>
    public void AddDirectory(string path)
    {
        string current = path;
        while (!string.Equals(current, DfsPath.Root, StringComparison.Ordinal))
        {
            _directories.Add(current);
            current = DfsPath.GetParent(current);
        }
    }

    /// <summary>A deep copy (contents are copied).</summary>
    public ExpectedVolumeContent Clone()
    {
        var copy = new ExpectedVolumeContent();
        foreach (string d in _directories)
            copy._directories.Add(d);
        foreach ((string path, ExpectedFile file) in _files)
            copy._files[path] = file with { Content = (byte[])file.Content.Clone() };
        return copy;
    }
}

/// <summary>One namespace entry as observed through the SDK.</summary>
public sealed record ObservedEntry(
    string Path,
    DfsEntryKind Kind,
    InodeId Inode,
    long Length,
    byte[]? Content,
    string Owner,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc,
    DfsInodeAccess? Access,
    string? StoragePolicy,
    bool IsSparse,
    bool IsInline,
    int EntryCount);

/// <summary>A complete recursive capture of a namespace (live volume, snapshot view or backup view).</summary>
public sealed class ObservedVolumeContent
{
    /// <summary>Creates a capture.</summary>
    public ObservedVolumeContent(IReadOnlyDictionary<string, ObservedEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        Entries = entries;
    }

    /// <summary>All entries except the root, by path (ordinal order).</summary>
    public IReadOnlyDictionary<string, ObservedEntry> Entries { get; }

    /// <summary>File entries.</summary>
    public IEnumerable<ObservedEntry> Files => Entries.Values.Where(e => e.Kind == DfsEntryKind.File);

    /// <summary>Directory entries.</summary>
    public IEnumerable<ObservedEntry> Directories => Entries.Values.Where(e => e.Kind == DfsEntryKind.Directory);
}

/// <summary>
/// Deterministic volume content used by recovery and security tests: populates a volume through the public SDK with
/// boundary-sized, sparse, multibyte-named, policy-tagged and access-tagged files, captures any namespace view
/// recursively, and compares views against the independent manifest. Pure test logic over the public SDK.
/// </summary>
public static class VolumeContentFixture
{
    /// <summary>Name of the extra named policy the fixture assigns to one file.</summary>
    public const string PairPolicy = "pair";

    /// <summary>The named policy definition (two copies in two zones, both required before acknowledgement).</summary>
    public static DfsStoragePolicy PairPolicyDefinition { get; } = new()
    {
        TargetCopies = 2,
        TargetDistinctZones = 2,
        DurableCopiesBeforeAck = 2,
        DurableZonesBeforeAck = 2,
    };

    /// <summary>Named policies a volume must define for <see cref="PopulateAsync"/> to exercise per-file policies.</summary>
    public static IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; } =
        new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { [PairPolicy] = PairPolicyDefinition };

    /// <summary>NFC directory name with multibyte characters.</summary>
    public const string UnicodeDirectory = "/ünïcödé";

    /// <summary>The sparse file's path.</summary>
    public const string SparsePath = "/data/sparse.bin";

    /// <summary>The policy-tagged file's path.</summary>
    public const string PolicyPath = "/data/pair.bin";

    /// <summary>The access-tagged file's path.</summary>
    public const string AccessPath = "/docs/read-only-for-others.bin";

    /// <summary>The text file's path.</summary>
    public const string TextPath = "/docs/reports/q1.txt";

    /// <summary>
    /// Populates <paramref name="volume"/> (which must be empty) and returns the independent manifest. Sizes are derived
    /// from the volume's inline threshold and the server's chunk size: 0, 1, t-1, t, t+1, c-1, c, c+1 and 2c+12345.
    /// </summary>
    public static async Task<ExpectedVolumeContent> PopulateAsync(DfsVolume volume, ulong seed, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(volume);
        DfsVolumeInfo info = await volume.GetInfoAsync(ct);
        int inline = Math.Max(0, info.InlineThreshold);
        int chunk = volume.Client.Capabilities.ChunkSize > 0 ? volume.Client.Capabilities.ChunkSize : 64 * 1024;
        var expected = new ExpectedVolumeContent();
        DfsFileClient files = volume.Files;
        DfsDirectoryClient directories = volume.Directories;
        var none = new DfsInodeAccess();

        foreach (string dir in new[] { "/docs", "/docs/reports", "/empty", "/data/nested/deep", UnicodeDirectory })
        {
            await directories.CreateDirectoryAsync(dir, ct);
            expected.AddDirectory(dir);
        }

        var sizes = new SortedSet<long> { 0, 1, inline, inline + 1, chunk - 1, chunk, chunk + 1, 2L * chunk + 12345 };
        if (inline > 1)
            sizes.Add(inline - 1);
        ulong next = seed;
        foreach (long size in sizes)
        {
            string path = size switch
            {
                0 => "/docs/empty.txt",
                1 => "/docs/one.bin",
                _ when size > 2L * chunk => "/data/nested/deep/multi-extent.bin",
                _ => $"/data/len-{size}.bin",
            };
            byte[] bytes = TestData.Bytes(unchecked(next++ * 0x9E3779B97F4A7C15UL + 17), checked((int)size));
            await files.WriteAllBytesAsync(path, bytes, ct);
            expected.SetFile(new ExpectedFile(path, bytes, null, none, []));
        }

        string text = TestData.Utf8Text(unchecked(seed + 101), 777);
        await files.WriteAllTextAsync(TextPath, text, null, ct);
        expected.SetFile(new ExpectedFile(TextPath, new UTF8Encoding(false).GetBytes(text), null, none, []));

        string unicodeFile = UnicodeDirectory + "/ñame-中文.bin";
        byte[] unicodeBytes = TestData.Bytes(unchecked(seed + 202), 300);
        await files.WriteAllBytesAsync(unicodeFile, unicodeBytes, ct);
        expected.SetFile(new ExpectedFile(unicodeFile, unicodeBytes, null, none, []));

        if (info.NamedPolicies.ContainsKey(PairPolicy))
        {
            byte[] policyBytes = TestData.Bytes(unchecked(seed + 303), chunk + 7);
            await files.WriteAllBytesAsync(PolicyPath, policyBytes, new DfsMutationOptions { StoragePolicy = PairPolicy }, ct);
            expected.SetFile(new ExpectedFile(PolicyPath, policyBytes, PairPolicy, none, []));
        }

        byte[] accessBytes = TestData.Bytes(unchecked(seed + 404), 100);
        await files.WriteAllBytesAsync(AccessPath, accessBytes, ct);
        var readOnlyForOthers = new DfsInodeAccess(ReadOnlyForOthers: true);
        await files.SetAccessAsync(AccessPath, readOnlyForOthers, ct);
        expected.SetFile(new ExpectedFile(AccessPath, accessBytes, null, readOnlyForOthers, []));

        // Sparse: [block][hole][block][trailing hole], unaligned to chunk boundaries.
        long total = 8L * chunk;
        long secondAt = 5L * chunk + 3;
        byte[] block1 = TestData.Bytes(unchecked(seed + 505), 1000);
        byte[] block2 = TestData.Bytes(unchecked(seed + 606), 1000);
        await using (DfsStream stream = await files.OpenAsync(SparsePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, ct))
        {
            await stream.WriteAsync(block1, ct);
            stream.Seek(secondAt, SeekOrigin.Begin);
            await stream.WriteAsync(block2, ct);
            await stream.SetLengthAsync(total, ct);
            await stream.FlushAsync(ct);
        }

        var sparse = new byte[total];
        block1.CopyTo(sparse, 0);
        block2.CopyTo(sparse, secondAt);
        expected.SetFile(new ExpectedFile(SparsePath, sparse, null, none,
            [(block1.Length, secondAt - block1.Length), (secondAt + block2.Length, total - secondAt - block2.Length)]));
        return expected;
    }

    /// <summary>Captures a live volume.</summary>
    public static Task<ObservedVolumeContent> CaptureAsync(DfsVolume volume, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return CaptureAsync(volume.Files, volume.Directories, ct);
    }

    /// <summary>Captures a snapshot view (live cluster or backup host).</summary>
    public static Task<ObservedVolumeContent> CaptureAsync(DfsSnapshotView view, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        return CaptureAsync(view.Files, view.Directories, ct);
    }

    /// <summary>Recursively enumerates from the root, reading every file's bytes and metadata.</summary>
    public static async Task<ObservedVolumeContent> CaptureAsync(DfsFileClient files, DfsDirectoryClient directories, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(directories);
        var entries = new SortedDictionary<string, ObservedEntry>(StringComparer.Ordinal);
        var pending = new Stack<string>();
        pending.Push(DfsPath.Root);
        while (pending.Count > 0)
        {
            string dir = pending.Pop();
            await foreach (DfsEntryInfo entry in directories.EnumerateAsync(dir, null, ct))
            {
                if (entry.Kind == DfsEntryKind.Directory)
                {
                    DfsDirectoryInfo d = await directories.GetInfoAsync(entry.Path, ct);
                    entries[entry.Path] = new ObservedEntry(entry.Path, DfsEntryKind.Directory, d.InodeId, 0, null, d.Owner,
                        d.CreatedUtc, d.ModifiedUtc, null, null, false, false, d.EntryCount);
                    pending.Push(entry.Path);
                }
                else
                {
                    DfsFileInfo f = await files.GetInfoAsync(entry.Path, ct);
                    byte[] content = await files.ReadAllBytesAsync(entry.Path, ct);
                    entries[entry.Path] = new ObservedEntry(entry.Path, DfsEntryKind.File, f.InodeId, f.Length, content, f.Owner,
                        f.CreatedUtc, f.ModifiedUtc, f.Access, f.StoragePolicy, f.IsSparse, f.IsInline, 0);
                }
            }
        }

        return new ObservedVolumeContent(entries);
    }

    /// <summary>
    /// Asserts that <paramref name="observed"/> contains exactly the manifest's directories and files (no extra or
    /// missing entries), with exact bytes, lengths, access flags, explicitly assigned policies and sparse indicators.
    /// </summary>
    public static void AssertMatches(ExpectedVolumeContent expected, ObservedVolumeContent observed, string because)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(observed);
        string[] expectedFiles = expected.Files.Keys.ToArray();
        string[] observedFiles = observed.Files.Select(f => f.Path).ToArray();
        AssertSameSet(expectedFiles, observedFiles, $"file paths ({because})");
        AssertSameSet(expected.Directories.ToArray(), observed.Directories.Select(d => d.Path).ToArray(), $"directory paths ({because})");

        foreach (ExpectedFile file in expected.Files.Values)
        {
            ObservedEntry entry = observed.Entries[file.Path];
            Assert.Equal(DfsEntryKind.File, entry.Kind);
            Assert.True(file.Content.LongLength == entry.Length, $"{file.Path}: length {entry.Length}, expected {file.Content.LongLength} ({because})");
            AssertEx.BytesEqual(file.Content, entry.Content, $"{file.Path} ({because})");
            Assert.True(file.Access == entry.Access, $"{file.Path}: access {entry.Access}, expected {file.Access} ({because})");
            if (file.PolicyName is not null)
                Assert.True(string.Equals(file.PolicyName, entry.StoragePolicy, StringComparison.Ordinal),
                    $"{file.Path}: policy '{entry.StoragePolicy}', expected '{file.PolicyName}' ({because})");
            Assert.True(file.IsSparse == entry.IsSparse, $"{file.Path}: IsSparse {entry.IsSparse}, expected {file.IsSparse} ({because})");
        }

        AssertDistinctInodes(observed, because);
    }

    /// <summary>
    /// Asserts that two captures describe the same namespace: same paths and kinds, identical bytes and lengths, owner,
    /// access flags, effective storage policy names, sparse/inline indicators, directory entry counts and (optionally)
    /// creation/modification timestamps. Inode identities are compared only for uniqueness within each capture.
    /// </summary>
    public static void AssertSameMetadata(ObservedVolumeContent reference, ObservedVolumeContent actual, string because, bool compareTimestamps = true)
    {
        ArgumentNullException.ThrowIfNull(reference);
        ArgumentNullException.ThrowIfNull(actual);
        AssertSameSet(reference.Entries.Keys.ToArray(), actual.Entries.Keys.ToArray(), $"entry paths ({because})");
        foreach (ObservedEntry r in reference.Entries.Values)
        {
            ObservedEntry a = actual.Entries[r.Path];
            string at = $"{r.Path} ({because})";
            Assert.True(r.Kind == a.Kind, $"{at}: kind {a.Kind}, expected {r.Kind}");
            Assert.True(r.Length == a.Length, $"{at}: length {a.Length}, expected {r.Length}");
            if (r.Content is not null)
                AssertEx.BytesEqual(r.Content, a.Content, at);
            Assert.True(string.Equals(r.Owner, a.Owner, StringComparison.Ordinal), $"{at}: owner '{a.Owner}', expected '{r.Owner}'");
            Assert.True(r.Access == a.Access, $"{at}: access {a.Access}, expected {r.Access}");
            Assert.True(string.Equals(r.StoragePolicy, a.StoragePolicy, StringComparison.Ordinal), $"{at}: policy '{a.StoragePolicy}', expected '{r.StoragePolicy}'");
            Assert.True(r.IsSparse == a.IsSparse, $"{at}: IsSparse {a.IsSparse}, expected {r.IsSparse}");
            Assert.True(r.IsInline == a.IsInline, $"{at}: IsInline {a.IsInline}, expected {r.IsInline}");
            Assert.True(r.EntryCount == a.EntryCount, $"{at}: entry count {a.EntryCount}, expected {r.EntryCount}");
            if (compareTimestamps)
            {
                Assert.True(r.CreatedUtc == a.CreatedUtc, $"{at}: created {a.CreatedUtc:O}, expected {r.CreatedUtc:O}");
                Assert.True(r.ModifiedUtc == a.ModifiedUtc, $"{at}: modified {a.ModifiedUtc:O}, expected {r.ModifiedUtc:O}");
            }
        }

        AssertDistinctInodes(actual, because);
    }

    /// <summary>A stable digest of a capture (paths, kinds, lengths, bytes, owners, access and policies), for "unchanged" checks.</summary>
    public static string Digest(ObservedVolumeContent capture)
    {
        ArgumentNullException.ThrowIfNull(capture);
        var sb = new StringBuilder();
        foreach (ObservedEntry e in capture.Entries.Values)
        {
            sb.Append(e.Path).Append('|').Append(e.Kind).Append('|').Append(e.Length).Append('|')
                .Append(e.Content is null ? "-" : Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(e.Content)))
                .Append('|').Append(e.Owner).Append('|').Append(e.Access).Append('|').Append(e.StoragePolicy)
                .Append('|').Append(e.ModifiedUtc.UtcTicks).Append('\n');
        }

        return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
    }

    private static void AssertDistinctInodes(ObservedVolumeContent observed, string because)
    {
        var seen = new Dictionary<InodeId, string>();
        foreach (ObservedEntry e in observed.Entries.Values)
        {
            Assert.True(e.Inode != InodeId.Root, $"{e.Path}: non-root entry reports the root inode ({because})");
            if (seen.TryGetValue(e.Inode, out string? other))
                Assert.Fail($"{e.Path} and {other} share inode {e.Inode.Value} ({because})");
            seen[e.Inode] = e.Path;
        }
    }

    private static void AssertSameSet(IReadOnlyCollection<string> expected, IReadOnlyCollection<string> actual, string what)
    {
        string[] missing = expected.Except(actual, StringComparer.Ordinal).ToArray();
        string[] extra = actual.Except(expected, StringComparer.Ordinal).ToArray();
        if (missing.Length > 0 || extra.Length > 0)
            Assert.Fail($"{what} differ. Missing: [{string.Join(", ", missing)}]; unexpected: [{string.Join(", ", extra)}]");
    }
}
