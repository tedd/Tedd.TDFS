using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Sec;

/// <summary>Kinds of authenticated principals used by the SEC authorization matrices.</summary>
public enum PrincipalKind
{
    /// <summary>Read on "vol".</summary>
    Reader,

    /// <summary>Read and Write on "vol".</summary>
    Writer,

    /// <summary>Read and Snapshot on "vol".</summary>
    Snapshotter,

    /// <summary>Read, Write and Snapshot on "vol" (a non-admin allowed to commit-and-snapshot).</summary>
    WriterSnapshotter,

    /// <summary>Authenticated, explicit None on "vol".</summary>
    NoGrants,

    /// <summary>All permissions on "vol2" only.</summary>
    OtherVolumeOnly,

    /// <summary>BackupAdmin (plus Read) on "vol".</summary>
    BackupAdmin,

    /// <summary>Cluster administrator.</summary>
    ClusterAdmin,
}

/// <summary>Shared helpers for SEC integration scenarios.</summary>
internal static class SecHelpers
{
    public const string Vol = "vol";
    public const string Vol2 = "vol2";
    public const string AllThreePolicy = "all3";

    /// <summary>Real QUIC + disk cluster with two volumes ("vol" with the fixture's and an ack-three policy, "vol2").</summary>
    public static TestClusterOptions Options(int nodeCount = 3, int backups = 0, bool encryptAtRest = true) => new()
    {
        NodeCount = nodeCount,
        ControlVoters = Math.Min(3, nodeCount),
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        BackupNodeCount = backups,
        EncryptAtRest = encryptAtRest,
        Volumes =
        [
            new TestVolumeSpec(Vol)
            {
                NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
                {
                    [VolumeContentFixture.PairPolicy] = VolumeContentFixture.PairPolicyDefinition,
                    [AllThreePolicy] = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 },
                },
            },
            new TestVolumeSpec(Vol2),
        ],
    };

    /// <summary>Grants for a principal kind.</summary>
    public static IReadOnlyDictionary<string, DfsPermissions> GrantsFor(PrincipalKind kind) => kind switch
    {
        PrincipalKind.Reader => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read },
        PrincipalKind.Writer => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write },
        PrincipalKind.Snapshotter => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Snapshot },
        PrincipalKind.WriterSnapshotter => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write | DfsPermissions.Snapshot },
        PrincipalKind.NoGrants => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.None },
        PrincipalKind.OtherVolumeOnly => new Dictionary<string, DfsPermissions> { [Vol2] = DfsPermissions.All },
        PrincipalKind.BackupAdmin => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.BackupAdmin },
        // The FSD/ARCHITECTURE do not say whether the cluster-level ClusterAdmin flag implies volume data permissions (the
        // harness's own "admin" holds both explicitly), so the cluster-admin principal is given explicit grants as well.
        PrincipalKind.ClusterAdmin => new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.All },
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>Effective permissions of a principal kind on "vol".</summary>
    public static DfsPermissions EffectiveOnVol(PrincipalKind kind) => kind switch
    {
        PrincipalKind.ClusterAdmin => DfsPermissions.All,
        PrincipalKind.OtherVolumeOnly => DfsPermissions.None,
        _ => GrantsFor(kind).TryGetValue(Vol, out DfsPermissions p) ? p : DfsPermissions.None,
    };

    /// <summary>Creates a principal of the given kind.</summary>
    public static Task<TestPrincipal> CreatePrincipalAsync(TestCluster cluster, PrincipalKind kind, string name) =>
        cluster.CreatePrincipalAsync(name, GrantsFor(kind), clusterAdmin: kind == PrincipalKind.ClusterAdmin);

    /// <summary>Connects a client as <paramref name="credentials"/>.</summary>
    public static Task<DfsClient> ConnectAsAsync(TestCluster cluster, DfsCredentials credentials, string name, CancellationToken ct) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, Credentials = credentials }, ct);

    /// <summary>Asserts an authenticated principal was refused for lack of permission (AccessDenied, never another code).</summary>
    public static void AssertAccessDenied(Exception? e, string because)
    {
        if (e is not UnauthorizedAccessException unauthorized)
        {
            Assert.Fail($"expected UnauthorizedAccessException (AccessDenied) but got {(e is null ? "success" : e.GetType().Name + ": " + e.Message)} ({because})");
            return;
        }

        Assert.True(DfsErrors.ToCode(unauthorized) == DfsErrorCode.AccessDenied, $"expected AccessDenied but got {DfsErrors.ToCode(unauthorized)} ({because})");
    }

    /// <summary>Asserts a request without valid authentication was refused (authentication/authorization error or connection closed).</summary>
    public static void AssertUnauthenticatedRefused(DfsErrorCode code, string because) =>
        Assert.True(code is DfsErrorCode.AuthenticationFailed or DfsErrorCode.AccessDenied or DfsErrorCode.ProtocolError,
            $"an unauthenticated request must be refused, got {code} ({because})");

    /// <summary>Stable 64-bit FNV-1a hash of a string (seeds, names).</summary>
    public static ulong StableHash(string s)
    {
        ulong h = 14695981039346656037UL;
        foreach (byte b in Encoding.UTF8.GetBytes(s))
        {
            h ^= b;
            h *= 1099511628211UL;
        }

        return h;
    }

    /// <summary>
    /// Digest of a subtree as seen by <paramref name="admin"/>: every directory and file with revision, length, content hash,
    /// access flags and policy. "&lt;absent&gt;" when the root does not exist.
    /// </summary>
    public static async Task<string> SubtreeDigestAsync(DfsVolume admin, string root, CancellationToken ct)
    {
        if (!await admin.Directories.ExistsAsync(root, ct))
            return "<absent>";
        var sb = new StringBuilder();
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            string dir = pending.Pop();
            DfsDirectoryInfo info = await admin.Directories.GetInfoAsync(dir, ct);
            sb.Append("D|").Append(dir).Append('|').Append(info.Revision).Append('|').Append(info.EntryCount).Append('\n');
            var children = new List<DfsEntryInfo>();
            await foreach (DfsEntryInfo e in admin.Directories.EnumerateAsync(dir, null, ct))
                children.Add(e);
            foreach (DfsEntryInfo e in children.OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                if (e.Kind == DfsEntryKind.Directory)
                {
                    pending.Push(e.Path);
                    continue;
                }

                DfsFileInfo f = await admin.Files.GetInfoAsync(e.Path, ct);
                byte[] content = await admin.Files.ReadAllBytesAsync(e.Path, ct);
                sb.Append("F|").Append(e.Path).Append('|').Append(f.Revision).Append('|').Append(f.Length).Append('|')
                    .Append(Convert.ToHexString(SHA256.HashData(content))).Append('|').Append(f.Access).Append('|').Append(f.StoragePolicy).Append('\n');
            }
        }

        return sb.ToString();
    }

    /// <summary>Digest of the snapshot list (names and retention) and the cluster's volume list.</summary>
    public static async Task<string> GlobalDigestAsync(DfsVolume admin, DfsAdminClient adminClient, CancellationToken ct)
    {
        IReadOnlyList<DfsSnapshotInfo> snapshots = await admin.Snapshots.ListAsync(ct);
        IReadOnlyList<string> volumes = await adminClient.ListVolumesAsync(ct);
        return string.Join(";", snapshots.OrderBy(s => s.Name, StringComparer.Ordinal).Select(s => s.Name + "|" + s.Retention))
            + "#" + string.Join(";", volumes.Order(StringComparer.Ordinal));
    }

    /// <summary>A raw frame: little-endian message type followed by <paramref name="body"/>.</summary>
    public static byte[] Frame(MessageType type, ReadOnlySpan<byte> body)
    {
        var frame = new byte[2 + body.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(frame, (ushort)type);
        body.CopyTo(frame.AsSpan(2));
        return frame;
    }

    /// <summary>A frame whose body starts with a length prefix claiming <paramref name="claimedLength"/> bytes but carries only a few.</summary>
    public static byte[] OversizedLengthFrame(MessageType type, ulong claimedLength)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        writer.WriteVarUInt(claimedLength);
        writer.WriteUInt64(0xDEADBEEFUL);
        return Frame(type, buffer.WrittenSpan);
    }

    /// <summary>Deterministic fuzz frames: empty, truncated, huge counts/lengths, overlong varints and random bodies over all message types.</summary>
    public static IReadOnlyList<byte[]> FuzzFrames(ulong seed, int perType)
    {
        var random = new SeededRandom(seed);
        var frames = new List<byte[]>();
        MessageType[] types = Enum.GetValues<MessageType>();
        foreach (MessageType type in types)
        {
            frames.Add(Frame(type, []));
            frames.Add(OversizedLengthFrame(type, ulong.MaxValue >> 1));
            frames.Add(OversizedLengthFrame(type, 1UL << 31));
            frames.Add(Frame(type, Enumerable.Repeat((byte)0xFF, 11).ToArray()));   // overlong varint
            for (int i = 0; i < perType; i++)
            {
                var body = new byte[random.Next(96)];
                random.Fill(body);
                frames.Add(Frame(type, body));
            }
        }

        frames.Add([]);
        frames.Add([0x01]);
        for (int i = 0; i < perType; i++)
        {
            var unknown = new byte[2 + random.Next(32)];
            random.Fill(unknown);
            BinaryPrimitives.WriteUInt16LittleEndian(unknown, (ushort)(60000 + random.Next(5000)));
            frames.Add(unknown);
        }

        return frames;
    }

    /// <summary>True when the frame decodes locally under strict limits (a canonical, well-formed message).</summary>
    public static bool DecodesLocally(byte[] frame)
    {
        try
        {
            _ = MessageCodec.Decode(frame, new WireLimits(MaxStringBytes: 4096, MaxBytesLength: 1 << 20, MaxCollectionCount: 4096));
            return true;
        }
        catch (DfsProtocolException)
        {
            return false;
        }
    }

    /// <summary>Counts occurrences of <paramref name="needle"/> in <paramref name="haystack"/>.</summary>
    public static bool Contains(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle) => needle.Length > 0 && haystack.IndexOf(needle) >= 0;

    /// <summary>Writes a self-signed node certificate that expired yesterday (PFX, no password) and returns its path.</summary>
    public static string WriteExpiredCertificate(string path, string subject)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=" + subject, key, HashAlgorithmName.SHA256);
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 certificate = request.CreateSelfSigned(now.AddDays(-30), now.AddDays(-1));
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pfx));
        return path;
    }

    /// <summary>Formats an int invariantly.</summary>
    public static string Inv(int value) => value.ToString(CultureInfo.InvariantCulture);
}


/// <summary>
/// One namespace/data/transaction/snapshot operation of the SEC-003 authorization matrix.
/// </summary>
/// <param name="Name">Operation name (also the fixture subdirectory).</param>
/// <param name="Required">Volume permissions an ordinary principal needs.</param>
/// <param name="Mutates">True when an allowed execution must change the observed state.</param>
/// <param name="Act">Executes the operation as the principal under test against the fixture rooted at the given path and verifies returned data.</param>
/// <param name="NeedsOwnSnapshot">The fixture creates a snapshot named <see cref="SecOperationCatalog.SnapshotFor"/>.</param>
/// <param name="AmbiguousForNonAdmin">The FSD does not fix whether a non-admin holding <paramref name="Required"/> may run it
/// (owner-only metadata, volume-creating restore); either success or AccessDenied-without-effect is accepted then.</param>
internal sealed record SecOperation(
    string Name,
    DfsPermissions Required,
    bool Mutates,
    Func<DfsVolume, string, CancellationToken, Task> Act,
    bool NeedsOwnSnapshot = false,
    bool AmbiguousForNonAdmin = false);

/// <summary>The SEC-003 operation catalog: every file, directory, transaction and snapshot operation family.</summary>
internal static class SecOperationCatalog
{
    public const string BaseSnapshot = "sec3-base";
    public const string BaseRoot = "/sec3-base";

    public static string Text(string root) => TestData.Utf8Text(SecHelpers.StableHash(root), 300, multibyte: false);

    public static byte[] TextBytes(string root) => Encoding.UTF8.GetBytes(Text(root));

    public static string SnapshotFor(string root) => "s" + SecHelpers.StableHash(root).ToString("x16", CultureInfo.InvariantCulture);

    public static string UniqueName(string prefix, string root) => prefix + SecHelpers.StableHash(root).ToString("x12", CultureInfo.InvariantCulture)[..12];

    /// <summary>Creates the per-operation fixture as the administrator.</summary>
    public static async Task SetupAsync(DfsVolume admin, string root, SecOperation op, CancellationToken ct)
    {
        await admin.Directories.CreateDirectoryAsync(root + "/d", ct);
        await admin.Directories.CreateDirectoryAsync(root + "/empty", ct);
        await admin.Files.WriteAllTextAsync(root + "/f.txt", Text(root), null, ct);
        await admin.Files.WriteAllTextAsync(root + "/d/child.txt", "child", null, ct);
        await admin.Files.WriteAllTextAsync(root + "/src.txt", "source", null, ct);
        if (op.NeedsOwnSnapshot)
            await admin.Snapshots.CreateAsync(SnapshotFor(root), null, ct);
    }

    /// <summary>Creates the shared base snapshot read by snapshot-read operations.</summary>
    public static async Task SetupBaseAsync(DfsVolume admin, CancellationToken ct)
    {
        await admin.Directories.CreateDirectoryAsync(BaseRoot, ct);
        await admin.Files.WriteAllTextAsync(BaseRoot + "/f.txt", Text(BaseRoot), null, ct);
        await admin.Snapshots.CreateAsync(BaseSnapshot, null, ct);
    }

    private static async Task ReadToEndEqualsAsync(DfsStream stream, byte[] expected, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        AssertEx.BytesEqual(expected, ms.ToArray(), "stream content");
    }

    /// <summary>All operations.</summary>
    public static IReadOnlyList<SecOperation> All { get; } =
    [
        // ---- read family -------------------------------------------------------------------------------------------
        new("ReadAllBytes", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            byte[] b = await v.Files.ReadAllBytesAsync(r + "/f.txt", ct);
            AssertEx.BytesEqual(TextBytes(r), b);
        }),
        new("ReadAllText", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(Text(r), await v.Files.ReadAllTextAsync(r + "/f.txt", null, ct))),
        new("ReadAllLines", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(new[] { Text(r) }, await v.Files.ReadAllLinesAsync(r + "/f.txt", null, ct))),
        new("ReadLines", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            var lines = new List<string>();
            await foreach (string line in v.Files.ReadLinesAsync(r + "/f.txt", null, ct))
                lines.Add(line);
            Assert.Equal(new[] { Text(r) }, lines);
        }),
        new("OpenRead", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenReadAsync(r + "/f.txt", ct);
            await ReadToEndEqualsAsync(s, TextBytes(r), ct);
        }),
        new("OpenReadShareReadWrite", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ct);
            await ReadToEndEqualsAsync(s, TextBytes(r), ct);
        }),
        new("OpenReadVersionPinned", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", new DfsOpenOptions { ReadView = DfsReadView.VersionPinned }, ct);
            await ReadToEndEqualsAsync(s, TextBytes(r), ct);
        }),
        new("FileExists", DfsPermissions.Read, false, async (v, r, ct) => Assert.True(await v.Files.ExistsAsync(r + "/f.txt", ct))),
        new("FileGetInfo", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(TextBytes(r).LongLength, (await v.Files.GetInfoAsync(r + "/f.txt", ct)).Length)),
        new("DirectoryExists", DfsPermissions.Read, false, async (v, r, ct) => Assert.True(await v.Directories.ExistsAsync(r + "/d", ct))),
        new("DirectoryGetInfo", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(1, (await v.Directories.GetInfoAsync(r + "/d", ct)).EntryCount)),
        new("Enumerate", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            var names = new List<string>();
            await foreach (DfsEntryInfo e in v.Directories.EnumerateAsync(r, null, ct))
                names.Add(e.Name);
            Assert.Equal(new[] { "d", "empty", "f.txt", "src.txt" }, names.Order(StringComparer.Ordinal));
        }),
        new("ListPage", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            // Pages may be short; follow continuations until exhausted (bounded) and compare the union.
            var names = new List<string>();
            string? continuation = null;
            for (int pages = 0; pages < 10; pages++)
            {
                DfsDirectoryPage page = await v.Directories.ListPageAsync(r, new DfsEnumerationOptions { PageSize = 2 }, continuation, ct);
                Assert.True(page.Entries.Count <= 2, $"a page of size 2 returned {page.Entries.Count} entries");
                names.AddRange(page.Entries.Select(e => e.Name));
                continuation = page.ContinuationToken;
                if (continuation is null)
                    break;
            }

            Assert.Null(continuation);
            Assert.Equal(new[] { "d", "empty", "f.txt", "src.txt" }, names.Order(StringComparer.Ordinal));
        }),
        new("VolumeGetInfo", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(SecHelpers.Vol, (await v.GetInfoAsync(ct)).Name)),
        new("TransactionRead", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            byte[] b = await tx.Files.ReadAllBytesAsync(r + "/f.txt", ct);
            AssertEx.BytesEqual(TextBytes(r), b);
            await tx.AbortAsync(ct);
        }),
        new("TransactionQueryOutcome", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            DfsTransactionId id;
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, ct))
            {
                id = tx.Id;
                _ = await tx.Files.ReadAllBytesAsync(r + "/f.txt", ct);
                await tx.AbortAsync(ct);
            }

            DfsOperationOutcome outcome = await v.Transactions.QueryOutcomeAsync(id, false, ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
        }),
        new("SnapshotList", DfsPermissions.Read, false, async (v, r, ct) => Assert.Contains(await v.Snapshots.ListAsync(ct), s => s.Name == BaseSnapshot)),
        new("SnapshotGet", DfsPermissions.Read, false, async (v, r, ct) => Assert.Equal(BaseSnapshot, (await v.Snapshots.GetAsync(BaseSnapshot, ct)).Name)),
        new("SnapshotOpenRead", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsSnapshotView view = await v.Snapshots.OpenAsync(BaseSnapshot, ct);
            byte[] b = await view.Files.ReadAllBytesAsync(BaseRoot + "/f.txt", ct);
            AssertEx.BytesEqual(TextBytes(BaseRoot), b);
        }),
        new("SnapshotEnumerate", DfsPermissions.Read, false, async (v, r, ct) =>
        {
            await using DfsSnapshotView view = await v.Snapshots.OpenAsync(BaseSnapshot, ct);
            var names = new List<string>();
            await foreach (DfsEntryInfo e in view.Directories.EnumerateAsync(BaseRoot, null, ct))
                names.Add(e.Name);
            Assert.Equal(new[] { "f.txt" }, names);
        }),

        // ---- write family ------------------------------------------------------------------------------------------
        new("WriteAllBytes", DfsPermissions.Write, true, (v, r, ct) => v.Files.WriteAllBytesAsync(r + "/f.txt", new byte[] { 1, 2, 3 }, ct)),
        new("WriteThenQueryOperation", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            DfsOperationId id = await v.NewOperationIdAsync(ct);
            CommitToken token = await v.Files.WriteAllBytesAsync(r + "/f.txt", new byte[] { 4, 2 }, new DfsMutationOptions { OperationId = id }, ct);
            DfsOperationOutcome outcome = await v.QueryOperationAsync(id, false, ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(token, outcome.Token);
        }),
        new("WriteAllText", DfsPermissions.Write, true, (v, r, ct) => v.Files.WriteAllTextAsync(r + "/f.txt", "replaced", null, ct)),
        new("WriteAllLines", DfsPermissions.Write, true, (v, r, ct) => v.Files.WriteAllLinesAsync(r + "/f.txt", ["a", "b"], null, ct)),
        new("AppendAllBytes", DfsPermissions.Write, true, (v, r, ct) => v.Files.AppendAllBytesAsync(r + "/f.txt", new byte[] { 9 }, ct)),
        new("AppendAllText", DfsPermissions.Write, true, (v, r, ct) => v.Files.AppendAllTextAsync(r + "/f.txt", "tail", null, ct)),
        new("AppendAllLines", DfsPermissions.Write, true, (v, r, ct) => v.Files.AppendAllLinesAsync(r + "/f.txt", ["tail"], null, ct)),
        new("Create", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.CreateAsync(r + "/created.bin", ct);
            await s.WriteAsync(new byte[] { 1, 2, 3 }, ct);
        }),
        new("OpenWrite", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenWriteAsync(r + "/f.txt", ct);
            await s.WriteAsync(new byte[] { 7, 7, 7 }, ct);
        }),
        new("OpenCreateNew", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/new.bin", FileMode.CreateNew, FileAccess.Write, FileShare.None, ct);
            await s.WriteAsync(new byte[] { 4 }, ct);
        }),
        new("OpenTruncate", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Truncate, FileAccess.Write, FileShare.None, ct);
        }),
        new("OpenAppend", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Append, FileAccess.Write, FileShare.None, ct);
            await s.WriteAsync(new byte[] { 5 }, ct);
        }),
        new("OpenOrCreateReadWrite", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/ooc.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, ct);
            await s.WriteAsync(new byte[] { 6 }, ct);
        }),
        new("OpenExistingWriteCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Open, FileAccess.Write, FileShare.Read, ct);
            await s.WriteAsync(new byte[] { 8 }, ct);
            await s.CommitAsync(ct);
        }),
        new("StreamSetLength", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Open, FileAccess.ReadWrite, FileShare.None, ct);
            await s.SetLengthAsync(1, ct);
        }),
        new("Copy", DfsPermissions.Write, true, (v, r, ct) => v.Files.CopyAsync(r + "/f.txt", r + "/copy.txt", false, ct)),
        new("CopyOverwrite", DfsPermissions.Write, true, (v, r, ct) => v.Files.CopyAsync(r + "/f.txt", r + "/src.txt", true, ct)),
        new("MoveFile", DfsPermissions.Write, true, (v, r, ct) => v.Files.MoveAsync(r + "/src.txt", r + "/moved.txt", false, ct)),
        new("DeleteFile", DfsPermissions.Write, true, (v, r, ct) => v.Files.DeleteAsync(r + "/f.txt", ct)),
        new("SetStoragePolicy", DfsPermissions.Write, true, (v, r, ct) => v.Files.SetStoragePolicyAsync(r + "/f.txt", VolumeContentFixture.PairPolicy, ct)),
        new("SetAccess", DfsPermissions.Write, true, (v, r, ct) => v.Files.SetAccessAsync(r + "/f.txt", new DfsInodeAccess(ReadOnlyForOthers: true), ct), AmbiguousForNonAdmin: true),
        new("CreateDirectory", DfsPermissions.Write, true, (v, r, ct) => v.Directories.CreateDirectoryAsync(r + "/newdir/a/b", ct)),
        new("MoveDirectory", DfsPermissions.Write, true, (v, r, ct) => v.Directories.MoveAsync(r + "/d", r + "/d2", ct)),
        new("DeleteEmptyDirectory", DfsPermissions.Write, true, (v, r, ct) => v.Directories.DeleteAsync(r + "/empty", false, ct)),
        new("DeleteDirectoryRecursive", DfsPermissions.Write, true, (v, r, ct) => v.Directories.DeleteAsync(r + "/d", true, ct)),
        new("TransactionWriteCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await tx.Files.WriteAllBytesAsync(r + "/f.txt", new byte[] { 1 }, ct);
            await tx.Files.WriteAllBytesAsync(r + "/tx-new.bin", new byte[] { 2 }, ct);
            await tx.CommitAsync(ct);
        }),
        new("TransactionCreateDirectoryCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await tx.Directories.CreateDirectoryAsync(r + "/tx-dir", ct);
            await tx.CommitAsync(ct);
        }),
        new("TransactionMoveCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await tx.Files.MoveAsync(r + "/src.txt", r + "/tx-moved.txt", false, ct);
            await tx.CommitAsync(ct);
        }),
        new("TransactionDeleteCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await tx.Files.DeleteAsync(r + "/f.txt", ct);
            await tx.Directories.DeleteAsync(r + "/empty", false, ct);
            await tx.CommitAsync(ct);
        }),
        new("TransactionBoundStreamCommit", DfsPermissions.Write, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await using (DfsStream s = await tx.Files.CreateAsync(r + "/tx-stream.bin", ct))
                await s.WriteAsync(TestData.Bytes(3, 5000), ct);
            await tx.CommitAsync(ct);
        }),

        // ---- snapshot family ---------------------------------------------------------------------------------------
        new("SnapshotCreate", DfsPermissions.Snapshot, true, (v, r, ct) => v.Snapshots.CreateAsync(UniqueName("c", r), null, ct)),
        new("SnapshotDelete", DfsPermissions.Snapshot, true, (v, r, ct) => v.Snapshots.DeleteAsync(SnapshotFor(r), ct), NeedsOwnSnapshot: true),
        // Whether live-snapshot retention needs Snapshot or RetentionAdmin is not fixed by the FSD: a Snapshot holder may be
        // allowed or denied (without effect); principals without Snapshot must be denied either way.
        new("SnapshotSetRetention", DfsPermissions.Snapshot, true, (v, r, ct) => v.Snapshots.SetRetentionAsync(SnapshotFor(r), new DfsRetention(null, Hold: true), ct),
            NeedsOwnSnapshot: true, AmbiguousForNonAdmin: true),
        new("WriteAllCommitAndSnapshot", DfsPermissions.Write | DfsPermissions.Snapshot, true, (v, r, ct) =>
            v.Files.WriteAllBytesAsync(r + "/f.txt", new byte[] { 3 }, new DfsMutationOptions { CommitSnapshotName = UniqueName("w", r) }, ct)),
        new("StreamCommitAndSnapshot", DfsPermissions.Write | DfsPermissions.Snapshot, true, async (v, r, ct) =>
        {
            DfsStream s = await v.Files.OpenAsync(r + "/f.txt", FileMode.Open, FileAccess.Write, FileShare.Read, ct);
            try
            {
                await s.WriteAsync(new byte[] { 2 }, ct);
                await s.CommitAsync(new DfsMutationOptions { CommitSnapshotName = UniqueName("x", r) }, ct);
            }
            catch
            {
                // A refused commit-and-snapshot must not be published by a later ordinary flush on dispose: discard first.
                try { await s.AbortAsync(ct); } catch (Exception) { }
                try { await s.DisposeAsync(); } catch (Exception) { }
                throw;
            }

            await s.DisposeAsync();
        }),
        new("TransactionCommitWithSnapshot", DfsPermissions.Write | DfsPermissions.Snapshot, true, async (v, r, ct) =>
        {
            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, ct);
            await tx.Files.WriteAllBytesAsync(r + "/f.txt", new byte[] { 4 }, ct);
            await tx.CommitWithSnapshotAsync(UniqueName("t", r), ct);
        }),
        new("SnapshotRestore", DfsPermissions.Snapshot, true, (v, r, ct) => v.Snapshots.RestoreAsync(BaseSnapshot, UniqueName("rs", r), ct), AmbiguousForNonAdmin: true),
    ];
}

/// <summary>Seeded parser fuzzers for SEC-015 (wire codec, remote parsers, journal, atomic files, object store, backup host).</summary>
internal static class SecFuzzing
{
    private static readonly WireLimits StrictLimits = new(MaxStringBytes: 4096, MaxBytesLength: 1 << 20, MaxCollectionCount: 4096);

    /// <summary>
    /// Decodes every fuzz frame locally: only <see cref="DfsProtocolException"/> may escape, allocation per decode is
    /// bounded, accepted frames re-encode/decode stably, every strict prefix and trailing-garbage variant of a valid frame
    /// is rejected, and the error codec rejects garbage with the same typed error.
    /// </summary>
    public static void FuzzWireCodec()
    {
        IReadOnlyList<byte[]> frames = SecHelpers.FuzzFrames(0x15_00C0, 40);
        int rejected = 0, accepted = 0;
        foreach (byte[] frame in frames)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            IWireMessage? message = null;
            try
            {
                message = MessageCodec.Decode(frame, StrictLimits);
                accepted++;
            }
            catch (DfsProtocolException)
            {
                rejected++;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(allocated < (4L << 20) + 16L * frame.Length, $"decoding a {frame.Length}-byte frame allocated {allocated} bytes (claimed lengths must not be pre-allocated)");
            if (message is not null)
            {
                byte[] reencoded = MessageCodec.Encode(message);
                IWireMessage again = MessageCodec.Decode(reencoded, StrictLimits);
                Assert.Equal(message.Type, again.Type);
                Assert.Equal(reencoded, MessageCodec.Encode(again));
            }
        }

        Assert.True(rejected > 0, "the fuzz corpus must contain rejected frames");
        TestContext.Current.TestOutputHelper?.WriteLine($"wire codec: {frames.Count} frames, {accepted} accepted, {rejected} rejected");

        byte[] valid = MessageCodec.Encode(new ErrorReplyMessage(DfsErrorCode.AccessDenied, "denied", new DfsOperationId(1, 2), false, "quic://127.0.0.1:1", null));
        Assert.Equal(MessageType.ErrorReply, MessageCodec.Decode(valid, StrictLimits).Type);
        for (int cut = 0; cut < valid.Length; cut++)
        {
            byte[] prefix = valid[..cut];
            Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(prefix, StrictLimits));
        }

        Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode([.. valid, 0x00], StrictLimits));

        var random = new SeededRandom(0x15_00C1);
        for (int i = 0; i < 500; i++)
        {
            var data = new byte[random.Next(64)];
            random.Fill(data);
            try
            {
                _ = ErrorCodec.Decode(data);
            }
            catch (DfsProtocolException)
            {
            }
        }
    }

    /// <summary>Sends the fuzz corpus to the volume leader and one follower, unauthenticated or as a principal without grants.</summary>
    public static async Task FuzzRemoteAsync(TestCluster cluster, bool authenticated, ulong seed, CancellationToken ct)
    {
        TestPrincipal? low = authenticated
            ? await cluster.CreatePrincipalAsync("fuzzer", new Dictionary<string, DfsPermissions> { [SecHelpers.Vol] = DfsPermissions.None })
            : null;
        await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, ct);
        DfsVolume vol = admin.GetVolume(SecHelpers.Vol);
        DfsVolume vol2 = admin.GetVolume(SecHelpers.Vol2);
        await VolumeContentFixture.PopulateAsync(vol, seed, ct);
        string before = await SecHelpers.SubtreeDigestAsync(vol, "/", ct) + await SecHelpers.SubtreeDigestAsync(vol2, "/", ct) + await SecHelpers.GlobalDigestAsync(vol, admin.Admin, ct);

        async Task<RawProtocolClient> OpenAsync(string node)
        {
            RawProtocolClient r = await cluster.ConnectRawAsync(node, ct);
            if (low is not null)
                Assert.Equal(DfsErrorCode.None, await r.AuthenticateAsync(low.Credentials));
            return r;
        }

        IReadOnlyList<byte[]> frames = SecHelpers.FuzzFrames(seed, 6);
        TestNode leader = await cluster.GetLeaderAsync(SecHelpers.Vol, ct);
        string follower = cluster.Nodes.First(n => n.Name != leader.Name).Name;
        int sent = 0, reconnects = 0;
        foreach (string target in new[] { leader.Name, follower })
        {
            RawProtocolClient raw = await OpenAsync(target);
            try
            {
                foreach (byte[] frame in frames)
                {
                    if (!raw.IsConnected)
                    {
                        await raw.DisposeAsync();
                        raw = await OpenAsync(target);
                        reconnects++;
                    }

                    DfsErrorCode code = await raw.SendFrameAsync(frame);
                    sent++;
                    Assert.True(code != DfsErrorCode.None || SecHelpers.DecodesLocally(frame),
                        $"{target} silently accepted a malformed frame (type bytes {Convert.ToHexString(frame.AsSpan(0, Math.Min(2, frame.Length)))}, {frame.Length} bytes)");
                }
            }
            finally
            {
                await raw.DisposeAsync();
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"remote fuzz ({(authenticated ? "authenticated, no grants" : "unauthenticated")}): {sent} frames, {reconnects} reconnects");
        Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
        string after = await SecHelpers.SubtreeDigestAsync(vol, "/", ct) + await SecHelpers.SubtreeDigestAsync(vol2, "/", ct) + await SecHelpers.GlobalDigestAsync(vol, admin.Admin, ct);
        Assert.True(before == after, "fuzzed requests changed namespace, snapshot or volume state");
        byte[] probe = TestData.Bytes(seed + 1, 10_000);
        await vol.Files.WriteAllBytesAsync("/after-fuzz.bin", probe, ct);
        await RecoveryScenario.AssertFileBytesAsync(vol.Files, "/after-fuzz.bin", probe, "service after fuzzing", ct);
        await cluster.WaitForReplicationAsync(SecHelpers.Vol, ct);
        await cluster.Inspect.AssertReplicasConvergedAsync(SecHelpers.Vol);
    }

    /// <summary>
    /// Corrupts (bit flips), truncates or extends journal segments of a durable journal: reopening either fails with
    /// <see cref="DfsIntegrityException"/> or yields an exact prefix of the appended payloads (torn tails only).
    /// </summary>
    public static async Task FuzzJournalAsync(ulong seed, CancellationToken ct)
    {
        var medium = new SimulatedMedium("fuzz-journal", 1L << 30, new SeededRandom(seed));
        byte[][] payloads = Enumerable.Range(0, 40).Select(i => TestData.Bytes(seed + (ulong)i, 50 + i * 37)).ToArray();
        var options = new JournalOptions(SegmentSize: 4096);
        await using (Journal journal = await Journal.OpenAsync(medium, "journal", options, ct))
        {
            foreach (byte[] payload in payloads)
                await journal.AppendAsync(payload, ct);
            await journal.FlushAsync(ct);
        }

        var random = new SeededRandom(seed ^ 0xF00D);
        int detected = 0, recovered = 0;
        for (int iteration = 0; iteration < 60; iteration++)
        {
            SimulatedMedium copy = medium.CloneDurable("fuzz-journal-" + SecHelpers.Inv(iteration));
            string[] files = copy.AllFiles().Where(f => f.Contains("journal", StringComparison.Ordinal) && copy.ReadDurable(f).Length > 0).Order(StringComparer.Ordinal).ToArray();
            Assert.NotEmpty(files);
            string target = files[random.Next(files.Length)];
            int length = copy.ReadDurable(target).Length;
            int mode = random.Next(3);
            await MutateAsync(copy, target, length, mode, random, ct);
            try
            {
                await using Journal reopened = await Journal.OpenAsync(copy, "journal", options, ct);
                IReadOnlyList<JournalRecord> records = reopened.Records;
                Assert.True(records.Count <= payloads.Length, $"iteration {iteration}: more records than were written");
                for (int i = 0; i < records.Count; i++)
                    AssertEx.BytesEqual(payloads[i], records[i].Payload.Span, $"iteration {iteration} (mode {mode}, {target}): record {i} must be an exact prefix element");
                if (mode == 2)
                    Assert.True(records.Count == payloads.Length, $"iteration {iteration}: appended garbage must be truncated as a torn tail without losing records");
                if (mode == 0)
                    Assert.True(records.Count >= payloads.Length - 1, $"iteration {iteration}: a corrupted interior record must fail with DfsIntegrityException, not silently drop {payloads.Length - records.Count} records");
                recovered++;
            }
            catch (DfsIntegrityException)
            {
                detected++;
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"journal fuzz: {recovered} recovered to exact prefixes, {detected} integrity failures");
    }

    /// <summary>Corrupts atomic files: reads return the exact content or fail with <see cref="DfsIntegrityException"/>, never other bytes or "absent".</summary>
    public static async Task FuzzAtomicFileAsync(ulong seed, CancellationToken ct)
    {
        var medium = new SimulatedMedium("fuzz-atomic", 1L << 30, new SeededRandom(seed));
        await medium.CreateDirectoryAsync("state", ct);
        byte[] content = TestData.Bytes(seed, 3000);
        await AtomicFile.WriteAsync(medium, "state/checkpoint.bin", content, ct);
        await AtomicFile.WriteAsync(medium, "state/checkpoint.bin", content, ct);
        byte[] pristineRead = await AtomicFile.ReadAsync(medium, "state/checkpoint.bin", ct) ?? [];
        AssertEx.BytesEqual(content, pristineRead, "pristine atomic file");

        var random = new SeededRandom(seed ^ 0xA70);
        int detected = 0, intact = 0;
        for (int iteration = 0; iteration < 60; iteration++)
        {
            SimulatedMedium copy = medium.CloneDurable("fuzz-atomic-" + SecHelpers.Inv(iteration));
            string[] files = copy.AllFiles().Where(f => f.StartsWith("state", StringComparison.Ordinal) && copy.ReadDurable(f).Length > 0).Order(StringComparer.Ordinal).ToArray();
            string target = files[random.Next(files.Length)];
            await MutateAsync(copy, target, copy.ReadDurable(target).Length, random.Next(3), random, ct);
            try
            {
                byte[]? read = await AtomicFile.ReadAsync(copy, "state/checkpoint.bin", ct);
                Assert.True(read is not null, $"iteration {iteration}: a present but damaged atomic file must not be reported absent");
                AssertEx.BytesEqual(content, read, $"iteration {iteration}: corrupting {target} must not yield different content");
                intact++;
            }
            catch (DfsIntegrityException)
            {
                detected++;
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"atomic file fuzz: {intact} intact reads, {detected} integrity failures");
    }

    /// <summary>
    /// Corrupts object segments: reopening fails with <see cref="DfsIntegrityException"/> or lists only originally stored
    /// objects, each of which reads back exactly or fails with <see cref="DfsIntegrityException"/>.
    /// </summary>
    public static async Task FuzzObjectStoreAsync(ulong seed, CancellationToken ct)
    {
        var medium = new SimulatedMedium("fuzz-objects", 1L << 30, new SeededRandom(seed));
        var options = new ObjectStoreOptions(SegmentSize: 64 << 10, ReservedBytes: 0, MaxObjectSize: 64 << 10);
        var random = new SeededRandom(seed);
        var volume = VolumeId.New(random);
        var stored = new Dictionary<ObjectId, byte[]>();
        await using (ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, ct))
        {
            for (int i = 0; i < 20; i++)
            {
                ObjectId id = ObjectId.New(random);
                byte[] data = TestData.Bytes(seed + (ulong)i, 1000 + i * 997);
                await store.PutAsync(id, volume, new StagingId((ulong)i + 1), data, ct);
                stored[id] = data;
            }
        }

        int detected = 0, verified = 0;
        for (int iteration = 0; iteration < 40; iteration++)
        {
            SimulatedMedium copy = medium.CloneDurable("fuzz-objects-" + SecHelpers.Inv(iteration));
            string[] files = copy.AllFiles().Where(f => f.StartsWith("objects", StringComparison.Ordinal) && copy.ReadDurable(f).Length > 0).Order(StringComparer.Ordinal).ToArray();
            string target = files[random.Next(files.Length)];
            await MutateAsync(copy, target, copy.ReadDurable(target).Length, random.Next(3), random, ct);
            try
            {
                await using ObjectStore reopened = await ObjectStore.OpenAsync(copy, "objects", options, NoFaults.Instance, ct);
                foreach (ObjectRecordInfo record in reopened.List())
                {
                    Assert.True(stored.TryGetValue(record.Id, out byte[]? expected), $"iteration {iteration}: index lists an object that was never stored: {record.Id}");
                    Assert.Equal(volume, record.Volume);
                    var buffer = new byte[expected.Length];
                    try
                    {
                        int total = 0;
                        while (total < buffer.Length)
                        {
                            int n = await reopened.ReadAsync(record.Id, total, buffer.AsMemory(total), ct);
                            Assert.True(n > 0, $"iteration {iteration}: short read of {record.Id} without an error");
                            total += n;
                        }

                        AssertEx.BytesEqual(expected, buffer, $"iteration {iteration}: object {record.Id} after corrupting {target}");
                        verified++;
                    }
                    catch (DfsIntegrityException)
                    {
                        detected++;
                    }
                }
            }
            catch (DfsIntegrityException)
            {
                detected++;
            }
        }

        TestContext.Current.TestOutputHelper?.WriteLine($"object store fuzz: {verified} verified reads, {detected} integrity failures");
    }

    /// <summary>
    /// Corrupts random files on a stopped backup host (manifests, pointers, segments): after restart the published snapshot
    /// is either served exactly or fails closed with a typed error; restoring the pristine image restores full service.
    /// </summary>
    public static async Task FuzzBackupHostAsync(TestCluster cluster, ulong seed, CancellationToken ct)
    {
        BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", seed, ct);
        TestBackupNode backup = source.Backup;
        await backup.StopAsync();
        string root = backup.Disk.RootDirectory ?? throw new InvalidOperationException("disk storage expected");
        string pristine = Path.Combine(cluster.DataRoot, "backup-pristine");
        CopyDirectory(root, pristine);
        var random = new SeededRandom(seed);
        int detected = 0, intact = 0;
        for (int iteration = 0; iteration < 4; iteration++)
        {
            ReplaceDirectory(root, pristine);
            string[] files = Directory.GetFiles(root, "*", SearchOption.AllDirectories).Where(f => new FileInfo(f).Length > 0).Order(StringComparer.Ordinal).ToArray();
            string victim = files[random.Next(files.Length)];
            byte[] bytes = await File.ReadAllBytesAsync(victim, ct);
            for (int k = 0; k < 4; k++)
                bytes[random.Next(bytes.Length)] ^= (byte)(1 + random.Next(255));
            await File.WriteAllBytesAsync(victim, bytes, ct);
            string relative = Path.GetRelativePath(root, victim);

            Exception? start = await RecoveryScenario.CaptureAsync(() => backup.RestartAsync());
            if (start is not null)
            {
                Assert.True(start is DfsException || start.InnerException is DfsException,
                    $"restart after corrupting {relative} must fail with a typed error, got {start}");
                detected++;
                continue;
            }

            try
            {
                await using DfsClient reader = await cluster.ConnectBackupAsync(backup.Index,
                    new TestClientOptions { Name = "fuzz-reader-" + SecHelpers.Inv(iteration), Credentials = backup.ReaderCredentials }, ct);
                await using DfsSnapshotView view = await reader.GetVolume(RecoveryScenario.Volume).Snapshots.OpenAsync("S", ct);
                VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, ct), $"backup host after corrupting {relative}");
                intact++;
            }
            catch (Exception ex) when (ex is DfsException or DfsAuthenticationException)
            {
                // Fail closed: a typed integrity/availability error, or refusing authentication when the host's own
                // principal store was damaged. Wrong bytes would surface as an assertion failure above instead.
                detected++;
            }

            await backup.StopAsync();
        }

        ReplaceDirectory(root, pristine);
        await backup.RestartAsync();
        await using (DfsClient reader = await cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = "fuzz-reader-final", Credentials = backup.ReaderCredentials }, ct))
        await using (DfsSnapshotView view = await reader.GetVolume(RecoveryScenario.Volume).Snapshots.OpenAsync("S", ct))
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, ct), "pristine backup image");
        TestContext.Current.TestOutputHelper?.WriteLine($"backup host fuzz: {intact} intact, {detected} fail-closed");
    }

    private static async Task MutateAsync(SimulatedMedium medium, string path, int length, int mode, IRandomSource random, CancellationToken ct)
    {
        switch (mode)
        {
            case 0:
                medium.CorruptByte(path, random.Next(Math.Max(1, length)), (byte)(1 + random.Next(255)));
                break;
            case 1:
            {
                await using IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
                await file.SetLengthAsync(random.Next(Math.Max(1, length)), ct);
                await file.FlushAsync(ct);
                break;
            }

            default:
            {
                var garbage = new byte[1 + random.Next(64)];
                random.Fill(garbage);
                await using IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
                await file.WriteFullyAsync(length, garbage, ct);
                await file.FlushAsync(ct);
                break;
            }
        }
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        foreach (string file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(to, Path.GetRelativePath(from, file)), overwrite: true);
    }

    private static void ReplaceDirectory(string target, string source)
    {
        if (Directory.Exists(target))
        {
            foreach (string file in Directory.GetFiles(target, "*", SearchOption.AllDirectories))
                File.Delete(file);
        }

        CopyDirectory(source, target);
    }
}
