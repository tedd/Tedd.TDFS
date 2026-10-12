using System.Globalization;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Open;

/// <summary>Result class of one open attempt as required by FSD §6.1, §7 (LOCK-01/02) and §5.4.</summary>
internal enum OpenOutcome
{
    /// <summary>Invalid or unsupported options; rejected locally before any mutation (argument / not-supported exception).</summary>
    Rejected,

    /// <summary><see cref="FileNotFoundException"/>; nothing is created.</summary>
    NotFound,

    /// <summary><see cref="DfsAlreadyExistsException"/> (CreateNew on an existing name); nothing changes.</summary>
    AlreadyExists,

    /// <summary><see cref="DfsSharingViolationException"/> (typed, retryable); nothing changes, in particular no truncation.</summary>
    SharingViolation,

    /// <summary>A new empty inode and its handle reservation appear in one committed open.</summary>
    Created,

    /// <summary>The existing inode is truncated to length zero at successful open (inode identity preserved).</summary>
    Truncated,

    /// <summary>The existing inode is opened; data and length are unchanged.</summary>
    Opened,
}

/// <summary>An active handle held (by another client) while the open under test is attempted.</summary>
internal readonly record struct HeldHandle(FileAccess Access, FileShare Share)
{
    public override string ToString() => OpenOracle.AccessName(Access) + "/" + OpenOracle.ShareName(Share);
}

/// <summary>The legal outcome of an open attempt and the exception types/codes that represent it.</summary>
internal sealed record OpenExpectation(OpenOutcome Outcome, IReadOnlyList<Type> ExceptionTypes, IReadOnlyList<DfsErrorCode> ErrorCodes, string Reason)
{
    public bool Succeeds => Outcome is OpenOutcome.Created or OpenOutcome.Truncated or OpenOutcome.Opened;
}

/// <summary>
/// Independent oracle for the open contract. It encodes FSD §6.1 (mode table), FSD §5.4 (unsupported OS options throw
/// <see cref="NotSupportedException"/>), FSD §13 (invalid options → argument exception; sharing conflict → typed
/// retryable IOException) and FSD §7 LOCK-01/LOCK-02 (one writer per inode; share checked in both directions across all
/// active handles; writable handles may not grant <see cref="FileShare.Write"/>; <see cref="FileShare.Inheritable"/>
/// rejected). It deliberately does not reuse any product code.
/// </summary>
internal static class OpenOracle
{
    /// <summary>Every bit <see cref="FileShare"/> defines.</summary>
    public const FileShare DefinedShareBits = FileShare.Read | FileShare.Write | FileShare.Delete | FileShare.Inheritable;

    public static IReadOnlyList<FileMode> ValidModes { get; } =
        [FileMode.CreateNew, FileMode.Create, FileMode.Open, FileMode.OpenOrCreate, FileMode.Truncate, FileMode.Append];

    public static IReadOnlyList<FileMode> InvalidModes { get; } = [(FileMode)0, (FileMode)7, (FileMode)(-1), (FileMode)int.MaxValue];

    public static IReadOnlyList<FileAccess> ValidAccesses { get; } = [FileAccess.Read, FileAccess.Write, FileAccess.ReadWrite];

    public static IReadOnlyList<FileAccess> InvalidAccesses { get; } = [(FileAccess)0, (FileAccess)4, (FileAccess)(-1)];

    /// <summary>All eight combinations of Read, Write and Delete (including None).</summary>
    public static IReadOnlyList<FileShare> ValidShares { get; } =
    [
        FileShare.None, FileShare.Read, FileShare.Write, FileShare.ReadWrite, FileShare.Delete,
        FileShare.Read | FileShare.Delete, FileShare.Write | FileShare.Delete, FileShare.ReadWrite | FileShare.Delete,
    ];

    /// <summary>Shares carrying the OS-specific inheritable-handle flag (FSD §5.4 / LOCK-02: rejected, NotSupportedException).</summary>
    public static IReadOnlyList<FileShare> InheritableShares { get; } =
        [FileShare.Inheritable, FileShare.Read | FileShare.Inheritable, FileShare.ReadWrite | FileShare.Delete | FileShare.Inheritable];

    /// <summary>Values with bits <see cref="FileShare"/> does not define (8 is a gap between Delete and Inheritable).</summary>
    public static IReadOnlyList<FileShare> UndefinedShares { get; } = [(FileShare)8, (FileShare)0x20, (FileShare)(-1)];

    /// <summary>Every share value exercised by the matrices (valid, inheritable and undefined).</summary>
    public static IReadOnlyList<FileShare> AllShares { get; } = [.. ValidShares, .. InheritableShares, .. UndefinedShares];

    /// <summary>Shares a writable handle may legally request (no Write bit).</summary>
    public static IReadOnlyList<FileShare> WriterShares { get; } = [.. ValidShares.Where(s => (s & FileShare.Write) == 0)];

    /// <summary>
    /// Active-handle sets for the conflicting-handle matrices: every single legal handle (16) plus multi-handle sets
    /// where only one member conflicts, proving that compatibility is checked across <em>all</em> active handles.
    /// </summary>
    public static IReadOnlyList<IReadOnlyList<HeldHandle>> HeldHandleSets { get; } = BuildHeldHandleSets();

    public static bool IsDefinedMode(FileMode mode) => mode is >= FileMode.CreateNew and <= FileMode.Append;

    public static bool IsDefinedAccess(FileAccess access) => access is FileAccess.Read or FileAccess.Write or FileAccess.ReadWrite;

    public static bool IsWritable(FileAccess access) => IsDefinedAccess(access) && (access & FileAccess.Write) != 0;

    public static bool IsReadable(FileAccess access) => IsDefinedAccess(access) && (access & FileAccess.Read) != 0;

    /// <summary>
    /// Bidirectional share compatibility of a new open with one active handle (LOCK-02) plus writer exclusivity
    /// (LOCK-01). <see cref="FileShare.Delete"/> only governs delete/rename, never opens.
    /// </summary>
    public static bool IsCompatible(HeldHandle active, FileAccess access, FileShare share)
    {
        bool newReads = (access & FileAccess.Read) != 0;
        bool newWrites = (access & FileAccess.Write) != 0;
        bool oldReads = (active.Access & FileAccess.Read) != 0;
        bool oldWrites = (active.Access & FileAccess.Write) != 0;
        if (newReads && (active.Share & FileShare.Read) == 0)
            return false;
        if (newWrites && (active.Share & FileShare.Write) == 0)
            return false;
        if (oldReads && (share & FileShare.Read) == 0)
            return false;
        if (oldWrites && (share & FileShare.Write) == 0)
            return false;
        return !(newWrites && oldWrites);
    }

    /// <summary>Computes the single legal outcome class of an open attempt.</summary>
    public static OpenExpectation Expect(FileMode mode, FileAccess access, FileShare share, bool exists, IReadOnlyList<HeldHandle> active)
    {
        ArgumentNullException.ThrowIfNull(active);
        var types = new List<Type>();
        var reasons = new List<string>();

        void Reject(string reason, params Type[] exceptionTypes)
        {
            reasons.Add(reason);
            foreach (Type t in exceptionTypes)
            {
                if (!types.Contains(t))
                    types.Add(t);
            }
        }

        if (!IsDefinedMode(mode))
            Reject($"FileMode {(int)mode} is not defined (FSD §13: invalid options -> argument exception)", typeof(ArgumentException));
        if (!IsDefinedAccess(access))
            Reject($"FileAccess {(int)access} is not defined (FSD §13)", typeof(ArgumentException));
        if ((share & ~DefinedShareBits) != 0)
            Reject($"FileShare 0x{(int)share:X} has undefined bits (FSD §13)", typeof(ArgumentException));
        if ((share & FileShare.Inheritable) != 0)
            Reject("FileShare.Inheritable requests an inherited OS handle (FSD §5.4 / LOCK-02: NotSupportedException)", typeof(NotSupportedException));

        if (IsDefinedMode(mode) && IsDefinedAccess(access))
        {
            bool writable = (access & FileAccess.Write) != 0;
            switch (mode)
            {
                case FileMode.CreateNew when !writable:
                    Reject("CreateNew requires write access (FSD §6.1)", typeof(ArgumentException));
                    break;
                case FileMode.Create when !writable:
                    Reject("Create requires write access (FSD §6.1)", typeof(ArgumentException));
                    break;
                case FileMode.Truncate when access != FileAccess.Write:
                    Reject("Truncate requires write-only access (FSD §6.1)", typeof(ArgumentException));
                    break;
                case FileMode.Append when access != FileAccess.Write:
                    Reject("Append is write-only (FSD §6.1)", typeof(ArgumentException));
                    break;
            }
        }

        if (IsWritable(access) && (share & FileShare.Write) != 0)
        {
            // FSD §7 LOCK-02: a writable handle SHALL reject options granting FileShare.Write (explicitly unsupported
            // multiple-writer sharing). The FSD does not fix whether this surfaces as an argument or a not-supported
            // exception; both are explicit option rejections and both are accepted.
            Reject("a writable handle may not grant FileShare.Write (FSD §7 LOCK-02)", typeof(ArgumentException), typeof(NotSupportedException));
        }

        if (types.Count > 0)
            return new OpenExpectation(OpenOutcome.Rejected, types, [], string.Join("; ", reasons));

        if (!exists)
        {
            return mode switch
            {
                FileMode.Open => new OpenExpectation(OpenOutcome.NotFound, [typeof(FileNotFoundException)], [], "Open requires an existing file (FSD §6.1)"),
                FileMode.Truncate => new OpenExpectation(OpenOutcome.NotFound, [typeof(FileNotFoundException)], [], "Truncate requires an existing file (FSD §6.1)"),
                _ => new OpenExpectation(OpenOutcome.Created, [], [], $"{mode} atomically creates a missing file (FSD §6.1)"),
            };
        }

        if (mode == FileMode.CreateNew)
            return new OpenExpectation(OpenOutcome.AlreadyExists, [typeof(DfsAlreadyExistsException)], [DfsErrorCode.AlreadyExists], "CreateNew fails when the name exists (FSD §6.1)");

        var conflicts = active.Where(h => !IsCompatible(h, access, share)).ToList();
        if (conflicts.Count > 0)
        {
            bool writerVsWriter = conflicts.Any(h => (h.Access & FileAccess.Write) != 0 && (access & FileAccess.Write) != 0);
            IReadOnlyList<DfsErrorCode> codes = writerVsWriter
                ? [DfsErrorCode.SharingViolation, DfsErrorCode.WriterConflict]
                : [DfsErrorCode.SharingViolation];
            return new OpenExpectation(OpenOutcome.SharingViolation, [typeof(DfsSharingViolationException)], codes,
                "conflicts with active handle(s) " + string.Join(", ", conflicts) + " (FSD §7 LOCK-01/02/03)");
        }

        return mode switch
        {
            FileMode.Create => new OpenExpectation(OpenOutcome.Truncated, [], [], "Create truncates an existing file at successful open (FSD §6.1, §5.4)"),
            FileMode.Truncate => new OpenExpectation(OpenOutcome.Truncated, [], [], "Truncate truncates at successful open (FSD §6.1)"),
            _ => new OpenExpectation(OpenOutcome.Opened, [], [], $"{mode} opens the existing file without changing it (FSD §6.1, §5.4)"),
        };
    }

    public static string ModeName(FileMode mode) =>
        IsDefinedMode(mode) ? mode.ToString() : "mode" + ((int)mode).ToString(CultureInfo.InvariantCulture);

    public static string AccessName(FileAccess access) =>
        IsDefinedAccess(access) ? access.ToString() : "access" + ((int)access).ToString(CultureInfo.InvariantCulture);

    public static string ShareName(FileShare share)
    {
        if ((share & ~DefinedShareBits) != 0)
            return "share" + ((int)share).ToString(CultureInfo.InvariantCulture);
        if (share == FileShare.None)
            return "None";
        var parts = new List<string>(4);
        if ((share & FileShare.Read) != 0)
            parts.Add("Read");
        if ((share & FileShare.Write) != 0)
            parts.Add("Write");
        if ((share & FileShare.Delete) != 0)
            parts.Add("Delete");
        if ((share & FileShare.Inheritable) != 0)
            parts.Add("Inheritable");
        return string.Join('+', parts);
    }

    private static List<IReadOnlyList<HeldHandle>> BuildHeldHandleSets()
    {
        var sets = new List<IReadOnlyList<HeldHandle>>();
        foreach (FileAccess access in ValidAccesses)
        {
            foreach (FileShare share in ValidShares)
            {
                if ((access & FileAccess.Write) != 0 && (share & FileShare.Write) != 0)
                    continue; // not a legal active handle
                sets.Add([new HeldHandle(access, share)]);
            }
        }

        // Multi-handle sets: the first member is permissive, a later member is the one that decides.
        sets.Add([new HeldHandle(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete), new HeldHandle(FileAccess.Read, FileShare.Read)]);
        sets.Add([new HeldHandle(FileAccess.Read, FileShare.ReadWrite), new HeldHandle(FileAccess.Write, FileShare.Read)]);
        sets.Add([new HeldHandle(FileAccess.Read, FileShare.ReadWrite), new HeldHandle(FileAccess.Read, FileShare.ReadWrite), new HeldHandle(FileAccess.ReadWrite, FileShare.Read)]);

        foreach (IReadOnlyList<HeldHandle> set in sets)
        {
            for (int i = 1; i < set.Count; i++)
            {
                for (int j = 0; j < i; j++)
                {
                    if (!IsCompatible(set[j], set[i].Access, set[i].Share))
                        throw new InvalidOperationException($"Held-handle set {string.Join(", ", set)} is not mutually compatible (test bug).");
                }
            }
        }

        return sets;
    }
}

/// <summary>Shared plumbing for OPEN unit tests.</summary>
internal static class OpenHelpers
{
    public const string Volume = "vol";

    /// <summary>Default content length of pre-existing files: above the 4 KiB inline threshold so truncation must drop extents.</summary>
    public const int DefaultInitialLength = 5000;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>The U-level SDK-contract configuration (three hosts, one per zone, in-memory, simulated storage).</summary>
    public static TestClusterOptions UnitOptions => TestClusterOptions.ThreeZones;

    public static FileAccess Access(int value) => (FileAccess)value;

    /// <summary>Theory data: every access value (defined and undefined) as (label, numeric value).</summary>
    public static TheoryData<string, int> AllAccessValues
    {
        get
        {
            var data = new TheoryData<string, int>();
            foreach (FileAccess a in OpenOracle.ValidAccesses.Concat(OpenOracle.InvalidAccesses))
                data.Add(OpenOracle.AccessName(a), (int)a);
            return data;
        }
    }

    /// <summary>Crashes every live node abruptly (unflushed state lost) and restarts each from its durable image.</summary>
    public static async Task CrashAndRestartAllAsync(TestCluster cluster)
    {
        TestNode[] nodes = [.. cluster.Nodes];
        foreach (TestNode node in nodes)
            await node.CrashAsync(CrashMode.LoseUnflushed);
        foreach (TestNode node in nodes)
            await node.RestartAsync();
        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte-for-byte equality (first mismatch offset reported).</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string? because = null)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Reads the whole committed content through a fresh client view.</summary>
    public static Task<byte[]> ReadCommittedAsync(DfsFileClient files, string path) => files.ReadAllBytesAsync(path, Ct);

    /// <summary>Live (non-expired) handles reserved on <paramref name="inode"/> at the volume leader.</summary>
    public static async Task<IReadOnlyList<HandleInspection>> LiveHandlesAsync(TestCluster cluster, InodeId inode)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        return [.. state.Handles.Where(h => h.Inode == inode && !h.Expired)];
    }

    /// <summary>Number of OpenHandle requests a client has put on the wire so far.</summary>
    public static long CountOpenRequests(TestCluster cluster, string clientName) =>
        cluster.Network.Log.CountMessages(new MessageFilter(From: "client:" + clientName, Type: MessageType.OpenHandle));

    /// <summary>Describes the first difference of two byte arrays, or returns null when they are equal.</summary>
    public static string? DescribeMismatch(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual)
    {
        if (expected.SequenceEqual(actual))
            return null;
        int common = expected.CommonPrefixLength(actual);
        return string.Create(CultureInfo.InvariantCulture,
            $"expected {expected.Length} bytes, got {actual.Length}; first difference at offset {common}");
    }

    public static string Describe(Exception? ex) =>
        ex is null ? "(no exception)" : $"{ex.GetType().Name}{(ex is DfsException d ? $"[{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    /// <summary>Expected bytes after overwriting <paramref name="original"/> at offset 0 with <paramref name="prefix"/>.</summary>
    public static byte[] Overlay(ReadOnlySpan<byte> original, ReadOnlySpan<byte> prefix, long offset = 0)
    {
        long length = Math.Max(original.Length, offset + prefix.Length);
        var result = new byte[length];
        original.CopyTo(result);
        prefix.CopyTo(result.AsSpan(checked((int)offset)));
        return result;
    }

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int at = 0;
        foreach (byte[] p in parts)
        {
            p.CopyTo(result, at);
            at += p.Length;
        }

        return result;
    }

    /// <summary>
    /// Reads the committed bytes through an explicit reader handle that is share-compatible with an active writer
    /// (Read access, FileShare.ReadWrite|Delete), as the test plan §5 requires for visibility checks while a writer is open.
    /// </summary>
    public static async Task<byte[]> ReadSharedAsync(DfsFileClient files, string path)
    {
        await using DfsStream reader = await files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, Ct);
        return await ReadToEndAsync(reader);
    }

    /// <summary>Reads a stream from its current position to EOF.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[7919]; // deliberately not aligned to any product boundary
        while (true)
        {
            int n = await stream.ReadAsync(chunk, Ct);
            if (n == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, n);
        }
    }
}

/// <summary>
/// Runs FileMode × FileAccess × FileShare × existence × active-handle combinations inside one cluster and checks each
/// against <see cref="OpenOracle"/>. Every combination is evaluated and logged (identified by its label); all
/// mismatches are reported together so one failing combination does not hide the others.
/// </summary>
internal sealed class OpenMatrix : IAsyncDisposable
{
    private readonly TestCluster _cluster;
    private readonly DfsClient _actor;
    private readonly DfsClient _holder;
    private readonly DfsClient _observer;
    private readonly string _directory;
    private readonly List<string> _failures = [];
    private readonly Dictionary<OpenOutcome, int> _outcomeCounts = [];
    private int _cases;

    private OpenMatrix(TestCluster cluster, DfsClient actor, DfsClient holder, DfsClient observer, string directory)
    {
        _cluster = cluster;
        _actor = actor;
        _holder = holder;
        _observer = observer;
        _directory = directory;
    }

    public const string ActorName = "actor";

    public int Cases => _cases;

    private DfsFileClient ActorFiles => _actor.GetVolume(OpenHelpers.Volume).Files;

    private DfsFileClient HolderFiles => _holder.GetVolume(OpenHelpers.Volume).Files;

    private DfsFileClient ObserverFiles => _observer.GetVolume(OpenHelpers.Volume).Files;

    /// <summary>Connects the actor (opens under test), holder (active handles) and observer (independent view) clients.</summary>
    public static async Task<OpenMatrix> StartAsync(TestCluster cluster, string directory)
    {
        DfsClient actor = await cluster.ConnectAsync(new TestClientOptions { Name = ActorName }, OpenHelpers.Ct);
        DfsClient holder = await cluster.ConnectAsync(new TestClientOptions { Name = "holder" }, OpenHelpers.Ct);
        DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, OpenHelpers.Ct);
        await observer.GetVolume(OpenHelpers.Volume).Directories.CreateDirectoryAsync(directory, OpenHelpers.Ct);
        return new OpenMatrix(cluster, actor, holder, observer, directory);
    }

    /// <summary>Runs one combination and records any deviation from the oracle.</summary>
    public async Task RunAsync(FileMode mode, FileAccess access, FileShare share, bool exists, IReadOnlyList<HeldHandle> held,
        int initialLength = OpenHelpers.DefaultInitialLength)
    {
        int index = ++_cases;
        string label = string.Create(CultureInfo.InvariantCulture,
            $"#{index} {OpenOracle.ModeName(mode)} {OpenOracle.AccessName(access)} {OpenOracle.ShareName(share)} {(exists ? "existing" : "missing")}")
            + (held.Count == 0 ? string.Empty : " held[" + string.Join(", ", held) + "]");
        string path = _directory + "/c" + index.ToString("D4", CultureInfo.InvariantCulture);
        OpenExpectation expected = OpenOracle.Expect(mode, access, share, exists, held);
        _outcomeCounts[expected.Outcome] = _outcomeCounts.GetValueOrDefault(expected.Outcome) + 1;
        var failures = new List<string>();
        byte[] initial = exists ? TestData.Bytes(0x0BE0_0000UL + (ulong)index, initialLength) : [];
        var holders = new List<DfsStream>();
        DfsStream? stream = null;
        InodeId? targetInode = null;
        try
        {
            if (exists)
                await ObserverFiles.WriteAllBytesAsync(path, initial, OpenHelpers.Ct);
            foreach (HeldHandle h in held)
                holders.Add(await HolderFiles.OpenAsync(path, FileMode.Open, h.Access, h.Share, OpenHelpers.Ct));

            DfsFileInfo? before = exists ? await ObserverFiles.GetInfoAsync(path, OpenHelpers.Ct) : null;
            int liveBefore = (await _cluster.Inspect.GetVolumeStateAsync(OpenHelpers.Volume)).Handles.Count(h => !h.Expired);
            long requestsBefore = OpenHelpers.CountOpenRequests(_cluster, ActorName);

            Exception? error = null;
            try
            {
                stream = await ActorFiles.OpenAsync(path, mode, access, share, OpenHelpers.Ct);
            }
            catch (Exception ex)
            {
                error = ex;
            }

            if (expected.Succeeds)
            {
                if (stream is null)
                {
                    failures.Add($"expected {expected.Outcome} ({expected.Reason}) but the open threw {OpenHelpers.Describe(error)}");
                }
                else
                {
                    targetInode = stream.InodeId;
                    await VerifyOpenedAsync(stream, path, mode, access, share, expected, before, held.Count, failures);
                }
            }
            else
            {
                if (stream is not null)
                    failures.Add($"expected {expected.Outcome} ({expected.Reason}) but the open succeeded on inode {stream.InodeId.Value}");
                else
                    VerifyError(expected, error!, failures);

                await VerifyUnchangedAsync(path, exists, before, liveBefore, failures);
                if (expected.Outcome == OpenOutcome.Rejected && OpenHelpers.CountOpenRequests(_cluster, ActorName) != requestsBefore)
                    failures.Add("invalid/unsupported options reached the server as an OpenHandle request instead of being rejected before any mutation (FSD §13, SDK-001)");
            }
        }
        catch (Exception ex)
        {
            failures.Add("scenario step threw " + OpenHelpers.Describe(ex));
        }
        finally
        {
            if (stream is not null)
                await DisposeAsync(stream, "handle under test", failures);
            foreach (DfsStream h in holders)
                await DisposeAsync(h, "held handle", failures);
        }

        try
        {
            await VerifyAfterCloseAsync(path, exists, expected, initial, targetInode, failures);
        }
        catch (Exception ex)
        {
            failures.Add("post-close verification threw " + OpenHelpers.Describe(ex));
        }

        OpenHelpers.Log($"{label}: expected {expected.Outcome}{(failures.Count == 0 ? " - ok" : " - FAILED")}");
        foreach (string f in failures)
            _failures.Add($"{label}: {f}");
    }

    /// <summary>Fails the test when any combination deviated or when the expected number of combinations did not run.</summary>
    public void AssertAllPassed(int expectedCases, params OpenOutcome[] requiredOutcomes)
    {
        OpenHelpers.Log("Outcome classes: " + string.Join(", ", _outcomeCounts.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}")));
        Assert.Equal(expectedCases, _cases);
        foreach (OpenOutcome o in requiredOutcomes)
            Assert.True(_outcomeCounts.GetValueOrDefault(o) > 0, $"the matrix did not exercise any {o} combination (family coverage is incomplete)");
        if (_failures.Count > 0)
            Assert.Fail($"{_failures.Count} deviation(s) in {_cases} combinations:{Environment.NewLine}{string.Join(Environment.NewLine, _failures)}");
    }

    public async ValueTask DisposeAsync()
    {
        await _actor.DisposeAsync();
        await _holder.DisposeAsync();
        await _observer.DisposeAsync();
    }

    private async Task VerifyOpenedAsync(DfsStream stream, string path, FileMode mode, FileAccess access, FileShare share,
        OpenExpectation expected, DfsFileInfo? before, int heldCount, List<string> failures)
    {
        bool writable = (access & FileAccess.Write) != 0;
        void Check(bool condition, string message)
        {
            if (!condition)
                failures.Add(message);
        }

        Check(stream.Path == path, $"stream.Path is '{stream.Path}', expected '{path}'");
        Check(stream.Access == access, $"stream.Access is {stream.Access}");
        Check(stream.Share == share, $"stream.Share is {stream.Share}");
        Check(stream.IsAppend == (mode == FileMode.Append), $"stream.IsAppend is {stream.IsAppend}");
        Check(stream.CanSeek == (mode != FileMode.Append), $"CanSeek is {stream.CanSeek} (append handles SHALL report CanSeek=false, others true)");
        Check(stream.CanRead == ((access & FileAccess.Read) != 0), $"CanRead is {stream.CanRead}");
        Check(stream.CanWrite == writable, $"CanWrite is {stream.CanWrite}");
        Check(!stream.IsTransactionBound, "stream is transaction-bound");
        Check(!stream.IsFaulted, "fresh stream is faulted: " + OpenHelpers.Describe(stream.Fault));
        Check(stream.ReadView == DfsReadView.Live, $"ReadView is {stream.ReadView}");
        Check(stream.WriteMode == DfsWriteMode.OnFlush, $"WriteMode is {stream.WriteMode}");
        Check(writable ? stream.WriterGeneration > 0 : stream.WriterGeneration == 0,
            $"WriterGeneration is {stream.WriterGeneration} for a {(writable ? "writable" : "read-only")} handle");

        long expectedLength = expected.Outcome == OpenOutcome.Opened ? before!.Length : 0;

        // Another client's view immediately after the open returned, before any write or flush.
        DfsFileInfo info = await ObserverFiles.GetInfoAsync(path, OpenHelpers.Ct);
        Check(info.InodeId == stream.InodeId, $"namespace entry resolves to inode {info.InodeId.Value}, the handle to {stream.InodeId.Value}");
        Check(info.Length == expectedLength, $"other clients see length {info.Length}, expected {expectedLength} right after the open");
        switch (expected.Outcome)
        {
            case OpenOutcome.Created:
                Check(await ObserverFiles.ExistsAsync(path, OpenHelpers.Ct), "created file is not visible to another client although the open returned");
                break;
            case OpenOutcome.Truncated:
                Check(info.InodeId == before!.InodeId, "truncating open replaced the inode instead of truncating it");
                Check(info.Revision > before.Revision, $"truncation was not published at open (revision {info.Revision} vs {before.Revision})");
                break;
            case OpenOutcome.Opened:
                Check(info.InodeId == before!.InodeId, "opening an existing file changed its inode");
                if (!writable)
                    Check(info.Revision == before.Revision, $"a read-only open changed the revision ({before.Revision} -> {info.Revision})");
                break;
        }

        if (mode != FileMode.Append)
        {
            Check(stream.Position == 0, $"initial Position is {stream.Position}");
            long length = await stream.GetLengthAsync(OpenHelpers.Ct);
            Check(length == expectedLength, $"handle reports length {length}, expected {expectedLength}");
        }

        FileInspection? file = await _cluster.Inspect.GetFileAsync(OpenHelpers.Volume, path);
        if (file is null)
        {
            failures.Add("the leader has no namespace entry for the opened path");
        }
        else
        {
            Check(file.Inode == stream.InodeId, $"leader maps the path to inode {file.Inode.Value}");
            Check(file.Length == expectedLength, $"leader length is {file.Length}");
            if (expected.Outcome == OpenOutcome.Truncated)
                Check(file.Extents.Count == 0, $"truncated file still maps {file.Extents.Count} extent(s)");
            if (writable)
                Check(file.WriterGeneration == stream.WriterGeneration, $"inode writer generation {file.WriterGeneration} != handle generation {stream.WriterGeneration}");
        }

        IReadOnlyList<HandleInspection> live = await OpenHelpers.LiveHandlesAsync(_cluster, stream.InodeId);
        Check(live.Count == heldCount + 1, $"{live.Count} live reservation(s) on the inode, expected {heldCount + 1} (held + new)");
        Check(live.Any(h => h.Access == access && h.Share == share && h.IsWriter == writable && (!writable || h.Generation == stream.WriterGeneration)),
            "no live reservation with the requested access/share/writer generation exists (create/truncate and reservation must be one committed open)");
        Check(live.Count(h => h.IsWriter) <= 1, "more than one writable reservation on one inode (LOCK-01)");
    }

    private static void VerifyError(OpenExpectation expected, Exception error, List<string> failures)
    {
        if (!expected.ExceptionTypes.Any(t => t.IsInstanceOfType(error)))
        {
            failures.Add($"expected {expected.Outcome} as {string.Join(" or ", expected.ExceptionTypes.Select(t => t.Name))} ({expected.Reason}) but got {OpenHelpers.Describe(error)}");
            return;
        }

        if (expected.ErrorCodes.Count > 0)
        {
            if (error is not DfsException dfs)
                failures.Add($"expected a DfsException with code {string.Join("/", expected.ErrorCodes)} but got {OpenHelpers.Describe(error)}");
            else if (!expected.ErrorCodes.Contains(dfs.ErrorCode))
                failures.Add($"error code {dfs.ErrorCode}, expected {string.Join(" or ", expected.ErrorCodes)}");
        }

        if (expected.Outcome == OpenOutcome.SharingViolation && error is DfsException { IsRetryable: false })
            failures.Add("sharing/writer conflict is not marked retryable (FSD §13)");
        if (expected.Outcome == OpenOutcome.Rejected && error is DfsSharingViolationException)
            failures.Add("an invalid option was reported as a sharing conflict");
    }

    private async Task VerifyUnchangedAsync(string path, bool exists, DfsFileInfo? before, int liveBefore, List<string> failures)
    {
        if (exists)
        {
            DfsFileInfo now = await ObserverFiles.GetInfoAsync(path, OpenHelpers.Ct);
            if (now.InodeId != before!.InodeId)
                failures.Add("a failed open replaced the inode");
            if (now.Length != before.Length)
                failures.Add($"a failed open changed the length {before.Length} -> {now.Length} (no truncation may happen without a successful open)");
            if (now.Revision != before.Revision)
                failures.Add($"a failed open published a new revision {before.Revision} -> {now.Revision}");
        }
        else
        {
            if (await ObserverFiles.ExistsAsync(path, OpenHelpers.Ct))
                failures.Add("a failed open created the file");
            if (await _cluster.Inspect.GetFileAsync(OpenHelpers.Volume, path) is not null)
                failures.Add("the leader holds a namespace entry for a failed open");
        }

        int liveAfter = (await _cluster.Inspect.GetVolumeStateAsync(OpenHelpers.Volume)).Handles.Count(h => !h.Expired);
        if (liveAfter != liveBefore)
            failures.Add($"a failed open leaked a reservation ({liveBefore} -> {liveAfter} live handles)");
    }

    private async Task VerifyAfterCloseAsync(string path, bool exists, OpenExpectation expected, byte[] initial, InodeId? targetInode, List<string> failures)
    {
        switch (expected.Outcome)
        {
            case OpenOutcome.Created:
            case OpenOutcome.Truncated:
                if (targetInode is null)
                    return; // the open itself failed; already reported
                byte[] empty = await OpenHelpers.ReadCommittedAsync(ObserverFiles, path);
                if (empty.Length != 0)
                    failures.Add($"after closing, the {expected.Outcome.ToString().ToLowerInvariant()} file holds {empty.Length} bytes instead of 0");
                DfsFileInfo info = await ObserverFiles.GetInfoAsync(path, OpenHelpers.Ct);
                if (info.InodeId != targetInode.Value)
                    failures.Add("after closing, the path resolves to a different inode than the handle had");
                break;
            default:
                if (exists)
                {
                    byte[] now = await OpenHelpers.ReadCommittedAsync(ObserverFiles, path);
                    string? mismatch = OpenHelpers.DescribeMismatch(initial, now);
                    if (mismatch is not null)
                        failures.Add("existing bytes changed: " + mismatch);
                }
                else if (await ObserverFiles.ExistsAsync(path, OpenHelpers.Ct))
                {
                    failures.Add("after closing, a file exists although the open did not create one");
                }

                break;
        }

        if (targetInode is { } inode)
        {
            IReadOnlyList<HandleInspection> live = await OpenHelpers.LiveHandlesAsync(_cluster, inode);
            if (live.Count != 0)
                failures.Add($"{live.Count} reservation(s) remain after every handle was disposed");
        }
    }

    private static async Task DisposeAsync(DfsStream stream, string what, List<string> failures)
    {
        try
        {
            await stream.DisposeAsync();
        }
        catch (Exception ex)
        {
            failures.Add($"disposing the {what} without pending writes threw {OpenHelpers.Describe(ex)}");
        }
    }
}
