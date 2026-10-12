using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Lock;

/// <summary>An open request: the access a handle asks for and the sharing it grants to other handles.</summary>
public readonly record struct HandleSpec(FileAccess Access, FileShare Share)
{
    /// <inheritdoc />
    public override string ToString() => $"{Access}/{Share}";
}

/// <summary>
/// Shared helpers of the LOCK unit tests: the independent sharing oracle (FSD §7 LOCK-01/LOCK-02), cluster/client setup,
/// handle inspection and exception classification.
/// </summary>
internal static class LockHelpers
{
    public const string Volume = "vol";

    /// <summary>Every <see cref="FileAccess"/> value.</summary>
    public static readonly FileAccess[] Accesses = [FileAccess.Read, FileAccess.Write, FileAccess.ReadWrite];

    /// <summary>Every combination of the supported <see cref="FileShare"/> bits (None, Read, Write, Delete).</summary>
    public static readonly FileShare[] Shares =
    [
        FileShare.None,
        FileShare.Read,
        FileShare.Write,
        FileShare.ReadWrite,
        FileShare.Delete,
        FileShare.Read | FileShare.Delete,
        FileShare.Write | FileShare.Delete,
        FileShare.ReadWrite | FileShare.Delete,
    ];

    /// <summary>All 24 access × share requests (including the ones the core release must reject).</summary>
    public static IEnumerable<HandleSpec> AllRequests()
    {
        foreach (FileAccess access in Accesses)
        {
            foreach (FileShare share in Shares)
                yield return new HandleSpec(access, share);
        }
    }

    public static bool IsWritable(FileAccess access) => (access & FileAccess.Write) != 0;

    /// <summary>Core release: a writable handle may not grant <see cref="FileShare.Write"/> (LOCK-005); everything else is valid.</summary>
    public static bool IsValidCoreRequest(HandleSpec spec) => !(IsWritable(spec.Access) && (spec.Share & FileShare.Write) != 0);

    /// <summary>True when sharing <paramref name="granted"/> by one handle permits another handle's <paramref name="requested"/> access.</summary>
    public static bool Permits(FileShare granted, FileAccess requested) =>
        ((requested & FileAccess.Read) == 0 || (granted & FileShare.Read) != 0)
        && ((requested & FileAccess.Write) == 0 || (granted & FileShare.Write) != 0);

    /// <summary>
    /// Independent oracle: two valid handles may coexist iff each grants the other's access (checked in both
    /// directions) and they are not both writers (LOCK-01). <see cref="FileShare.Delete"/> plays no part in open compatibility.
    /// </summary>
    public static bool Compatible(HandleSpec a, HandleSpec b) =>
        Permits(a.Share, b.Access) && Permits(b.Share, a.Access) && !(IsWritable(a.Access) && IsWritable(b.Access));

    /// <summary>A proposed handle is admissible iff it is compatible with every active handle on the inode.</summary>
    public static bool CompatibleWithAll(IEnumerable<HandleSpec> existing, HandleSpec proposed) => existing.All(e => Compatible(e, proposed));

    public static DfsOpenOptions Options(FileMode mode, HandleSpec spec, DfsReadView view = DfsReadView.Live) =>
        new() { Mode = mode, Access = spec.Access, Share = spec.Share, ReadView = view };

    /// <summary>Unit-level cluster: the default three-zone in-memory cluster (simulated storage).</summary>
    public static TestClusterOptions UnitCluster(ClusterMode mode = ClusterMode.Realtime) =>
        TestClusterOptions.ThreeZones with { Mode = mode };

    public static TimeSpan Lease(TestCluster cluster) => cluster.Options.Timings.LeaseDuration;

    public static async Task<DfsClient> ConnectAsync(TestCluster cluster, string name, CancellationToken ct) =>
        await cluster.ConnectAsync(new TestClientOptions { Name = name }, ct);

    public static async Task<Exception?> CatchAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>Opens <paramref name="path"/>; returns the stream or the exception.</summary>
    public static async Task<(DfsStream? Stream, Exception? Error)> TryOpenAsync(DfsVolume volume, string path, DfsOpenOptions options, CancellationToken ct)
    {
        try
        {
            return (await volume.Files.OpenAsync(path, options, ct), null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    /// <summary>Reads committed bytes through a maximally sharing live reader (compatible with any valid writer that shares Read).</summary>
    public static async Task<byte[]> ReadCommittedAsync(DfsVolume volume, string path, CancellationToken ct)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path,
            new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite | FileShare.Delete }, ct);
        return await ReadToEndAsync(stream, ct);
    }

    public static async Task<byte[]> ReadToEndAsync(Stream stream, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    /// <summary>Active (non-expired) handles on <paramref name="inode"/> in the leader's applied state.</summary>
    public static async Task<HandleInspection[]> ActiveHandlesAsync(TestCluster cluster, InodeId inode, string volume = Volume)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
        return state.Handles.Where(h => h.Inode == inode && !h.Expired).ToArray();
    }

    /// <summary>Array overload of <see cref="AssertEx.BytesEqual"/> (spans cannot be held across an awaited argument).</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    public static string Describe(Exception? ex) => ex is null
        ? "no exception (the operation succeeded)"
        : $"{ex.GetType().FullName}{(ex is DfsException d ? $" [{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    /// <summary>Asserts a typed, retryable sharing/writer conflict (FSD §13: typed IOException subtype with retryability).</summary>
    public static void AssertSharingConflict(Exception? ex, string because)
    {
        if (ex is not DfsSharingViolationException conflict)
        {
            Assert.Fail($"{because}: expected DfsSharingViolationException but got {Describe(ex)}");
            return;
        }

        Assert.True(conflict.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict,
            $"{because}: sharing conflicts carry SharingViolation or WriterConflict, got {conflict.ErrorCode}");
        Assert.True(conflict.IsRetryable, $"{because}: a sharing conflict must be reported as retryable");
    }

    /// <summary>
    /// Asserts an explicit rejection of an unsupported/invalid open option: <see cref="NotSupportedException"/> (deferred
    /// feature) or an <see cref="ArgumentException"/> (invalid option). A retryable sharing conflict or a silent success
    /// would imply the option is merely contended or supported, so both fail.
    /// </summary>
    public static void AssertExplicitOptionRejection(Exception? ex, string because)
    {
        if (ex is NotSupportedException or ArgumentException)
            return;
        Assert.Fail($"{because}: expected NotSupportedException or ArgumentException (explicit option rejection) but got {Describe(ex)}");
    }

    /// <summary>Completes <paramref name="action"/> within <paramref name="bound"/> (harness time) or fails: used to prove "never waits".</summary>
    public static async Task<Exception?> CatchWithinAsync(TestCluster cluster, Func<Task> action, TimeSpan bound, string because, CancellationToken ct)
    {
        Task<Exception?> attempt = CatchAsync(action);
        Task deadline = cluster.DelayAsync(bound, ct);
        if (await Task.WhenAny(attempt, deadline) != attempt)
            Assert.Fail($"{because}: the call was still blocked after {bound}; conflicts must fail promptly instead of waiting");
        return await attempt;
    }
}
