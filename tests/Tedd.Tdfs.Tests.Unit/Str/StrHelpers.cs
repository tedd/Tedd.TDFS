using System.Buffers;
using System.Runtime.InteropServices;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Str;

/// <summary>The <see cref="Stream"/> read entry points exercised against <see cref="DfsStream"/>.</summary>
public enum ReadOverload
{
    /// <summary><see cref="Stream.Read(byte[], int, int)"/>.</summary>
    ArraySync,
    /// <summary><see cref="Stream.Read(Span{byte})"/>.</summary>
    SpanSync,
    /// <summary><see cref="Stream.ReadAsync(byte[], int, int, CancellationToken)"/>.</summary>
    ArrayAsync,
    /// <summary><see cref="Stream.ReadAsync(Memory{byte}, CancellationToken)"/>.</summary>
    MemoryAsync,
    /// <summary><see cref="Stream.ReadByte"/>.</summary>
    ReadByte,
    /// <summary><see cref="Stream.ReadExactly(byte[], int, int)"/>.</summary>
    ReadExactly,
    /// <summary><see cref="Stream.ReadExactlyAsync(Memory{byte}, CancellationToken)"/>.</summary>
    ReadExactlyAsync,
    /// <summary><see cref="Stream.ReadAtLeast(Span{byte}, int, bool)"/>.</summary>
    ReadAtLeast,
    /// <summary><see cref="Stream.ReadAtLeastAsync(Memory{byte}, int, bool, CancellationToken)"/>.</summary>
    ReadAtLeastAsync,
    /// <summary><see cref="Stream.BeginRead"/> / <see cref="Stream.EndRead"/> (APM).</summary>
    BeginEndRead,
}

/// <summary>The <see cref="Stream"/> write entry points exercised against <see cref="DfsStream"/>.</summary>
public enum WriteOverload
{
    /// <summary><see cref="Stream.Write(byte[], int, int)"/>.</summary>
    ArraySync,
    /// <summary><see cref="Stream.Write(ReadOnlySpan{byte})"/>.</summary>
    SpanSync,
    /// <summary><see cref="Stream.WriteAsync(byte[], int, int, CancellationToken)"/>.</summary>
    ArrayAsync,
    /// <summary><see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    MemoryAsync,
    /// <summary><see cref="Stream.WriteByte"/>.</summary>
    WriteByte,
    /// <summary><see cref="Stream.BeginWrite"/> / <see cref="Stream.EndWrite"/> (APM).</summary>
    BeginEndWrite,
}

/// <summary>Shared helpers for the STR unit rows.</summary>
internal static class StrHelpers
{
    /// <summary>The test volume created by every <see cref="TestClusterOptions"/> preset.</summary>
    public const string Vol = "vol";

    /// <summary>Inline threshold of <see cref="TestVolumeSpec"/> (default 4 KiB).</summary>
    public const int Inline = 4096;

    /// <summary>Default <see cref="DfsOpenOptions.BufferSize"/>.</summary>
    public const int StreamBuffer = 64 * 1024;

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB).</summary>
    public static int Chunk => NodeTimings.Fast.ChunkSize;

    /// <summary>Bound for every liveness wait in these rows.</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(15);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary><see cref="AssertEx.BytesEqual"/> over memory, so an argument list may contain an await.</summary>
    public static void BytesEqual(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string because) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    /// <summary>0, 1, inline ±1, chunk ±1, buffer ±1, 2×chunk ±1 (test plan §5).</summary>
    public static IReadOnlyList<long> BoundaryLengths() => TestData.BoundaryLengths(Inline, Chunk, StreamBuffer);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, TimeSpan? operationTimeout = null, TestClock? clock = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, OperationTimeout = operationTimeout, Clock = clock }, Ct);

    /// <summary>Deterministic bytes without any zero byte, so a resurrected/zeroed range is always detectable.</summary>
    public static byte[] NonZero(ulong seed, int length)
    {
        byte[] data = TestData.Bytes(seed, length);
        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] == 0)
                data[i] = (byte)(1 + (i % 251));
        }

        return data;
    }

    /// <summary>A live/pinned reader that grants every share so writers, deleters and renamers can coexist (test plan §5).</summary>
    public static DfsOpenOptions Reader(DfsReadView view = DfsReadView.Live) => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = view,
    };

    /// <summary>A writer that grants readers (writers may not grant <see cref="FileShare.Write"/>, LOCK-02).</summary>
    public static DfsOpenOptions Writer(FileMode mode = FileMode.Open, FileAccess access = FileAccess.ReadWrite,
        DfsWriteMode writeMode = DfsWriteMode.OnFlush, FileShare share = FileShare.Read) => new()
    {
        Mode = mode,
        Access = access,
        Share = share,
        WriteMode = writeMode,
    };

    /// <summary>
    /// Performs exactly one call of <paramref name="overload"/> into <c>buffer[offset..offset+count)</c> and returns the
    /// number of bytes the call delivered (ReadByte: 1, or 0 at EOF).
    /// </summary>
    public static async Task<int> ReadOnceAsync(Stream s, ReadOverload overload, byte[] buffer, int offset, int count)
    {
        switch (overload)
        {
            case ReadOverload.ArraySync:
                return s.Read(buffer, offset, count);
            case ReadOverload.SpanSync:
                return s.Read(buffer.AsSpan(offset, count));
            case ReadOverload.ArrayAsync:
                return await s.ReadAsync(buffer, offset, count, Ct);
            case ReadOverload.MemoryAsync:
                return await s.ReadAsync(buffer.AsMemory(offset, count), Ct);
            case ReadOverload.ReadByte:
            {
                int b = s.ReadByte();
                if (b < 0)
                    return 0;
                Assert.InRange(b, 0, 255);
                buffer[offset] = (byte)b;
                return 1;
            }
            case ReadOverload.ReadExactly:
                s.ReadExactly(buffer, offset, count);
                return count;
            case ReadOverload.ReadExactlyAsync:
                await s.ReadExactlyAsync(buffer.AsMemory(offset, count), Ct);
                return count;
            case ReadOverload.ReadAtLeast:
                return s.ReadAtLeast(buffer.AsSpan(offset, count), count, throwOnEndOfStream: false);
            case ReadOverload.ReadAtLeastAsync:
                return await s.ReadAtLeastAsync(buffer.AsMemory(offset, count), count, throwOnEndOfStream: false, Ct);
            case ReadOverload.BeginEndRead:
                return await Task.Factory.FromAsync(s.BeginRead, s.EndRead, buffer, offset, count, null);
            default:
                throw new ArgumentOutOfRangeException(nameof(overload), overload, null);
        }
    }

    /// <summary>Writes <c>buffer[offset..offset+count)</c> with one call of <paramref name="overload"/> (WriteByte: one call per byte).</summary>
    public static async Task WriteOnceAsync(Stream s, WriteOverload overload, byte[] buffer, int offset, int count)
    {
        switch (overload)
        {
            case WriteOverload.ArraySync:
                s.Write(buffer, offset, count);
                break;
            case WriteOverload.SpanSync:
                s.Write(buffer.AsSpan(offset, count));
                break;
            case WriteOverload.ArrayAsync:
                await s.WriteAsync(buffer, offset, count, Ct);
                break;
            case WriteOverload.MemoryAsync:
                await s.WriteAsync(buffer.AsMemory(offset, count), Ct);
                break;
            case WriteOverload.WriteByte:
                for (int i = 0; i < count; i++)
                    s.WriteByte(buffer[offset + i]);
                break;
            case WriteOverload.BeginEndWrite:
                await Task.Factory.FromAsync(s.BeginWrite, s.EndWrite, buffer, offset, count, null);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(overload), overload, null);
        }
    }

    /// <summary>Reads from the current position until <paramref name="count"/> bytes or EOF, asserting Position tracks every call.</summary>
    public static async Task<byte[]> ReadUpToAsync(Stream s, int count)
    {
        var result = new byte[count];
        int total = 0;
        while (total < count)
        {
            long before = s.Position;
            int n = await s.ReadAsync(result.AsMemory(total, count - total), Ct);
            Assert.InRange(n, 0, count - total);
            Assert.Equal(before + n, s.Position);
            if (n == 0)
                break;
            total += n;
        }

        return result.AsSpan(0, total).ToArray();
    }

    /// <summary>
    /// Reads the committed contents as another client sees them, through a pinned reader that grants every share (test
    /// plan §5), so a writer handle that is still open is never a setup-level sharing conflict.
    /// </summary>
    public static async Task<byte[]> ReadCommittedAsync(DfsVolume volume, string path)
    {
        await using DfsStream r = await volume.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
        return await ReadWholeAsync(r);
    }

    /// <summary>
    /// Creates <paramref name="path"/> with <paramref name="data"/> published one <see cref="Chunk"/>-sized piece per flush,
    /// so every chunk is a separate published object (the extent layout does not depend on how WriteAll packs objects).
    /// </summary>
    public static async Task SeedOneObjectPerChunkAsync(DfsVolume volume, string path, byte[] data)
    {
        await using DfsStream w = await volume.Files.OpenAsync(path, Writer(FileMode.CreateNew, FileAccess.Write), Ct);
        for (int offset = 0; offset < data.Length; offset += Chunk)
        {
            await w.WriteAsync(data.AsMemory(offset, Math.Min(Chunk, data.Length - offset)), Ct);
            await w.FlushAsync(Ct);
        }
    }

    /// <summary>Seeks to 0 and reads to EOF.</summary>
    public static async Task<byte[]> ReadWholeAsync(Stream s)
    {
        s.Seek(0, SeekOrigin.Begin);
        using var ms = new MemoryStream();
        var buffer = new byte[96 * 1024];
        while (true)
        {
            int n = await s.ReadAsync(buffer, Ct);
            if (n == 0)
                break;
            ms.Write(buffer, 0, n);
        }

        Assert.Equal(ms.Length, s.Position);
        return ms.ToArray();
    }

    /// <summary>
    /// Asserts that <paramref name="ex"/> is an explicit availability failure (or, when allowed, an integrity failure for
    /// data that cannot be produced) — never EOF, absence or a generic success substitute (FSD §§6.2, 13).
    /// <paramref name="allowExpiry"/> additionally accepts a typed lease-lost/pin-expired failure: when the client is cut
    /// off from authority for longer than the 2 s lease, reporting the handle's expiry is an equally explicit failure.
    /// </summary>
    public static void AssertAvailabilityFailure(Exception? ex, bool allowIntegrity, string because, bool allowExpiry = false)
    {
        Assert.True(ex is not null, $"Expected an availability failure but the call succeeded ({because}).");
        bool ok = ex switch
        {
            DfsUnavailableException => true,
            DfsIntegrityException => allowIntegrity,
            DfsLeaseLostException or DfsPinExpiredException => allowExpiry,
            DfsException d => d.ErrorCode is DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable
                or DfsErrorCode.Unavailable or DfsErrorCode.Timeout or DfsErrorCode.NotLeader
                || (allowIntegrity && d.ErrorCode == DfsErrorCode.Integrity),
            TimeoutException => true,
            _ => false,
        };
        Assert.True(ok, $"Expected an explicit availability{(allowIntegrity ? "/integrity" : string.Empty)} failure ({because}) but got {ex!.GetType().FullName}{(ex is DfsException de ? $" ({de.ErrorCode})" : string.Empty)}: {ex.Message}");
        Assert.False(ex is EndOfStreamException or FileNotFoundException or DirectoryNotFoundException,
            $"Unavailability must not be reported as EOF or absence ({because}).");
    }

    /// <summary>Objects referenced by the current revision of <paramref name="path"/> (leader view).</summary>
    public static async Task<IReadOnlyList<ExtentRefInfo>> GetExtentsAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.True(file is not null, $"inspection must find {path}");
        return file!.Extents.Select(e => new ExtentRefInfo(e.Offset, e.Length, e.Object, e.ObjectOffset)).ToArray();
    }

    /// <summary>Asserts every byte in <paramref name="data"/> is zero.</summary>
    public static void AssertAllZero(ReadOnlySpan<byte> data, long baseOffset, string because)
    {
        int index = data.IndexOfAnyExcept((byte)0);
        Assert.True(index < 0, $"Expected zeroes but found 0x{(index < 0 ? 0 : data[index]):X2} at file offset {baseOffset + Math.Max(0, index)} ({because}).");
    }

    /// <summary>Asserts every byte of <paramref name="buffer"/> in [start, start+length) still holds the guard value.</summary>
    public static void AssertGuard(byte[] buffer, int start, int length, byte guard, string because)
    {
        int index = buffer.AsSpan(start, length).IndexOfAnyExcept(guard);
        Assert.True(index < 0, $"The read wrote outside its [offset, offset+count) range at buffer index {start + index} ({because}).");
    }

    /// <summary>Awaits <paramref name="task"/> with a bound on the cluster clock (virtual in deterministic mode).</summary>
    public static async Task<T> Within<T>(this Task<T> task, TestCluster cluster, string because)
    {
        try
        {
            return await task.WaitAsync(Bounded, cluster.Time, Ct);
        }
        catch (TimeoutException) when (!task.IsCompleted)
        {
            throw new TimeoutException($"Not completed within {Bounded}: {because}");
        }
    }

    /// <summary>Awaits <paramref name="task"/> with a bound on the cluster clock (virtual in deterministic mode).</summary>
    public static async Task Within(this Task task, TestCluster cluster, string because)
    {
        try
        {
            await task.WaitAsync(Bounded, cluster.Time, Ct);
        }
        catch (TimeoutException) when (!task.IsCompleted)
        {
            throw new TimeoutException($"Not completed within {Bounded}: {because}");
        }
    }
}

/// <summary>Logical extent of a file as reported by the inspector.</summary>
internal sealed record ExtentRefInfo(long Offset, long Length, ObjectId Object, long ObjectOffset);

/// <summary>
/// A <see cref="MemoryManager{T}"/> over a private array that records every access after the caller has taken its
/// ownership back (STR-028): <see cref="GetSpan"/> and <see cref="Pin"/> after <see cref="Release"/> are violations, and
/// pins still outstanding at release are reported.
/// </summary>
internal sealed unsafe class TrackingMemoryManager : MemoryManager<byte>
{
    private readonly byte[] _array;
    private int _released;
    private int _violations;
    private int _pins;

    public TrackingMemoryManager(int length) => _array = new byte[length];

    /// <summary>The backing array (for the test's own sentinel writes/checks only).</summary>
    public byte[] Array => _array;

    public bool IsReleased => Volatile.Read(ref _released) != 0;

    public int Violations => Volatile.Read(ref _violations);

    public int OutstandingPins => Volatile.Read(ref _pins);

    /// <summary>Marks the memory as returned to the application; any later SDK access is a violation.</summary>
    public void Release() => Volatile.Write(ref _released, 1);

    public override Span<byte> GetSpan()
    {
        if (IsReleased)
            Interlocked.Increment(ref _violations);
        return _array;
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        if (IsReleased)
            Interlocked.Increment(ref _violations);
        Interlocked.Increment(ref _pins);
        GCHandle handle = GCHandle.Alloc(_array, GCHandleType.Pinned);
        return new MemoryHandle((byte*)handle.AddrOfPinnedObject() + elementIndex, handle, this);
    }

    public override void Unpin() => Interlocked.Decrement(ref _pins);

    protected override void Dispose(bool disposing)
    {
    }
}
