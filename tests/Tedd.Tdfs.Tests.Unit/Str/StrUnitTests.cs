using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Str.StrHelpers;

namespace Tedd.Tdfs.Tests.Unit.Str;

/// <summary>Scenario of STR-005.</summary>
public enum EofScenario { EmptyFile, AfterFullRead, PositionBeyondEof, ClientIsolated, QuorumLost, DataLostOnAllReplicas, DataHoldersOffline }

/// <summary>Scenario of STR-007.</summary>
public enum ZeroLengthCase
{
    ReadOnEmptyFile, ReadAtStart, ReadInMiddle, ReadAtEof, ReadBeyondEof,
    WriteBeyondEofOnFlush, WriteBeyondEofWriteThrough, WriteOnNewEmptyFile, WriteOnAppendStream,
    ReadOnWriteOnlyStreamValidates, WriteOnReadOnlyStreamValidates, ReadAfterDisposeValidates, WriteAfterDisposeValidates,
}

/// <summary>Handle shapes of STR-009.</summary>
public enum SeekHandleKind { ReadOnly, WriteOnly, ReadWriteOnFlush, ReadWriteWriteThrough }

/// <summary>Declared views of STR-018.</summary>
public enum LengthView { Live, VersionPinned, WriterStaged, WriterWriteThrough, TransactionBound, AppendStaged }

/// <summary>The in-flight call of an STR-019 overlap pair.</summary>
public enum OverlapFirst { ReadAsyncArray, ReadAsyncMemory, CopyToAsync, WriteAsyncWriteThrough, SetLengthAsyncWriteThrough, FlushAsync, DisposeAsync }

/// <summary>The overlapping call of an STR-019 overlap pair.</summary>
public enum OverlapSecond
{
    ReadSync, ReadAsync, ReadByte, WriteSync, WriteAsync, WriteByte, Seek, SetPosition, SetLength,
    FlushSync, FlushAsync, CommitAsync, DisposeAsync, CopyToAsync, GetLengthAsync,
}

/// <summary>Destination of STR-020.</summary>
public enum CopyDestination { Memory, DfsOnFlush, DfsWriteThrough }

/// <summary>Stream shapes of STR-025.</summary>
public enum DisposedKind { ReadOnlyLive, ReadOnlyPinned, WriteOnly, ReadWrite, Append, TransactionBound }

/// <summary>Disposal entry point of STR-025.</summary>
public enum DisposeMethod { Dispose, DisposeAsync, Close }

/// <summary>Failure of STR-026.</summary>
public enum ReadFailure { TransportFaultAtLeaderReply, TransportFaultMidMultiExtentRead, IntegrityOnAllReplicas, OperationTimeout, ClientIsolated }

/// <summary>Memory-based operation of STR-028.</summary>
public enum MemoryOp { ReadInline, ReadExtent, WriteThroughCommit, OnFlushUpload }

/// <summary>
/// STR — stream reads, writes, seeking and interoperability: the U-level rows (FSD §§5,6; test plan §7.5).
/// Every row runs against a three-zone in-memory cluster through the public SDK; the plan's required assertion is
/// checked against an independent oracle (deterministic source bytes or <see cref="ReferenceFileModel"/>).
/// </summary>
public sealed class StrUnitTests
{
    private static TestClusterOptions Core => TestClusterOptions.ThreeZones;

    private static Task Sync(Action action)
    {
        action();
        return Task.CompletedTask;
    }

    private static async Task<int> AsInt(Task task)
    {
        await task;
        return 0;
    }

    // ------------------------------------------------------------------ STR-001

    public static TheoryData<ReadOverload> AllReadOverloads()
    {
        var data = new TheoryData<ReadOverload>();
        foreach (ReadOverload overload in Enum.GetValues<ReadOverload>())
            data.Add(overload);
        return data;
    }

    /// <summary>
    /// Every read entry point delivers exactly the committed bytes for every boundary length (0, 1, inline ±1, chunk ±1,
    /// buffer ±1, 2×chunk ±1), never writes outside [offset, offset+count), advances Position by exactly the delivered
    /// count and follows the byte/EOF contract (0 / -1 at EOF, EndOfStreamException only for a genuine premature EOF).
    /// </summary>
    [Theory]
    [MemberData(nameof(AllReadOverloads))]
    [Catalog("STR-001", "U", "INV-03", "INV-07")]
    public async Task STR_001_AllReadOverloadsAgree(ReadOverload overload)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            foreach (long length in BoundaryLengths())
            {
                int len = checked((int)length);
                string path = $"/str001-{overload}-{len}.bin";
                byte[] expected = TestData.Bytes(0x5001_0000UL + (ulong)len, len); // the same sequence for every overload
                await vol.Files.WriteAllBytesAsync(path, expected, Ct);

                await using DfsStream s = await vol.Files.OpenReadAsync(path, Ct);
                Assert.True(s.CanRead, "OpenRead returns a readable stream");
                Assert.False(s.CanWrite, "OpenRead returns a read-only stream");
                Assert.True(s.CanSeek, "OpenRead returns a seekable stream");
                Assert.Equal(len, s.Length);
                Assert.Equal(0, s.Position);

                if (overload == ReadOverload.ReadByte && len > 3 * Inline)
                    VerifyReadByteWindows(s, expected);
                else
                    BytesEqual(expected, await ReadThroughAsync(s, overload, expected), $"{overload} over a {len}-byte file");

                await AssertEofContractAsync(s, overload, expected);
            }
        }, Ct);
    }

    private static async Task<byte[]> ReadThroughAsync(DfsStream s, ReadOverload overload, byte[] expected)
    {
        const int Guard = 8;
        const byte GuardValue = 0xCD;
        int len = expected.Length;
        int[] schedule = len <= 3 * Inline ? [1, 7, Inline, 3, 513] : [Inline - 3, StreamBuffer, StreamBuffer + 3, 1 << 20, 1];
        bool exact = overload is ReadOverload.ReadExactly or ReadOverload.ReadExactlyAsync
            or ReadOverload.ReadAtLeast or ReadOverload.ReadAtLeastAsync;
        var result = new byte[len];
        int pos = 0;
        for (int call = 0; pos < len; call++)
        {
            int request = schedule[call % schedule.Length];
            if (exact)
                request = Math.Min(request, len - pos); // past-EOF behaviour of the filling helpers is checked separately
            var buffer = new byte[request + 2 * Guard];
            buffer.AsSpan().Fill(GuardValue);
            int n = await ReadOnceAsync(s, overload, buffer, Guard, request);
            string label = $"{overload} call #{call} at offset {pos} requesting {request} of a {len}-byte file";
            Assert.True(n >= 1 && n <= request, $"A nonempty read before EOF returns 1..count bytes; got {n} ({label}).");
            if (exact)
                Assert.Equal(request, n);
            AssertGuard(buffer, 0, Guard, GuardValue, label);
            AssertGuard(buffer, Guard + request, Guard, GuardValue, label);
            BytesEqual(expected.AsMemory(pos, n), buffer.AsMemory(Guard, n), label);
            buffer.AsSpan(Guard, n).CopyTo(result.AsSpan(pos));
            pos += n;
            Assert.Equal(pos, s.Position);
        }

        return result;
    }

    private static void VerifyReadByteWindows(DfsStream s, byte[] expected)
    {
        int len = expected.Length;
        foreach (int start in new[] { 0, Inline - 300, Chunk - 300, len - 300 })
        {
            int from = Math.Clamp(start, 0, len);
            s.Seek(from, SeekOrigin.Begin);
            for (int p = from; p < Math.Min(len, from + 600); p++)
            {
                int b = s.ReadByte();
                Assert.True(b == expected[p], $"ReadByte at offset {p} returned {b}, expected {expected[p]}.");
                Assert.Equal(p + 1, s.Position);
            }
        }
    }

    private static async Task AssertEofContractAsync(DfsStream s, ReadOverload overload, byte[] expected)
    {
        int len = expected.Length;
        Assert.Equal(len, s.Seek(0, SeekOrigin.End));
        var buffer = new byte[16];
        switch (overload)
        {
            case ReadOverload.ReadExactly or ReadOverload.ReadExactlyAsync:
                await Assert.ThrowsAsync<EndOfStreamException>(() => ReadOnceAsync(s, overload, buffer, 0, 1));
                Assert.Equal(len, s.Position);
                if (len >= 3)
                {
                    s.Seek(len - 3, SeekOrigin.Begin);
                    await Assert.ThrowsAsync<EndOfStreamException>(() => ReadOnceAsync(s, overload, buffer, 0, 5));
                    Assert.Equal(len, s.Position); // the three genuine bytes were consumed before the genuine EOF
                    BytesEqual(expected.AsMemory(len - 3), buffer.AsMemory(0, 3), "bytes delivered before a genuine premature EOF");
                }

                break;
            case ReadOverload.ReadAtLeast or ReadOverload.ReadAtLeastAsync:
                Assert.Equal(0, await ReadOnceAsync(s, overload, buffer, 0, 16));
                Assert.Equal(len, s.Position);
                await Assert.ThrowsAsync<EndOfStreamException>(async () =>
                {
                    if (overload == ReadOverload.ReadAtLeast)
                        s.ReadAtLeast(buffer, 1, throwOnEndOfStream: true);
                    else
                        await s.ReadAtLeastAsync(buffer, 1, throwOnEndOfStream: true, Ct);
                });
                if (len >= 3)
                {
                    s.Seek(len - 3, SeekOrigin.Begin);
                    Assert.Equal(3, await ReadOnceAsync(s, overload, buffer, 0, 16));
                    BytesEqual(expected.AsMemory(len - 3), buffer.AsMemory(0, 3), "ReadAtLeast without throwing returns the genuine tail");
                    Assert.Equal(len, s.Position);
                }

                break;
            default:
                for (int i = 0; i < 2; i++)
                {
                    Assert.Equal(0, await ReadOnceAsync(s, overload, buffer, 0, 16));
                    Assert.Equal(len, s.Position);
                }

                if (overload == ReadOverload.ReadByte)
                    Assert.Equal(-1, s.ReadByte());
                break;
        }

        // Beyond EOF: reads report EOF and neither move Position nor extend the file.
        s.Seek(len + 10, SeekOrigin.Begin);
        if (overload is ReadOverload.ReadExactly or ReadOverload.ReadExactlyAsync)
            await Assert.ThrowsAsync<EndOfStreamException>(() => ReadOnceAsync(s, overload, buffer, 0, 1));
        else
            Assert.Equal(0, await ReadOnceAsync(s, overload, buffer, 0, 16));
        Assert.Equal(len + 10, s.Position);
        Assert.Equal(len, s.Length);
    }

    // ------------------------------------------------------------------ STR-002

    public static TheoryData<WriteOverload, DfsWriteMode> WriteOverloadsAndModes()
    {
        var data = new TheoryData<WriteOverload, DfsWriteMode>();
        foreach (WriteOverload overload in Enum.GetValues<WriteOverload>())
        {
            data.Add(overload, DfsWriteMode.OnFlush);
            data.Add(overload, DfsWriteMode.WriteThrough);
        }

        return data;
    }

    /// <summary>
    /// The same byte sequence written through every write entry point yields identical staged contents (the writer's own
    /// view), identical visibility (OnFlush private until flush, WriteThrough published per call) and identical committed
    /// contents, length and inode identity.
    /// </summary>
    [Theory]
    [MemberData(nameof(WriteOverloadsAndModes))]
    [Catalog("STR-002", "U", "INV-02", "INV-07")]
    public async Task STR_002_AllWriteOverloadsAgree(WriteOverload overload, DfsWriteMode mode)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            IReadOnlyList<long> lengths = overload == WriteOverload.WriteByte && mode == DfsWriteMode.WriteThrough
                ? [0, 1, 2, 63] // one commit per byte: keep the publish-per-call family small
                : BoundaryLengths();
            int[] schedule = [1, Inline - 1, 7, Chunk, Chunk + 1, 333];
            foreach (long length in lengths)
            {
                int len = checked((int)length);
                string path = $"/str002-{overload}-{mode}-{len}.bin";
                byte[] expected = TestData.Bytes(0x5002_0000UL + (ulong)len, len); // same sequence for every overload
                InodeId inode;
                DfsStream w = await wv.Files.OpenAsync(path, Writer(FileMode.CreateNew, FileAccess.ReadWrite, mode), Ct);
                await using (w)
                {
                    inode = w.InodeId;
                    int pos = 0;
                    for (int call = 0; pos < len; call++)
                    {
                        int n = Math.Min(schedule[call % schedule.Length], len - pos);
                        var source = new byte[n + 5]; // the data sits at a nonzero offset of a larger array
                        expected.AsSpan(pos, n).CopyTo(source.AsSpan(3));
                        await WriteOnceAsync(w, overload, source, 3, n);
                        pos += n;
                        Assert.Equal(pos, w.Position);
                        Assert.Equal(pos, w.Length);
                    }

                    BytesEqual(expected, await ReadWholeAsync(w), $"staged contents seen by the writer ({overload}, {mode}, {len} bytes)");
                    byte[] visible = await ReadCommittedAsync(ov, path);
                    if (mode == DfsWriteMode.OnFlush)
                    {
                        Assert.True(visible.Length == 0, $"OnFlush writes stay private until flush, but another client saw {visible.Length} bytes.");
                        Assert.Equal(len > 0, w.PendingBytes > 0);
                    }
                    else
                    {
                        BytesEqual(expected, visible, "WriteThrough publishes each call before returning");
                        Assert.Equal(0, w.PendingBytes);
                    }

                    await w.FlushAsync(Ct);
                    Assert.Equal(0, w.PendingBytes);
                }

                BytesEqual(expected, await ReadCommittedAsync(ov, path), $"committed contents ({overload}, {mode}, {len} bytes)");
                DfsFileInfo info = await ov.Files.GetInfoAsync(path, Ct);
                Assert.Equal(len, info.Length);
                Assert.Equal(inode, info.InodeId);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-003

    private sealed record ArgumentCase(Type? Expected, Func<DfsStream, Task> Act);

    private static readonly IReadOnlyDictionary<string, ArgumentCase> BufferArgumentCases =
        new Dictionary<string, ArgumentCase>(StringComparer.Ordinal)
        {
            ["Read(null,0,0)"] = new(typeof(ArgumentNullException), s => Sync(() => s.Read(null!, 0, 0))),
            ["Read(buf,-1,4)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.Read(new byte[32], -1, 4))),
            ["Read(buf,0,-1)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.Read(new byte[32], 0, -1))),
            ["Read(buf,8,32)"] = new(typeof(ArgumentException), s => Sync(() => s.Read(new byte[32], 8, 32))),
            ["Read(buf,33,0)"] = new(typeof(ArgumentException), s => Sync(() => s.Read(new byte[32], 33, 0))),
            ["Read(buf,MaxInt,MaxInt)"] = new(typeof(ArgumentException), s => Sync(() => s.Read(new byte[32], int.MaxValue, int.MaxValue))),
            ["ReadAsync(null,0,0)"] = new(typeof(ArgumentNullException), s => s.ReadAsync(null!, 0, 0, Ct)),
            ["ReadAsync(buf,-1,4)"] = new(typeof(ArgumentOutOfRangeException), s => s.ReadAsync(new byte[32], -1, 4, Ct)),
            ["ReadAsync(buf,0,-1)"] = new(typeof(ArgumentOutOfRangeException), s => s.ReadAsync(new byte[32], 0, -1, Ct)),
            ["ReadAsync(buf,8,32)"] = new(typeof(ArgumentException), s => s.ReadAsync(new byte[32], 8, 32, Ct)),
            ["ReadExactly(null,0,0)"] = new(typeof(ArgumentNullException), s => Sync(() => s.ReadExactly(null!, 0, 0))),
            ["ReadExactly(buf,0,33)"] = new(typeof(ArgumentException), s => Sync(() => s.ReadExactly(new byte[32], 0, 33))),
            ["ReadAtLeast(buf,-1)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.ReadAtLeast(new byte[32], -1))),
            ["ReadAtLeast(buf,33)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.ReadAtLeast(new byte[32], 33))),
            ["Write(null,0,0)"] = new(typeof(ArgumentNullException), s => Sync(() => s.Write(null!, 0, 0))),
            ["Write(buf,-1,4)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.Write(new byte[32], -1, 4))),
            ["Write(buf,0,-1)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.Write(new byte[32], 0, -1))),
            ["Write(buf,8,32)"] = new(typeof(ArgumentException), s => Sync(() => s.Write(new byte[32], 8, 32))),
            ["Write(buf,MaxInt,MaxInt)"] = new(typeof(ArgumentException), s => Sync(() => s.Write(new byte[32], int.MaxValue, int.MaxValue))),
            ["WriteAsync(null,0,0)"] = new(typeof(ArgumentNullException), s => s.WriteAsync(null!, 0, 0, Ct)),
            ["WriteAsync(buf,-1,4)"] = new(typeof(ArgumentOutOfRangeException), s => s.WriteAsync(new byte[32], -1, 4, Ct)),
            ["WriteAsync(buf,0,-1)"] = new(typeof(ArgumentOutOfRangeException), s => s.WriteAsync(new byte[32], 0, -1, Ct)),
            ["WriteAsync(buf,8,32)"] = new(typeof(ArgumentException), s => s.WriteAsync(new byte[32], 8, 32, Ct)),
            ["CopyTo(null)"] = new(typeof(ArgumentNullException), s => Sync(() => s.CopyTo(null!))),
            ["CopyTo(dest,0)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.CopyTo(new MemoryStream(), 0))),
            ["CopyTo(dest,-1)"] = new(typeof(ArgumentOutOfRangeException), s => Sync(() => s.CopyTo(new MemoryStream(), -1))),
            ["CopyToAsync(null)"] = new(typeof(ArgumentNullException), s => s.CopyToAsync(null!, Ct)),
            ["CopyToAsync(dest,0)"] = new(typeof(ArgumentOutOfRangeException), s => s.CopyToAsync(new MemoryStream(), 0, Ct)),
            // Zero-sized buffers are valid requests with no effect.
            ["Read(empty,0,0)"] = new(null, s => Sync(() => Assert.Equal(0, s.Read([], 0, 0)))),
            ["Read(buf,32,0)"] = new(null, s => Sync(() => Assert.Equal(0, s.Read(new byte[32], 32, 0)))),
            ["Read(Span.Empty)"] = new(null, s => Sync(() => Assert.Equal(0, s.Read(Span<byte>.Empty)))),
            ["ReadAsync(empty,0,0)"] = new(null, async s => Assert.Equal(0, await s.ReadAsync(Array.Empty<byte>(), 0, 0, Ct))),
            ["ReadAsync(Memory.Empty)"] = new(null, async s => Assert.Equal(0, await s.ReadAsync(Memory<byte>.Empty, Ct))),
            ["Write(empty,0,0)"] = new(null, s => Sync(() => s.Write([], 0, 0))),
            ["Write(buf,32,0)"] = new(null, s => Sync(() => s.Write(new byte[32], 32, 0))),
            ["Write(ReadOnlySpan.Empty)"] = new(null, s => Sync(() => s.Write(ReadOnlySpan<byte>.Empty))),
            ["WriteAsync(empty,0,0)"] = new(null, s => s.WriteAsync(Array.Empty<byte>(), 0, 0, Ct)),
            ["WriteAsync(ReadOnlyMemory.Empty)"] = new(null, s => s.WriteAsync(ReadOnlyMemory<byte>.Empty, Ct).AsTask()),
        };

    public static TheoryData<string> BufferArgumentCaseNames()
    {
        var data = new TheoryData<string>();
        foreach (string name in BufferArgumentCases.Keys)
            data.Add(name);
        return data;
    }

    private sealed record StreamState(long Position, long Length, long PendingBytes, bool IsFaulted, long CommittedLength, ulong CommittedRevision);

    private static async Task<StreamState> CaptureAsync(DfsStream s, DfsVolume observer, string path)
    {
        DfsFileInfo info = await observer.Files.GetInfoAsync(path, Ct);
        return new StreamState(s.Position, s.Length, s.PendingBytes, s.IsFaulted, info.Length, info.Revision);
    }

    /// <summary>
    /// Invalid buffer arguments raise the standard argument exception and zero-sized buffers are accepted, in both cases
    /// without moving Position, changing the staged/committed length or revision, adding pending bytes or faulting the stream.
    /// </summary>
    [Theory]
    [MemberData(nameof(BufferArgumentCaseNames))]
    [Catalog("STR-003", "U", "INV-07", "INV-16")]
    public async Task STR_003_BufferArgumentValidation(string caseName)
    {
        ArgumentCase c = BufferArgumentCases[caseName];
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str003.bin";
            byte[] initial = NonZero(0x5003_0000, 10_000);
            await wv.Files.WriteAllBytesAsync(path, initial, Ct);
            var model = new ReferenceFileModel(initial);

            await using DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite, DfsWriteMode.OnFlush), Ct);
            byte[] pending = NonZero(0x5003_0001, 10);
            s.Seek(50, SeekOrigin.Begin);
            s.Write(pending);
            model.Write(50, pending);
            s.Seek(100, SeekOrigin.Begin);
            StreamState before = await CaptureAsync(s, ov, path);

            Exception? ex = await Record.ExceptionAsync(() => c.Act(s));
            if (c.Expected is null)
            {
                Assert.True(ex is null, $"{caseName}: a zero-sized buffer is a valid request and must not throw, got {ex}");
            }
            else
            {
                Assert.True(ex is not null, $"{caseName}: expected {c.Expected.Name} but the call succeeded");
                Assert.True(c.Expected.IsInstanceOfType(ex), $"{caseName}: expected {c.Expected.Name} but got {ex!.GetType().FullName}: {ex.Message}");
            }

            StreamState after = await CaptureAsync(s, ov, path);
            Assert.Equal(before, after);
            BytesEqual(model.Staged, await ReadWholeAsync(s), $"{caseName}: the writer's staged view is unchanged");
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{caseName}: the committed view is unchanged");
            await s.FlushAsync(Ct);
            model.Flush();
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{caseName}: the next flush publishes only the accepted bytes");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-004

    public static TheoryData<ReadOverload, int> ShortReadCases()
    {
        var data = new TheoryData<ReadOverload, int>();
        foreach (ReadOverload overload in new[] { ReadOverload.ArraySync, ReadOverload.SpanSync, ReadOverload.ArrayAsync, ReadOverload.MemoryAsync, ReadOverload.BeginEndRead })
        {
            foreach (int max in new[] { 1, 997, 65_535 })
                data.Add(overload, max);
        }

        return data;
    }

    /// <summary>
    /// With the backend forced to return verified short results, every call returns a count in 1..requested of exactly
    /// the right bytes, Position advances by that count only, Length is unaffected and EOF is reported only at the end.
    /// </summary>
    [Theory]
    [MemberData(nameof(ShortReadCases))]
    [Catalog("STR-004", "U", "INV-07", "INV-16")]
    public async Task STR_004_ShortReadIsValid(ReadOverload overload, int maxBytesPerResult)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            int[] lengths = maxBytesPerResult == 1 ? [3000, Inline + 1904] : [3000, 3 * Chunk + 777];
            var files = new List<(string Path, byte[] Data)>();
            foreach (int len in lengths)
            {
                string path = $"/str004-{len}.bin";
                byte[] data = TestData.Bytes(0x5004_0000UL + (ulong)len, len);
                await vol.Files.WriteAllBytesAsync(path, data, Ct);
                files.Add((path, data));
            }

            ReadResultLimitRule rule = cluster.Faults.LimitReadResultBytes(maxBytesPerResult, volume: Vol);
            int apiShortReads = 0;
            int calls = 0;
            foreach ((string path, byte[] expected) in files)
            {
                await using DfsStream s = await vol.Files.OpenReadAsync(path, Ct);
                int[] requests = [expected.Length, Inline, Chunk + 5];
                int pos = 0;
                for (int call = 0; ; call++)
                {
                    int request = requests[call % requests.Length];
                    var buffer = new byte[request];
                    long before = s.Position;
                    int n = await ReadOnceAsync(s, overload, buffer, 0, request);
                    calls++;
                    string label = $"{overload} call #{call} at {pos} of {path}, backend limit {maxBytesPerResult}";
                    if (pos == expected.Length)
                    {
                        Assert.True(n == 0, $"At EOF a read returns 0, got {n} ({label}).");
                        Assert.Equal(pos, s.Position);
                        break;
                    }

                    int available = Math.Min(request, expected.Length - pos);
                    Assert.True(n >= 1 && n <= request, $"A short read returns the actual count (1..{request}); got {n} ({label}).");
                    if (n < available)
                        apiShortReads++;
                    BytesEqual(expected.AsMemory(pos, n), buffer.AsMemory(0, n), label);
                    pos += n;
                    Assert.Equal(before + n, s.Position);
                    Assert.Equal(pos, s.Seek(0, SeekOrigin.Current));
                }

                Assert.Equal(expected.Length, pos);
                Assert.Equal(expected.Length, s.Length);
            }

            Assert.True(rule.Applied > 0, "The backend must actually have produced shortened read results.");
            Log($"STR-004 {overload}/{maxBytesPerResult}: {rule.Applied} backend short results, {apiShortReads} short API reads in {calls} calls.");
            rule.Cancel();
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-005

    private static readonly ReadOverload[] DirectReadOverloads =
        [ReadOverload.ArraySync, ReadOverload.SpanSync, ReadOverload.ArrayAsync, ReadOverload.MemoryAsync, ReadOverload.ReadByte, ReadOverload.BeginEndRead];

    /// <summary>
    /// Zero is returned only at a genuine EOF of the selected revision. When the authority or the data needed to answer is
    /// unavailable, every read overload (and the network-backed length) throws an explicit availability/integrity
    /// failure, never 0, never zero-filled bytes, and Position is unchanged; once the prerequisites return, reads succeed.
    /// </summary>
    [Theory]
    [InlineData(EofScenario.EmptyFile)]
    [InlineData(EofScenario.AfterFullRead)]
    [InlineData(EofScenario.PositionBeyondEof)]
    [InlineData(EofScenario.ClientIsolated)]
    [InlineData(EofScenario.QuorumLost)]
    [InlineData(EofScenario.DataLostOnAllReplicas)]
    [InlineData(EofScenario.DataHoldersOffline)]
    [Catalog("STR-005", "U", "INV-03", "INV-07")]
    public async Task STR_005_EofIsNotUnavailability(EofScenario scenario)
    {
        TestClusterOptions options = scenario == EofScenario.DataHoldersOffline
            ? TestClusterOptions.FiveNodes with
            {
                DefaultPolicy = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 },
            }
            : Core;
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            DfsVolume wv = writer.GetVolume(Vol);
            const string path = "/str005.bin";
            switch (scenario)
            {
                case EofScenario.EmptyFile:
                {
                    await wv.Files.WriteAllBytesAsync(path, ReadOnlyMemory<byte>.Empty, Ct);
                    await using DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    Assert.Equal(0, s.Length);
                    await AssertEofAllOverloadsAsync(s, 0, "empty file");
                    await Assert.ThrowsAsync<EndOfStreamException>(() => s.ReadExactlyAsync(new byte[1], Ct).AsTask());
                    Assert.Equal(0, s.Position);
                    break;
                }

                case EofScenario.AfterFullRead:
                {
                    byte[] data = NonZero(0x5005_0001, 3 * Chunk + 5);
                    await wv.Files.WriteAllBytesAsync(path, data, Ct);
                    await using DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    BytesEqual(data, await ReadWholeAsync(s), "full read before EOF");
                    await AssertEofAllOverloadsAsync(s, data.Length, "after reading the whole file");
                    break;
                }

                case EofScenario.PositionBeyondEof:
                {
                    byte[] data = NonZero(0x5005_0002, 10_000);
                    await wv.Files.WriteAllBytesAsync(path, data, Ct);
                    await using DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    s.Seek(data.Length + 4096, SeekOrigin.Begin);
                    await AssertEofAllOverloadsAsync(s, data.Length + 4096, "positioned beyond EOF");
                    Assert.Equal(data.Length, s.Length);
                    break;
                }

                case EofScenario.ClientIsolated:
                case EofScenario.QuorumLost:
                {
                    byte[] data = NonZero(0x5005_0003, 3 * Chunk);
                    await wv.Files.WriteAllBytesAsync(path, data, Ct);
                    await using DfsClient reader = await ConnectAsync(cluster, "r", operationTimeout: TimeSpan.FromSeconds(1));
                    DfsVolume rv = reader.GetVolume(Vol);
                    DfsStream s = await rv.Files.OpenReadAsync(path, Ct);
                    BytesEqual(data.AsMemory(0, 100), await ReadUpToAsync(s, 100), "read before the outage");

                    TestNode[] crashed = [];
                    if (scenario == EofScenario.ClientIsolated)
                    {
                        cluster.Network.Isolate("client:r");
                    }
                    else
                    {
                        crashed = [cluster[1], cluster[2]];
                        foreach (TestNode node in crashed)
                            await node.CrashAsync();
                    }

                    foreach (ReadOverload overload in new[] { ReadOverload.ArraySync, ReadOverload.MemoryAsync, ReadOverload.ReadByte })
                    {
                        foreach (long position in new long[] { 100, data.Length })
                            await AssertReadFailsWithoutEofAsync(s, overload, position, allowIntegrity: false, $"{scenario}, {overload} at {position}");
                    }

                    foreach (ReadOverload overload in new[] { ReadOverload.SpanSync, ReadOverload.ArrayAsync })
                        await AssertReadFailsWithoutEofAsync(s, overload, 100, allowIntegrity: false, $"{scenario}, {overload} at 100");

                    AssertAvailabilityFailure(await Record.ExceptionAsync(() => s.GetLengthAsync(Ct).AsTask()), false,
                        $"{scenario}: the live length cannot be answered without authority and must not be reported as 0 or stale");

                    if (scenario == EofScenario.ClientIsolated)
                    {
                        cluster.Network.Heal();
                    }
                    else
                    {
                        foreach (TestNode node in crashed)
                            await node.RestartAsync();
                        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                    }

                    await AssertOldOrNewStreamRecoversAsync(cluster, rv, s, path, data, 100, $"{scenario} after recovery");
                    break;
                }

                case EofScenario.DataLostOnAllReplicas:
                case EofScenario.DataHoldersOffline:
                {
                    byte[] data = NonZero(0x5005_0004, 3 * Chunk);
                    await wv.Files.WriteAllBytesAsync(path, data, Ct);
                    long target = Chunk + 10;
                    ExtentRefInfo extent = (await GetExtentsAsync(cluster, path)).Single(e => e.Offset <= target && target < e.Offset + e.Length);
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(extent.Object);
                    Assert.True(holders.Count > 0, "precondition: the extent has durable holders");
                    var crashed = new List<TestNode>();
                    if (scenario == EofScenario.DataLostOnAllReplicas)
                    {
                        foreach (string holder in holders)
                            await cluster.Node(holder).Disk.DeleteObjectAsync(extent.Object);
                    }
                    else
                    {
                        Assert.True(holders.Count <= 2, $"precondition: policy 2 copies, holders {string.Join(",", holders)}");
                        foreach (string holder in holders)
                        {
                            TestNode node = cluster.Node(holder);
                            await node.CrashAsync();
                            crashed.Add(node);
                        }
                    }

                    // A reader that never cached the object (fresh client).
                    await using DfsClient reader = await ConnectAsync(cluster, "r", operationTimeout: TimeSpan.FromSeconds(2));
                    DfsVolume rv = reader.GetVolume(Vol);
                    DfsStream s = await rv.Files.OpenReadAsync(path, Ct);
                    Assert.Equal(data.Length, s.Length);
                    foreach (ReadOverload overload in new[] { ReadOverload.ArraySync, ReadOverload.SpanSync, ReadOverload.ArrayAsync, ReadOverload.MemoryAsync, ReadOverload.ReadByte })
                        await AssertReadFailsWithoutEofAsync(s, overload, target, allowIntegrity: true, $"{scenario}, {overload}");
                    AssertAvailabilityFailure(await Record.ExceptionAsync(() => rv.Files.ReadAllBytesAsync(path, Ct)), true,
                        $"{scenario}: whole-file read must not zero-fill the unavailable extent");

                    if (scenario == EofScenario.DataHoldersOffline)
                    {
                        foreach (TestNode node in crashed)
                            await node.RestartAsync();
                        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                        await AssertOldOrNewStreamRecoversAsync(cluster, rv, s, path, data, target, "holders back online");
                    }
                    else
                    {
                        // No surviving copy: the failure is permanent and is never replaced by invented bytes.
                        await AssertReadFailsWithoutEofAsync(s, ReadOverload.MemoryAsync, target, allowIntegrity: true, "lost data, second attempt");
                        await s.DisposeAsync();
                    }

                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null);
            }
        }, Ct);
    }

    private static async Task AssertEofAllOverloadsAsync(DfsStream s, long position, string because)
    {
        foreach (ReadOverload overload in DirectReadOverloads.Append(ReadOverload.ReadAtLeast).Append(ReadOverload.ReadAtLeastAsync))
        {
            for (int i = 0; i < 2; i++)
            {
                int n = await ReadOnceAsync(s, overload, new byte[64], 0, 64);
                Assert.True(n == 0, $"{overload} at EOF must return 0 ({because}), got {n}.");
                Assert.Equal(position, s.Position);
            }
        }

        Assert.Equal(-1, s.ReadByte());
        Assert.Equal(position, s.Position);
    }

    private static async Task AssertReadFailsWithoutEofAsync(DfsStream s, ReadOverload overload, long position, bool allowIntegrity, string because)
    {
        s.Seek(position, SeekOrigin.Begin);
        var buffer = new byte[64];
        int? returned = null;
        Exception? ex = await Record.ExceptionAsync(async () => returned = await ReadOnceAsync(s, overload, buffer, 0, buffer.Length));
        Assert.True(returned is null, $"The read must fail, but it returned {returned} ({because}); 0 would be a fabricated EOF.");
        AssertAvailabilityFailure(ex, allowIntegrity, because);
        Assert.Equal(position, s.Position);
    }

    /// <summary>
    /// After the outage the same live stream either resumes with correct bytes or (when its lease/pin expired during the
    /// outage) fails explicitly; in that case a new stream must read the correct bytes within the bound.
    /// </summary>
    private static async Task AssertOldOrNewStreamRecoversAsync(TestCluster cluster, DfsVolume volume, DfsStream old, string path, byte[] data, long position, string because)
    {
        bool oldUsable = true;
        await cluster.WaitUntilAsync(async () =>
        {
            DfsStream s = old;
            try
            {
                if (!oldUsable)
                    s = await volume.Files.OpenReadAsync(path, Ct);
                s.Seek(position, SeekOrigin.Begin);
                byte[] got = await ReadUpToAsync(s, 64);
                BytesEqual(data.AsMemory((int)position, got.Length), got, because);
                return got.Length > 0;
            }
            catch (Exception ex) when (ex is DfsLeaseLostException or DfsPinExpiredException)
            {
                oldUsable = false;
                return false;
            }
            catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException || ex is DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.Timeout or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable })
            {
                return false;
            }
            finally
            {
                if (!ReferenceEquals(s, old))
                    await s.DisposeAsync();
            }
        }, Bounded, $"reads succeed with the committed bytes once the prerequisites are restored ({because})", Ct);
        try
        {
            await old.DisposeAsync();
        }
        catch (Exception ex) when (ex is DfsLeaseLostException or DfsPinExpiredException)
        {
            // An expired read-only handle may report its expiry on disposal; nothing was pending.
        }
    }

    // ------------------------------------------------------------------ STR-006

    public static TheoryData<int, bool> ReadExactlyCases()
    {
        var data = new TheoryData<int, bool>();
        foreach (int max in new[] { 1, 100, 4097 })
        {
            data.Add(max, false);
            data.Add(max, true);
        }

        return data;
    }

    private static async Task ExactAsync(Stream s, byte[] buffer, int offset, int count, bool useAsync)
    {
        if (useAsync)
            await s.ReadExactlyAsync(buffer.AsMemory(offset, count), Ct);
        else
            s.ReadExactly(buffer, offset, count);
    }

    /// <summary>
    /// With forced backend short results, ReadExactly/ReadAtLeast assemble exactly the requested bytes from multiple
    /// short reads; a request crossing the genuine end throws EndOfStreamException after delivering the genuine tail.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReadExactlyCases))]
    [Catalog("STR-006", "U", "INV-07")]
    public async Task STR_006_ReadExactlyHandlesShortReads(int maxBytesPerResult, bool useAsync)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            const string path = "/str006.bin";
            int len = maxBytesPerResult == 1 ? Inline + 1904 : 2 * Chunk + 17;
            byte[] data = TestData.Bytes(0x5006_0000UL + (ulong)maxBytesPerResult, len);
            await vol.Files.WriteAllBytesAsync(path, data, Ct);
            ReadResultLimitRule rule = cluster.Faults.LimitReadResultBytes(maxBytesPerResult, volume: Vol);

            await using DfsStream s = await vol.Files.OpenReadAsync(path, Ct);
            var whole = new byte[len];
            await ExactAsync(s, whole, 0, len, useAsync);
            BytesEqual(data, whole, "ReadExactly of the whole file");
            Assert.Equal(len, s.Position);

            var windows = new List<(int Start, int Count)> { (Inline - 3, 10), (0, 1), (len / 2, len / 3) };
            if (len > Chunk + Chunk / 2)
                windows.Add((Chunk - 5, Chunk / 2));
            foreach ((int start, int count) in windows)
            {
                s.Seek(start, SeekOrigin.Begin);
                var buffer = new byte[count + 4];
                await ExactAsync(s, buffer, 2, count, useAsync);
                BytesEqual(data.AsMemory(start, count), buffer.AsMemory(2, count), $"ReadExactly window {start}+{count}");
                Assert.Equal(start + count, s.Position);
            }

            s.Seek(0, SeekOrigin.Begin);
            var atLeast = new byte[Math.Min(5000, len)];
            int minimum = atLeast.Length / 2;
            int n = useAsync
                ? await s.ReadAtLeastAsync(atLeast, minimum, throwOnEndOfStream: true, Ct)
                : s.ReadAtLeast(atLeast, minimum, throwOnEndOfStream: true);
            Assert.InRange(n, minimum, atLeast.Length);
            BytesEqual(data.AsMemory(0, n), atLeast.AsMemory(0, n), "ReadAtLeast assembles at least the minimum");
            Assert.Equal(n, s.Position);

            // A genuine premature EOF: the genuine tail is delivered, then EndOfStreamException.
            s.Seek(len - 10, SeekOrigin.Begin);
            var tail = new byte[25];
            await Assert.ThrowsAsync<EndOfStreamException>(() => ExactAsync(s, tail, 0, 25, useAsync));
            Assert.Equal(len, s.Position);
            BytesEqual(data.AsMemory(len - 10), tail.AsMemory(0, 10), "the genuine tail before the premature EOF");

            s.Seek(len - 10, SeekOrigin.Begin);
            int partial = useAsync
                ? await s.ReadAtLeastAsync(tail, 25, throwOnEndOfStream: false, Ct)
                : s.ReadAtLeast(tail, 25, throwOnEndOfStream: false);
            Assert.Equal(10, partial);
            BytesEqual(data.AsMemory(len - 10), tail.AsMemory(0, 10), "ReadAtLeast without throwing returns the genuine tail");

            Assert.True(rule.Applied > 0, "The backend must actually have produced shortened read results.");
            Log($"STR-006 limit {maxBytesPerResult} async={useAsync}: {rule.Applied} backend short results.");
            rule.Cancel();
        }, Ct);
    }

    /// <summary>
    /// STR-006 variant: when authority becomes unreachable in the middle of assembling, ReadExactly reports the
    /// availability failure, never a premature EndOfStreamException, and no byte is consumed.
    /// </summary>
    [Fact]
    [Catalog("STR-006", "U", "INV-07")]
    public async Task STR_006_ReadExactlyHandlesShortReads_UnavailabilityIsNotPrematureEof()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            const string path = "/str006u.bin";
            int len = 2 * Chunk + 17;
            byte[] data = NonZero(0x5006_1000, len);
            await writer.GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);
            ReadResultLimitRule rule = cluster.Faults.LimitReadResultBytes(100, volume: Vol);

            await using DfsClient reader = await ConnectAsync(cluster, "r", operationTimeout: TimeSpan.FromSeconds(1));
            DfsVolume rv = reader.GetVolume(Vol);
            DfsStream s = await rv.Files.OpenReadAsync(path, Ct);
            s.Seek(len - 50, SeekOrigin.Begin);
            cluster.Network.Isolate("client:r");
            Exception? ex = await Record.ExceptionAsync(() => s.ReadExactlyAsync(new byte[50], Ct).AsTask());
            Assert.False(ex is EndOfStreamException, "Unavailability must never be reported as a premature EOF.");
            AssertAvailabilityFailure(ex, false, "ReadExactly while the client is isolated");
            Assert.Equal(len - 50, s.Position);

            cluster.Network.Heal();
            await AssertOldOrNewStreamRecoversAsync(cluster, rv, s, path, data, len - 50, "after healing the client partition");
            rule.Cancel();
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-007

    private static readonly (string Name, Func<DfsStream, Task<int>> Call)[] ZeroReads =
    [
        ("Read(byte[],5,0)", s => Task.FromResult(s.Read(new byte[8], 5, 0))),
        ("Read(Span.Empty)", s => Task.FromResult(s.Read(Span<byte>.Empty))),
        ("ReadAsync(byte[],5,0)", s => s.ReadAsync(new byte[8], 5, 0, Ct)),
        ("ReadAsync(Memory.Empty)", s => s.ReadAsync(Memory<byte>.Empty, Ct).AsTask()),
    ];

    private static readonly (string Name, Func<DfsStream, Task<int>> Call)[] ZeroReadHelpers =
    [
        ("ReadExactly(Span.Empty)", s =>
        {
            s.ReadExactly(Span<byte>.Empty);
            return Task.FromResult(0);
        }),
        ("ReadAtLeast(Span.Empty,0)", s => Task.FromResult(s.ReadAtLeast(Span<byte>.Empty, 0))),
    ];

    private static readonly (string Name, Func<DfsStream, Task> Call)[] ZeroWrites =
    [
        ("Write(byte[],5,0)", s => Sync(() => s.Write(new byte[8], 5, 0))),
        ("Write(ReadOnlySpan.Empty)", s => Sync(() => s.Write(ReadOnlySpan<byte>.Empty))),
        ("WriteAsync(byte[],5,0)", s => s.WriteAsync(new byte[8], 5, 0, Ct)),
        ("WriteAsync(ReadOnlyMemory.Empty)", s => s.WriteAsync(ReadOnlyMemory<byte>.Empty, Ct).AsTask()),
    ];

    /// <summary>
    /// Zero-length reads return 0 without implying EOF and without moving Position; zero-length writes stage nothing and
    /// never extend the file (even beyond EOF, in either write mode, or on append handles); access and disposal
    /// validation still applies to the direct overloads.
    /// </summary>
    [Theory]
    [InlineData(ZeroLengthCase.ReadOnEmptyFile)]
    [InlineData(ZeroLengthCase.ReadAtStart)]
    [InlineData(ZeroLengthCase.ReadInMiddle)]
    [InlineData(ZeroLengthCase.ReadAtEof)]
    [InlineData(ZeroLengthCase.ReadBeyondEof)]
    [InlineData(ZeroLengthCase.WriteBeyondEofOnFlush)]
    [InlineData(ZeroLengthCase.WriteBeyondEofWriteThrough)]
    [InlineData(ZeroLengthCase.WriteOnNewEmptyFile)]
    [InlineData(ZeroLengthCase.WriteOnAppendStream)]
    [InlineData(ZeroLengthCase.ReadOnWriteOnlyStreamValidates)]
    [InlineData(ZeroLengthCase.WriteOnReadOnlyStreamValidates)]
    [InlineData(ZeroLengthCase.ReadAfterDisposeValidates)]
    [InlineData(ZeroLengthCase.WriteAfterDisposeValidates)]
    [Catalog("STR-007", "U", "INV-02", "INV-07")]
    public async Task STR_007_ZeroLengthReadAndWrite(ZeroLengthCase zeroCase)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str007.bin";
            byte[] data = zeroCase == ZeroLengthCase.ReadOnEmptyFile ? [] : NonZero(0x5007_0000, 10_000);
            if (zeroCase != ZeroLengthCase.WriteOnNewEmptyFile)
                await wv.Files.WriteAllBytesAsync(path, data, Ct);

            switch (zeroCase)
            {
                case ZeroLengthCase.ReadOnEmptyFile:
                case ZeroLengthCase.ReadAtStart:
                case ZeroLengthCase.ReadInMiddle:
                case ZeroLengthCase.ReadAtEof:
                case ZeroLengthCase.ReadBeyondEof:
                {
                    long position = zeroCase switch
                    {
                        ZeroLengthCase.ReadInMiddle => 5000,
                        ZeroLengthCase.ReadAtEof => data.Length,
                        ZeroLengthCase.ReadBeyondEof => data.Length + 777,
                        _ => 0,
                    };
                    await using DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    s.Seek(position, SeekOrigin.Begin);
                    foreach ((string name, Func<DfsStream, Task<int>> call) in ZeroReads.Concat(ZeroReadHelpers))
                    {
                        Assert.True(await call(s) == 0, $"{name} returns 0 ({zeroCase})");
                        Assert.Equal(position, s.Position);
                        Assert.Equal(data.Length, s.Length);
                    }

                    if (position < data.Length)
                    {
                        // A zero-length read is not EOF: data is still delivered at the same position.
                        Assert.Equal(data[position], s.ReadByte());
                        Assert.Equal(position + 1, s.Position);
                    }

                    break;
                }

                case ZeroLengthCase.WriteBeyondEofOnFlush:
                case ZeroLengthCase.WriteBeyondEofWriteThrough:
                {
                    DfsWriteMode mode = zeroCase == ZeroLengthCase.WriteBeyondEofOnFlush ? DfsWriteMode.OnFlush : DfsWriteMode.WriteThrough;
                    DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite, mode), Ct);
                    s.Seek(20_000, SeekOrigin.Begin);
                    foreach ((string name, Func<DfsStream, Task> call) in ZeroWrites)
                    {
                        await call(s);
                        Assert.Equal(20_000, s.Position);
                        Assert.True(s.Length == data.Length, $"{name} beyond EOF must not extend the file ({mode}); length {s.Length}.");
                        Assert.Equal(0, s.PendingBytes);
                        Assert.Equal(data.Length, (await ov.Files.GetInfoAsync(path, Ct)).Length);
                    }

                    await s.FlushAsync(Ct);
                    await s.DisposeAsync();
                    BytesEqual(data, await ReadCommittedAsync(ov, path), $"zero-length writes have no data effect ({mode})");
                    break;
                }

                case ZeroLengthCase.WriteOnNewEmptyFile:
                {
                    DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.CreateNew, FileAccess.ReadWrite), Ct);
                    foreach ((string name, Func<DfsStream, Task> call) in ZeroWrites)
                    {
                        await call(s);
                        Assert.Equal(0, s.Position);
                        Assert.True(s.Length == 0, $"{name} on a new empty file must not extend it.");
                    }

                    await s.DisposeAsync();
                    Assert.True(await ov.Files.ExistsAsync(path, Ct), "the created file exists");
                    Assert.Equal(0, (await ov.Files.GetInfoAsync(path, Ct)).Length);
                    break;
                }

                case ZeroLengthCase.WriteOnAppendStream:
                {
                    DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Append, FileAccess.Write), Ct);
                    foreach ((string name, Func<DfsStream, Task> call) in ZeroWrites)
                    {
                        await call(s);
                        Assert.True(s.Length == data.Length, $"{name} on an append stream must not extend it.");
                        Assert.Equal(0, s.PendingBytes);
                    }

                    await s.FlushAsync(Ct);
                    await s.DisposeAsync();
                    BytesEqual(data, await ReadCommittedAsync(ov, path), "zero-length appends have no data effect");
                    break;
                }

                case ZeroLengthCase.ReadOnWriteOnlyStreamValidates:
                {
                    await using DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.Write), Ct);
                    Assert.False(s.CanRead);
                    foreach ((string name, Func<DfsStream, Task<int>> call) in ZeroReads)
                    {
                        Exception? ex = await Record.ExceptionAsync(() => call(s));
                        Assert.True(ex is NotSupportedException, $"{name} on a write-only stream must throw NotSupportedException even for zero bytes, got {ex?.GetType().FullName ?? "success"}.");
                        Assert.Equal(0, s.Position);
                    }

                    break;
                }

                case ZeroLengthCase.WriteOnReadOnlyStreamValidates:
                {
                    await using DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    Assert.False(s.CanWrite);
                    foreach ((string name, Func<DfsStream, Task> call) in ZeroWrites)
                    {
                        Exception? ex = await Record.ExceptionAsync(() => call(s));
                        Assert.True(ex is NotSupportedException, $"{name} on a read-only stream must throw NotSupportedException even for zero bytes, got {ex?.GetType().FullName ?? "success"}.");
                    }

                    BytesEqual(data, await ReadCommittedAsync(ov, path), "rejected writes have no effect");
                    break;
                }

                case ZeroLengthCase.ReadAfterDisposeValidates:
                {
                    DfsStream s = await wv.Files.OpenReadAsync(path, Ct);
                    await s.DisposeAsync();
                    foreach ((string name, Func<DfsStream, Task<int>> call) in ZeroReads)
                    {
                        Exception? ex = await Record.ExceptionAsync(() => call(s));
                        Assert.True(ex is ObjectDisposedException, $"{name} after disposal must throw ObjectDisposedException even for zero bytes, got {ex?.GetType().FullName ?? "success"}.");
                    }

                    break;
                }

                case ZeroLengthCase.WriteAfterDisposeValidates:
                {
                    DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
                    await s.DisposeAsync();
                    foreach ((string name, Func<DfsStream, Task> call) in ZeroWrites)
                    {
                        Exception? ex = await Record.ExceptionAsync(() => call(s));
                        Assert.True(ex is ObjectDisposedException, $"{name} after disposal must throw ObjectDisposedException even for zero bytes, got {ex?.GetType().FullName ?? "success"}.");
                    }

                    BytesEqual(data, await ReadCommittedAsync(ov, path), "rejected writes have no effect");
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(zeroCase), zeroCase, null);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-008

    /// <summary>
    /// Seek with Begin/Current/End and positive/negative offsets lands exactly on the computed position of the stream's
    /// declared view (committed for readers, staged for writers); negative results, overflow, negative Position and an
    /// undefined origin are rejected with Position and Length unchanged.
    /// </summary>
    [Theory]
    [InlineData(SeekOrigin.Begin, false)]
    [InlineData(SeekOrigin.Current, false)]
    [InlineData(SeekOrigin.End, false)]
    [InlineData(SeekOrigin.Begin, true)]
    [InlineData(SeekOrigin.Current, true)]
    [InlineData(SeekOrigin.End, true)]
    [Catalog("STR-008", "U", "INV-07")]
    public async Task STR_008_SeekOrigins(SeekOrigin origin, bool writerView)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            const string path = "/str008.bin";
            byte[] committed = NonZero(0x5008_0000, 10_000);
            await vol.Files.WriteAllBytesAsync(path, committed, Ct);
            var model = new ReferenceFileModel(committed);

            DfsStream s = writerView
                ? await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct)
                : await vol.Files.OpenReadAsync(path, Ct);
            if (writerView)
            {
                byte[] extension = NonZero(0x5008_0001, 2000);
                s.Seek(committed.Length, SeekOrigin.Begin);
                s.Write(extension);
                model.Write(committed.Length, extension); // the writer's view is 12 000 bytes, the committed one 10 000
            }

            byte[] view = model.Staged;
            long viewLength = view.Length;
            const long Start = 5000;
            (long Offset, long Expected)[] valid = origin switch
            {
                SeekOrigin.Begin => [(0, 0), (1, 1), (viewLength - 1, viewLength - 1), (viewLength, viewLength), (viewLength + 1, viewLength + 1), (1L << 40, 1L << 40)],
                SeekOrigin.Current => [(0, Start), (1, Start + 1), (-1, Start - 1), (-Start, 0), (viewLength - Start, viewLength), (viewLength - Start + 7, viewLength + 7), (1L << 40, Start + (1L << 40))],
                _ => [(0, viewLength), (-1, viewLength - 1), (-viewLength, 0), (7, viewLength + 7), (1L << 40, viewLength + (1L << 40))],
            };
            foreach ((long offset, long expected) in valid)
            {
                s.Position = Start;
                long result = s.Seek(offset, origin);
                string label = $"Seek({offset}, {origin}) from {Start}, view length {viewLength}";
                Assert.True(result == expected, $"{label} returned {result}, expected {expected}.");
                Assert.Equal(expected, s.Position);
                int b = s.ReadByte();
                if (expected < viewLength)
                {
                    Assert.True(b == view[expected], $"{label}: byte at the new position is {b}, expected {view[expected]}.");
                    Assert.Equal(expected + 1, s.Position);
                }
                else
                {
                    Assert.True(b == -1, $"{label}: a position at/after the end reads EOF, got {b}.");
                    Assert.Equal(expected, s.Position);
                }

                Assert.Equal(viewLength, s.Length);
            }

            long[] invalid = origin switch
            {
                SeekOrigin.Begin => [-1, long.MinValue],
                SeekOrigin.Current => [-(Start + 1), long.MinValue, long.MaxValue],
                _ => [-(viewLength + 1), long.MinValue, long.MaxValue],
            };
            foreach (long offset in invalid)
            {
                s.Position = Start;
                Exception? ex = Record.Exception(() => s.Seek(offset, origin));
                Assert.True(ex is IOException or ArgumentException,
                    $"Seek({offset}, {origin}) from {Start} must be rejected (negative or overflowing position), got {ex?.GetType().FullName ?? "success"}.");
                Assert.Equal(Start, s.Position);
                Assert.Equal(viewLength, s.Length);
            }

            s.Position = Start;
            Assert.ThrowsAny<ArgumentOutOfRangeException>(() => { s.Position = -1; });
            Assert.Equal(Start, s.Position);
            Assert.ThrowsAny<ArgumentException>(() => { s.Seek(0, (SeekOrigin)3); });
            Assert.Equal(Start, s.Position);

            await s.DisposeAsync();
            if (writerView)
                model.Flush();
            BytesEqual(model.Committed, await ReadCommittedAsync(observer.GetVolume(Vol), path), "seeking never changes contents");
        }, Ct);
    }

    /// <summary>STR-008 variant: append handles are not seekable (FSD §6.1); every seek/position/length change is NotSupported.</summary>
    [Fact]
    [Catalog("STR-008", "U", "INV-07")]
    public async Task STR_008_SeekOrigins_AppendStreamIsNotSeekable()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            const string path = "/str008a.bin";
            byte[] data = NonZero(0x5008_1000, 10_000);
            await vol.Files.WriteAllBytesAsync(path, data, Ct);

            DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.Append, FileAccess.Write), Ct);
            Assert.True(s.IsAppend);
            Assert.False(s.CanSeek);
            foreach (SeekOrigin origin in Enum.GetValues<SeekOrigin>())
                Assert.Throws<NotSupportedException>(() => s.Seek(0, origin));
            Assert.Throws<NotSupportedException>(() => { s.Position = 0; });
            Assert.Throws<NotSupportedException>(() => s.SetLength(5));
            await Assert.ThrowsAsync<NotSupportedException>(() => s.SetLengthAsync(5, Ct));

            byte[] tail = NonZero(0x5008_1001, 100);
            await s.WriteAsync(tail, Ct);
            await s.DisposeAsync();
            BytesEqual(data.Concat(tail).ToArray(), await vol.Files.ReadAllBytesAsync(path, Ct), "append writes target the logical end");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-009

    /// <summary>
    /// Seeking past EOF (by 1 byte .. 16 GiB) leaves Length unchanged on the stream, through GetLengthAsync and for other
    /// clients, also across a flush; reads there return EOF; only a later write or SetLength extends the file.
    /// </summary>
    [Theory]
    [InlineData(SeekHandleKind.ReadOnly)]
    [InlineData(SeekHandleKind.WriteOnly)]
    [InlineData(SeekHandleKind.ReadWriteOnFlush)]
    [InlineData(SeekHandleKind.ReadWriteWriteThrough)]
    [Catalog("STR-009", "U", "INV-07")]
    public async Task STR_009_SeekBeyondEofDoesNotExtend(SeekHandleKind kind)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            long[] distances = [1, Inline, 1L << 20, 1L << 34];
            for (int i = 0; i < distances.Length; i++)
            {
                long d = distances[i];
                string path = $"/str009-{kind}-{i}.bin";
                byte[] data = NonZero(0x5009_0000UL + (ulong)i, 10_000);
                long len = data.Length;
                await vol.Files.WriteAllBytesAsync(path, data, Ct);
                DfsStream s = kind switch
                {
                    SeekHandleKind.ReadOnly => await vol.Files.OpenReadAsync(path, Ct),
                    SeekHandleKind.WriteOnly => await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.Write), Ct),
                    SeekHandleKind.ReadWriteOnFlush => await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct),
                    _ => await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite, DfsWriteMode.WriteThrough), Ct),
                };
                long target = len + d;
                string label = $"{kind}, seek {d} past EOF";
                Assert.Equal(target, s.Seek(d, SeekOrigin.End));
                await AssertLengthEverywhereAsync(s, ov, path, len, $"{label}: seeking alone");
                if (s.CanRead)
                {
                    Assert.Equal(0, await s.ReadAsync(new byte[16], Ct));
                    Assert.Equal(target, s.Position);
                }

                if (s.CanWrite)
                {
                    await s.FlushAsync(Ct);
                    await AssertLengthEverywhereAsync(s, ov, path, len, $"{label}: a flush with no data must not extend");
                }

                Assert.Equal(target, s.Position);
                if (!s.CanWrite)
                {
                    await s.DisposeAsync();
                    Assert.Equal(len, (await ov.Files.GetInfoAsync(path, Ct)).Length);
                    continue;
                }

                long expectedLength;
                byte[] tail = NonZero(0x5009_1000UL + (ulong)i, 3);
                if (i % 2 == 0)
                {
                    await s.WriteAsync(tail, Ct);
                    expectedLength = target + tail.Length;
                    Assert.Equal(expectedLength, s.Position);
                }
                else
                {
                    s.SetLength(target);
                    expectedLength = target;
                    Assert.Equal(target, s.Position);
                }

                Assert.Equal(expectedLength, s.Length);
                long visibleNow = (await ov.Files.GetInfoAsync(path, Ct)).Length;
                Assert.Equal(kind == SeekHandleKind.ReadWriteWriteThrough ? expectedLength : len, visibleNow);
                await s.DisposeAsync();
                Assert.Equal(expectedLength, (await ov.Files.GetInfoAsync(path, Ct)).Length);

                await using DfsStream r = await ov.Files.OpenReadAsync(path, Ct);
                r.Seek(len, SeekOrigin.Begin);
                AssertAllZero(await ReadUpToAsync(r, (int)Math.Min(d, 4096)), len, $"{label}: the extension reads as zeroes");
                BytesEqual(data, (await ReadWholePrefixAsync(r, (int)len)), $"{label}: original bytes intact");
                if (i % 2 == 0)
                {
                    r.Seek(target, SeekOrigin.Begin);
                    BytesEqual(tail, await ReadUpToAsync(r, 16), $"{label}: the bytes written at the far position");
                }
            }
        }, Ct);
    }

    private static async Task AssertLengthEverywhereAsync(DfsStream s, DfsVolume observer, string path, long expected, string because)
    {
        Assert.True(s.Length == expected, $"Stream Length {s.Length}, expected {expected} ({because}).");
        Assert.True(await s.GetLengthAsync(Ct) == expected, $"GetLengthAsync differs from {expected} ({because}).");
        long committed = (await observer.Files.GetInfoAsync(path, Ct)).Length;
        Assert.True(committed == expected, $"Committed length {committed}, expected {expected} ({because}).");
    }

    private static async Task<byte[]> ReadWholePrefixAsync(Stream s, int count)
    {
        s.Seek(0, SeekOrigin.Begin);
        return await ReadUpToAsync(s, count);
    }

    // ------------------------------------------------------------------ STR-010

    public static TheoryData<long> SparseGaps()
    {
        var data = new TheoryData<long>();
        foreach (long gap in new long[] { 1, 3000, Inline, 64 * 1024, 200_000, 64L << 20 })
            data.Add(gap);
        return data;
    }

    /// <summary>
    /// A write beyond EOF, committed by flush/CommitAsync/WriteThrough/disposal, creates a hole that reads back as zeroes
    /// (writer's staged view, a reopened pinned view and whole-file reads) between intact base bytes and the written
    /// tail; large holes are reported as sparse and are not backed by extents.
    /// </summary>
    [Theory]
    [MemberData(nameof(SparseGaps))]
    [Catalog("STR-010", "U", "INV-02", "INV-07")]
    public async Task STR_010_SparseWriteReturnsZeroGap(long gap)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            int[] bases = [0, 1000, 70_000];
            string[] mechanisms = ["Flush", "CommitAsync", "WriteThrough", "Dispose"];
            for (int i = 0; i < bases.Length; i++)
            {
                int baseLength = bases[i];
                string mechanism = mechanisms[(i + (int)(gap % mechanisms.Length)) % mechanisms.Length];
                string path = $"/str010-{gap}-{baseLength}.bin";
                string label = $"gap {gap} after {baseLength} bytes, committed by {mechanism}";
                byte[] baseData = NonZero(0x5010_0000UL + (ulong)i, baseLength);
                byte[] tail = NonZero(0x5010_1000UL + (ulong)i, 333);
                await wv.Files.WriteAllBytesAsync(path, baseData, Ct);
                long tailAt = baseLength + gap;
                long total = tailAt + tail.Length;

                DfsStream s = await wv.Files.OpenAsync(path,
                    Writer(FileMode.Open, FileAccess.ReadWrite, mechanism == "WriteThrough" ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
                s.Seek(tailAt, SeekOrigin.Begin);
                await s.WriteAsync(tail, Ct);
                Assert.Equal(total, s.Length);
                await AssertGapZeroAsync(s, baseLength, gap, $"{label}: writer's staged view");
                switch (mechanism)
                {
                    case "Flush":
                        await s.FlushAsync(Ct);
                        break;
                    case "CommitAsync":
                        Assert.False((await s.CommitAsync(Ct)).IsNone, $"{label}: CommitAsync returns a real commit token");
                        break;
                }

                await s.DisposeAsync();

                await using DfsStream r = await ov.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
                Assert.Equal(total, r.Length);
                BytesEqual(baseData, await ReadWholePrefixAsync(r, baseLength), $"{label}: base bytes");
                await AssertGapZeroAsync(r, baseLength, gap, $"{label}: reopened pinned view");
                r.Seek(tailAt, SeekOrigin.Begin);
                BytesEqual(tail, await ReadUpToAsync(r, tail.Length + 10), $"{label}: tail bytes, then EOF");
                if (total <= 1 << 20)
                {
                    var expected = new byte[total];
                    baseData.CopyTo(expected, 0);
                    tail.CopyTo(expected, tailAt);
                    BytesEqual(expected, await ReadCommittedAsync(ov, path), $"{label}: whole-file read");
                }

                DfsFileInfo info = await ov.Files.GetInfoAsync(path, Ct);
                Assert.Equal(total, info.Length);
                if (gap >= 2 * Chunk)
                {
                    Assert.True(info.IsSparse, $"{label}: a file with a multi-chunk hole reports IsSparse.");
                    long holeFrom = (baseLength + Chunk - 1) / Chunk * Chunk;
                    long holeTo = tailAt / Chunk * Chunk;
                    foreach (ExtentRefInfo extent in await GetExtentsAsync(cluster, path))
                    {
                        Assert.False(extent.Offset < holeTo && holeFrom < extent.Offset + extent.Length,
                            $"{label}: extent [{extent.Offset}, {extent.Offset + extent.Length}) backs the hole [{holeFrom}, {holeTo}); holes must be unmapped.");
                    }
                }
            }
        }, Ct);
    }

    private static async Task AssertGapZeroAsync(DfsStream s, long gapStart, long gapLength, string because)
    {
        const int Window = 64 * 1024;
        (long Start, int Length)[] windows = gapLength <= 4 * Window
            ? [(gapStart, (int)gapLength)]
            : [(gapStart, Window), (gapStart + gapLength / 2 - Window / 2, Window), (gapStart + gapLength - Window, Window)];
        foreach ((long start, int length) in windows)
        {
            s.Seek(start, SeekOrigin.Begin);
            byte[] bytes = await ReadUpToAsync(s, length);
            Assert.True(bytes.Length == length, $"The hole [{start}, {start + length}) is inside the file and must be fully readable, got {bytes.Length} bytes ({because}).");
            AssertAllZero(bytes, start, because);
        }
    }

    // ------------------------------------------------------------------ STR-011

    public static TheoryData<int, int, int> TruncateExtendCases() => new()
    {
        { 3000, 1000, 3000 },          // inside an inline file
        { 5000, 4000, 10_000 },        // across the inline threshold
        { 200_000, 70_000, 200_000 },  // mid-extent truncation
        { 200_000, 64 * 1024, 300_000 }, // chunk-boundary truncation, extended beyond the original end
        { 200_000, 0, 150_000 },       // truncate to empty
    };

    /// <summary>
    /// Truncate then extend (same handle or across flushes; by SetLength or by a write beyond EOF), flush, churn storage so
    /// freed space is reused, crash and restart every node: the discarded range reads as zeroes, never as old bytes.
    /// </summary>
    [Theory]
    [MemberData(nameof(TruncateExtendCases))]
    [Catalog("STR-011", "U", "INV-02", "INV-07")]
    public async Task STR_011_TruncateThenExtendErasesTail(int initialLength, int truncateTo, int extendTo)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            var files = new List<(string Path, byte[] Expected, bool BySetLength, string Label)>();
            DfsClient writer = await ConnectAsync(cluster, "w");
            DfsVolume wv = writer.GetVolume(Vol);
            int variant = 0;
            foreach (bool sameHandle in new[] { true, false })
            {
                foreach (bool bySetLength in new[] { true, false })
                {
                    variant++;
                    string path = $"/str011-{variant}.bin";
                    string label = $"{initialLength}->{truncateTo}->{extendTo}, sameHandle={sameHandle}, bySetLength={bySetLength}";
                    byte[] initial = NonZero(0x5011_0000UL + (ulong)variant, initialLength);
                    await wv.Files.WriteAllBytesAsync(path, initial, Ct);
                    byte[] marker = NonZero(0x5011_1000UL + (ulong)variant, 4);
                    var expected = new byte[extendTo];
                    initial.AsSpan(0, truncateTo).CopyTo(expected);
                    if (!bySetLength)
                        marker.CopyTo(expected, extendTo - marker.Length);

                    DfsStream s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
                    s.SetLength(truncateTo);
                    Assert.Equal(truncateTo, s.Length);
                    if (!sameHandle)
                    {
                        await s.FlushAsync(Ct);
                        await s.DisposeAsync();
                        s = await wv.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
                        Assert.Equal(truncateTo, s.Length);
                    }

                    if (bySetLength)
                    {
                        s.SetLength(extendTo);
                    }
                    else
                    {
                        s.Seek(extendTo - marker.Length, SeekOrigin.Begin);
                        s.Write(marker);
                    }

                    Assert.Equal(extendTo, s.Length);
                    BytesEqual(expected, await ReadWholeAsync(s), $"{label}: the writer's staged view never shows the discarded tail");
                    await s.FlushAsync(Ct);
                    await s.DisposeAsync();
                    BytesEqual(expected, await ReadCommittedAsync(wv, path), $"{label}: committed view before restart");
                    files.Add((path, expected, bySetLength, label));
                }
            }

            // Churn: allocate, delete and reallocate objects so reclaimed space/segments are reused.
            for (int k = 0; k < 4; k++)
                await wv.Files.WriteAllBytesAsync($"/str011-churn-{k}.bin", NonZero(0x5011_2000UL + (ulong)k, 256 * 1024), Ct);
            for (int k = 0; k < 4; k += 2)
                await wv.Files.DeleteAsync($"/str011-churn-{k}.bin", Ct);
            for (int k = 4; k < 8; k++)
                await wv.Files.WriteAllBytesAsync($"/str011-churn-{k}.bin", NonZero(0x5011_2000UL + (ulong)k, 256 * 1024), Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct); // several GC passes (250 ms)
            await writer.DisposeAsync();

            TestNode[] nodes = cluster.Nodes.ToArray();
            foreach (TestNode node in nodes)
                await node.CrashAsync(Tedd.Tdfs.Storage.CrashMode.LoseUnflushed);
            foreach (TestNode node in nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

            await using DfsClient fresh = await ConnectAsync(cluster, "after-restart");
            DfsVolume fv = fresh.GetVolume(Vol);
            foreach ((string path, byte[] expected, bool bySetLength, string label) in files)
            {
                BytesEqual(expected, await fv.Files.ReadAllBytesAsync(path, Ct), $"{label}: after crash/restart of every node");
                await using DfsStream pinned = await fv.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
                pinned.Seek(truncateTo, SeekOrigin.Begin);
                byte[] region = await ReadUpToAsync(pinned, extendTo - truncateTo - (bySetLength ? 0 : 4));
                AssertAllZero(region, truncateTo, $"{label}: discarded range through a pinned stream after restart");
                if (bySetLength)
                {
                    long from = ((long)truncateTo + Chunk - 1) / Chunk * Chunk;
                    foreach (ExtentRefInfo extent in await GetExtentsAsync(cluster, path))
                    {
                        Assert.False(extent.Offset + extent.Length > from && extent.Offset < extendTo,
                            $"{label}: extent [{extent.Offset}, {extent.Offset + extent.Length}) maps the extension [{from}, {extendTo}) — clipped data must not be remapped.");
                    }
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-012

    /// <summary>
    /// SetLength never moves Position (shrinking below it, growing above it, sync or async, OnFlush or WriteThrough); reads
    /// at a Position beyond the new end return EOF; writes there extend with a zero gap; invalid lengths are rejected
    /// without effect; read-only and append handles reject SetLength.
    /// </summary>
    [Theory]
    [InlineData(DfsWriteMode.OnFlush, false)]
    [InlineData(DfsWriteMode.OnFlush, true)]
    [InlineData(DfsWriteMode.WriteThrough, false)]
    [InlineData(DfsWriteMode.WriteThrough, true)]
    [Catalog("STR-012", "U", "INV-02", "INV-07")]
    public async Task STR_012_SetLengthAndPosition(DfsWriteMode mode, bool useAsync)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str012.bin";
            byte[] data = NonZero(0x5012_0000, 10_000);
            await vol.Files.WriteAllBytesAsync(path, data, Ct);
            var model = new ReferenceFileModel(data);
            DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite, mode), Ct);

            async Task SetLen(long value)
            {
                if (useAsync)
                    await s.SetLengthAsync(value, Ct);
                else
                    s.SetLength(value);
                model.SetLength(value);
                if (mode == DfsWriteMode.WriteThrough)
                    model.Flush();
            }

            async Task Write(byte[] bytes)
            {
                long at = s.Position;
                await s.WriteAsync(bytes, Ct);
                model.Write(at, bytes);
                if (mode == DfsWriteMode.WriteThrough)
                    model.Flush();
            }

            async Task Check(string step)
            {
                Assert.True(s.Length == model.StagedLength, $"Length {s.Length}, expected {model.StagedLength} ({step}).");
                Assert.Equal(model.StagedLength, await s.GetLengthAsync(Ct));
                BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"committed view ({step}, {mode})");
            }

            s.Seek(8000, SeekOrigin.Begin);
            await SetLen(5000);
            Assert.Equal(8000, s.Position);
            await Check("shrink below Position");
            Assert.Equal(0, await s.ReadAsync(new byte[10], Ct));
            Assert.Equal(8000, s.Position);

            await Write(NonZero(0x5012_0001, 10));
            Assert.Equal(8010, s.Position);
            await Check("write at the old Position beyond the new end");
            BytesEqual(model.Staged, await ReadWholeAsync(s), "staged view: [5000,8000) is a zero gap");
            s.Seek(8010, SeekOrigin.Begin);

            await SetLen(9000);
            Assert.Equal(8010, s.Position);
            await Check("grow above Position");
            await SetLen(2000);
            Assert.Equal(8010, s.Position);
            await Check("shrink far below Position");

            s.Seek(0, SeekOrigin.Begin);
            BytesEqual(model.Read(0, 4000, staged: true), await ReadUpToAsync(s, 4000), "reads stop at the new end");
            Assert.Equal(2000, s.Position);
            s.Seek(8010, SeekOrigin.Begin);
            await Write(NonZero(0x5012_0002, 4));
            Assert.Equal(8014, s.Position);
            await s.FlushAsync(Ct);
            model.Flush();
            await Check("after flush");

            // Invalid lengths are rejected with no effect.
            Exception? negative = Record.Exception(() => s.SetLength(-1));
            Assert.True(negative is ArgumentOutOfRangeException, $"SetLength(-1) must throw ArgumentOutOfRangeException, got {negative?.GetType().FullName ?? "success"}.");
            long max = client.Capabilities.MaxFileLength;
            Exception? tooLong = await Record.ExceptionAsync(() => SetLengthRaw(s, max + 1, useAsync));
            Assert.True(tooLong is ArgumentOutOfRangeException || tooLong is DfsException { ErrorCode: DfsErrorCode.LimitExceeded },
                $"SetLength beyond the advertised maximum ({max}) must be rejected, got {tooLong?.GetType().FullName ?? "success"}.");
            Assert.Equal(8014, s.Position);
            await Check("after rejected lengths");
            await s.DisposeAsync();
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), "final committed contents");

            await using (DfsStream readOnly = await vol.Files.OpenReadAsync(path, Ct))
            {
                Assert.Throws<NotSupportedException>(() => readOnly.SetLength(1));
                await Assert.ThrowsAsync<NotSupportedException>(() => readOnly.SetLengthAsync(1, Ct));
            }

            await using (DfsStream append = await vol.Files.OpenAsync(path, Writer(FileMode.Append, FileAccess.Write), Ct))
            {
                Assert.Throws<NotSupportedException>(() => append.SetLength(1));
                await Assert.ThrowsAsync<NotSupportedException>(() => append.SetLengthAsync(1, Ct));
            }

            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), "rejected SetLength calls have no effect");
        }, Ct);
    }

    private static async Task SetLengthRaw(DfsStream s, long value, bool useAsync)
    {
        if (useAsync)
            await s.SetLengthAsync(value, Ct);
        else
            s.SetLength(value);
    }

    // ------------------------------------------------------------------ STR-013

    public static TheoryData<int> OwnStagingSeeds()
    {
        var data = new TheoryData<int>();
        for (int seed = 1; seed <= 8; seed++)
            data.Add(seed);
        return data;
    }

    /// <summary>
    /// A seeded random interleaving of writes (all overloads), seeks, reads, truncations/extensions and flushes on one
    /// read/write handle: every read equals the independent model's staged image (staged ranges merged over the base
    /// revision), while another client always sees exactly the model's committed image.
    /// </summary>
    [Theory]
    [MemberData(nameof(OwnStagingSeeds))]
    [Catalog("STR-013", "U", "INV-02", "INV-07")]
    public async Task STR_013_WriterReadsOwnStaging(int seed)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            var random = new SeededRandom(0x5013_0000UL + (ulong)seed);
            int[] initialSizes = [0, 3000, 70_000, 200_000];
            string path = $"/str013-{seed}.bin";
            byte[] initial = NonZero(0x5013_1000UL + (ulong)seed, initialSizes[seed % initialSizes.Length]);
            await vol.Files.WriteAllBytesAsync(path, initial, Ct);
            var model = new ReferenceFileModel(initial);
            int[] sizes = [1, 17, Inline, Inline + 1, Chunk, Chunk + 4465];
            WriteOverload[] overloads = [WriteOverload.ArraySync, WriteOverload.SpanSync, WriteOverload.ArrayAsync, WriteOverload.MemoryAsync];

            DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
            for (int step = 0; step < 80; step++)
            {
                int op = random.Next(100);
                string label = $"seed {seed} step {step}";
                if (op < 35)
                {
                    int offset = random.Next((int)model.StagedLength + 5001);
                    int size = sizes[random.Next(sizes.Length)];
                    byte[] bytes = TestData.Bytes(random.NextUInt64(), size);
                    s.Seek(offset, SeekOrigin.Begin);
                    await WriteOnceAsync(s, overloads[random.Next(overloads.Length)], bytes, 0, size);
                    model.Write(offset, bytes);
                    Assert.Equal(offset + size, s.Position);
                    Assert.True(s.Length == model.StagedLength, $"{label}: Length {s.Length} after write, model {model.StagedLength}.");
                }
                else if (op < 65)
                {
                    int offset = random.Next((int)model.StagedLength + 101);
                    int count = sizes[random.Next(sizes.Length)];
                    s.Seek(offset, SeekOrigin.Begin);
                    BytesEqual(model.Read(offset, count, staged: true), await ReadUpToAsync(s, count), $"{label}: read {count} at {offset}");
                }
                else if (op < 75)
                {
                    int newLength = random.Next((int)model.StagedLength + 10_001);
                    long position = s.Position;
                    s.SetLength(newLength);
                    model.SetLength(newLength);
                    Assert.Equal(position, s.Position);
                    Assert.Equal(newLength, s.Length);
                }
                else if (op < 82)
                {
                    int target = random.Next((int)model.StagedLength + 3000);
                    Assert.Equal(target, s.Seek(target, SeekOrigin.Begin));
                }
                else if (op < 90)
                {
                    await s.FlushAsync(Ct);
                    model.Flush();
                    Assert.Equal(0, s.PendingBytes);
                }
                else
                {
                    BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{label}: other clients see only the committed image");
                }

                if (step % 10 == 9)
                {
                    BytesEqual(model.Staged, await ReadWholeAsync(s), $"{label}: full staged view");
                    BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{label}: full committed view");
                }
            }

            await s.DisposeAsync();
            model.Flush();
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"seed {seed}: final committed contents after disposal");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-018

    /// <summary>
    /// Length, GetLengthAsync, GetLength and GetInfoAsync().Length all follow the stream's declared view across grow,
    /// truncate and sparse-extend operations: live = current committed, pinned = opening revision, writer = staged,
    /// write-through = committed per call, transaction = transaction view (own writes, not later outside commits), append = staged end.
    /// </summary>
    [Theory]
    [InlineData(LengthView.Live)]
    [InlineData(LengthView.VersionPinned)]
    [InlineData(LengthView.WriterStaged)]
    [InlineData(LengthView.WriterWriteThrough)]
    [InlineData(LengthView.TransactionBound)]
    [InlineData(LengthView.AppendStaged)]
    [Catalog("STR-018", "U", "INV-03", "INV-07")]
    public async Task STR_018_LengthUsesDeclaredView(LengthView view)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient other = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = other.GetVolume(Vol);
            const string path = "/str018.bin";
            await ov.Files.WriteAllBytesAsync(path, NonZero(0x5018_0000, 10_000), Ct);

            async Task StreamLength(DfsStream s, long expected, string step)
            {
                Assert.True(s.Length == expected, $"{view}: Length {s.Length}, expected {expected} ({step}).");
                Assert.True(await s.GetLengthAsync(Ct) == expected, $"{view}: GetLengthAsync differs from {expected} ({step}).");
                Assert.True(s.GetLength() == expected, $"{view}: GetLength differs from {expected} ({step}).");
                Assert.True((await s.GetInfoAsync(Ct)).Length == expected, $"{view}: GetInfoAsync().Length differs from {expected} ({step}).");
            }

            async Task Committed(long expected, string step) =>
                Assert.True((await ov.Files.GetInfoAsync(path, Ct)).Length == expected, $"{view}: committed length differs from {expected} ({step}).");

            async Task GrowByOther() => await ov.Files.AppendAllBytesAsync(path, NonZero(0x5018_0001, 190_000), Ct);

            async Task TruncateByOther()
            {
                await using DfsStream t = await ov.Files.OpenAsync(path, Writer(FileMode.Truncate, FileAccess.Write), Ct);
                await t.WriteAsync(NonZero(0x5018_0002, 3000), Ct);
            }

            async Task SparseGrowByOther()
            {
                await using DfsStream t = await ov.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.Write), Ct);
                await t.SetLengthAsync(1 << 20, Ct);
            }

            switch (view)
            {
                case LengthView.Live:
                case LengthView.VersionPinned:
                {
                    bool pinned = view == LengthView.VersionPinned;
                    await using DfsStream s = await vol.Files.OpenAsync(path, Reader(pinned ? DfsReadView.VersionPinned : DfsReadView.Live), Ct);
                    await StreamLength(s, 10_000, "open");
                    (Func<Task> Mutate, long Committed, string Step)[] steps =
                    [
                        (GrowByOther, 200_000, "grown by another client"),
                        (TruncateByOther, 3000, "truncated by another client"),
                        (SparseGrowByOther, 1 << 20, "sparse-extended by another client"),
                    ];
                    foreach ((Func<Task> mutate, long committedLength, string step) in steps)
                    {
                        await mutate();
                        await Committed(committedLength, step);
                        await StreamLength(s, pinned ? 10_000 : committedLength, step);
                        if (pinned)
                            Assert.Equal(s.OpenRevision, (await s.GetInfoAsync(Ct)).Revision);
                    }

                    break;
                }

                case LengthView.WriterStaged:
                case LengthView.WriterWriteThrough:
                {
                    bool through = view == LengthView.WriterWriteThrough;
                    await using DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite, through ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
                    await StreamLength(s, 10_000, "open");
                    s.Seek(199_990, SeekOrigin.Begin);
                    await s.WriteAsync(NonZero(0x5018_0003, 10), Ct);
                    await StreamLength(s, 200_000, "write beyond EOF");
                    await Committed(through ? 200_000 : 10_000, "write beyond EOF");
                    await s.SetLengthAsync(3000, Ct);
                    await StreamLength(s, 3000, "truncate");
                    await Committed(through ? 3000 : 10_000, "truncate");
                    s.SetLength(1 << 20);
                    await StreamLength(s, 1 << 20, "sparse extend");
                    await Committed(through ? 1 << 20 : 10_000, "sparse extend");
                    await s.FlushAsync(Ct);
                    await StreamLength(s, 1 << 20, "after flush");
                    await Committed(1 << 20, "after flush");
                    break;
                }

                case LengthView.TransactionBound:
                {
                    await using (DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct))
                    {
                        DfsStream ts = await tx.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
                        Assert.True(ts.IsTransactionBound);
                        await StreamLength(ts, 10_000, "transaction open");
                        ts.Seek(199_990, SeekOrigin.Begin);
                        await ts.WriteAsync(NonZero(0x5018_0004, 10), Ct);
                        await StreamLength(ts, 200_000, "transaction write beyond EOF");
                        await ts.FlushAsync(Ct); // stages for the transaction, publishes nothing
                        await Committed(10_000, "transaction stream flushed, not committed");
                        Assert.Equal(200_000, (await tx.Files.GetInfoAsync(path, Ct)).Length);
                        await ts.SetLengthAsync(3000, Ct);
                        await StreamLength(ts, 3000, "transaction truncate");
                        await ts.DisposeAsync();
                        await using (DfsStream tr = await tx.Files.OpenAsync(path, Reader(DfsReadView.Live), Ct))
                            await StreamLength(tr, 3000, "transaction reader sees its own writes");
                        await Committed(10_000, "before transaction commit");
                        Assert.False((await tx.CommitAsync(Ct)).IsNone);
                    }

                    await Committed(3000, "after transaction commit");
                    await using (DfsTransaction tx2 = await vol.Transactions.BeginAsync(null, Ct))
                    {
                        DfsStream tr2 = await tx2.Files.OpenAsync(path, Reader(DfsReadView.Live), Ct);
                        await StreamLength(tr2, 3000, "second transaction view");
                        await ov.Files.AppendAllBytesAsync(path, NonZero(0x5018_0005, 100), Ct);
                        await Committed(3100, "outside append after the transaction began");
                        await StreamLength(tr2, 3000, "the transaction view does not include commits after its read version");
                        await tr2.DisposeAsync();
                        await tx2.AbortAsync(Ct);
                    }

                    break;
                }

                case LengthView.AppendStaged:
                {
                    await using DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.Append, FileAccess.Write), Ct);
                    await StreamLength(s, 10_000, "append open");
                    await s.WriteAsync(NonZero(0x5018_0006, 500), Ct);
                    await StreamLength(s, 10_500, "staged append");
                    await Committed(10_000, "staged append");
                    await s.FlushAsync(Ct);
                    await Committed(10_500, "flushed append");
                    await s.WriteAsync(NonZero(0x5018_0007, 7), Ct);
                    await StreamLength(s, 10_507, "second staged append");
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(view), view, null);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-019

    public static TheoryData<OverlapFirst, OverlapSecond> OverlapPairs() => new()
    {
        // read / read
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.ReadAsync },
        { OverlapFirst.ReadAsyncArray, OverlapSecond.ReadSync },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.ReadByte },
        { OverlapFirst.CopyToAsync, OverlapSecond.ReadAsync },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.CopyToAsync },
        // write / write
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.WriteAsync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.WriteSync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.WriteByte },
        // read / write
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.WriteAsync },
        { OverlapFirst.ReadAsyncArray, OverlapSecond.WriteSync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.ReadAsync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.ReadSync },
        // seek and length / I/O
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.Seek },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.SetPosition },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.Seek },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.SetLength },
        { OverlapFirst.SetLengthAsyncWriteThrough, OverlapSecond.ReadAsync },
        { OverlapFirst.SetLengthAsyncWriteThrough, OverlapSecond.Seek },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.GetLengthAsync },
        // flush / I/O
        { OverlapFirst.FlushAsync, OverlapSecond.ReadAsync },
        { OverlapFirst.FlushAsync, OverlapSecond.WriteAsync },
        { OverlapFirst.FlushAsync, OverlapSecond.Seek },
        { OverlapFirst.FlushAsync, OverlapSecond.FlushAsync },
        { OverlapFirst.FlushAsync, OverlapSecond.CommitAsync },
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.FlushAsync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.FlushSync },
        // disposal / I/O
        { OverlapFirst.ReadAsyncMemory, OverlapSecond.DisposeAsync },
        { OverlapFirst.WriteAsyncWriteThrough, OverlapSecond.DisposeAsync },
        { OverlapFirst.FlushAsync, OverlapSecond.DisposeAsync },
        { OverlapFirst.DisposeAsync, OverlapSecond.ReadAsync },
        { OverlapFirst.DisposeAsync, OverlapSecond.WriteAsync },
        { OverlapFirst.DisposeAsync, OverlapSecond.Seek },
    };

    private static readonly byte[] OverlapWrite = [0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58];

    private static Task InvokeSecond(DfsStream s, OverlapSecond op) => op switch
    {
        OverlapSecond.ReadSync => Sync(() => s.Read(new byte[16], 0, 16)),
        OverlapSecond.ReadAsync => s.ReadAsync(new byte[16], Ct).AsTask(),
        OverlapSecond.ReadByte => Sync(() => s.ReadByte()),
        OverlapSecond.WriteSync => Sync(() => s.Write(OverlapWrite, 0, OverlapWrite.Length)),
        OverlapSecond.WriteAsync => s.WriteAsync(OverlapWrite, Ct).AsTask(),
        OverlapSecond.WriteByte => Sync(() => s.WriteByte(0x58)),
        OverlapSecond.Seek => Sync(() => s.Seek(1, SeekOrigin.Begin)),
        OverlapSecond.SetPosition => Sync(() => { s.Position = 1; }),
        OverlapSecond.SetLength => Sync(() => s.SetLength(1)),
        OverlapSecond.FlushSync => Sync(s.Flush),
        OverlapSecond.FlushAsync => s.FlushAsync(Ct),
        OverlapSecond.CommitAsync => s.CommitAsync(Ct),
        OverlapSecond.DisposeAsync => s.DisposeAsync().AsTask(),
        OverlapSecond.CopyToAsync => s.CopyToAsync(new MemoryStream(), Ct),
        OverlapSecond.GetLengthAsync => s.GetLengthAsync(Ct).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(op), op, null),
    };

    /// <summary>
    /// While one operation is in flight (its request held at the client send boundary), every overlapping operation of the
    /// listed kind is rejected with InvalidOperationException immediately and deterministically (twice in a row), without
    /// disturbing the in-flight call, Position, staged data or committed state; the stream stays usable afterwards.
    /// </summary>
    [Theory]
    [MemberData(nameof(OverlapPairs))]
    [Catalog("STR-019", "U", "INV-07", "INV-16")]
    public async Task STR_019_ConcurrentStreamCallsRejected(OverlapFirst first, OverlapSecond second)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str019.bin";
            byte[] initial = NonZero(0x5019_0000, 3 * Chunk);
            await vol.Files.WriteAllBytesAsync(path, initial, Ct);
            var model = new ReferenceFileModel(initial);
            bool writeThrough = first is OverlapFirst.WriteAsyncWriteThrough or OverlapFirst.SetLengthAsyncWriteThrough;
            DfsOperationKind blocked = first is OverlapFirst.ReadAsyncArray or OverlapFirst.ReadAsyncMemory or OverlapFirst.CopyToAsync
                ? DfsOperationKind.Read
                : DfsOperationKind.CommitFile;

            for (int iteration = 0; iteration < 2; iteration++)
            {
                string label = $"{first} overlapped by {second}, iteration {iteration}";
                DfsStream s = await vol.Files.OpenAsync(path,
                    Writer(FileMode.Open, FileAccess.ReadWrite, writeThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
                s.Seek(1000 + iteration * 100, SeekOrigin.Begin);
                if (first is OverlapFirst.FlushAsync or OverlapFirst.DisposeAsync)
                {
                    byte[] pending = NonZero(0x5019_1000UL + (ulong)iteration, 100);
                    model.Write(s.Position, pending);
                    await s.WriteAsync(pending, Ct);
                }

                long positionBefore = s.Position;
                long lengthBefore = model.StagedLength;
                var readBuffer = new byte[4096];
                var copyTarget = new MemoryStream();
                byte[] firstWrite = NonZero(0x5019_2000UL + (ulong)iteration, 64);

                FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientRequestSend, Operation: blocked), FaultAction.Block());
                Task<int> firstTask = first switch
                {
                    OverlapFirst.ReadAsyncArray => s.ReadAsync(readBuffer, 0, readBuffer.Length, Ct),
                    OverlapFirst.ReadAsyncMemory => s.ReadAsync(readBuffer.AsMemory(), Ct).AsTask(),
                    OverlapFirst.CopyToAsync => AsInt(s.CopyToAsync(copyTarget, 81_920, Ct)),
                    OverlapFirst.WriteAsyncWriteThrough => AsInt(s.WriteAsync(firstWrite, Ct).AsTask()),
                    OverlapFirst.SetLengthAsyncWriteThrough => AsInt(s.SetLengthAsync(lengthBefore - 1000, Ct)),
                    OverlapFirst.FlushAsync => AsInt(s.FlushAsync(Ct)),
                    OverlapFirst.DisposeAsync => AsInt(s.DisposeAsync().AsTask()),
                    _ => throw new ArgumentOutOfRangeException(nameof(first), first, null),
                };
                await arm.WaitTriggeredAsync(Bounded);
                Assert.False(firstTask.IsCompleted, $"{label}: the first call must still be in flight while its request is blocked.");

                // The overlapping call must be rejected immediately, not queued behind the in-flight call.
                Task secondTask = Task.Run(() => InvokeSecond(s, second), Ct);
                Exception? rejected = null;
                try
                {
                    await secondTask.WaitAsync(TimeSpan.FromSeconds(5), Ct);
                }
                catch (TimeoutException) when (!secondTask.IsCompleted)
                {
                    Assert.Fail($"{label}: the overlapping call waited instead of being rejected deterministically.");
                }
                catch (Exception ex)
                {
                    rejected = ex;
                }

                Assert.True(rejected is InvalidOperationException,
                    $"{label}: the overlapping call must throw InvalidOperationException, got {rejected?.GetType().FullName ?? "success"}.");
                if (first != OverlapFirst.DisposeAsync)
                    Assert.False(rejected is ObjectDisposedException, $"{label}: the stream is not disposed; the overlap itself is the error.");
                Assert.False(firstTask.IsCompleted, $"{label}: rejecting the overlap must not complete or cancel the first call.");

                arm.Release();
                int firstResult = await firstTask.Within(cluster, $"{label}: the first call completes after release");
                cluster.Faults.AssertReached(arm);
                arm.Disarm();

                switch (first)
                {
                    case OverlapFirst.ReadAsyncArray:
                    case OverlapFirst.ReadAsyncMemory:
                        Assert.InRange(firstResult, 1, readBuffer.Length);
                        BytesEqual(model.Read(positionBefore, firstResult, staged: true), readBuffer.AsMemory(0, firstResult), $"{label}: first read's bytes");
                        Assert.Equal(positionBefore + firstResult, s.Position);
                        break;
                    case OverlapFirst.CopyToAsync:
                        BytesEqual(model.Read(positionBefore, (int)(model.StagedLength - positionBefore), staged: true), copyTarget.ToArray(), $"{label}: copied bytes");
                        Assert.Equal(model.StagedLength, s.Position);
                        break;
                    case OverlapFirst.WriteAsyncWriteThrough:
                        model.Write(positionBefore, firstWrite);
                        model.Flush();
                        Assert.Equal(positionBefore + firstWrite.Length, s.Position);
                        break;
                    case OverlapFirst.SetLengthAsyncWriteThrough:
                        model.SetLength(lengthBefore - 1000);
                        model.Flush();
                        Assert.Equal(positionBefore, s.Position);
                        break;
                    case OverlapFirst.FlushAsync:
                        model.Flush();
                        Assert.Equal(0, s.PendingBytes);
                        Assert.Equal(positionBefore, s.Position);
                        break;
                    case OverlapFirst.DisposeAsync:
                        model.Flush();
                        Assert.False(s.CanRead || s.CanWrite, $"{label}: the first DisposeAsync completed the disposal.");
                        break;
                }

                if (first != OverlapFirst.DisposeAsync)
                {
                    Assert.True(s.CanRead && s.CanWrite, $"{label}: a rejected overlapping call (including DisposeAsync) leaves the stream open.");
                    Assert.False(s.IsFaulted, $"{label}: rejecting an overlap must not fault the stream.");
                    Assert.Equal(model.StagedLength, s.Length);
                    BytesEqual(model.Staged, await ReadWholeAsync(s), $"{label}: staged view has exactly the first call's effect");
                    await s.DisposeAsync();
                    model.Flush();
                }

                BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{label}: committed state has exactly the first call's effect");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-020

    public static TheoryData<bool, CopyDestination> CopyCases()
    {
        var data = new TheoryData<bool, CopyDestination>();
        foreach (bool useAsync in new[] { false, true })
        {
            foreach (CopyDestination destination in Enum.GetValues<CopyDestination>())
                data.Add(useAsync, destination);
        }

        return data;
    }

    /// <summary>
    /// CopyTo/CopyToAsync (default and explicit buffer sizes) transfer exactly source[Position..EOF) to the destination's
    /// current position and advance the source to EOF and the destination by the transferred count; a source at/after EOF
    /// transfers nothing. Destinations: MemoryStream and DfsStream (OnFlush and WriteThrough).
    /// </summary>
    [Theory]
    [MemberData(nameof(CopyCases))]
    [Catalog("STR-020", "U", "INV-02", "INV-07")]
    public async Task STR_020_CopyStartsAtCurrentPosition(bool useAsync, CopyDestination destination)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string sourcePath = "/str020-src.bin";
            const int L = 200_000;
            byte[] source = NonZero(0x5020_0000, L);
            await vol.Files.WriteAllBytesAsync(sourcePath, source, Ct);
            long[] sourcePositions = [0, 1, Inline + 1, Chunk + 3, L - 1, L, L + 100];
            int?[] bufferSizes = [null, 4096, 7777, 1 << 20];
            const int DestPosition = 500;

            for (int i = 0; i < sourcePositions.Length; i++)
            {
                long p = sourcePositions[i];
                int? bufferSize = bufferSizes[i % bufferSizes.Length];
                string label = $"copy from {p} (buffer {bufferSize?.ToString() ?? "default"}, async={useAsync}) into {destination}";
                byte[] destInitial = NonZero(0x5020_1000UL + (ulong)i, 1000);
                long copied = Math.Max(0, L - p);
                var expected = new ReferenceFileModel(destInitial);
                if (copied > 0)
                    expected.Write(DestPosition, source.AsSpan((int)p));

                await using DfsStream src = await vol.Files.OpenReadAsync(sourcePath, Ct);
                src.Seek(p, SeekOrigin.Begin);
                Stream dest;
                string destPath = $"/str020-dst-{i}.bin";
                if (destination == CopyDestination.Memory)
                {
                    var ms = new MemoryStream();
                    ms.Write(destInitial);
                    dest = ms;
                }
                else
                {
                    await vol.Files.WriteAllBytesAsync(destPath, destInitial, Ct);
                    dest = await vol.Files.OpenAsync(destPath, Writer(FileMode.Open, FileAccess.ReadWrite,
                        destination == CopyDestination.DfsWriteThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
                }

                dest.Seek(DestPosition, SeekOrigin.Begin);
                if (useAsync)
                    await (bufferSize is int b ? src.CopyToAsync(dest, b, Ct) : src.CopyToAsync(dest, Ct));
                else if (bufferSize is int b)
                    src.CopyTo(dest, b);
                else
                    src.CopyTo(dest);

                Assert.True(src.Position == Math.Max(L, p), $"{label}: source Position {src.Position}, expected {Math.Max(L, p)}.");
                Assert.True(dest.Position == DestPosition + copied, $"{label}: destination Position {dest.Position}, expected {DestPosition + copied}.");
                Assert.Equal(L, src.Length);
                if (dest is MemoryStream memory)
                {
                    BytesEqual(expected.Staged, memory.ToArray(), label);
                }
                else
                {
                    BytesEqual(expected.Staged, await ReadWholeAsync(dest), $"{label}: destination staged view");
                    await dest.DisposeAsync();
                    BytesEqual(expected.Staged, await ReadCommittedAsync(ov, destPath), $"{label}: destination committed");
                }
            }
        }, Ct);
    }

    /// <summary>
    /// STR-020 variant: CopyTo/CopyToAsync are compositions, not atomic replacements. A source failing mid-copy leaves the
    /// already-written (WriteThrough-committed) bytes in place, and a failing destination leaves the DfsStream source
    /// advanced by exactly the bytes it delivered.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("STR-020", "U", "INV-02", "INV-07")]
    public async Task STR_020_CopyStartsAtCurrentPosition_FailedCopyIsNotAtomic(bool useAsync)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);

            // Failing source -> DfsStream destination (WriteThrough publishes each delivered chunk).
            byte[] payload = NonZero(0x5020_2000, 20_000);
            var failingSource = new FailingReadStream(payload, chunk: 1000, failOnRead: 6);
            await using (DfsStream dest = await vol.Files.OpenAsync("/str020-fail-dst.bin",
                             Writer(FileMode.CreateNew, FileAccess.ReadWrite, DfsWriteMode.WriteThrough), Ct))
            {
                Exception? ex = await Record.ExceptionAsync(() => useAsync ? failingSource.CopyToAsync(dest, 1000, Ct) : Sync(() => failingSource.CopyTo(dest, 1000)));
                Assert.True(ex is IOException { Message: FailingReadStream.Message }, $"the injected source failure surfaces, got {ex}");
                Assert.Equal(5000, dest.Position);
            }

            BytesEqual(payload.AsMemory(0, 5000), await ReadCommittedAsync(ov, "/str020-fail-dst.bin"),
                "a failed copy does not undo earlier successful writes");

            // DfsStream source -> failing destination.
            byte[] data = NonZero(0x5020_3000, 3 * Chunk);
            await vol.Files.WriteAllBytesAsync("/str020-fail-src.bin", data, Ct);
            await using DfsStream src = await vol.Files.OpenReadAsync("/str020-fail-src.bin", Ct);
            src.Seek(123, SeekOrigin.Begin);
            var failingDest = new FailingWriteStream(failOnWrite: 3);
            Exception? destEx = await Record.ExceptionAsync(() => useAsync ? src.CopyToAsync(failingDest, 4096, Ct) : Sync(() => src.CopyTo(failingDest, 4096)));
            Assert.True(destEx is IOException { Message: FailingWriteStream.Message }, $"the destination failure surfaces, got {destEx}");
            BytesEqual(data.AsMemory(123, (int)failingDest.Accepted.Length), failingDest.Accepted.ToArray(), "bytes delivered before the failure");
            Assert.Equal(123 + failingDest.Accepted.Length + failingDest.RejectedCount, src.Position);
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-025

    public static TheoryData<DisposedKind, DisposeMethod> DisposedCases()
    {
        var data = new TheoryData<DisposedKind, DisposeMethod>();
        foreach (DisposedKind kind in Enum.GetValues<DisposedKind>())
        {
            foreach (DisposeMethod method in Enum.GetValues<DisposeMethod>())
                data.Add(kind, method);
        }

        return data;
    }

    private static readonly (string Name, Func<DfsStream, Task> Op)[] DisposedOps =
    [
        ("Read(byte[],int,int)", s => Sync(() => s.Read(new byte[4], 0, 4))),
        ("Read(Span)", s => Sync(() => s.Read(new byte[4].AsSpan()))),
        ("ReadAsync(byte[],int,int)", s => s.ReadAsync(new byte[4], 0, 4, Ct)),
        ("ReadAsync(Memory)", s => s.ReadAsync(new byte[4].AsMemory(), Ct).AsTask()),
        ("ReadByte", s => Sync(() => s.ReadByte())),
        ("ReadExactly", s => Sync(() => s.ReadExactly(new byte[4]))),
        ("ReadAtLeast", s => Sync(() => s.ReadAtLeast(new byte[4], 1))),
        ("Write(byte[],int,int)", s => Sync(() => s.Write(new byte[4], 0, 4))),
        ("Write(ReadOnlySpan)", s => Sync(() => s.Write(new byte[4].AsSpan()))),
        ("WriteAsync(byte[],int,int)", s => s.WriteAsync(new byte[4], 0, 4, Ct)),
        ("WriteAsync(ReadOnlyMemory)", s => s.WriteAsync(new byte[4].AsMemory(), Ct).AsTask()),
        ("WriteByte", s => Sync(() => s.WriteByte(1))),
        ("Seek", s => Sync(() => s.Seek(0, SeekOrigin.Begin))),
        ("Position.get", s => Sync(() => _ = s.Position)),
        ("Position.set", s => Sync(() => { s.Position = 0; })),
        ("Length", s => Sync(() => _ = s.Length)),
        ("SetLength", s => Sync(() => s.SetLength(0))),
        ("Flush", s => Sync(s.Flush)),
        ("FlushAsync", s => s.FlushAsync(Ct)),
        ("CopyTo", s => Sync(() => s.CopyTo(new MemoryStream()))),
        ("CopyToAsync", s => s.CopyToAsync(new MemoryStream(), Ct)),
        ("GetLengthAsync", s => s.GetLengthAsync(Ct).AsTask()),
        ("GetInfoAsync", s => s.GetInfoAsync(Ct).AsTask()),
        ("SetLengthAsync", s => s.SetLengthAsync(0, Ct)),
        ("CommitAsync", s => s.CommitAsync(Ct)),
        ("AbortAsync", s => s.AbortAsync(Ct)),
    ];

    /// <summary>
    /// After a successful Dispose/DisposeAsync/Close (which first publishes pending writes), CanRead/CanWrite/CanSeek are
    /// false, every further operation throws ObjectDisposedException, repeated disposal is a no-op, the server-side handle
    /// is closed and its share reservation released (an exclusive open succeeds).
    /// </summary>
    [Theory]
    [MemberData(nameof(DisposedCases))]
    [Catalog("STR-025", "U", "INV-02", "INV-16")]
    public async Task STR_025_DisposedStreamFails(DisposedKind kind, DisposeMethod method)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str025.bin";
            byte[] data = NonZero(0x5025_0000, 10_000);
            await vol.Files.WriteAllBytesAsync(path, data, Ct);
            var model = new ReferenceFileModel(data);

            DfsTransaction? tx = kind == DisposedKind.TransactionBound ? await vol.Transactions.BeginAsync(null, Ct) : null;
            DfsStream s = kind switch
            {
                DisposedKind.ReadOnlyLive => await vol.Files.OpenReadAsync(path, Ct),
                DisposedKind.ReadOnlyPinned => await vol.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct),
                DisposedKind.WriteOnly => await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.Write), Ct),
                DisposedKind.ReadWrite => await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct),
                DisposedKind.Append => await vol.Files.OpenAsync(path, Writer(FileMode.Append, FileAccess.Write), Ct),
                DisposedKind.TransactionBound => await tx!.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite), Ct),
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
            };
            InodeId inode = s.InodeId;
            if (s.CanWrite)
            {
                byte[] pending = NonZero(0x5025_0001, 50);
                long at = kind == DisposedKind.Append ? data.Length : 100;
                if (kind != DisposedKind.Append)
                    s.Seek(at, SeekOrigin.Begin);
                await s.WriteAsync(pending, Ct);
                model.Write(at, pending);
            }

            switch (method)
            {
                case DisposeMethod.Dispose:
                    s.Dispose();
                    break;
                case DisposeMethod.DisposeAsync:
                    await s.DisposeAsync();
                    break;
                case DisposeMethod.Close:
                    s.Close();
                    break;
            }

            Assert.False(s.CanRead, $"{kind}/{method}: CanRead after disposal");
            Assert.False(s.CanWrite, $"{kind}/{method}: CanWrite after disposal");
            Assert.False(s.CanSeek, $"{kind}/{method}: CanSeek after disposal");
            Assert.False(s.CanTimeout);
            foreach ((string name, Func<DfsStream, Task> op) in DisposedOps)
            {
                Exception? ex = await Record.ExceptionAsync(() => op(s));
                Assert.True(ex is ObjectDisposedException, $"{kind}/{method}: {name} after disposal must throw ObjectDisposedException, got {ex?.GetType().FullName ?? "success"}.");
            }

            // Repeated disposal through any entry point is a no-op.
            s.Dispose();
            await s.DisposeAsync();
            s.Close();

            if (tx is not null)
            {
                Assert.False((await tx.CommitAsync(Ct)).IsNone, "the transaction commits the data staged by the disposed stream");
                await tx.DisposeAsync();
            }

            model.Flush();
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), $"{kind}/{method}: disposal published the pending writes exactly once");

            await cluster.WaitUntilAsync(async () =>
                (await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles.All(h => h.Inode != inode || h.Expired),
                Bounded, $"{kind}/{method}: disposal closes the server-side handle", Ct);
            await using (DfsStream exclusive = await ov.Files.OpenAsync(path,
                             new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.None }, Ct))
            {
                Assert.True(exclusive.CanWrite, "an exclusive open succeeds once the disposed handle released its reservation");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-026

    public static TheoryData<ReadFailure, ReadOverload> ReadFailureCases()
    {
        var data = new TheoryData<ReadFailure, ReadOverload>();
        foreach (ReadFailure failure in Enum.GetValues<ReadFailure>())
        {
            foreach (ReadOverload overload in new[] { ReadOverload.ArraySync, ReadOverload.SpanSync, ReadOverload.ArrayAsync, ReadOverload.MemoryAsync, ReadOverload.ReadByte })
            {
                // A one-byte ReadByte never spans two extents, so the mid-read fault point would be unreachable.
                if (failure == ReadFailure.TransportFaultMidMultiExtentRead && overload == ReadOverload.ReadByte)
                    continue;
                data.Add(failure, overload);
            }
        }

        return data;
    }

    /// <summary>
    /// A read that fails without a successful return leaves Position unchanged and reports an error (never a count);
    /// whatever it wrote into the caller's buffer is never presented as data — the next successful read delivers the
    /// correct bytes from the unchanged position. A mid-read failure may instead return a verified short prefix.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReadFailureCases))]
    [Catalog("STR-026", "U", "INV-07")]
    public async Task STR_026_ReadFailureDoesNotMovePosition(ReadFailure failure, ReadOverload overload)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            const string path = "/str026.bin";
            int len = 3 * Chunk;
            byte[] data = NonZero(0x5026_0000, len);
            await writer.GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);

            TimeSpan? timeout = failure is ReadFailure.OperationTimeout or ReadFailure.ClientIsolated ? TimeSpan.FromSeconds(1) : null;
            await using DfsClient reader = await ConnectAsync(cluster, "r", operationTimeout: timeout);
            DfsVolume rv = reader.GetVolume(Vol);
            DfsStream s = await rv.Files.OpenReadAsync(path, Ct);
            s.Seek(1000, SeekOrigin.Begin);
            BytesEqual(data.AsMemory(1000, 10), await ReadUpToAsync(s, 10), "warm-up read in the first extent");
            long before = Chunk + 10; // inside the second extent, which the reader has not fetched yet
            s.Seek(before, SeekOrigin.Begin);
            int request = failure == ReadFailure.TransportFaultMidMultiExtentRead ? Chunk + 4096 : 4096;
            var buffer = new byte[request];
            buffer.AsSpan().Fill(0xEE);

            FaultArm? arm = null;
            MessageHold? hold = null;
            switch (failure)
            {
                case ReadFailure.TransportFaultAtLeaderReply:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientReplyReceived, Operation: DfsOperationKind.Read, Count: int.MaxValue),
                        FaultAction.Throw(() => new IOException("injected: connection reset while receiving the read reply")));
                    break;
                case ReadFailure.TransportFaultMidMultiExtentRead:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientReplyReceived, Operation: DfsOperationKind.ReadObject, Occurrence: 2, Count: int.MaxValue),
                        FaultAction.Throw(() => new IOException("injected: connection reset while receiving object data")));
                    break;
                case ReadFailure.IntegrityOnAllReplicas:
                {
                    ExtentRefInfo extent = (await GetExtentsAsync(cluster, path)).Single(e => e.Offset <= before && before < e.Offset + e.Length);
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(extent.Object);
                    Assert.True(holders.Count > 0, "precondition: the extent has holders");
                    foreach (string holder in holders)
                        await cluster.Node(holder).Disk.CorruptObjectAsync(extent.Object, offset: extent.ObjectOffset + (before - extent.Offset));
                    break;
                }

                case ReadFailure.OperationTimeout:
                    hold = cluster.Network.Hold(new MessageFilter(From: "client:r", Type: MessageType.ReadRange));
                    break;
                case ReadFailure.ClientIsolated:
                    cluster.Network.Isolate("client:r");
                    break;
            }

            int? returned = null;
            Exception? ex = await Record.ExceptionAsync(async () => returned = await ReadOnceAsync(s, overload, buffer, 0, request));
            string label = $"{failure} via {overload}";
            if (ex is null)
            {
                Assert.True(failure == ReadFailure.TransportFaultMidMultiExtentRead,
                    $"{label}: the read must fail, but it returned {returned} (a count would present the caller buffer as valid data).");
                int n = returned!.Value;
                Assert.InRange(n, 1, request);
                BytesEqual(data.AsMemory((int)before, n), buffer.AsMemory(0, n), $"{label}: a successful short prefix is verified data");
                Assert.Equal(before + n, s.Position);
            }
            else
            {
                Assert.False(ex is EndOfStreamException, $"{label}: a failure is not EOF");
                switch (failure)
                {
                    case ReadFailure.IntegrityOnAllReplicas:
                        AssertAvailabilityFailure(ex, allowIntegrity: true, label);
                        break;
                    case ReadFailure.OperationTimeout:
                    case ReadFailure.ClientIsolated:
                        AssertAvailabilityFailure(ex, allowIntegrity: false, label);
                        break;
                    default:
                        Assert.True(ex is IOException or TimeoutException, $"{label}: the transport failure surfaces as an I/O/availability error, got {ex.GetType().FullName}: {ex.Message}");
                        break;
                }

                Assert.True(s.Position == before, $"{label}: a failed read must leave Position at {before}, found {s.Position}.");
            }

            if (arm is not null)
            {
                cluster.Faults.AssertReached(arm);
                arm.Disarm();
            }

            if (hold is not null)
            {
                hold.DropAll();
                hold.Dispose();
            }

            if (failure == ReadFailure.ClientIsolated)
                cluster.Network.Heal();

            if (failure == ReadFailure.IntegrityOnAllReplicas)
            {
                // No healthy copy exists: the corrupt range keeps failing; uncorrupted ranges still read correctly.
                await AssertReadFailsWithoutEofAsync(s, ReadOverload.MemoryAsync, before, allowIntegrity: true, $"{label}: second attempt");
                s.Seek(0, SeekOrigin.Begin);
                BytesEqual(data.AsMemory(0, 100), await ReadUpToAsync(s, 100), $"{label}: the intact first extent");
                await s.DisposeAsync();
            }
            else
            {
                await AssertOldOrNewStreamRecoversAsync(cluster, rv, s, path, data, s.Position, $"{label}: resume at the unchanged position");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-027

    public static TheoryData<WriteOverload, DfsWriteMode> BufferOwnershipCases()
    {
        var data = new TheoryData<WriteOverload, DfsWriteMode>();
        foreach (WriteOverload overload in new[] { WriteOverload.ArraySync, WriteOverload.SpanSync, WriteOverload.ArrayAsync, WriteOverload.MemoryAsync, WriteOverload.BeginEndWrite })
        {
            data.Add(overload, DfsWriteMode.OnFlush);
            data.Add(overload, DfsWriteMode.WriteThrough);
        }

        return data;
    }

    /// <summary>
    /// Once a write call returns, the SDK owns an independent copy: reusing one buffer for 30 different writes and returning
    /// pooled buffers that are immediately rented and scribbled never changes the staged or committed bytes.
    /// </summary>
    [Theory]
    [MemberData(nameof(BufferOwnershipCases))]
    [Catalog("STR-027", "U", "INV-02", "INV-07", "INV-16")]
    public async Task STR_027_CallerWriteBufferOwnership(WriteOverload overload, DfsWriteMode mode)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            const string path = "/str027.bin";
            var accepted = new MemoryStream();
            DfsStream s = await vol.Files.OpenAsync(path, Writer(FileMode.CreateNew, FileAccess.ReadWrite, mode), Ct);

            var reused = new byte[8192];
            int[] sizes = [10, Inline, 8192, 1, 777];
            for (int call = 0; call < 30; call++)
            {
                int size = sizes[call % sizes.Length];
                TestData.Bytes(0x5027_0000UL + (ulong)call, size).CopyTo(reused, 0);
                await WriteOnceAsync(s, overload, reused, 0, size);
                accepted.Write(reused, 0, size);
                reused.AsSpan().Fill((byte)(0xA0 + call % 16)); // scribble right after the call returned
            }

            foreach (int size in new[] { Chunk + 1, 3 * Chunk + 7, 2 * StreamBuffer })
            {
                byte[] bytes = TestData.Bytes(0x5027_1000UL + (ulong)size, size);
                byte[] rented = ArrayPool<byte>.Shared.Rent(size);
                bytes.CopyTo(rented, 0);
                await WriteOnceAsync(s, overload, rented, 0, size);
                accepted.Write(bytes);
                rented.AsSpan().Fill(0x5A);
                ArrayPool<byte>.Shared.Return(rented);
                byte[] again = ArrayPool<byte>.Shared.Rent(size); // very likely the same array: reuse it
                again.AsSpan().Fill(0xA5);
                ArrayPool<byte>.Shared.Return(again);
            }

            byte[] expected = accepted.ToArray();
            BytesEqual(expected, await ReadWholeAsync(s), $"{overload}/{mode}: the staged view holds the accepted bytes");
            await s.FlushAsync(Ct);
            await s.DisposeAsync();
            BytesEqual(expected, await ReadCommittedAsync(observer.GetVolume(Vol), path), $"{overload}/{mode}: committed bytes are the accepted ones");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-028

    /// <summary>
    /// Cancelling memory-based I/O whose request is held in the network completes the call within the bound; from then on
    /// the SDK never touches the application's memory (no span/pin access, no outstanding pin, the sentinel written after
    /// completion survives the late reply) and never publishes bytes read from it afterwards. Cancellation is reported
    /// as OperationCanceledException only when publication cannot have happened; otherwise as an unknown outcome.
    /// </summary>
    [Theory]
    [InlineData(MemoryOp.ReadInline)]
    [InlineData(MemoryOp.ReadExtent)]
    [InlineData(MemoryOp.WriteThroughCommit)]
    [InlineData(MemoryOp.OnFlushUpload)]
    [Catalog("STR-028", "U", "INV-07", "INV-16")]
    public async Task STR_028_CancellationDoesNotRetainMemoryUnsafely(MemoryOp op)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient seeder = await ConnectAsync(cluster, "w");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str028.bin";
            byte[] data = NonZero(0x5028_0000, op == MemoryOp.ReadInline ? 3000 : 3 * Chunk);
            await seeder.GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);

            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            bool isRead = op is MemoryOp.ReadInline or MemoryOp.ReadExtent;
            using var memory = new TrackingMemoryManager(op == MemoryOp.OnFlushUpload ? 2 * StreamBuffer + 5 : 2048);
            byte[] payload = NonZero(0x5028_0001, memory.Array.Length);
            if (!isRead)
                payload.CopyTo(memory.Array, 0);
            MessageType held = op switch
            {
                MemoryOp.ReadInline => MessageType.ReadRange,
                MemoryOp.ReadExtent => MessageType.GetObject,
                MemoryOp.WriteThroughCommit => MessageType.CommitFile,
                _ => MessageType.PutObject,
            };

            DfsStream s = isRead
                ? await vol.Files.OpenAsync(path, Reader(DfsReadView.Live), Ct)
                : await vol.Files.OpenAsync(path, Writer(FileMode.Open, FileAccess.ReadWrite,
                    op == MemoryOp.WriteThroughCommit ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
            long before = op == MemoryOp.ReadExtent ? Chunk + 10 : 100;
            s.Seek(before, SeekOrigin.Begin);

            using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c0", Type: held));
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            Task<int> io = isRead
                ? s.ReadAsync(memory.Memory, cts.Token).AsTask()
                : AsInt(s.WriteAsync(memory.Memory, cts.Token).AsTask());
            await cluster.WaitUntilAsync(() => Task.FromResult(io.IsCompleted || hold.Count >= 1), Bounded,
                $"{op}: the request serving the caller's memory is in flight (held) or the call already completed", Ct);
            bool heldBeforeCancel = hold.Count >= 1;
            cts.Cancel();

            Exception? outcome = await Record.ExceptionAsync(() => io.Within(cluster, $"{op}: a cancelled memory-based call completes without waiting for the held request"));
            Assert.False(outcome is TimeoutException && !io.IsCompleted, $"{op}: the cancelled call did not complete.");
            int? readCount = outcome is null && isRead ? io.Result : null;
            byte[]? readBytes = readCount is int rc ? memory.Array.AsSpan(0, rc).ToArray() : null;

            // Ownership is back with the application from here on.
            memory.Release();
            Assert.True(memory.OutstandingPins == 0, $"{op}: {memory.OutstandingPins} pin(s) of application memory survive the call's completion.");
            memory.Array.AsSpan().Fill(0xEE);
            hold.ReleaseAll();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct); // the late request/reply arrives and is processed
            Assert.True(memory.Violations == 0, $"{op}: the SDK accessed application memory {memory.Violations} time(s) after the call completed.");
            Assert.True(memory.Array.AsSpan().IndexOfAnyExcept((byte)0xEE) < 0, $"{op}: a late reply was written into application memory after the call completed.");
            Log($"STR-028 {op}: heldBeforeCancel={heldBeforeCancel}, outcome={outcome?.GetType().Name ?? "completed"}");

            switch (op)
            {
                case MemoryOp.ReadInline:
                case MemoryOp.ReadExtent:
                    if (outcome is null)
                    {
                        Assert.InRange(readCount!.Value, 1, memory.Array.Length);
                        BytesEqual(data.AsMemory((int)before, readCount.Value), readBytes!, $"{op}: a read that completed before cancellation returned valid bytes");
                        Assert.Equal(before + readCount.Value, s.Position);
                    }
                    else
                    {
                        Assert.True(outcome is OperationCanceledException, $"{op}: a cancelled read reports OperationCanceledException, got {outcome.GetType().FullName}: {outcome.Message}");
                        Assert.Equal(before, s.Position);
                    }

                    await s.DisposeAsync();
                    break;

                case MemoryOp.WriteThroughCommit:
                {
                    Assert.True(outcome is null or DfsCommitOutcomeUnknownException || (outcome is OperationCanceledException && !heldBeforeCancel),
                        $"{op}: once the commit request was sent, cancellation must surface as an unknown outcome (or success), got {outcome?.GetType().FullName}.");
                    byte[] committed = await ReadCommittedAsync(ov, path);
                    var withPayload = new ReferenceFileModel(data);
                    withPayload.Write(before, payload);
                    bool old = committed.AsSpan().SequenceEqual(data);
                    bool applied = committed.AsSpan().SequenceEqual(withPayload.Staged);
                    Assert.True(old || applied, $"{op}: committed state is either unchanged or has exactly the original payload (never bytes read from memory after completion).");
                    if (outcome is null)
                        Assert.True(applied, $"{op}: a successful WriteThrough call is published.");
                    await DisposeQuietlyAsync(s);
                    break;
                }

                case MemoryOp.OnFlushUpload:
                {
                    Assert.True(outcome is null or OperationCanceledException,
                        $"{op}: an OnFlush write never publishes, so cancellation is definite (or the write completed), got {outcome?.GetType().FullName}.");
                    BytesEqual(data, await ReadCommittedAsync(ov, path), $"{op}: nothing was published by a cancelled/unflushed OnFlush write");
                    if (outcome is null && !s.IsFaulted)
                    {
                        await s.FlushAsync(Ct);
                        var withPayload = new ReferenceFileModel(data);
                        withPayload.Write(before, payload);
                        BytesEqual(withPayload.Staged, await ReadCommittedAsync(ov, path), $"{op}: the flushed bytes are the originally accepted ones, not the sentinel");
                        await s.DisposeAsync();
                    }
                    else
                    {
                        if (!s.IsFaulted)
                            await s.AbortAsync(Ct);
                        await DisposeQuietlyAsync(s);
                        BytesEqual(data, await ReadCommittedAsync(ov, path), $"{op}: a cancelled write is never published later");
                    }

                    break;
                }
            }
        }, Ct);
    }

    private static async Task DisposeQuietlyAsync(DfsStream s)
    {
        try
        {
            await s.DisposeAsync();
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException)
        {
            // A faulted stream (unknown outcome) reports its fault on disposal; the assertions above already covered it.
        }
    }
}

/// <summary>Returns <c>chunk</c> bytes per read and throws on the n-th read (STR-020).</summary>
internal sealed class FailingReadStream(byte[] data, int chunk, int failOnRead) : Stream
{
    public const string Message = "injected source failure";
    private int _reads;
    private int _position;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => _position; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        if (++_reads >= failOnRead)
            throw new IOException(Message);
        int n = Math.Min(Math.Min(chunk, buffer.Length), data.Length - _position);
        data.AsSpan(_position, n).CopyTo(buffer);
        _position += n;
        return n;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(Read(buffer.Span));

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        Task.FromResult(Read(buffer.AsSpan(offset, count)));
}

/// <summary>Accepts writes until the n-th, which throws (STR-020).</summary>
internal sealed class FailingWriteStream(int failOnWrite) : Stream
{
    public const string Message = "injected destination failure";
    private int _writes;

    public MemoryStream Accepted { get; } = new();
    public long RejectedCount { get; private set; }
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (++_writes >= failOnWrite)
        {
            RejectedCount = buffer.Length;
            throw new IOException(Message);
        }

        Accepted.Write(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        Write(buffer.AsSpan(offset, count));
        return Task.CompletedTask;
    }
}
