using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Str;

/// <summary>Shared helpers for the STR integration rows (real QUIC transport and on-disk storage).</summary>
internal static class StrHelpers
{
    public const string Vol = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Three zones, one node each, over QUIC with on-disk storage (real transport/persistence boundaries).</summary>
    public static TestClusterOptions RealBoundaries() =>
        TestClusterOptions.ThreeZones with { Transport = TransportKind.Quic, Storage = StorageKind.Disk };

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name }, Ct);

    /// <summary><see cref="AssertEx.BytesEqual"/> over memory, so an argument list may contain an await.</summary>
    public static void BytesEqual(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string because) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    /// <summary>A reader granting every share so writers can coexist (test plan §5).</summary>
    public static DfsOpenOptions Reader(DfsReadView view = DfsReadView.Live) => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = view,
    };

    /// <summary>
    /// Reads the committed contents as another client sees them, through a pinned reader that grants every share (test
    /// plan §5), so a writer handle that is still open is never a setup-level sharing conflict.
    /// </summary>
    public static async Task<byte[]> ReadCommittedAsync(DfsVolume volume, string path)
    {
        await using DfsStream r = await volume.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
        using var ms = new MemoryStream();
        await r.CopyToAsync(ms, Ct);
        Assert.Equal(ms.Length, r.Position);
        return ms.ToArray();
    }

    /// <summary>Committed length as seen by another client.</summary>
    public static async Task<long> CommittedLengthAsync(DfsVolume observer, string path) =>
        (await observer.Files.GetInfoAsync(path, Ct)).Length;
}

/// <summary>
/// A transparent pass-through used only to observe which calls an outer consumer (BufferedStream) issues to the
/// underlying <see cref="DfsStream"/>: every member forwards unchanged; underlying flushes and delivered bytes are counted.
/// </summary>
internal sealed class FlushObservingStream(DfsStream inner) : Stream
{
    public DfsStream Inner { get; } = inner;

    /// <summary>Number of Flush/FlushAsync calls the consumer forwarded to the DfsStream.</summary>
    public int Flushes { get; private set; }

    /// <summary>Staged length of the DfsStream at its most recent forwarded flush (-1 before any).</summary>
    public long LengthAtLastFlush { get; private set; } = -1;

    public override bool CanRead => Inner.CanRead;
    public override bool CanSeek => Inner.CanSeek;
    public override bool CanWrite => Inner.CanWrite;
    public override long Length => Inner.Length;

    public override long Position
    {
        get => Inner.Position;
        set => Inner.Position = value;
    }

    public override void Flush()
    {
        Inner.Flush();
        Flushes++;
        LengthAtLastFlush = Inner.Length;
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await Inner.FlushAsync(cancellationToken);
        Flushes++;
        LengthAtLastFlush = Inner.Length;
    }

    public override int Read(byte[] buffer, int offset, int count) => Inner.Read(buffer, offset, count);
    public override int Read(Span<byte> buffer) => Inner.Read(buffer);
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => Inner.ReadAsync(buffer, offset, count, cancellationToken);
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => Inner.ReadAsync(buffer, cancellationToken);
    public override int ReadByte() => Inner.ReadByte();
    public override long Seek(long offset, SeekOrigin origin) => Inner.Seek(offset, origin);
    public override void SetLength(long value) => Inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => Inner.Write(buffer, offset, count);
    public override void Write(ReadOnlySpan<byte> buffer) => Inner.Write(buffer);
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => Inner.WriteAsync(buffer, offset, count, cancellationToken);
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => Inner.WriteAsync(buffer, cancellationToken);
    public override void WriteByte(byte value) => Inner.WriteByte(value);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Inner.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await Inner.DisposeAsync();
        await base.DisposeAsync();
    }
}

/// <summary>A non-seekable view of a MemoryStream, used to build streaming-mode reference archives (STR-024).</summary>
internal sealed class NonSeekableStream(Stream inner) : Stream
{
    public override bool CanRead => false; // mirrors a write-only append DfsStream
    public override bool CanSeek => false;
    public override bool CanWrite => inner.CanWrite;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() => inner.Flush();
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
}
