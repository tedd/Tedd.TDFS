using System.Text;
using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Client.Internal.Overlay;
using Tedd.Tdfs.Client.Internal.Text;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Client;

/// <summary>Pure SDK internals: the staged overlay, text rules, representation rules, sequence allocation and validation.</summary>
public sealed class ClientInternalsTests
{
    // ------------------------------------------------------------------------------------------------ overlay

    /// <summary>Reads the staged view through <see cref="StagedOverlay.Map"/> with a base image.</summary>
    private static byte[] View(StagedOverlay overlay, byte[] baseBytes)
    {
        var result = new byte[overlay.Length];
        foreach (StagedPiece piece in overlay.Map(0, (int)overlay.Length))
        {
            switch (piece.Source)
            {
                case StagedSource.Buffered:
                    piece.Segment!.Bytes.Slice((int)(piece.Offset - piece.Segment.Offset), piece.Length).Span.CopyTo(result.AsSpan((int)piece.Offset));
                    break;
                case StagedSource.Base:
                    baseBytes.AsSpan((int)piece.Offset, piece.Length).CopyTo(result.AsSpan((int)piece.Offset));
                    break;
                case StagedSource.Zero:
                    break;
                default:
                    throw new InvalidOperationException(piece.Source.ToString());
            }
        }

        return result;
    }

    private static void ModelWrite(ref byte[] model, long offset, byte[] data)
    {
        if (model.Length < offset + data.Length)
            Array.Resize(ref model, (int)(offset + data.Length));
        data.CopyTo(model, offset);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void Overlay_MatchesAReferenceModelUnderRandomWritesAndTruncations(int seed)
    {
        var random = new Random(seed);
        byte[] baseBytes = new byte[random.Next(0, 5000)];
        random.NextBytes(baseBytes);
        var overlay = new StagedOverlay(baseBytes.Length);
        byte[] model = baseBytes.ToArray();
        for (int step = 0; step < 400; step++)
        {
            int op = random.Next(10);
            if (op < 6)
            {
                long offset = random.Next(0, model.Length + 300);
                byte[] data = new byte[random.Next(1, 700)];
                random.NextBytes(data);
                if (random.Next(2) == 0)
                    overlay.Write(offset, data);
                else
                    overlay.WriteOwned(offset, data.ToArray());
                ModelWrite(ref model, offset, data);
            }
            else if (op < 8)
            {
                // Sequential small appends exercise coalescing.
                long offset = overlay.Length;
                for (int i = 0; i < 20; i++)
                {
                    byte[] one = [(byte)random.Next(256)];
                    overlay.Write(offset + i, one);
                    ModelWrite(ref model, offset + i, one);
                }
            }
            else
            {
                int length = random.Next(0, model.Length + 500);
                overlay.SetLength(length);
                Array.Resize(ref model, length);
            }

            Assert.Equal(model.Length, overlay.Length);
            Assert.Equal(model, View(overlay, baseBytes));
        }
    }

    [Fact]
    public void Overlay_RestoreUndoesAFailedCallExactly()
    {
        var overlay = new StagedOverlay(100);
        overlay.Write(10, new byte[] { 1, 2, 3 });
        StagedOverlay.Snapshot before = overlay.Capture();
        byte[] baseBytes = new byte[100];
        byte[] expected = View(overlay, baseBytes);

        overlay.Write(13, new byte[] { 4, 5 }); // coalesces into the previous buffer
        overlay.Write(50, new byte[200]);
        overlay.SetLength(20);
        overlay.Restore(before);

        Assert.Equal(100, overlay.Length);
        Assert.Equal(expected, View(overlay, baseBytes));
        Assert.Equal(3, overlay.PendingBytes);

        // Further coalescing after a restore never corrupts the restored bytes.
        overlay.Write(13, new byte[] { 9 });
        byte[] view = View(overlay, baseBytes);
        Assert.Equal(new byte[] { 1, 2, 3, 9 }, view[10..14]);
    }

    [Fact]
    public void Overlay_TruncationIsPublishedAsTruncateTo()
    {
        var overlay = new StagedOverlay(1000);
        Assert.False(overlay.IsDirty);
        Assert.Equal(-1, overlay.TruncateTo);
        overlay.SetLength(10);
        overlay.SetLength(1000);
        Assert.True(overlay.IsDirty);
        Assert.Equal(10, overlay.TruncateTo);
        overlay.Reset(1000);
        Assert.False(overlay.IsDirty);
    }

    // ------------------------------------------------------------------------------------------------ text

    [Theory]
    [InlineData(new byte[] { 0xEF, 0xBB, 0xBF, (byte)'a' }, 3, "utf-8")]
    [InlineData(new byte[] { 0xFF, 0xFE, (byte)'a', 0 }, 2, "utf-16")]
    [InlineData(new byte[] { 0xFE, 0xFF, 0, (byte)'a' }, 2, "utf-16BE")]
    [InlineData(new byte[] { 0xFF, 0xFE, 0, 0 }, 4, "utf-32")]
    [InlineData(new byte[] { (byte)'a', 0xEF, 0xBB, 0xBF }, 0, "utf-8")]
    public void Text_DetectsByteOrderMarks(byte[] bytes, int skip, string webName)
    {
        (Encoding encoding, int detectedSkip) = TextCodec.Detect(bytes, null);
        Assert.Equal(skip, detectedSkip);
        Assert.Equal(webName, encoding.WebName);
    }

    [Fact]
    public void Text_LinesSplitLikeStreamReaderAcrossChunkBoundaries()
    {
        const string text = "a\r\nb\rc\n\r\nlast";
        string[] expected = ["a", "b", "c", "", "last"];
        Assert.Equal(expected, TextCodec.SplitLines(text));
        for (int split = 0; split <= text.Length; split++)
        {
            var lines = new List<string>();
            var splitter = new TextCodec.LineSplitter();
            splitter.Append(text.AsSpan(0, split), lines);
            splitter.Append(text.AsSpan(split), lines);
            splitter.Complete(lines);
            Assert.Equal(expected, lines);
        }

        Assert.Equal(["x"], TextCodec.SplitLines("x\n"));
        Assert.Empty(TextCodec.SplitLines(string.Empty));
    }

    [Fact]
    public void Text_EncoderFailureIsAnArgumentExceptionWithoutThePayload()
    {
        var strict = Encoding.GetEncoding("us-ascii", EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback);
        const string secret = "s3cr3t-payload-é";
        ArgumentException ex = Assert.Throws<ArgumentException>(() => TextCodec.Encode(secret, strict, includePreamble: false));
        Assert.DoesNotContain("s3cr3t", ex.Message, StringComparison.Ordinal);
        Assert.Empty(TextCodec.Encode(string.Empty, null, includePreamble: true));
    }

    // ------------------------------------------------------------------------------------------------ representation

    private static VolumeEntry VolumeWith(int voters, out Dictionary<NodeId, DfsTopologyLabels> labels)
    {
        var ids = new List<NodeId>();
        labels = [];
        for (int i = 0; i < voters; i++)
        {
            var id = new NodeId(new Guid(i + 1, 0, 0, new byte[8]));
            ids.Add(id);
            labels[id] = new DfsTopologyLabels("r1", $"z{i}", $"rack{i}", $"host{i}", "d");
        }

        return new VolumeEntry(new VolumeId(Guid.NewGuid()), "v", 1, WireVolumeState.Active, ids, [], null, new DfsMetadataPolicy(), DfsStoragePolicy.Default,
            new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal), 4096, new DfsVolumeLimits());
    }

    [Theory]
    [InlineData(3, 2, 2, true)]
    [InlineData(3, 3, 3, false)]
    [InlineData(5, 3, 3, true)]
    [InlineData(1, 2, 2, false)]
    public void Layout_InlineOnlyWhenEveryMajoritySatisfiesThePolicy(int voters, int copies, int zones, bool expected)
    {
        VolumeEntry volume = VolumeWith(voters, out Dictionary<NodeId, DfsTopologyLabels> labels);
        var policy = new DfsStoragePolicy { TargetCopies = Math.Max(3, copies), TargetDistinctZones = Math.Max(3, zones), DurableCopiesBeforeAck = copies, DurableZonesBeforeAck = zones };
        Assert.Equal(expected, ContentLayout.InlinePermitted(volume, policy, id => labels.GetValueOrDefault(id)));
    }

    [Fact]
    public void Layout_ChunksAlignToAbsoluteBoundaries()
    {
        Assert.Equal([(0L, 10), (10L, 10), (20L, 5)], ContentLayout.Chunks(0, 25, 10).ToArray());
        Assert.Equal([(7L, 3), (10L, 10), (20L, 1)], ContentLayout.Chunks(7, 21, 10).ToArray());
        Assert.Empty(ContentLayout.Chunks(5, 5, 10));
    }

    // ------------------------------------------------------------------------------------------------ sequences

    [Fact]
    public void Session_WatermarkNeverPassesProtectedOrApplicationIds()
    {
        var session = new VolumeSession(7, 1, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(90), default, 1);
        Assert.False(session.TryAllocate(out _), "no sequence is used before the leader reserved it");
        session.AddBlock(1, 4);
        Assert.True(session.TryAllocate(out DfsOperationId a));
        Assert.True(session.TryAllocate(out DfsOperationId b));
        Assert.Equal(new DfsOperationId(7, 1), a);
        Assert.Equal(1UL, session.Watermark);
        session.Complete(a);
        Assert.Equal(2UL, session.Watermark);
        session.Release(b); // never transmitted: reused next, still protected
        Assert.Equal(2UL, session.Watermark);
        Assert.True(session.TryAllocate(out DfsOperationId reused));
        Assert.Equal(b, reused);
        session.Complete(reused);
        Assert.Equal(3UL, session.Watermark);

        session.AddApplicationVisible(10);
        session.Complete(new DfsOperationId(7, 10));
        Assert.Equal(3UL, session.Watermark);
        Assert.True(session.TryAllocate(out DfsOperationId c));
        Assert.Equal(3UL, c.Sequence);

        // The leader retired sequences of the block (OperationExpired on an id's only transmission, PROTOCOL.md §5.1): the
        // rest of the block is abandoned and allocation continues in a new block; the watermark still never passes the
        // in-flight id or the application id.
        session.AbandonBlock();
        Assert.False(session.TryAllocate(out _), "an abandoned block is not used any further");
        session.AddBlock(11, 4);
        Assert.True(session.TryAllocate(out DfsOperationId d));
        Assert.Equal(11UL, d.Sequence);
        Assert.Equal(3UL, session.Watermark);
        session.Complete(c);
        session.Complete(d);
        Assert.Equal(10UL, session.Watermark);
    }

    // ------------------------------------------------------------------------------------------------ validation

    [Fact]
    public void Validation_PathsNameTheirParameter()
    {
        Assert.Equal("/a/b", Validation.Path("/a/b"));
        Assert.Throws<ArgumentNullException>(() => Validation.Path(null, "sourcePath"));
        Assert.Equal("sourcePath", Assert.Throws<ArgumentException>(() => Validation.Path("a", "sourcePath")).ParamName);
        Assert.Equal("path", Assert.Throws<ArgumentException>(() => Validation.DirectoryPath("/a:s")).ParamName);
        Assert.Throws<ArgumentException>(() => Validation.FilePath("/"));
        Assert.Equal("/f:s", Validation.FilePath("/f:s:$DATA"));
    }

    [Fact]
    public void Validation_OpenOptionsFollowTheFileStreamRules()
    {
        Validation.OpenOptions(new DfsOpenOptions(), transactional: false);
        Assert.Throws<ArgumentException>(() => Validation.OpenOptions(new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.Read }, false));
        Assert.Throws<ArgumentException>(() => Validation.OpenOptions(new DfsOpenOptions { Mode = FileMode.Append, Access = FileAccess.ReadWrite }, false));
        Assert.Throws<NotSupportedException>(() => Validation.OpenOptions(new DfsOpenOptions { Share = FileShare.Read | FileShare.Inheritable }, false));
        Assert.Throws<ArgumentException>(() => Validation.OpenOptions(new DfsOpenOptions { Share = (FileShare)0x40 }, false));
        Assert.Throws<ArgumentException>(() => Validation.OpenOptions(new DfsOpenOptions { ReadView = DfsReadView.VersionPinned }, true));
        Assert.Throws<ArgumentException>(() => Validation.OpenOptions(new DfsOpenOptions { OperationId = default(DfsOperationId) }, false));
        Assert.Throws<ArgumentOutOfRangeException>(() => Validation.OpenOptions(new DfsOpenOptions { BufferSize = 0 }, false));
        Assert.Throws<NotSupportedException>(() => Validation.OpenOptions(new DfsOpenOptions { Options = FileOptions.Encrypted }, false));
        Assert.Throws<ArgumentNullException>(() => Validation.OpenOptions(null, false));
    }
}
