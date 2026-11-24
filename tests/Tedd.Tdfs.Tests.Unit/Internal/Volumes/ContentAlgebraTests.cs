using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// The extent-map algebra of publications against an independent byte-array reference model: random sequences of data,
/// zero and inline changes with truncation and extension must always produce the same logical bytes, never resurrect
/// discarded data (STR-011), keep extents sorted, non-overlapping and inside their objects, and coalesce contiguous ranges.
/// </summary>
public sealed class ContentAlgebraTests
{
    /// <summary>Deterministic pretend object payload: byte i of object n.</summary>
    private static byte ObjectByte(ObjectId obj, long i) => (byte)((obj.Value.GetHashCode() * 31) ^ (int)(i * 7 + 3));

    private static byte[] Materialize(StreamRecord stream)
    {
        if (stream.IsInline)
            return stream.Inline.ToArray();
        var bytes = new byte[stream.Length];
        long previousEnd = 0;
        foreach (ExtentRef e in stream.Extents)
        {
            Assert.True(e.Length > 0);
            Assert.True(e.Offset >= previousEnd, "extents overlap or are unsorted");
            Assert.True(e.End <= stream.Length, "extent beyond the length");
            Assert.True(e.ObjectOffset >= 0 && e.ObjectOffset + e.Length <= e.ObjectLength, "extent maps outside its object");
            for (long i = 0; i < e.Length; i++)
                bytes[e.Offset + i] = ObjectByte(e.Object, e.ObjectOffset + i);
            previousEnd = e.End;
        }

        return bytes;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void RandomPublicationsMatchTheReferenceModel(int seed)
    {
        var random = new Random(seed);
        StreamRecord? stream = null;
        byte[] model = [];
        ulong index = 10;
        for (int step = 0; step < 300; step++)
        {
            long length = model.Length;
            long truncate = random.Next(5) == 0 ? random.Next((int)length + 1) : -1;
            byte[] working = truncate >= 0 ? model[..(int)truncate] : (byte[])model.Clone();
            var changes = new List<StreamChange>();
            int count = random.Next(4);
            long newLength = random.Next(4) == 0 ? random.Next(40_000) : Math.Max(length, random.Next(40_000));
            for (int i = 0; i < count; i++)
            {
                long offset = random.Next(30_000);
                long len = 1 + random.Next(6000);
                Array.Resize(ref working, (int)Math.Max(working.Length, offset + len));
                switch (random.Next(3))
                {
                    case 0:
                    {
                        ObjectId obj = Obj((ulong)random.Next(1, 1 << 20));
                        long objectOffset = random.Next(100);
                        changes.Add(DataChange(offset, len, obj, 1, objectOffset, objectOffset + len + random.Next(50)));
                        for (long k = 0; k < len; k++)
                            working[offset + k] = ObjectByte(obj, objectOffset + k);
                        break;
                    }

                    case 1:
                        changes.Add(Zero(offset, len));
                        Array.Clear(working, (int)offset, (int)len);
                        break;
                    default:
                    {
                        // Inline bytes would force the small (mixed) path; use them only while the result stays small.
                        var bytes = new byte[len];
                        random.NextBytes(bytes);
                        changes.Add(Inline(offset, bytes));
                        bytes.CopyTo(working, offset);
                        break;
                    }
                }
            }

            Array.Resize(ref working, (int)newLength);
            (bool hasObjects, bool hasInline) = Segments(stream, truncate, changes, newLength);
            bool inline = hasInline && !hasObjects;
            if (hasInline && hasObjects)
            {
                // Mixed content needs leader materialization: apply must refuse rather than guess.
                ContentResult refused = ContentAlgebra.Apply(stream, truncate, changes, newLength, false, int.MaxValue, ++index);
                Assert.Null(refused.Stream);
                continue;
            }

            ContentResult result = ContentAlgebra.Apply(stream, truncate, changes, newLength, inline, int.MaxValue, ++index);
            Assert.True(result.Stream is not null, result.Message);
            stream = result.Stream;
            model = working;
            Assert.Equal(model.Length, (int)stream.Length);
            Assert.Equal(model, Materialize(stream));
            Assert.Equal(index, stream.Revision);
            if (!stream.IsInline)
            {
                for (int i = 1; i < stream.Extents.Count; i++)
                {
                    ExtentRef a = stream.Extents[i - 1];
                    ExtentRef b = stream.Extents[i];
                    Assert.False(a.End == b.Offset && a.Object == b.Object && a.ObjectOffset + a.Length == b.ObjectOffset, "adjacent extents were not coalesced");
                }
            }
        }
    }

    private static (bool HasObjects, bool HasInline) Segments(StreamRecord? stream, long truncate, IReadOnlyList<StreamChange> changes, long newLength)
    {
        Assert.Null(ContentAlgebra.Plan(stream, truncate, changes, newLength, out List<IContentSegment>? small, out ImmutableList<ExtentRef>.Builder? large, out _));
        return (large is { Count: > 0 } || (small?.Any(s => s is ExtentRef) ?? false), small?.Any(s => s is InlineSegment) ?? false);
    }

    // Three positions cover equality, adjacency, containment, and partial overlap in either order.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AllThreeByteTwoEditPublicationsMatchArraySemantics(bool inline)
    {
        const int width = 3;
        StreamChange Write(int start, int length, ulong source) => inline
            ? Inline(start, Enumerable.Range(0, length).Select(i => ObjectByte(Obj(source), i + 1)).ToArray())
            : DataChange(start, length, Obj(source), 1, 1, width + 1);

        IEnumerable<StreamChange> Edits(ulong source)
        {
            for (int start = 0; start < width; start++)
                for (int length = 1; start + length <= width; length++)
                {
                    yield return Zero(start, length);
                    yield return Write(start, length, source);
                }
        }

        StreamRecord initial = ContentAlgebra.Apply(null, -1, [Write(0, width, 1)], width, inline, width, 1).Stream!;
        byte[] original = Enumerable.Range(0, width).Select(i => ObjectByte(Obj(1), i + 1)).ToArray();
        foreach (StreamChange first in Edits(2))
        foreach (StreamChange second in Edits(3))
        for (int cut = 0; cut <= width; cut++)
        for (int length = 0; length <= width + 1; length++)
        {
            byte[] expected = new byte[width + 1];
            Array.Copy(original, expected, cut);
            ApplyBytes(first, 2);
            ApplyBytes(second, 3);
            ContentResult result = ContentAlgebra.Apply(initial, cut, [first, second], length, inline, width + 1, 2);
            Assert.True(result.Stream is not null, result.Message);
            Assert.Equal(expected[..length], Materialize(result.Stream));
            Assert.Equal(original, Materialize(initial));

            void ApplyBytes(StreamChange edit, ulong source)
            {
                for (int i = 0; i < edit.Length; i++)
                    expected[edit.Offset + i] = edit.ChangeKind == RangeChangeKind.Zero ? (byte)0 : ObjectByte(Obj(source), i + 1);
            }
        }
    }

    [Fact]
    public void TruncateThenExtendLeavesAHole()
    {
        ContentResult first = ContentAlgebra.Apply(null, -1, [DataChange(0, 10_000, Obj(1), 1)], 10_000, false, 4096, 1);
        ContentResult second = ContentAlgebra.Apply(first.Stream, 1000, [], 8000, false, 4096, 2);
        StreamRecord s = second.Stream!;
        Assert.Equal(8000, s.Length);
        Assert.Equal(1000, s.Extents.Single().Length);
        Assert.True(s.IsSparse);
    }

    [Fact]
    public void ContiguousRewritesOfTheSameObjectCoalesce()
    {
        ContentResult first = ContentAlgebra.Apply(null, -1, [DataChange(0, 100, Obj(1), 1, 0, 300)], 300, false, 0, 1);
        ContentResult second = ContentAlgebra.Apply(first.Stream, -1, [DataChange(100, 100, Obj(1), 1, 100, 300)], 300, false, 0, 2);
        ContentResult third = ContentAlgebra.Apply(second.Stream, -1, [DataChange(200, 100, Obj(1), 1, 200, 300)], 300, false, 0, 3);
        ExtentRef single = Assert.Single(third.Stream!.Extents);
        Assert.Equal((0L, 300L, 0L), (single.Offset, single.Length, single.ObjectOffset));
    }

    [Fact]
    public void InvalidChangesAreRejected()
    {
        Assert.NotNull(ContentAlgebra.Validate(new StreamChange(-1, 10, RangeChangeKind.Zero, null, default)));
        Assert.NotNull(ContentAlgebra.Validate(new StreamChange(long.MaxValue - 5, 10, RangeChangeKind.Zero, null, default)));
        Assert.NotNull(ContentAlgebra.Validate(new StreamChange(0, 0, RangeChangeKind.Zero, null, default)));
        Assert.NotNull(ContentAlgebra.Validate(DataChange(0, 10, Obj(1), 1, 5, 12)));
        Assert.NotNull(ContentAlgebra.Validate(new StreamChange(0, 3, RangeChangeKind.Inline, null, new byte[2])));
        Assert.NotNull(ContentAlgebra.Validate(new StreamChange(0, 3, (RangeChangeKind)9, null, default)));
        Assert.Null(ContentAlgebra.Validate(DataChange(0, 10, Obj(1), 1, 2, 12)));
        Assert.Equal(DfsErrorCode.InvalidArgument, ContentAlgebra.Apply(null, -1, [], -5, false, 0, 1).Code);
    }
}
