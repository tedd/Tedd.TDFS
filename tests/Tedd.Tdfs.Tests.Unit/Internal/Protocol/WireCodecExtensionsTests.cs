using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>Shared value codecs: canonical sets/maps, integer and enum ranges, fixed-size fields, Core value types.</summary>
public sealed class WireCodecExtensionsTests
{
    private delegate void WriteAction(ref WireWriter writer);

    private delegate T ReadFunc<out T>(ref WireReader reader);

    private static byte[] Encode(WriteAction write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        write(ref writer);
        return buffer.WrittenSpan.ToArray();
    }

    private static T Decode<T>(byte[] data, ReadFunc<T> read)
    {
        var reader = new WireReader(data, WireLimits.Default);
        T value = read(ref reader);
        reader.EnsureEnd();
        return value;
    }

    [Fact]
    public void StringSetsAreSortedOrdinallyAndRejectNonCanonicalInput()
    {
        byte[] encoded = Encode((ref WireWriter w) => w.WriteStringSet(["b", "B", "a", "é"]));
        IReadOnlySet<string> decoded = Decode(encoded, (ref WireReader r) => r.ReadStringSet());
        Assert.Equal(["B", "a", "b", "é"], decoded.ToArray());

        byte[] unsorted = Encode((ref WireWriter w) => w.WriteStringList(["b", "a"]));
        Assert.Throws<DfsProtocolException>(() => Decode(unsorted, (ref WireReader r) => r.ReadStringSet()));
        byte[] duplicate = Encode((ref WireWriter w) => w.WriteStringList(["a", "a"]));
        Assert.Throws<DfsProtocolException>(() => Decode(duplicate, (ref WireReader r) => r.ReadStringSet()));
        Assert.Throws<ArgumentException>(() => Encode((ref WireWriter w) => w.WriteStringSet(["a", "a"])));
    }

    [Fact]
    public void MapsRejectUnsortedOrDuplicateKeys()
    {
        byte[] unsorted = Encode((ref WireWriter w) =>
        {
            w.WriteCount(2);
            w.WriteString("b");
            w.WriteString("1");
            w.WriteString("a");
            w.WriteString("2");
        });
        Assert.Throws<DfsProtocolException>(() => Decode(unsorted, (ref WireReader r) => r.ReadStringMap()));

        byte[] duplicate = Encode((ref WireWriter w) =>
        {
            w.WriteCount(2);
            w.WriteString("a");
            w.WriteStoragePolicy(DfsStoragePolicy.Default);
            w.WriteString("a");
            w.WriteStoragePolicy(DfsStoragePolicy.Default);
        });
        Assert.Throws<DfsProtocolException>(() => Decode(duplicate, (ref WireReader r) => r.ReadPolicyMap()));
    }

    [Fact]
    public void Int32AndUInt16FieldsAreRangeChecked()
    {
        byte[] big = Encode((ref WireWriter w) => w.WriteVarInt((long)int.MaxValue + 1));
        Assert.Throws<DfsProtocolException>(() => Decode(big, (ref WireReader r) => r.ReadVarInt32()));
        byte[] small = Encode((ref WireWriter w) => w.WriteVarInt((long)int.MinValue - 1));
        Assert.Throws<DfsProtocolException>(() => Decode(small, (ref WireReader r) => r.ReadVarInt32()));
        Assert.Equal(int.MinValue, Decode(Encode((ref WireWriter w) => w.WriteVarInt32(int.MinValue)), (ref WireReader r) => r.ReadVarInt32()));
        byte[] wide = Encode((ref WireWriter w) => w.WriteVarUInt(ushort.MaxValue + 1));
        Assert.Throws<DfsProtocolException>(() => Decode(wide, (ref WireReader r) => r.ReadVarUInt16()));
    }

    [Fact]
    public void EnumsAreRangeCheckedAgainstTheirUnderlyingType()
    {
        // PeerRole is a byte enum: 256 must not wrap to 0.
        byte[] wrapped = Encode((ref WireWriter w) => w.WriteVarInt(256));
        Assert.Throws<DfsProtocolException>(() => Decode(wrapped, (ref WireReader r) => r.ReadEnum<PeerRole>()));
        Assert.Throws<DfsProtocolException>(() => Decode(wrapped, (ref WireReader r) => r.ReadEnumRaw<PeerRole>()));
        // Raw reads keep undefined-but-representable values (server-side validation); strict reads reject them.
        byte[] undefined = Encode((ref WireWriter w) => w.WriteVarInt(77));
        Assert.Equal((PeerRole)77, Decode(undefined, (ref WireReader r) => r.ReadEnumRaw<PeerRole>()));
        Assert.Throws<DfsProtocolException>(() => Decode(undefined, (ref WireReader r) => r.ReadEnum<PeerRole>()));
        // Negative values for an unsigned underlying type are rejected; signed ones round-trip.
        byte[] negative = Encode((ref WireWriter w) => w.WriteVarInt(-1));
        Assert.Throws<DfsProtocolException>(() => Decode(negative, (ref WireReader r) => r.ReadEnumRaw<AdminOp>()));
        Assert.Equal((FileOptions)int.MinValue, Decode(Encode((ref WireWriter w) => w.WriteEnum((FileOptions)int.MinValue)), (ref WireReader r) => r.ReadEnumRaw<FileOptions>()));
    }

    [Fact]
    public void FixedSizeBytesAreExact()
    {
        byte[] ok = Encode((ref WireWriter w) => w.WriteBytes(new byte[32]));
        Assert.Equal(32, Decode(ok, (ref WireReader r) => r.ReadFixedBytes(32, "Mac")).Length);
        byte[] shortField = Encode((ref WireWriter w) => w.WriteBytes(new byte[31]));
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => Decode(shortField, (ref WireReader r) => r.ReadFixedBytes(32, "Mac")));
        Assert.Contains("Mac", ex.Message, StringComparison.Ordinal);
        byte[] empty = Encode((ref WireWriter w) => w.WriteBytes([]));
        Assert.True(Decode(empty, (ref WireReader r) => r.ReadEmptyOrFixedBytes(32, "ServerMac")).IsEmpty);
        Assert.Throws<DfsProtocolException>(() => Decode(shortField, (ref WireReader r) => r.ReadEmptyOrFixedBytes(32, "ServerMac")));
    }

    [Fact]
    public void DecodedBytesDoNotAliasTheFrame()
    {
        byte[] frame = MessageCodec.Encode(new HelloProofMessage(Enumerable.Repeat((byte)5, 32).ToArray()));
        var decoded = MessageCodec.Decode<HelloProofMessage>(frame);
        Array.Clear(frame);
        Assert.All(decoded.Proof.ToArray(), b => Assert.Equal(5, b));
    }

    [Fact]
    public void CoreValueTypesRoundTripExactly()
    {
        var capabilities = new DfsServerCapabilities
        {
            ProtocolVersion = 1,
            Features = new HashSet<string>(["z", "a"]),
            MaxFileLength = long.MaxValue / 2,
            MaxPathBytes = 4096,
            MaxComponentBytes = 255,
            MaxPublishBytes = 1L << 40,
            InlineThreshold = 4096,
            ChunkSize = 1 << 20,
            MaxObjectSize = 4 << 20,
            TransactionLimits = new DfsVolumeLimits { MaxTransactionDuration = TimeSpan.FromMinutes(3), MaxPinsPerSession = 9 },
            LeaseDuration = TimeSpan.FromSeconds(2),
        };
        DfsServerCapabilities decodedCaps = Decode(Encode((ref WireWriter w) => w.WriteServerCapabilities(capabilities)), (ref WireReader r) => r.ReadServerCapabilities());
        WireDeepEquality.AssertEqual(capabilities, decodedCaps);

        var metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] };
        WireDeepEquality.AssertEqual(metadata, Decode(Encode((ref WireWriter w) => w.WriteMetadataPolicy(metadata)), (ref WireReader r) => r.ReadMetadataPolicy()));
        var noExplicit = new DfsMetadataPolicy();
        Assert.Null(Decode(Encode((ref WireWriter w) => w.WriteMetadataPolicy(noExplicit)), (ref WireReader r) => r.ReadMetadataPolicy()).ExplicitVoterNodes);

        // Invalid policies are transported unchanged so the receiver can answer InvalidPolicy rather than ProtocolError.
        var invalid = new DfsStoragePolicy { TargetCopies = -1, TargetDistinctZones = 0, DurableCopiesBeforeAck = 5, DurableZonesBeforeAck = 9 };
        DfsStoragePolicy decodedPolicy = Decode(Encode((ref WireWriter w) => w.WriteStoragePolicy(invalid)), (ref WireReader r) => r.ReadStoragePolicy());
        Assert.Equal(invalid, decodedPolicy);
        Assert.NotNull(decodedPolicy.GetValidationError());

        var checkpoint = new DfsBackupCheckpoint("b0", new SnapshotId(Guid.NewGuid()), "s1", new CommitToken(Guid.NewGuid(), new VolumeId(Guid.NewGuid()), 1, 9),
            DateTimeOffset.UnixEpoch.AddDays(30), Held: true, IsExactSnapshot: false);
        Assert.Equal(checkpoint, Decode(Encode((ref WireWriter w) => w.WriteBackupCheckpoint(checkpoint)), (ref WireReader r) => r.ReadBackupCheckpoint()));

        var retention = new DfsRetention(null, Hold: true);
        Assert.Equal(retention, Decode(Encode((ref WireWriter w) => w.WriteRetention(retention)), (ref WireReader r) => r.ReadRetention()));

        var labels = new DfsTopologyLabels("r1", "z1", "rk1", "h1", "d1");
        Assert.Equal(labels, Decode(Encode((ref WireWriter w) => w.WriteTopologyLabels(labels)), (ref WireReader r) => r.ReadTopologyLabels()));
    }

    [Fact]
    public void TimestampsAreTransportedAsUtcInstants()
    {
        var local = new DateTimeOffset(2026, 10, 12, 12, 0, 0, TimeSpan.FromHours(2));
        DateTimeOffset decoded = Decode(Encode((ref WireWriter w) => w.WriteDateTimeOffset(local)), (ref WireReader r) => r.ReadDateTimeOffset());
        Assert.Equal(local, decoded);
        Assert.Equal(TimeSpan.Zero, decoded.Offset);
        byte[] outOfRange = Encode((ref WireWriter w) => w.WriteVarInt(DateTime.MaxValue.Ticks + 1));
        Assert.Throws<DfsProtocolException>(() => Decode(outOfRange, (ref WireReader r) => r.ReadDateTimeOffset()));
    }

    [Fact]
    public void ProtocolGroupNamesAreKeyedByVolumeId()
    {
        var volume = new VolumeId(Guid.Parse("10203040-5060-7080-90a0-b0c0d0e0f001"));
        Assert.Equal("vol:10203040-5060-7080-90a0-b0c0d0e0f001", ProtocolInfo.VolumeGroup(volume));
        Assert.Equal("control", ProtocolInfo.ControlGroup);
    }
}
