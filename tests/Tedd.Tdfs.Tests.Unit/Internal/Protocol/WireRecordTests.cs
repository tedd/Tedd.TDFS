using System.Reflection;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Tests for every nested protocol record (<see cref="IWireRecord{TSelf}"/>): round trips, declared minimum sizes,
/// truncation, and the standalone record codec.
/// </summary>
public sealed class WireRecordTests
{
    public static TheoryData<string> AllRecordTypes()
    {
        var data = new TheoryData<string>();
        foreach (Type type in ProtocolSamples.RecordTypes())
            data.Add(type.FullName!);
        return data;
    }

    [Fact]
    public void RecordCatalogueIsComplete()
    {
        IReadOnlyList<Type> records = ProtocolSamples.RecordTypes();
        Assert.Contains(typeof(RequestHeader), records);
        Assert.Contains(typeof(ObjectReceipt), records);
        Assert.Contains(typeof(DataCapability), records);
        Assert.Contains(typeof(RaftWireMessage), records);
        Assert.Contains(typeof(AdminEmptyBody), records);
        foreach (Type type in records)
        {
            Assert.True(type.IsSealed, $"{type} must be sealed");
            Assert.NotNull(type.GetProperty("Extensions"));
        }
    }

    [Theory]
    [MemberData(nameof(AllRecordTypes))]
    public void RandomRecordsRoundTrip(string typeName)
    {
        Type type = Resolve(typeName);
        for (int seed = 0; seed < 40; seed++)
        {
            object record = ProtocolSamples.Random(seed * 31 + typeName.Length).Create(type);
            Invoke(nameof(RoundTrip), type, record, $"{type.Name} seed {seed}");
        }
    }

    [Theory]
    [MemberData(nameof(AllRecordTypes))]
    public void MinimalRecordMatchesDeclaredMinimumSize(string typeName)
    {
        Type type = Resolve(typeName);
        object record = ProtocolSamples.Minimal().Create(type);
        byte[] encoded = (byte[])Invoke(nameof(EncodeRecord), type, record)!;
        int declared = (int)type.GetProperty("MinEncodedSize", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.True(declared >= 1, $"{type.Name}.MinEncodedSize must be positive");
        Assert.True(declared <= encoded.Length, $"{type.Name}.MinEncodedSize {declared} exceeds the minimal encoding of {encoded.Length} bytes");
        Invoke(nameof(RoundTrip), type, record, $"{type.Name} minimal");
    }

    [Theory]
    [MemberData(nameof(AllRecordTypes))]
    public void TruncatedRecordsAreRejected(string typeName)
    {
        Type type = Resolve(typeName);
        byte[] encoded = (byte[])Invoke(nameof(EncodeRecord), type, ProtocolSamples.Random(3).Create(type))!;
        for (int length = 0; length < encoded.Length; length++)
        {
            byte[] prefix = encoded[..length];
            var ex = Assert.IsType<DfsProtocolException>(Assert.ThrowsAny<Exception>(() => Invoke(nameof(DecodeRecord), type, prefix)));
            Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
        }
    }

    [Fact]
    public void RecordCodecRejectsTrailingBytes()
    {
        byte[] encoded = WireRecordCodec.Encode(new RequestHeader("v"));
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => WireRecordCodec.Decode<RequestHeader>([.. encoded, 0]));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
    }

    [Fact]
    public void RequestHeaderDefaultsAddressAnyAuthority()
    {
        var header = new RequestHeader("vol");
        Assert.True(header.ExpectedVolumeId.IsEmpty);
        Assert.Equal(0UL, header.ExpectedVolumeEpoch);
        Assert.True(header.Operation.IsEmpty);
        Assert.Null(header.Transaction);
        RequestHeader decoded = WireRecordCodec.Decode<RequestHeader>(WireRecordCodec.Encode(header with { Transaction = new DfsTransactionId(new DfsOperationId(3, 4)) }));
        Assert.Equal(new DfsTransactionId(new DfsOperationId(3, 4)), decoded.Transaction);
    }

    [Fact]
    public void RangeChangeFactoriesProduceConsistentKinds()
    {
        var obj = new ObjectId(42);
        RangeChange data = RangeChange.ForData(10, 5, obj, 2, 100, 77);
        Assert.Equal(RangeChangeKind.Data, data.Kind);
        Assert.Equal(obj, data.Object);
        Assert.True(data.InlineData.IsEmpty);

        RangeChange zero = RangeChange.ForZero(0, 9);
        Assert.Equal(RangeChangeKind.Zero, zero.Kind);
        Assert.True(zero.Object.IsEmpty);

        RangeChange inline = RangeChange.ForInline(4, new byte[] { 1, 2, 3 });
        Assert.Equal(RangeChangeKind.Inline, inline.Kind);
        Assert.Equal(3, inline.Length);
        Invoke(nameof(RoundTrip), typeof(RangeChange), inline, "inline");
    }

    [Fact]
    public void SetsAndMapsAreEncodedCanonically()
    {
        // The same logical value built in different orders encodes to identical bytes (FMT-012).
        var a = new AdapterRecordData("db", "1", new Dictionary<string, string> { ["b"] = "2", ["a"] = "1", ["é"] = "3" });
        var b = new AdapterRecordData("db", "1", new Dictionary<string, string> { ["é"] = "3", ["a"] = "1", ["b"] = "2" });
        Assert.Equal(WireRecordCodec.Encode(a), WireRecordCodec.Encode(b));

        var policiesA = new Dictionary<string, DfsStoragePolicy> { ["z"] = DfsStoragePolicy.Default, ["a"] = new() { TargetCopies = 5 } };
        var policiesB = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { ["a"] = new() { TargetCopies = 5 }, ["z"] = DfsStoragePolicy.Default };
        VolumeEntry entry = (VolumeEntry)ProtocolSamples.Minimal().Create(typeof(VolumeEntry));
        Assert.Equal(WireRecordCodec.Encode(entry with { NamedPolicies = policiesA }), WireRecordCodec.Encode(entry with { NamedPolicies = policiesB }));
    }

    internal static void RoundTrip<T>(T record, string because) where T : IWireRecord<T>
    {
        byte[] encoded = WireRecordCodec.Encode(record);
        T decoded = WireRecordCodec.Decode<T>(encoded);
        string? difference = WireDeepEquality.FindDifference(record, decoded, because);
        Assert.True(difference is null, difference);
        Assert.Equal(encoded, WireRecordCodec.Encode(decoded));
    }

    internal static byte[] EncodeRecord<T>(T record) where T : IWireRecord<T> => WireRecordCodec.Encode(record);

    internal static T DecodeRecord<T>(byte[] data) where T : IWireRecord<T> => WireRecordCodec.Decode<T>(data);

    private static Type Resolve(string typeName) => typeof(IWireRecord<>).Assembly.GetType(typeName, throwOnError: true)!;

    private static object? Invoke(string method, Type type, params object[] args)
    {
        MethodInfo generic = typeof(WireRecordTests).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type);
        try
        {
            return generic.Invoke(null, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }
}
