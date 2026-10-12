using System.Reflection;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Structure-aware mutation fuzzing of every message and record decoder (SEC-014/015, FMT-012): bit flips, byte
/// insertions/deletions, duplicated or swapped slices, extreme varint bytes and splices between samples. A mutated input
/// either fails with <see cref="DfsProtocolException"/> (<see cref="DfsErrorCode.ProtocolError"/> or
/// <see cref="DfsErrorCode.Unsupported"/>) or decodes to a value whose canonical encoding is exactly the input. The
/// iteration count can be raised for soak runs with the <c>TDFS_PROTOCOL_FUZZ_ITERATIONS</c> environment variable.
/// </summary>
public sealed class ProtocolFuzzTests
{
    private static readonly int Iterations =
        int.TryParse(Environment.GetEnvironmentVariable("TDFS_PROTOCOL_FUZZ_ITERATIONS"), out int n) && n > 0 ? n : 150;

    public static TheoryData<MessageType> AllTypes()
    {
        var data = new TheoryData<MessageType>();
        foreach (MessageType type in MessageTypes.All)
            data.Add(type);
        return data;
    }

    public static TheoryData<string> AllRecordTypes()
    {
        var data = new TheoryData<string>();
        foreach (Type type in ProtocolSamples.RecordTypes())
            data.Add(type.FullName!);
        return data;
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void MutatedFramesDecodeCanonicallyOrFailWithProtocolErrors(MessageType type)
    {
        var random = new Random(0x5EED + (int)type);
        for (int iteration = 0; iteration < Iterations; iteration++)
        {
            byte[] frame = MessageCodec.Encode(ProtocolSamples.Random(iteration * 13 + (int)type).CreateMessage(type));
            byte[] donor = MessageCodec.Encode(ProtocolSamples.Random(iteration * 17 + 1 + (int)type).CreateMessage(type));
            byte[] mutated = Mutate(random, frame, donor, keepHeader: true);

            IWireMessage decoded;
            try
            {
                decoded = MessageCodec.Decode(mutated);
            }
            catch (DfsProtocolException ex)
            {
                Assert.True(ex.ErrorCode is DfsErrorCode.ProtocolError or DfsErrorCode.Unsupported, $"{type} iteration {iteration}: {ex.ErrorCode}");
                continue;
            }

            byte[] reencoded = MessageCodec.Encode(decoded);
            Assert.True(mutated.AsSpan().SequenceEqual(reencoded),
                $"{type} iteration {iteration}: accepted a non-canonical frame\n in:  {Convert.ToHexString(mutated)}\n out: {Convert.ToHexString(reencoded)}");
        }
    }

    [Theory]
    [MemberData(nameof(AllRecordTypes))]
    public void MutatedRecordsDecodeCanonicallyOrFailWithProtocolErrors(string typeName)
    {
        Type type = typeof(IWireRecord<>).Assembly.GetType(typeName, throwOnError: true)!;
        MethodInfo check = typeof(ProtocolFuzzTests).GetMethod(nameof(FuzzRecord), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(type);
        try
        {
            check.Invoke(null, [typeName.Length * 31]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }

    [Fact]
    public void RandomGarbageNeverEscapesAsAnotherExceptionType()
    {
        var random = new Random(77);
        foreach (MessageType type in MessageTypes.All)
        {
            for (int i = 0; i < 40; i++)
            {
                byte[] body = new byte[random.Next(0, 96)];
                random.NextBytes(body);
                byte[] frame = [(byte)((ushort)type & 0xFF), (byte)((ushort)type >> 8), .. body];
                try
                {
                    IWireMessage decoded = MessageCodec.Decode(frame);
                    Assert.Equal(frame, MessageCodec.Encode(decoded));
                }
                catch (DfsProtocolException)
                {
                }
            }
        }
    }

    [Fact]
    public void CapabilityTokensOnlyFailWithProtocolErrors()
    {
        var random = new Random(91);
        byte[] key = new byte[32];
        for (int i = 0; i < Iterations * 4; i++)
        {
            byte[] token = ((DataCapability)ProtocolSamples.Random(i).Create(typeof(DataCapability))).Sign(key).Encode();
            byte[] donor = ((DataCapability)ProtocolSamples.Random(i + 1).Create(typeof(DataCapability))).Encode();
            byte[] mutated = Mutate(random, token, donor, keepHeader: false);
            try
            {
                DataCapability decoded = DataCapability.Decode(mutated);
                Assert.Equal(mutated, decoded.Encode());
            }
            catch (DfsProtocolException ex)
            {
                Assert.True(ex.ErrorCode is DfsErrorCode.ProtocolError or DfsErrorCode.Unsupported);
            }

            Assert.Equal(DataCapability.TryDecode(mutated, out DataCapability? viaTry), viaTry is not null);
        }
    }

    private static void FuzzRecord<T>(int seed) where T : IWireRecord<T>
    {
        var random = new Random(seed);
        for (int iteration = 0; iteration < Iterations; iteration++)
        {
            byte[] encoded = WireRecordCodec.Encode((T)ProtocolSamples.Random(seed + iteration).Create(typeof(T)));
            byte[] donor = WireRecordCodec.Encode((T)ProtocolSamples.Random(seed + iteration + 7).Create(typeof(T)));
            byte[] mutated = Mutate(random, encoded, donor, keepHeader: false);
            T decoded;
            try
            {
                decoded = WireRecordCodec.Decode<T>(mutated);
            }
            catch (DfsProtocolException ex)
            {
                Assert.True(ex.ErrorCode is DfsErrorCode.ProtocolError or DfsErrorCode.Unsupported, $"{typeof(T).Name}: {ex.ErrorCode}");
                continue;
            }

            byte[] reencoded = WireRecordCodec.Encode(decoded);
            Assert.True(mutated.AsSpan().SequenceEqual(reencoded),
                $"{typeof(T).Name} iteration {iteration}: accepted a non-canonical record\n in:  {Convert.ToHexString(mutated)}\n out: {Convert.ToHexString(reencoded)}");
        }
    }

    /// <summary>Applies one to three random mutations; the two-byte frame header is kept when <paramref name="keepHeader"/> is set.</summary>
    internal static byte[] Mutate(Random random, byte[] input, byte[] donor, bool keepHeader)
    {
        int start = keepHeader ? MessageCodec.HeaderSize : 0;
        var data = new List<byte>(input);
        int mutations = random.Next(1, 4);
        for (int m = 0; m < mutations; m++)
        {
            int span = data.Count - start;
            switch (random.Next(8))
            {
                case 0 when span > 0: // bit flip
                    data[start + random.Next(span)] ^= (byte)(1 << random.Next(8));
                    break;
                case 1 when span > 0: // byte replaced by an interesting value
                    data[start + random.Next(span)] = random.Next(6) switch
                    {
                        0 => 0x00,
                        1 => 0x01,
                        2 => 0x7F,
                        3 => 0x80,
                        4 => 0xFF,
                        _ => (byte)random.Next(256),
                    };
                    break;
                case 2: // insertion
                    data.Insert(start + random.Next(span + 1), (byte)random.Next(256));
                    break;
                case 3 when span > 0: // deletion
                    data.RemoveAt(start + random.Next(span));
                    break;
                case 4 when span > 1: // duplicate a slice
                {
                    int at = start + random.Next(span);
                    int length = Math.Min(random.Next(1, 9), data.Count - at);
                    data.InsertRange(at, data.GetRange(at, length));
                    break;
                }
                case 5 when span > 2: // swap two bytes
                {
                    int a = start + random.Next(span), b = start + random.Next(span);
                    (data[a], data[b]) = (data[b], data[a]);
                    break;
                }
                case 6 when donor.Length > start: // splice the tail of another sample
                {
                    int cut = start + random.Next(span + 1);
                    int from = start + random.Next(donor.Length - start);
                    data.RemoveRange(cut, data.Count - cut);
                    data.AddRange(donor.AsSpan(from).ToArray());
                    break;
                }
                default: // truncation
                    if (span > 0)
                    {
                        int cut = start + random.Next(span);
                        data.RemoveRange(cut, data.Count - cut);
                    }

                    break;
            }
        }

        return [.. data];
    }
}
