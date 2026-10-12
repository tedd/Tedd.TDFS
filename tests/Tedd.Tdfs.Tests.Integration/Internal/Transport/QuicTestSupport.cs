using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Integration.Internal.Transport;

/// <summary>One development certificate (and its SPKI pin) shared by the tests of a class.</summary>
public sealed class QuicCertificateFixture : IDisposable
{
    public QuicCertificateFixture()
    {
        Certificate = QuiclyTransportFactory.CreateDevelopmentCertificate();
        Pin = QuiclyTransportFactory.ComputeSpkiPin(Certificate);
    }

    public X509Certificate2 Certificate { get; }

    public byte[] Pin { get; }

    public void Dispose() => Certificate.Dispose();
}

/// <summary>An <see cref="IRpcHandler"/> driven by a delegate; records every invocation.</summary>
internal sealed class DelegateHandler : IRpcHandler
{
    private readonly Func<IRpcConnection, ReadOnlyMemory<byte>, bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> _handle;

    public DelegateHandler(Func<IRpcConnection, ReadOnlyMemory<byte>, bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> handle)
    {
        _handle = handle;
    }

    public ConcurrentQueue<(byte[] Payload, bool ExpectsReply, IRpcConnection Connection)> Received { get; } = new();

    public int Count => Received.Count;

    public static DelegateHandler Echo() => new((_, request, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(request.ToArray()));

    public static DelegateHandler Silent() => new((_, _, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>?>(null));

    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        Received.Enqueue((request.ToArray(), expectsReply, connection));
        return _handle(connection, request, expectsReply, ct);
    }
}

/// <summary>A link policy driven by delegates; records every decision request.</summary>
internal sealed class ScriptedLinkPolicy : ILinkPolicy
{
    private volatile Func<string, string, LinkDirection, byte[], LinkDecision> _decide = (_, _, _, _) => LinkDecision.Deliver;
    private volatile Func<string, string, bool> _canConnect = (_, _) => true;

    public ConcurrentQueue<(string From, string To, LinkDirection Direction, byte[] Payload)> Decisions { get; } = new();

    public ConcurrentQueue<(string From, string To)> ConnectChecks { get; } = new();

    public void OnDecide(Func<string, string, LinkDirection, byte[], LinkDecision> decide) => _decide = decide;

    public void OnCanConnect(Func<string, string, bool> canConnect) => _canConnect = canConnect;

    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
    {
        byte[] payload = payloadPrefix.ToArray();
        Decisions.Enqueue((fromName, toName, direction, payload));
        return _decide(fromName, toName, direction, payload);
    }

    public bool CanConnect(string fromName, string toName)
    {
        ConnectChecks.Enqueue((fromName, toName));
        return _canConnect(fromName, toName);
    }
}

/// <summary>Helpers shared by the QUIC transport tests.</summary>
internal static class QuicTestData
{
    /// <summary>Generous bound for anything that must happen "soon" over real loopback QUIC.</summary>
    public static readonly TimeSpan Soon = TimeSpan.FromSeconds(30);

    public const string AnyLoopback = "quic://127.0.0.1:0";

    public static byte[] Frame(ushort type, uint sequence, int fillerBytes = 0)
    {
        byte[] payload = new byte[6 + fillerBytes];
        BinaryPrimitives.WriteUInt16LittleEndian(payload, type);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(2), sequence);
        for (int i = 0; i < fillerBytes; i++)
            payload[6 + i] = (byte)(sequence * 31 + i);
        return payload;
    }

    public static ushort TypeOf(ReadOnlySpan<byte> frame) => BinaryPrimitives.ReadUInt16LittleEndian(frame);

    public static uint SequenceOf(ReadOnlySpan<byte> frame) => BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(2));

    public static byte[] Pattern(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    public static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        DateTime deadline = DateTime.UtcNow + Soon;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting for: {what}");
            await Task.Delay(5);
        }
    }

    public static TaskCompletionSource<Exception?> WatchClosed(IRpcConnection connection)
    {
        var closed = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Closed += (_, ex) => closed.TrySetResult(ex);
        return closed;
    }
}
