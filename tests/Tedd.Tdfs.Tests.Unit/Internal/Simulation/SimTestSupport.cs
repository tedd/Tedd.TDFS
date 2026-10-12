using System.Text;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>A link policy whose decisions are supplied by delegates (defaults: allow everything).</summary>
internal sealed class DelegateLinkPolicy : ILinkPolicy
{
    public Func<string, string, LinkDirection, byte[], LinkDecision>? OnDecide { get; set; }

    public Func<string, string, bool>? OnConnect { get; set; }

    public List<(string From, string To, LinkDirection Direction, string Text)> Seen { get; } = [];

    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
    {
        byte[] payload = payloadPrefix.ToArray();
        Seen.Add((fromName, toName, direction, Encoding.UTF8.GetString(payload)));
        return OnDecide?.Invoke(fromName, toName, direction, payload) ?? LinkDecision.Deliver;
    }

    public bool CanConnect(string fromName, string toName) => OnConnect?.Invoke(fromName, toName) ?? true;
}

/// <summary>An RPC handler driven by a delegate; records what it saw.</summary>
internal sealed class DelegateHandler : IRpcHandler
{
    private readonly Func<IRpcConnection, byte[], bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> _handle;

    public DelegateHandler(Func<IRpcConnection, byte[], bool, CancellationToken, ValueTask<ReadOnlyMemory<byte>?>> handle)
    {
        _handle = handle;
    }

    public List<(string Text, bool ExpectsReply)> Received { get; } = [];

    public List<IRpcConnection> Connections { get; } = [];

    /// <summary>Replies to requests with "re:" + the request text; one-way messages get no reply.</summary>
    public static DelegateHandler Echo() =>
        new((_, request, expectsReply, _) => new ValueTask<ReadOnlyMemory<byte>?>(
            expectsReply ? Encoding.UTF8.GetBytes("re:" + Encoding.UTF8.GetString(request)) : null));

    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        byte[] copy = request.ToArray();
        Received.Add((Encoding.UTF8.GetString(copy), expectsReply));
        if (!Connections.Contains(connection))
            Connections.Add(connection);
        return _handle(connection, copy, expectsReply, ct);
    }
}

internal static class SimText
{
    public static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    public static string Str(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);
}
