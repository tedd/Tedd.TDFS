namespace Tedd.Tdfs.Transport.Internal;

/// <summary>Kind of an RPC envelope. Values are part of the QUIC wire format and must never change.</summary>
internal enum RpcMessageKind : byte
{
    /// <summary>A request expecting a <see cref="Reply"/> with the same correlation id.</summary>
    Request = 1,

    /// <summary>A reply to a <see cref="Request"/>; its correlation id is the request's.</summary>
    Reply = 2,

    /// <summary>A one-way message (no reply).</summary>
    OneWay = 3,

    /// <summary>Transport-level preamble carrying the sender's network identity (QUIC only; never delivered to handlers).</summary>
    Preamble = 4,
}

/// <summary>Helpers for <see cref="RpcMessageKind"/>.</summary>
internal static class RpcMessageKinds
{
    /// <summary>True for the kinds that carry application payloads (request, reply, one-way).</summary>
    public static bool IsApplication(RpcMessageKind kind) => kind is RpcMessageKind.Request or RpcMessageKind.Reply or RpcMessageKind.OneWay;

    /// <summary>The link-policy direction of an application message kind.</summary>
    public static LinkDirection ToDirection(RpcMessageKind kind) => kind switch
    {
        RpcMessageKind.Request => LinkDirection.Request,
        RpcMessageKind.Reply => LinkDirection.Reply,
        _ => LinkDirection.OneWay,
    };
}
