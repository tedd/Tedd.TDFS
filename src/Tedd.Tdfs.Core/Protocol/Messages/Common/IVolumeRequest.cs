namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// A volume-scoped request carrying a <see cref="RequestHeader"/>. Lets the server apply common routing, authority
/// (volume id/epoch) and idempotency handling to every such request without knowing its concrete type.
/// </summary>
public interface IVolumeRequest : IWireMessage
{
    /// <summary>The request header.</summary>
    RequestHeader Header { get; }

    /// <summary>Returns a copy of this request with <paramref name="header"/> (all other fields and extensions unchanged).</summary>
    IVolumeRequest WithHeader(RequestHeader header);
}
