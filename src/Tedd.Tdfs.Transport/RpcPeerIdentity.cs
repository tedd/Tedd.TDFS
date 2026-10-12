namespace Tedd.Tdfs.Transport;

/// <summary>Authenticated identity of the remote side of a connection, set after the application handshake.</summary>
/// <param name="NodeName">Node name, for node peers.</param>
/// <param name="NodeId">Node id, for node peers.</param>
/// <param name="Incarnation">Node incarnation, for node peers.</param>
/// <param name="Principal">Authenticated principal, for client peers.</param>
/// <param name="IsNode">True for live-node peers.</param>
/// <param name="IsBackup">True for backup-node peers.</param>
public sealed record RpcPeerIdentity(string? NodeName, NodeId? NodeId, ulong Incarnation, string? Principal, bool IsNode, bool IsBackup);
