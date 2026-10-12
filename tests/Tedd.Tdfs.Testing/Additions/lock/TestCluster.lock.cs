namespace Tedd.Tdfs.Testing;

// Additions for the LOCK suite (docs/api-additions/lock.md).
public sealed partial class TestCluster
{
    /// <summary>
    /// Connects a <see cref="RawProtocolClient"/> to live node <paramref name="nodeName"/> that presents the node-level
    /// identity of backup node <c>b{backupIndex}</c>: the handshake carries the backup's name, role = backup and its
    /// backup credentials exactly as the backup's own connections do, so the live side sees an
    /// <see cref="Tedd.Tdfs.Transport.RpcPeerIdentity"/> with <c>IsBackup = true</c> (LOCK-020, BAK-002).
    /// </summary>
    /// <remarks>
    /// The network identity of the connection is the backup node's name (for example <c>"b0"</c>), so
    /// <see cref="TestNetwork"/> rules naming the backup apply to it. If the live node refuses the backup-role handshake,
    /// the method still returns a client: <see cref="RawProtocolClient.IsConnected"/> is false and every call returns
    /// the refusal error code instead of throwing. The returned client needs no further
    /// <see cref="RawProtocolClient.AuthenticateAsync"/>.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">No backup node with that index exists.</exception>
    /// <exception cref="ArgumentException"><paramref name="nodeName"/> is not a live node of this cluster.</exception>
    public Task<RawProtocolClient> ConnectRawAsBackupAsync(int backupIndex, string nodeName, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ConnectRawAsBackupAsync");
}
