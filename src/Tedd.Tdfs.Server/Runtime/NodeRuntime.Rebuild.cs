using System.Buffers.Binary;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Empty-disk replacement of a bootstrap member (OFF-020, LAY-014, RSY-021): a node configured with the static bootstrap
/// whose data directory is empty asks the other bootstrap members whether the cluster already knows it. When its member
/// record carries a registered incarnation, the node is a replacement of a member that lost its disk: it keeps its
/// identity, continues the incarnation sequence (so its first start registers a newer incarnation instead of being fenced
/// as stale), writes no bootstrap entries, and rebuilds every group that existed before the loss from the leaders without
/// voting until caught up (<see cref="Raft.GroupOpenSpec.Rebuilding"/>). The control index the cluster had reached is kept
/// in <c>rebuild.state</c> (written before the superblock, so a crash during the replacement cannot turn the node into a
/// fresh bootstrap member): groups of volumes created up to that index are rebuilt, later ones start normally.
/// </summary>
internal sealed partial class NodeRuntime
{
    /// <summary>Node-level marker of an empty-disk replacement (8-byte little-endian control index).</summary>
    internal const string RebuildStateFile = "rebuild.state";

    /// <summary>Reads the node's rebuild marker (null when the node never lost its state).</summary>
    private async Task<ulong?> LoadRebuildIndexAsync(CancellationToken ct)
    {
        byte[]? content = await AtomicFile.ReadAsync(Services.Env.Medium, Services.DataPath(RebuildStateFile), ct);
        return content is { Length: 8 } ? BinaryPrimitives.ReadUInt64LittleEndian(content) : null;
    }

    /// <summary>
    /// Asks the other bootstrap members (with a throwaway identity, as a joining node) for the cluster map and returns this
    /// member's registered incarnation and the control index the answer reflects, the highest over every member that
    /// answered; null when nobody answered or the cluster does not know this member yet (a fresh bootstrap).
    /// </summary>
    private async Task<(ulong Incarnation, ulong ControlIndex)?> ProbeReplacementAsync(ClusterBootstrap bootstrap, BootstrapMember self, CancellationToken ct)
    {
        NodeServices s = Services;
        var credentials = new NodeCredentials(Guid.Empty, s.NodeName, NodeId.New(s.Random), 0, s.BootId, s.Keys.NodeAuthKey, ProtocolFeatures.Supported);
        ulong incarnation = 0;
        ulong controlIndex = 0;
        bool answered = false;
        foreach (BootstrapMember member in bootstrap.Members)
        {
            if (member.Id == self.Id)
                continue;
            foreach (string endpoint in member.Endpoints)
            {
                try
                {
                    (IRpcConnection connection, HandshakeOutcome outcome) = await s.Peers.ConnectUnpooledAsync(endpoint, credentials, ct);
                    await using (connection)
                    {
                        if (outcome.Challenge.ClusterId != bootstrap.ClusterId)
                            continue;
                        ReadOnlyMemory<byte> frame = await connection.CallAsync(MessageCodec.Encode(new GetClusterMapMessage()), s.Peers.DefaultCallTimeout, ct);
                        if (MessageCodec.Decode(frame.Span) is not ClusterMapResultMessage { Map: { } map } || map.ClusterId != bootstrap.ClusterId)
                            continue;
                        answered = true;
                        s.Peers.LearnEndpoints(map); // a replacement knows no member until its control group caught up
                        controlIndex = Math.Max(controlIndex, map.ConfigIndex);
                        if (map.Nodes.FirstOrDefault(n => n.Id == self.Id) is { } entry)
                            incarnation = Math.Max(incarnation, entry.Incarnation);
                    }

                    break;
                }
                catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or RpcConnectionException
                                               or InvalidCastException)
                {
                    if (ct.IsCancellationRequested)
                        throw;
                    s.Debug("node", NodeServices.Inv($"Bootstrap member {member.Name} at {endpoint} did not answer the replacement probe: {ex.Message}"));
                }
            }
        }

        return answered && incarnation > 0 ? (incarnation, controlIndex) : null;
    }

    /// <summary>
    /// Creates the superblock of a node starting from an empty data directory with a static bootstrap: a fresh bootstrap
    /// member (incarnation 0), or the replacement of a member the cluster already registered (its incarnation continues;
    /// <c>rebuild.state</c> is written first).
    /// </summary>
    private async Task<Superblock> CreateBootstrapSuperblockAsync(ClusterBootstrap bootstrap, BootstrapMember self, string superblockPath, CancellationToken ct)
    {
        NodeServices s = Services;
        if (await ProbeReplacementAsync(bootstrap, self, ct) is not { } replacement)
        {
            s.Info("node", NodeServices.Inv($"Bootstrapping cluster {bootstrap.ClusterName} ({bootstrap.ClusterId}) as {self.Id}."));
            return await Superblock.CreateAsync(s.Env.Medium, superblockPath, bootstrap.ClusterId, self.Id, s.NodeName, ct);
        }

        var marker = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(marker, replacement.ControlIndex);
        await AtomicFile.WriteAsync(s.Env.Medium, s.DataPath(RebuildStateFile), marker, ct);
        s.RebuildIndex = replacement.ControlIndex;
        s.Warn("node", NodeServices.Inv(
            $"{s.NodeName} ({self.Id}) starts with an empty data directory although the cluster registered incarnation {replacement.Incarnation}: rebuilding its state from the cluster (control index {replacement.ControlIndex})."));
        return await Superblock.CreateAsync(s.Env.Medium, superblockPath, bootstrap.ClusterId, self.Id, s.NodeName, replacement.Incarnation, ct);
    }
}
