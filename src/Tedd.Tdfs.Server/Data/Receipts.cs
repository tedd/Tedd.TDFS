using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;

namespace Tedd.Tdfs.Server.Data;

/// <summary>Durable copies established by a set of receipts for one object (PROTOCOL.md §7.1 step 4).</summary>
/// <param name="Copies">Distinct hosts holding a trusted durable copy.</param>
/// <param name="Zones">Distinct (region, zone) pairs among them.</param>
/// <param name="Holders">Trusted holders (receipt order, distinct).</param>
internal sealed record ReceiptEvaluation(int Copies, int Zones, IReadOnlyList<NodeId> Holders);

/// <summary>Issuing and validating object receipts (PROTOCOL.md §7.2): HMAC-SHA256 over the fixed-width MAC input.</summary>
internal static class Receipts
{
    /// <summary>Issues a signed receipt for a durable copy held by this node.</summary>
    public static ObjectReceipt Issue(byte[] receiptKey, ObjectId obj, VolumeId volume, StagingId staging, UInt128 checksum, long length, byte encoding,
        NodeId node, ulong incarnation, ulong placementEpoch, DateTimeOffset durableAt)
        => new ObjectReceipt(obj, volume, staging, checksum, length, encoding, node, incarnation, placementEpoch, durableAt.UtcTicks, ReadOnlyMemory<byte>.Empty)
            .Sign(receiptKey);

    /// <summary>
    /// Evaluates receipts for one object as the leader must before proposing a publication: MAC valid, bound to the object
    /// (id, checksum, length, encoding), volume and staging session, issued by a live, non-pending member with exactly its
    /// registered incarnation, for a placement epoch not newer than the current one, and (when <paramref name="pool"/> is
    /// given) by a member of that pool. Copies on the same host count once (REP-005); backups never count.
    /// </summary>
    public static ReceiptEvaluation Evaluate(IEnumerable<ObjectReceipt> receipts, byte[] receiptKey, ControlState state, ObjectId obj, UInt128 checksum,
        long length, byte encoding, VolumeId volume, StagingId staging, string? pool = null)
    {
        ArgumentNullException.ThrowIfNull(receipts);
        ArgumentNullException.ThrowIfNull(state);
        HashSet<NodeId>? poolMembers = pool is null ? null : state.PoolMembers(pool).Select(m => m.Id).ToHashSet();
        var holders = new List<NodeId>();
        var labels = new List<DfsTopologyLabels>();
        foreach (ObjectReceipt receipt in receipts)
        {
            if (!receipt.Covers(obj, checksum, length, encoding) || receipt.Volume != volume || receipt.Staging != staging)
                continue;
            if (!receipt.VerifyMac(receiptKey))
                continue;
            if (state.FindMember(receipt.Node) is not { IsLive: true, PendingAdmission: false } member || member.Incarnation != receipt.Incarnation)
                continue;
            if (receipt.PlacementEpoch > state.PlacementEpoch)
                continue;
            if (poolMembers is not null && !poolMembers.Contains(receipt.Node))
                continue;
            if (holders.Contains(receipt.Node))
                continue;
            holders.Add(receipt.Node);
            labels.Add(member.Labels);
        }

        (int copies, int zones) = Placement.CountDomains(labels);
        return new ReceiptEvaluation(copies, zones, holders);
    }
}
