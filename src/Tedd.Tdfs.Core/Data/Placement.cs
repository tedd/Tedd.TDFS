using System.IO.Hashing;

namespace Tedd.Tdfs.Data;

/// <summary>
/// Deterministic replica placement (ARCHITECTURE §6): rendezvous (highest-random-weight) hashing over the nodes that
/// can receive copies, keyed by (object id, placement epoch, node id). The result depends only on its inputs — never
/// on the order of <c>nodes</c> (REP-008) — and never contains ineligible or full nodes (REP-009).
/// </summary>
/// <remarks>
/// Score = XxHash64 over <c>[ObjectId 16 bytes LE][placementEpoch 8 bytes LE][NodeId 16 bytes]</c>. Order: score
/// descending, then node id, then node name (ordinal). Copies are counted by distinct host; zones by distinct
/// (region, zone).
/// </remarks>
public static class Placement
{
    /// <summary>
    /// Selects the ordered target list for an object: greedily in HRW order, first one node per new zone (on a new
    /// host) until <see cref="DfsStoragePolicy.TargetDistinctZones"/> zones are covered, then nodes on new hosts, then
    /// any remaining nodes, until <see cref="DfsStoragePolicy.TargetCopies"/> targets are chosen. Shorter when too few
    /// nodes can receive copies. Each prefix of the result is as diverse as the greedy order allows.
    /// </summary>
    /// <exception cref="DfsException"><paramref name="policy"/> is invalid (<see cref="DfsErrorCode.InvalidPolicy"/>).</exception>
    public static IReadOnlyList<PlacementNode> SelectTargets(ObjectId obj, ulong placementEpoch, DfsStoragePolicy policy, IReadOnlyList<PlacementNode> nodes)
    {
        ArgumentNullException.ThrowIfNull(policy);
        policy.Validate();
        IReadOnlyList<PlacementNode> order = FallbackOrder(obj, placementEpoch, nodes);

        int targetCopies = policy.TargetCopies;
        int targetZones = Math.Min(policy.TargetDistinctZones, targetCopies);
        var selected = new List<PlacementNode>(Math.Min(targetCopies, order.Count));
        var used = new bool[order.Count];
        var hosts = new HashSet<string>(StringComparer.Ordinal);
        var zones = new HashSet<(string Region, string Zone)>();

        // Phase 1: one node per new zone (also on a new host) until the zone target is met.
        for (int i = 0; i < order.Count && selected.Count < targetCopies && zones.Count < targetZones; i++)
        {
            DfsTopologyLabels labels = order[i].Labels;
            if (zones.Contains(labels.ZoneKey) || hosts.Contains(labels.Host))
                continue;
            Take(i);
        }

        // Phase 2: nodes on hosts not yet used.
        for (int i = 0; i < order.Count && selected.Count < targetCopies; i++)
        {
            if (!used[i] && !hosts.Contains(order[i].Labels.Host))
                Take(i);
        }

        // Phase 3: fill with the remaining nodes in HRW order.
        for (int i = 0; i < order.Count && selected.Count < targetCopies; i++)
        {
            if (!used[i])
                Take(i);
        }

        return selected.AsReadOnly();

        void Take(int index)
        {
            PlacementNode node = order[index];
            used[index] = true;
            selected.Add(node);
            hosts.Add(node.Labels.Host);
            zones.Add(node.Labels.ZoneKey);
        }
    }

    /// <summary>
    /// Returns every node that can receive copies (<see cref="PlacementNode.Eligible"/> with free capacity) in HRW
    /// order for the object. Uploaders continue down this order when a target is unavailable.
    /// </summary>
    public static IReadOnlyList<PlacementNode> FallbackOrder(ObjectId obj, ulong placementEpoch, IReadOnlyList<PlacementNode> nodes)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        var scored = new List<(ulong Score, PlacementNode Node)>(nodes.Count);
        foreach (PlacementNode? node in nodes)
        {
            if (node is null)
                throw new ArgumentException("Placement nodes must not contain null entries.", nameof(nodes));
            if (node.Labels is null)
                throw new ArgumentException($"Placement node '{node.Name}' has no topology labels.", nameof(nodes));
            if (node.CanReceive)
                scored.Add((Score(obj, placementEpoch, node.Id), node));
        }

        scored.Sort(static (a, b) => CompareScored(a.Score, a.Node, b.Score, b.Node));
        var result = new PlacementNode[scored.Count];
        for (int i = 0; i < result.Length; i++)
            result[i] = scored[i].Node;
        return Array.AsReadOnly(result);
    }

    /// <summary>Counts failure domains of a set of holders: copies = distinct hosts, zones = distinct (region, zone).</summary>
    public static (int Copies, int Zones) CountDomains(IEnumerable<DfsTopologyLabels> holders)
    {
        ArgumentNullException.ThrowIfNull(holders);
        var hosts = new HashSet<string>(StringComparer.Ordinal);
        var zones = new HashSet<(string Region, string Zone)>();
        foreach (DfsTopologyLabels? labels in holders)
        {
            if (labels is null)
                throw new ArgumentException("Holder labels must not contain null entries.", nameof(holders));
            hosts.Add(labels.Host);
            zones.Add(labels.ZoneKey);
        }

        return (hosts.Count, zones.Count);
    }

    /// <summary>
    /// True when the nodes able to receive copies span at least <see cref="DfsStoragePolicy.TargetCopies"/> distinct
    /// hosts and <see cref="DfsStoragePolicy.TargetDistinctZones"/> distinct zones (and the policy is valid).
    /// </summary>
    /// <param name="policy">Policy to check.</param>
    /// <param name="nodes">Candidate nodes.</param>
    /// <param name="reason">Why the policy cannot be satisfied, or null when it can.</param>
    public static bool IsSatisfiable(DfsStoragePolicy policy, IReadOnlyList<PlacementNode> nodes, out string? reason)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(nodes);
        reason = policy.GetValidationError();
        if (reason is not null)
        {
            reason = "Invalid policy: " + reason;
            return false;
        }

        var receiving = new List<DfsTopologyLabels>(nodes.Count);
        foreach (PlacementNode? node in nodes)
        {
            if (node is not null && node.Labels is not null && node.CanReceive)
                receiving.Add(node.Labels);
        }

        if (receiving.Count == 0)
        {
            reason = "No eligible storage nodes with free capacity are available.";
            return false;
        }

        (int hosts, int zones) = CountDomains(receiving);
        if (hosts < policy.TargetCopies)
        {
            reason = $"The policy requires {policy.TargetCopies} copies on distinct hosts but only {hosts} eligible hosts are available.";
            return false;
        }

        if (zones < policy.TargetDistinctZones)
        {
            reason = $"The policy requires {policy.TargetDistinctZones} distinct zones but only {zones} eligible zones are available.";
            return false;
        }

        return true;
    }

    /// <summary>The rendezvous score of a node for an object at a placement epoch.</summary>
    public static ulong Score(ObjectId obj, ulong placementEpoch, NodeId node)
    {
        Span<byte> buffer = stackalloc byte[40];
        obj.WriteBytes(buffer);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(buffer[16..], placementEpoch);
        if (!node.Value.TryWriteBytes(buffer[24..], bigEndian: false, out _))
            throw new InvalidOperationException("Failed to encode node id.");
        return XxHash64.HashToUInt64(buffer);
    }

    private static int CompareScored(ulong scoreA, PlacementNode a, ulong scoreB, PlacementNode b)
    {
        int c = scoreB.CompareTo(scoreA); // higher score first
        if (c != 0)
            return c;
        c = a.Id.Value.CompareTo(b.Id.Value);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Name, b.Name);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Labels.Host, b.Labels.Host);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Labels.Region, b.Labels.Region);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Labels.Zone, b.Labels.Zone);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Labels.Rack, b.Labels.Rack);
        if (c != 0)
            return c;
        c = string.CompareOrdinal(a.Labels.Device, b.Labels.Device);
        return c != 0 ? c : a.FreeBytes.CompareTo(b.FreeBytes);
    }
}
