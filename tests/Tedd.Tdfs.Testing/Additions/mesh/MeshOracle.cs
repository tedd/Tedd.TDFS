using System.Globalization;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>
/// Independent availability oracle for mesh scenarios: quorum arithmetic over <b>configured</b> voters (never over
/// reachable nodes — RCV-01/MUT-004), data acknowledgement policy over usable hosts and zones, replica reachability and
/// the inline-storage rule. Shares no code with the implementation.
/// </summary>
/// <remarks>
/// <para>
/// Exactness. When the live part of the link graph that holds a quorum is a clique (every full-mesh scenario: crashes,
/// pauses, whole-group partitions) the answer is exact: <see cref="MeshTri.Must"/> or <see cref="MeshTri.Never"/>.
/// Otherwise (partial link meshes, single-link cuts) the specification does not fix liveness — Raft without relaying
/// may or may not elect a leader depending on which nodes hold the newest log — so the oracle answers
/// <see cref="MeshTri.Either"/> unless an impossibility applies: if no connected component of live nodes contains a
/// majority of the configured voters, no implementation can make progress (<see cref="MeshTri.Never"/>).
/// </para>
/// <para>
/// Clients talk to the volume leader for metadata and to storage nodes directly for data (ARCHITECTURE §2.1, §6). A
/// client that reaches no node of the quorum side can never succeed; one that reaches only some of that side's voters
/// may or may not reach the leader (Either).
/// </para>
/// </remarks>
public static class MeshOracle
{
    /// <summary>Majority of <paramref name="voters"/> configured voters.</summary>
    public static int Majority(int voters) => voters / 2 + 1;

    /// <summary>Number of configured voters that may fail while a majority survives.</summary>
    public static int Tolerance(int voters) => (voters - 1) / 2;

    /// <summary>Connected components of live nodes over bidirectional links (nodes only).</summary>
    public static IReadOnlyList<IReadOnlySet<string>> Components(MeshNetworkModel net)
    {
        ArgumentNullException.ThrowIfNull(net);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<IReadOnlySet<string>>();
        foreach (string start in net.AliveNodes)
        {
            if (!seen.Add(start))
                continue;
            var component = new SortedSet<string>(StringComparer.Ordinal) { start };
            var queue = new Queue<string>();
            queue.Enqueue(start);
            while (queue.Count > 0)
            {
                string current = queue.Dequeue();
                foreach (string other in net.AliveNodes)
                {
                    if (!seen.Contains(other) && net.Linked(current, other))
                    {
                        seen.Add(other);
                        component.Add(other);
                        queue.Enqueue(other);
                    }
                }
            }

            result.Add(component);
        }

        return result;
    }

    /// <summary>True when every pair of the given live nodes is linked in both directions.</summary>
    public static bool IsClique(MeshNetworkModel net, IEnumerable<string> nodes)
    {
        string[] list = nodes.ToArray();
        for (int i = 0; i < list.Length; i++)
        {
            for (int j = i + 1; j < list.Length; j++)
            {
                if (!net.Linked(list[i], list[j]))
                    return false;
            }
        }

        return true;
    }

    /// <summary>The component that holds a majority of the configured voters, or null.</summary>
    public static IReadOnlySet<string>? QuorumSide(MeshNetworkModel net, IReadOnlyCollection<string> voters)
    {
        ArgumentNullException.ThrowIfNull(voters);
        int majority = Majority(voters.Count);
        return Components(net).FirstOrDefault(c => voters.Count(c.Contains) >= majority);
    }

    /// <summary>
    /// Expectation for an operation that needs the group with the given configured voters, issued by
    /// <paramref name="client"/> (a network name such as <c>client:p0</c>).
    /// </summary>
    /// <param name="net">Current liveness and links.</param>
    /// <param name="voters">The group's configured voters (committed configuration, not the reachable set).</param>
    /// <param name="client">Client network name.</param>
    /// <param name="quorumCause"><see cref="MeshCause.MetadataQuorum"/> or <see cref="MeshCause.ControlQuorum"/>.</param>
    public static MeshExpectation Quorum(MeshNetworkModel net, IReadOnlyCollection<string> voters, string client, MeshCause quorumCause)
    {
        ArgumentNullException.ThrowIfNull(net);
        ArgumentNullException.ThrowIfNull(voters);
        if (voters.Count == 0)
            throw new ArgumentException("A group has at least one configured voter.", nameof(voters));
        int majority = Majority(voters.Count);
        string[] aliveVoters = voters.Where(v => !net.IsDown(v)).Order(StringComparer.Ordinal).ToArray();
        string arithmetic = string.Create(CultureInfo.InvariantCulture,
            $"{voters.Count} configured voters [{string.Join(",", voters.Order(StringComparer.Ordinal))}], majority {majority}, live [{string.Join(",", aliveVoters)}]");
        IReadOnlySet<string>? side = QuorumSide(net, voters);
        bool reachesLiveVoter = aliveVoters.Any(v => net.Linked(client, v));
        if (side is null)
        {
            return reachesLiveVoter
                ? MeshExpectation.Never(quorumCause, arithmetic + ": no connected set of live nodes holds a majority")
                : MeshExpectation.Never(MeshCause.Unreachable, arithmetic + $": no majority anywhere and {client} reaches no live voter");
        }

        string[] sideVoters = voters.Where(side.Contains).Order(StringComparer.Ordinal).ToArray();
        string sideText = $"{arithmetic}; quorum side [{string.Join(",", side)}] with voters [{string.Join(",", sideVoters)}]";
        if (!side.Any(n => net.Linked(client, n)))
            return MeshExpectation.Never(MeshCause.Unreachable, sideText + $": {client} reaches no node of the quorum side");
        if (IsClique(net, side) && sideVoters.All(v => net.Linked(client, v)))
            return MeshExpectation.Must(sideText + $": fully connected and {client} reaches every voter of it");
        var causes = new List<MeshCause> { MeshCause.Unreachable };
        if (reachesLiveVoter)
            causes.Add(quorumCause);
        return MeshExpectation.Either(causes, sideText + ": partially connected (leader election/reachability not fixed by the specification)");
    }

    /// <summary>Distinct host and zone counts of the given nodes (copies by host, zones by region+zone; ARCHITECTURE §2.3).</summary>
    public static (int Hosts, int Zones) CountDomains(MeshNetworkModel net, IEnumerable<string> nodes)
    {
        TestNodeSpec[] specs = nodes.Select(net.Spec).ToArray();
        return (specs.Select(s => s.EffectiveHost).Distinct(StringComparer.Ordinal).Count(),
            specs.Select(s => s.Region + "\0" + s.Zone).Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// Expectation for obtaining durable receipts meeting <paramref name="policy"/>'s acknowledgement counts for a new
    /// object uploaded by <paramref name="client"/>. Upper bound: live, eligible nodes the client reaches; lower bound: of
    /// those, the ones on the volume quorum side (whose receipts the leader can certainly validate).
    /// </summary>
    public static MeshExpectation Placement(MeshNetworkModel net, DfsStoragePolicy policy, string client, IReadOnlySet<string>? quorumSide)
    {
        ArgumentNullException.ThrowIfNull(net);
        ArgumentNullException.ThrowIfNull(policy);
        string[] upper = net.AliveNodes.Where(n => net.IsEligible(n) && net.Linked(client, n)).ToArray();
        (int uh, int uz) = CountDomains(net, upper);
        string need = $"ack {policy.DurableCopiesBeforeAck} copies/{policy.DurableZonesBeforeAck} zones";
        if (uh < policy.DurableCopiesBeforeAck || uz < policy.DurableZonesBeforeAck)
            return MeshExpectation.Never(MeshCause.Placement, $"{need}; {client} reaches {uh} usable hosts/{uz} zones [{string.Join(",", upper)}]");
        string[] lower = quorumSide is null ? [] : upper.Where(quorumSide.Contains).ToArray();
        (int lh, int lz) = CountDomains(net, lower);
        if (lh >= policy.DurableCopiesBeforeAck && lz >= policy.DurableZonesBeforeAck && IsClique(net, lower))
            return MeshExpectation.Must($"{need}; {lh} hosts/{lz} zones usable on the quorum side [{string.Join(",", lower)}]");
        return MeshExpectation.Either([MeshCause.Placement],
            $"{need}; {uh} hosts/{uz} zones reachable but only {lh}/{lz} certainly usable [{string.Join(",", lower)}]");
    }

    /// <summary>
    /// Expectation for reading every object of a file whose verified holders were recorded (per object) before the fault:
    /// each object needs at least one live holder the client reaches; repair can only copy from a surviving holder.
    /// </summary>
    public static MeshExpectation Holders(MeshNetworkModel net, IReadOnlyList<IReadOnlyList<string>> holdersPerObject, string client)
    {
        ArgumentNullException.ThrowIfNull(net);
        ArgumentNullException.ThrowIfNull(holdersPerObject);
        for (int i = 0; i < holdersPerObject.Count; i++)
        {
            IReadOnlyList<string> holders = holdersPerObject[i];
            if (!holders.Any(h => net.IsNode(h) && !net.IsDown(h) && net.Linked(client, h)))
                return MeshExpectation.Never(MeshCause.NoHealthyReplica, $"object #{i} held by [{string.Join(",", holders)}], none live and reachable from {client}");
        }

        return MeshExpectation.Must($"every one of {holdersPerObject.Count} objects has a live holder reachable from {client}");
    }

    /// <summary>
    /// The durability a metadata group guarantees by itself: the minimum, over every majority subset of its voters, of
    /// distinct hosts and of distinct zones (ARCHITECTURE §5.4).
    /// </summary>
    public static (int Copies, int Zones) GuaranteedMetadataDurability(IReadOnlyList<TestNodeSpec> voters)
    {
        ArgumentNullException.ThrowIfNull(voters);
        if (voters.Count == 0)
            throw new ArgumentException("At least one voter is required.", nameof(voters));
        int majority = Majority(voters.Count);
        int minHosts = int.MaxValue;
        int minZones = int.MaxValue;
        foreach (int[] subset in Subsets(voters.Count, majority))
        {
            TestNodeSpec[] members = subset.Select(i => voters[i]).ToArray();
            minHosts = Math.Min(minHosts, members.Select(m => m.EffectiveHost).Distinct(StringComparer.Ordinal).Count());
            minZones = Math.Min(minZones, members.Select(m => m.Region + "\0" + m.Zone).Distinct(StringComparer.Ordinal).Count());
        }

        return (minHosts, minZones);
    }

    /// <summary>
    /// True when a file of <paramref name="length"/> bytes is stored inline: length ≤ inline threshold and the metadata
    /// group's guaranteed durability satisfies the acknowledgement policy (ARCHITECTURE §5.4, LAY-002; inlining never
    /// bypasses stricter data placement).
    /// </summary>
    public static bool StoresInline(long length, int inlineThreshold, DfsStoragePolicy policy, IReadOnlyList<TestNodeSpec> voters)
    {
        ArgumentNullException.ThrowIfNull(policy);
        if (length > inlineThreshold)
            return false;
        (int copies, int zones) = GuaranteedMetadataDurability(voters);
        return copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck;
    }

    /// <summary>All k-subsets of {0..n-1} in lexicographic order.</summary>
    public static IEnumerable<int[]> Subsets(int n, int k)
    {
        if (k < 0 || k > n)
            yield break;
        int[] indices = Enumerable.Range(0, k).ToArray();
        while (true)
        {
            yield return (int[])indices.Clone();
            int i = k - 1;
            while (i >= 0 && indices[i] == n - k + i)
                i--;
            if (i < 0)
                yield break;
            indices[i]++;
            for (int j = i + 1; j < k; j++)
                indices[j] = indices[j - 1] + 1;
        }
    }

    /// <summary>
    /// Checks that a failure is legal for an expectation's causes. Mutations may additionally report an unknown outcome
    /// (carrying a non-empty operation id) for every cause except <see cref="MeshCause.Placement"/> and
    /// <see cref="MeshCause.NoHealthyReplica"/>, which are decided before anything is proposed.
    /// </summary>
    /// <returns>Null when legal, otherwise the reason it is not.</returns>
    public static string? CheckFailure(Exception exception, IReadOnlySet<MeshCause> causes, bool mutation, DfsStoragePolicy policy)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(causes);
        ArgumentNullException.ThrowIfNull(policy);
        Exception ex = Unwrap(exception);
        if (causes.Count == 0)
            return $"no failure is legal here, got {Describe(ex)}";

        if (ex is DfsCommitOutcomeUnknownException unknown)
        {
            bool unknownLegal = mutation && causes.Any(c => c is MeshCause.MetadataQuorum or MeshCause.ControlQuorum or MeshCause.Unreachable);
            if (!unknownLegal)
                return $"an unknown outcome is not legal for {(mutation ? "this mutation" : "a read")} with causes [{string.Join(",", causes)}]: {Describe(ex)}";
            return unknown.OperationId.IsEmpty ? $"unknown outcome without an operation id: {Describe(ex)}" : null;
        }

        foreach (MeshCause cause in causes)
        {
            switch (cause)
            {
                case MeshCause.MetadataQuorum:
                case MeshCause.ControlQuorum:
                    if (ex is DfsQuorumUnavailableException { ErrorCode: DfsErrorCode.QuorumUnavailable })
                        return null;
                    break;
                case MeshCause.Unreachable:
                    if (ex is DfsUnavailableException and not DfsPlacementUnavailableException and not DfsCapacityException)
                        return null;
                    break;
                case MeshCause.Placement:
                    if (ex is DfsPlacementUnavailableException p && p.ErrorCode == DfsErrorCode.PlacementUnavailable)
                    {
                        if (p.RequiredCopies != policy.DurableCopiesBeforeAck || p.RequiredZones != policy.DurableZonesBeforeAck)
                            return $"placement failure reports required {p.RequiredCopies}/{p.RequiredZones} but the policy requires {policy.DurableCopiesBeforeAck}/{policy.DurableZonesBeforeAck} (silently changed policy): {Describe(ex)}";
                        if (p.AchievedCopies >= p.RequiredCopies && p.AchievedZones >= p.RequiredZones)
                            return $"placement failure reports achieved {p.AchievedCopies}/{p.AchievedZones} which satisfies required {p.RequiredCopies}/{p.RequiredZones}: {Describe(ex)}";
                        return null;
                    }

                    break;
                case MeshCause.NoHealthyReplica:
                    if (ex is DfsUnavailableException or DfsIntegrityException)
                        return null;
                    break;
            }
        }

        return $"{Describe(ex)} is not a legal failure for causes [{string.Join(",", causes.OrderBy(c => c))}]";
    }

    /// <summary>Unwraps aggregate/target-invocation wrappers.</summary>
    public static Exception Unwrap(Exception exception)
    {
        Exception current = exception;
        while (current is AggregateException { InnerExceptions.Count: 1 } aggregate)
            current = aggregate.InnerExceptions[0];
        return current;
    }

    /// <summary>Type, error code and message of an exception.</summary>
    public static string Describe(Exception? ex) => ex switch
    {
        null => "(none)",
        DfsException d => $"{d.GetType().Name}({d.ErrorCode}): {d.Message}",
        _ => $"{ex.GetType().Name}: {ex.Message}",
    };
}
