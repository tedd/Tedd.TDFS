using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Initial Raft log of a group created by bootstrap (static cluster bootstrap, volume creation). Every initial member
/// writes the identical prefix — membership changes listing the initial members, then initial commands — committed at
/// term 1, so a member added later reconstructs the same history from the leader's log or snapshot (ARCHITECTURE §2.5).
/// </summary>
internal static class RaftBootstrap
{
    /// <summary>Term of bootstrap entries.</summary>
    public const ulong Term = 1;

    /// <summary>Membership entries (AddLearner + PromoteLearner per voter, AddLearner per learner) followed by <paramref name="commands"/>.</summary>
    public static IReadOnlyList<RaftEntry> BuildEntries(IReadOnlyList<NodeId> voters, IReadOnlyList<NodeId> learners, IReadOnlyList<ReadOnlyMemory<byte>> commands)
    {
        ArgumentNullException.ThrowIfNull(voters);
        ArgumentNullException.ThrowIfNull(learners);
        ArgumentNullException.ThrowIfNull(commands);
        if (voters.Count == 0)
            throw new ArgumentException("A group needs at least one voter.", nameof(voters));
        var entries = new List<RaftEntry>();
        ulong index = 1;
        foreach (NodeId voter in voters)
        {
            entries.Add(new RaftEntry(index++, Term, RaftEntryKind.ConfChange, new RaftConfChange(RaftConfChangeKind.AddLearner, voter).Encode()));
            entries.Add(new RaftEntry(index++, Term, RaftEntryKind.ConfChange, new RaftConfChange(RaftConfChangeKind.PromoteLearner, voter).Encode()));
        }

        foreach (NodeId learner in learners)
            entries.Add(new RaftEntry(index++, Term, RaftEntryKind.ConfChange, new RaftConfChange(RaftConfChangeKind.AddLearner, learner).Encode()));
        foreach (ReadOnlyMemory<byte> command in commands)
            entries.Add(new RaftEntry(index++, Term, RaftEntryKind.Normal, command));
        return entries;
    }
}

/// <summary>Static cluster bootstrap (ARCHITECTURE §2.5): validation and the identical initial control log.</summary>
internal static class ControlBootstrap
{
    /// <summary>Default number of control voters for a single-member (seed-join) bootstrap.</summary>
    public const int DefaultControlVoters = 3;

    /// <summary>Validates a bootstrap description; returns the member describing <paramref name="nodeName"/>.</summary>
    /// <exception cref="ArgumentException">The description is inconsistent or does not list this node.</exception>
    public static BootstrapMember Validate(ClusterBootstrap bootstrap, string nodeName)
    {
        ArgumentNullException.ThrowIfNull(bootstrap);
        if (bootstrap.ClusterId == Guid.Empty)
            throw new ArgumentException("The bootstrap cluster id must not be empty.", nameof(bootstrap));
        if (bootstrap.Members is null || bootstrap.Members.Count == 0)
            throw new ArgumentException("The bootstrap lists no members.", nameof(bootstrap));
        var ids = new HashSet<NodeId>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (BootstrapMember m in bootstrap.Members)
        {
            if (m.Id.IsEmpty || string.IsNullOrWhiteSpace(m.Name))
                throw new ArgumentException("Bootstrap members need an id and a name.", nameof(bootstrap));
            if (!ids.Add(m.Id) || !names.Add(m.Name))
                throw new ArgumentException($"Bootstrap member '{m.Name}' is listed twice.", nameof(bootstrap));
        }

        if (bootstrap.ControlVoters is null || bootstrap.ControlVoters.Count == 0)
            throw new ArgumentException("The bootstrap lists no control voters.", nameof(bootstrap));
        foreach (string voter in bootstrap.ControlVoters)
        {
            if (!names.Contains(voter))
                throw new ArgumentException($"Control voter '{voter}' is not a bootstrap member.", nameof(bootstrap));
        }

        if (bootstrap.ControlVoters.Distinct(StringComparer.Ordinal).Count() != bootstrap.ControlVoters.Count)
            throw new ArgumentException("A control voter is listed twice.", nameof(bootstrap));
        if (bootstrap.ControlVoterTarget is < 1)
            throw new ArgumentException("The control voter target must be at least 1.", nameof(bootstrap));
        foreach (BootstrapPrincipal p in bootstrap.Principals ?? [])
        {
            if (ControlStateMachine.GetPrincipalNameError(p.Name) is { } error)
                throw new ArgumentException(error, nameof(bootstrap));
        }

        return bootstrap.Members.FirstOrDefault(m => string.Equals(m.Name, nodeName, StringComparison.Ordinal))
            ?? throw new ArgumentException($"Node '{nodeName}' is not a member of the bootstrap description.", nameof(bootstrap));
    }

    /// <summary>The initial control log: membership of the control group, then the bootstrap command.</summary>
    /// <param name="bootstrap">The bootstrap description.</param>
    /// <param name="keys">Keys derived from the cluster secret.</param>
    /// <param name="memberFeatures">
    /// Features of the bootstrapping release (every initial member runs it; default: this build's), recorded for each member
    /// (feature eligibility, FMT-009).
    /// </param>
    public static IReadOnlyList<RaftEntry> BuildEntries(ClusterBootstrap bootstrap, KeyHierarchy keys, IReadOnlySet<string>? memberFeatures = null)
    {
        ArgumentNullException.ThrowIfNull(bootstrap);
        ArgumentNullException.ThrowIfNull(keys);
        var byName = bootstrap.Members.ToDictionary(m => m.Name, StringComparer.Ordinal);
        NodeId[] voters = bootstrap.ControlVoters.Select(n => byName[n].Id).ToArray();
        NodeId[] learners = bootstrap.Members.Where(m => !voters.Contains(m.Id)).Select(m => m.Id).ToArray();
        int target = bootstrap.ControlVoterTarget is { } configured
            ? Math.Max(Math.Max(1, configured), bootstrap.ControlVoters.Count)
            : bootstrap.Members.Count == 1 ? Math.Max(DefaultControlVoters, bootstrap.ControlVoters.Count) : bootstrap.ControlVoters.Count;

        var principals = new List<PrincipalData>();
        foreach (BootstrapPrincipal p in bootstrap.Principals ?? [])
        {
            PrincipalVerifier verifier = HandshakeAuth.CreateVerifier(p.Secret, keys.BootstrapSalt(p.Name), HandshakeAuth.DefaultIterations);
            principals.Add(new PrincipalData(p.Name, verifier.Salt, verifier.Iterations, verifier.StoredKey, verifier.ServerKey, p.ClusterAdmin));
        }

        var command = new BootstrapClusterCommand(
            new CommandHeader(UInt128.Zero, string.Empty, DateTimeOffset.UnixEpoch),
            bootstrap.ClusterId,
            bootstrap.ClusterName ?? string.Empty,
            target,
            bootstrap.Members.Select(m => new BootstrapMemberData(m.Id, m.Name, m.Labels, m.Endpoints ?? [])).ToArray(),
            principals,
            (memberFeatures ?? ProtocolFeatures.Supported).Order(StringComparer.Ordinal).ToArray());
        return RaftBootstrap.BuildEntries(voters, learners, [ControlCommandCodec.Encode(command)]);
    }
}
