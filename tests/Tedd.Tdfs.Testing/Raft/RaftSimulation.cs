using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Testing;

/// <summary>Deterministic harness over pure <see cref="RaftNode"/> cores (CON/MOD/MUT tests).</summary>
public sealed partial class RaftSimulation
{
    /// <summary>Creates a simulation of <paramref name="voters"/> voters and <paramref name="learners"/> learners.</summary>
    public RaftSimulation(int voters, ulong seed, int learners = 0, SafetyMutations? mutations = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(voters);
        ArgumentOutOfRangeException.ThrowIfNegative(learners);
        Voters = voters;
        Learners = learners;
        Seed = seed;
        Mutations = mutations ?? SafetyMutations.None;
    }

    /// <summary>Number of voters.</summary>
    public int Voters { get; }

    /// <summary>Number of learners.</summary>
    public int Learners { get; }

    /// <summary>Simulation seed.</summary>
    public ulong Seed { get; }

    /// <summary>Safety mutations applied to the simulated hosts (negative controls only).</summary>
    public SafetyMutations Mutations { get; }

    /// <summary>The Raft cores (voters first, then learners).</summary>
    public IReadOnlyList<RaftNode> Nodes => throw Stub.NotYet("RaftSimulation.Nodes");

    /// <summary>Index of the current unique leader of the highest term, or null.</summary>
    public int? LeaderIndex => throw Stub.NotYet("RaftSimulation.LeaderIndex");

    /// <summary>Simulation events (replay artifact).</summary>
    public IReadOnlyList<string> Trace => throw Stub.NotYet("RaftSimulation.Trace");

    /// <summary>Nodes in different groups cannot exchange messages.</summary>
    public void Partition(params int[][] groups) => throw Stub.NotYet("RaftSimulation.Partition");

    /// <summary>Removes all partitions.</summary>
    public void Heal() => throw Stub.NotYet("RaftSimulation.Heal");

    /// <summary>Crashes a node (volatile state lost).</summary>
    public void Crash(int node) => throw Stub.NotYet("RaftSimulation.Crash");

    /// <summary>Restarts a node from its persisted hard state and persisted log only.</summary>
    public void Restart(int node) => throw Stub.NotYet("RaftSimulation.Restart");

    /// <summary>Probability that a message is dropped.</summary>
    public void DropProbability(double p) => throw Stub.NotYet("RaftSimulation.DropProbability");

    /// <summary>Probability that a message is duplicated.</summary>
    public void DuplicateProbability(double p) => throw Stub.NotYet("RaftSimulation.DuplicateProbability");

    /// <summary>Enables random reordering of in-flight messages.</summary>
    public void ReorderMessages(bool enable) => throw Stub.NotYet("RaftSimulation.ReorderMessages");

    /// <summary>Proposes data at a node; false when it is not the leader.</summary>
    public bool Propose(int node, byte[] data) => throw Stub.NotYet("RaftSimulation.Propose");

    /// <summary>Runs the given number of ticks (delivering messages in between).</summary>
    public void RunTicks(int ticks) => throw Stub.NotYet("RaftSimulation.RunTicks");

    /// <summary>Runs until <paramref name="condition"/> holds or <paramref name="maxTicks"/> elapse.</summary>
    public void RunUntil(Func<bool> condition, int maxTicks) => throw Stub.NotYet("RaftSimulation.RunUntil");

    /// <summary>The applied normal-entry payloads of a node, in order.</summary>
    public IReadOnlyList<byte[]> AppliedLog(int node) => throw Stub.NotYet("RaftSimulation.AppliedLog");

    /// <summary>Checks election safety, log matching, leader completeness and state-machine safety; null when OK.</summary>
    public string? CheckSafety() => throw Stub.NotYet("RaftSimulation.CheckSafety");
}
