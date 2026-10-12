using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Deterministic harness over pure <see cref="RaftNode"/> cores (CON/MOD/MUT tests). Each simulated host keeps a durable
/// <see cref="InMemoryRaftLog"/>, a durable hard state and a durable state-machine image (its latest snapshot); everything
/// else (the Raft core, the applied state machine, read states) is volatile and lost on <see cref="Crash"/>.
/// </summary>
/// <remarks>
/// <para>Time advances in ticks. Each tick, every running node is ticked (in index order) and then messages due at the
/// current tick are delivered in rounds until none are left. Ready processing follows the RaftNode host contract:
/// install snapshot, persist hard state and entries, send messages, apply committed entries, advance.</para>
/// <para>The network is seeded: drops, duplicates and (when enabled) reordering with random delays are drawn from the
/// seed, so a run replays exactly. Partitions and directional cuts are checked at delivery time; messages to a crashed
/// node are lost.</para>
/// <para>Safety oracles run continuously: election safety (one leader per term ever observed), state-machine safety (no
/// two applications of different entries at one index), leader completeness (a new leader's log holds every entry known
/// committed before it was elected), commit monotonicity; <see cref="CheckSafety"/> adds pairwise log matching over
/// the durable logs.</para>
/// </remarks>
public sealed partial class RaftSimulation
{
    private readonly RaftSimulationOptions _options;
    private readonly RaftConfState _bootstrap;
    private readonly IRandomSource _net;
    private readonly IRandomSource _idRandom;
    private readonly List<SimHost> _hosts = [];
    private readonly List<Envelope> _inflight = [];
    private readonly HashSet<(int From, int To)> _cuts = [];
    private readonly List<string> _trace = [];
    private readonly List<string> _violations = [];
    private readonly Dictionary<ulong, int> _leaderOfTerm = [];
    private readonly HashSet<ulong> _splitBrainTerms = [];
    private readonly SortedDictionary<ulong, AppliedRecord> _applied = [];
    private int[] _groups;
    private double _dropProbability;
    private double _duplicateProbability;
    private bool _reorder;
    private long _now;
    private long _seq;
    private ulong _maxTermObserved;
    private ulong _maxAppliedIndex;

    /// <summary>Creates a simulation of <paramref name="voters"/> voters and <paramref name="learners"/> learners.</summary>
    public RaftSimulation(int voters, ulong seed, int learners = 0, SafetyMutations? mutations = null)
        : this(new RaftSimulationOptions { Voters = voters, Learners = learners, Seed = seed, Mutations = mutations ?? SafetyMutations.None })
    {
    }

    /// <summary>Creates a simulation from <paramref name="options"/>.</summary>
    public RaftSimulation(RaftSimulationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        var root = new SeededRandom(options.Seed);
        _net = root.Fork("raft-sim/net");
        _idRandom = root.Fork("raft-sim/ids");

        int total = options.Voters + options.Learners;
        var ids = new NodeId[total];
        for (int i = 0; i < total; i++)
            ids[i] = NodeId.New(_idRandom);
        _bootstrap = new RaftConfState(ids[..options.Voters], ids[options.Voters..]);
        _groups = new int[total];
        for (int i = 0; i < total; i++)
        {
            _hosts.Add(new SimHost(i, ids[i], _bootstrap));
            StartNode(_hosts[i]);
        }

        AddTrace($"start voters={options.Voters} learners={options.Learners} seed={options.Seed}");
    }

    /// <summary>The simulation options.</summary>
    public RaftSimulationOptions Options => _options;

    /// <summary>Number of voters at start.</summary>
    public int Voters => _options.Voters;

    /// <summary>Number of learners at start.</summary>
    public int Learners => _options.Learners;

    /// <summary>Simulation seed.</summary>
    public ulong Seed => _options.Seed;

    /// <summary>Safety mutations applied to the simulated hosts (negative controls only).</summary>
    public SafetyMutations Mutations => _options.Mutations;

    /// <summary>Current simulation tick.</summary>
    public long Now => _now;

    /// <summary>Number of simulated hosts (including nodes added with <see cref="AddNode"/>).</summary>
    public int NodeCount => _hosts.Count;

    /// <summary>The bootstrap membership.</summary>
    public RaftConfState BootstrapConf => _bootstrap;

    /// <summary>
    /// The Raft cores (voters first, then learners, then added nodes). A crashed node exposes its last (frozen) instance
    /// until it is restarted.
    /// </summary>
    public IReadOnlyList<RaftNode> Nodes => _hosts.Select(h => h.Raft).ToArray();

    /// <summary>Index of the current unique leader of the highest term among running nodes, or null.</summary>
    public int? LeaderIndex
    {
        get
        {
            int? best = null;
            ulong bestTerm = 0;
            bool unique = false;
            foreach (SimHost h in _hosts)
            {
                if (!h.Up || h.Raft.Role != RaftRole.Leader)
                    continue;
                if (best is null || h.Raft.Term > bestTerm)
                {
                    best = h.Index;
                    bestTerm = h.Raft.Term;
                    unique = true;
                }
                else if (h.Raft.Term == bestTerm)
                {
                    unique = false;
                }
            }

            return unique ? best : null;
        }
    }

    /// <summary>Simulation events (replay artifact).</summary>
    public IReadOnlyList<string> Trace => _trace;

    /// <summary>Messages handed to the network.</summary>
    public long MessagesSent { get; private set; }

    /// <summary>Messages delivered to a running node.</summary>
    public long MessagesDelivered { get; private set; }

    /// <summary>Messages lost (random drops, partitions, cuts, crashed destinations).</summary>
    public long MessagesDropped { get; private set; }

    /// <summary>Extra copies injected by random duplication.</summary>
    public long MessagesDuplicated { get; private set; }

    /// <summary>Highest index known to be committed (applied by any node).</summary>
    public ulong MaxCommittedIndex => _maxAppliedIndex;

    /// <summary>Node id of host <paramref name="node"/>.</summary>
    public NodeId IdOf(int node) => Host(node).Id;

    /// <summary>Host index of <paramref name="id"/>, or -1.</summary>
    public int IndexOf(NodeId id) => _hosts.FindIndex(h => h.Id == id);

    /// <summary>True when the node is running (not crashed).</summary>
    public bool IsUp(int node) => Host(node).Up;

    /// <summary>The Raft core of a node (the frozen last instance when crashed).</summary>
    public RaftNode Node(int node) => Host(node).Raft;

    /// <summary>The node's durable log.</summary>
    public InMemoryRaftLog PersistedLog(int node) => Host(node).Log;

    /// <summary>The node's durable hard state.</summary>
    public RaftHardState PersistedHardState(int node) => Host(node).Hard;

    /// <summary>Index of the last entry applied to the node's (volatile) state machine.</summary>
    public ulong AppliedIndex(int node) => Host(node).AppliedIndex;

    /// <summary>The membership as of the node's applied index.</summary>
    public RaftConfState AppliedConf(int node) => Host(node).AppliedConf;

    /// <summary>Read states received by the node since it (re)started.</summary>
    public IReadOnlyList<SimReadState> ReadStates(int node) => Host(node).Reads;

    /// <summary>Nodes in different groups cannot exchange messages; unlisted nodes are isolated.</summary>
    public void Partition(params int[][] groups)
    {
        ArgumentNullException.ThrowIfNull(groups);
        var assigned = new int[_hosts.Count];
        for (int i = 0; i < assigned.Length; i++)
            assigned[i] = -(i + 1); // unlisted: alone
        for (int g = 0; g < groups.Length; g++)
        {
            foreach (int node in groups[g] ?? [])
            {
                Host(node);
                if (assigned[node] > 0)
                    throw new ArgumentException($"Node {node} appears in more than one group.", nameof(groups));
                assigned[node] = g + 1;
            }
        }

        _groups = assigned;
        AddTrace($"partition {string.Join(" | ", groups.Select(g => "[" + string.Join(",", g ?? []) + "]"))}");
    }

    /// <summary>Cuts every link of <paramref name="node"/>.</summary>
    public void Isolate(int node)
    {
        Host(node);
        _groups[node] = -(node + 1) - 1000;
        AddTrace($"isolate n{node}");
    }

    /// <summary>Cuts the link from <paramref name="from"/> to <paramref name="to"/> (one direction only).</summary>
    public void Cut(int from, int to)
    {
        Host(from);
        Host(to);
        _cuts.Add((from, to));
        AddTrace($"cut n{from}->n{to}");
    }

    /// <summary>Removes all partitions and cuts.</summary>
    public void Heal()
    {
        _groups = new int[_hosts.Count];
        _cuts.Clear();
        AddTrace("heal");
    }

    /// <summary>Crashes a node: its Raft core, state machine and read states are lost; durable state is kept.</summary>
    public void Crash(int node)
    {
        SimHost h = Host(node);
        if (!h.Up)
            return;
        h.Up = false;
        h.ArmedCrash = null;
        AddTrace($"crash n{node} (term {h.Raft.Term}, role {h.Raft.Role}, durable last {h.Log.LastIndex})");
    }

    /// <summary>Restarts a node from its persisted hard state and persisted log only (crashing it first when running).</summary>
    public void Restart(int node)
    {
        SimHost h = Host(node);
        if (h.Up)
            Crash(node);
        StartNode(h);
        AddTrace($"restart n{node} (incarnation {h.Incarnation}, term {h.Hard.Term}, commit {h.Hard.Commit}, durable last {h.Log.LastIndex})");
        Drive(h);
    }

    /// <summary>Arms a crash that interrupts the node's processing of its next ready at <paramref name="point"/>.</summary>
    public void ArmCrash(int node, RaftCrashPoint point)
    {
        SimHost h = Host(node);
        h.ArmedCrash = point;
        AddTrace($"arm crash n{node} at {point}");
    }

    /// <summary>Cancels every armed crash that has not fired yet.</summary>
    public void DisarmCrashes()
    {
        foreach (SimHost h in _hosts)
            h.ArmedCrash = null;
        AddTrace("disarm crashes");
    }

    /// <summary>Probability that a message is dropped.</summary>
    public void DropProbability(double p)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(p);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(p, 1.0);
        _dropProbability = p;
        AddTrace($"drop probability {p}");
    }

    /// <summary>Probability that a message is duplicated.</summary>
    public void DuplicateProbability(double p)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(p);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(p, 1.0);
        _duplicateProbability = p;
        AddTrace($"duplicate probability {p}");
    }

    /// <summary>Enables random reordering (random delivery delays and shuffled delivery order) of in-flight messages.</summary>
    public void ReorderMessages(bool enable)
    {
        _reorder = enable;
        AddTrace($"reorder {enable}");
    }

    /// <summary>Proposes data at a node; false when it is not running or not the leader.</summary>
    public bool Propose(int node, byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        SimHost h = Host(node);
        if (!h.Up || !h.Raft.TryPropose(data, out ulong index))
            return false;
        AddTrace($"propose n{node} index {index} ({data.Length} bytes)");
        Drive(h);
        return true;
    }

    /// <summary>Proposes a membership change at a node; false when it is not running or the leader refused it.</summary>
    public bool ProposeConfChange(int node, RaftConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        SimHost h = Host(node);
        if (!h.Up || !h.Raft.TryProposeConfChange(change, out ulong index))
            return false;
        AddTrace($"propose conf n{node} index {index} {change.Kind} n{IndexOf(change.Node)}");
        Drive(h);
        return true;
    }

    /// <summary>
    /// Asks a node for a ReadIndex; confirmed reads appear in <see cref="ReadStates"/>. A confirmed read whose index is
    /// below the highest index known committed when the read was requested is recorded as a linearizability violation.
    /// </summary>
    public void ReadIndex(int node, ulong context)
    {
        SimHost h = Host(node);
        if (!h.Up)
            return;
        // Linearizability floor: the read must observe at least every entry already known committed now.
        h.ReadFloors[context] = _maxAppliedIndex;
        h.Raft.ReadIndex(context);
        Drive(h);
    }

    /// <summary>Asks a leader to transfer leadership to <paramref name="target"/>.</summary>
    public void TransferLeadership(int node, int target)
    {
        SimHost h = Host(node);
        NodeId targetId = Host(target).Id;
        if (!h.Up)
            return;
        AddTrace($"transfer n{node} -> n{target}");
        h.Raft.TransferLeadership(targetId);
        Drive(h);
    }

    /// <summary>
    /// Adds a new, empty node that is not a member (it starts with the bootstrap membership as of index 0 and learns the
    /// rest from the leader). Returns its index; add it with <see cref="RaftConfChangeKind.AddLearner"/>.
    /// </summary>
    public int AddNode()
    {
        int index = _hosts.Count;
        var h = new SimHost(index, NodeId.New(_idRandom), _bootstrap);
        _hosts.Add(h);
        Array.Resize(ref _groups, _hosts.Count);
        StartNode(h);
        AddTrace($"add node n{index} id {h.Id}");
        return index;
    }

    /// <summary>Compacts the node's durable log up to its applied index (taking a state-machine snapshot).</summary>
    public void Compact(int node)
    {
        SimHost h = Host(node);
        if (h.Up)
            CompactHost(h);
    }

    /// <summary>Runs the given number of ticks (delivering messages in between).</summary>
    public void RunTicks(int ticks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ticks);
        for (int i = 0; i < ticks; i++)
            RunOneTick();
    }

    /// <summary>Runs until <paramref name="condition"/> holds or <paramref name="maxTicks"/> elapse.</summary>
    public void RunUntil(Func<bool> condition, int maxTicks) => TryRunUntil(condition, maxTicks);

    /// <summary>Runs until <paramref name="condition"/> holds or <paramref name="maxTicks"/> elapse; true when it holds.</summary>
    public bool TryRunUntil(Func<bool> condition, int maxTicks)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentOutOfRangeException.ThrowIfNegative(maxTicks);
        for (int i = 0; i < maxTicks; i++)
        {
            if (condition())
                return true;
            RunOneTick();
        }

        return condition();
    }

    /// <summary>Delivers every message that is due now without advancing time.</summary>
    public void DeliverPending() => DeliverDue();

    /// <summary>The applied normal-entry payloads of a node, in order.</summary>
    public IReadOnlyList<byte[]> AppliedLog(int node) => Host(node).StateMachine.Select(e => e.Data).ToArray();

    /// <summary>The applied normal entries (index, payload) of a node, in order.</summary>
    public IReadOnlyList<(ulong Index, byte[] Data)> AppliedEntries(int node) => Host(node).StateMachine.ToArray();

    private SimHost Host(int node)
    {
        if ((uint)node >= (uint)_hosts.Count)
            throw new ArgumentOutOfRangeException(nameof(node), node, $"No node {node} (count {_hosts.Count}).");
        return _hosts[node];
    }

    private void AddTrace(string message) => _trace.Add($"[{_now}] {message}");
}
