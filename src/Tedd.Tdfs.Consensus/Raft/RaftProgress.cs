namespace Tedd.Tdfs.Consensus;

/// <summary>Leader-side replication tracking for one peer (etcd/raft "Progress").</summary>
internal sealed class RaftProgress
{
    private readonly Queue<ulong> _inflights = new();
    private readonly int _maxInflight;

    public RaftProgress(NodeId peer, ulong next, int maxInflight, bool recentActive)
    {
        Peer = peer;
        Next = Math.Max(next, 1);
        _maxInflight = maxInflight;
        RecentActive = recentActive;
    }

    public NodeId Peer { get; }

    /// <summary>Highest index known to match the leader's log.</summary>
    public ulong Match { get; set; }

    /// <summary>Next index to send.</summary>
    public ulong Next { get; set; }

    public RaftProgressState State { get; private set; } = RaftProgressState.Probe;

    /// <summary>Probe state: an append was sent and no response or heartbeat response has arrived since.</summary>
    public bool ProbeSent { get; set; }

    /// <summary>Snapshot state: index of the snapshot being sent.</summary>
    public ulong PendingSnapshot { get; private set; }

    /// <summary>Snapshot being transferred (kept so chunks stay consistent even if the host compacts further).</summary>
    public RaftSnapshot? Snapshot { get; private set; }

    /// <summary>Next byte offset the peer is known to expect for <see cref="Snapshot"/>.</summary>
    public long SnapshotOffset { get; set; }

    /// <summary>The peer responded since the last CheckQuorum interval started.</summary>
    public bool RecentActive { get; set; }

    public int InflightCount => _inflights.Count;

    public bool InflightsFull => _inflights.Count >= _maxInflight;

    /// <summary>True when no further append may be sent right now.</summary>
    public bool IsPaused => State switch
    {
        RaftProgressState.Probe => ProbeSent,
        RaftProgressState.Replicate => InflightsFull,
        _ => true,
    };

    /// <summary>Forgets the snapshot being sent (failed transfer) so the next probe restarts from Match + 1.</summary>
    public void ClearPendingSnapshot() => PendingSnapshot = 0;

    public void BecomeProbe()
    {
        if (State == RaftProgressState.Snapshot)
        {
            ulong pending = PendingSnapshot;
            ResetState(RaftProgressState.Probe);
            Next = Math.Max(Match + 1, pending + 1);
        }
        else
        {
            ResetState(RaftProgressState.Probe);
            Next = Match + 1;
        }
    }

    public void BecomeReplicate()
    {
        ResetState(RaftProgressState.Replicate);
        Next = Match + 1;
    }

    public void BecomeSnapshot(RaftSnapshot snapshot)
    {
        ResetState(RaftProgressState.Snapshot);
        PendingSnapshot = snapshot.Meta.Index;
        Snapshot = snapshot;
        SnapshotOffset = 0;
    }

    /// <summary>Records a successful acknowledgement of <paramref name="index"/>; true when Match advanced.</summary>
    public bool MaybeUpdate(ulong index)
    {
        bool updated = false;
        if (Match < index)
        {
            Match = index;
            updated = true;
            ProbeSent = false;
        }

        Next = Math.Max(Next, index + 1);
        return updated;
    }

    /// <summary>
    /// Handles a rejection of the append whose previous index was <paramref name="rejected"/>; false when the rejection is
    /// stale. <paramref name="matchHint"/> is the highest index that might still match.
    /// </summary>
    public bool MaybeDecrTo(ulong rejected, ulong matchHint)
    {
        if (State == RaftProgressState.Replicate)
        {
            if (rejected <= Match)
                return false;
            Next = Match + 1;
            return true;
        }

        if (Next == 0 || Next - 1 != rejected)
            return false;
        Next = Math.Max(Math.Min(rejected, matchHint + 1), Match + 1);
        ProbeSent = false;
        return true;
    }

    /// <summary>Records an append ending at <paramref name="lastIndex"/> sent in Replicate state.</summary>
    public void AddInflight(ulong lastIndex) => _inflights.Enqueue(lastIndex);

    /// <summary>Releases in-flight appends acknowledged up to <paramref name="index"/>.</summary>
    public void FreeInflightsUpTo(ulong index)
    {
        while (_inflights.Count > 0 && _inflights.Peek() <= index)
            _inflights.Dequeue();
    }

    /// <summary>Releases the oldest in-flight append (unblocks a pipeline whose messages were lost).</summary>
    public void FreeFirstInflight()
    {
        if (_inflights.Count > 0)
            _inflights.Dequeue();
    }

    public RaftPeerProgress ToPublic(bool isLearner) => new(Peer, Match, Next, State, RecentActive, isLearner);

    private void ResetState(RaftProgressState state)
    {
        State = state;
        ProbeSent = false;
        PendingSnapshot = 0;
        Snapshot = null;
        SnapshotOffset = 0;
        _inflights.Clear();
    }
}
