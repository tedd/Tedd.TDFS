using System.Buffers.Binary;
using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Testing;

public sealed partial class RaftSimulation
{
    private const int MaxDeliveryRoundsPerTick = 10_000;

    private void StartNode(SimHost h)
    {
        h.Incarnation++;
        h.StateMachine.Clear();
        h.Reads.Clear();
        h.ReadFloors.Clear();
        h.ArmedCrash = null;
        RaftSnapshotMeta snap = h.Log.Snapshot;
        if (snap.Index > 0)
        {
            h.StateMachine.AddRange(DeserializeStateMachine(h.SnapshotData));
            h.AppliedIndex = snap.Index;
            h.AppliedConf = snap.ConfState;
        }
        else
        {
            h.AppliedIndex = 0;
            h.AppliedConf = h.BaseConf;
        }

        var nodeOptions = new RaftNodeOptions
        {
            Mutations = _options.Mutations,
            SnapshotChunkBytes = _options.SnapshotChunkBytes,
            SnapshotSource = new DelegateRaftSnapshotSource(() =>
                h.Log.Snapshot.Index == 0 ? null : new RaftSnapshot(h.Log.Snapshot, h.SnapshotData)),
        };
        IRandomSource random = new SeededRandom(_options.Seed).Fork($"raft-sim/n{h.Index}/inc{h.Incarnation}");
        h.Raft = new RaftNode(_options.BuildConfig(h.Id), h.Hard, h.AppliedConf, h.Log, random, nodeOptions);
        h.Up = true;
        h.LastCommit = h.Raft.CommitIndex;
        h.LastRole = h.Raft.Role;
        h.LastTerm = h.Raft.Term;
    }

    private void RunOneTick()
    {
        _now++;
        foreach (SimHost h in _hosts)
        {
            if (!h.Up)
                continue;
            h.Raft.Tick();
            Drive(h);
        }

        DeliverDue();
        if (_options.SnapshotThreshold > 0)
        {
            foreach (SimHost h in _hosts)
            {
                if (h.Up && h.AppliedIndex - h.Log.Snapshot.Index >= (ulong)_options.SnapshotThreshold)
                    CompactHost(h);
            }
        }
    }

    private void Drive(SimHost h)
    {
        while (h.Up && h.Raft.HasReady())
        {
            RaftReady ready = h.Raft.GetReady();
            if (!ProcessReady(h, ready))
                return; // crashed while processing
            h.Raft.Advance(ready);
            Observe(h);
        }

        if (h.Up)
            Observe(h);
    }

    /// <summary>Handles one ready per the host contract; false when an armed crash interrupted it.</summary>
    private bool ProcessReady(SimHost h, RaftReady ready)
    {
        if (TryCrashAt(h, RaftCrashPoint.BeforePersist))
            return false;

        if (ready.SnapshotToInstall is { } meta)
        {
            byte[] data = ready.SnapshotData.ToArray();
            List<(ulong Index, byte[] Data)> image = DeserializeStateMachine(data);
            h.Log.ApplySnapshot(meta);
            h.SnapshotData = data;
            h.StateMachine.Clear();
            h.StateMachine.AddRange(image);
            h.AppliedIndex = meta.Index;
            h.AppliedConf = meta.ConfState;
            foreach ((ulong index, byte[] payload) in image)
                CheckApplied(h, index, null, RaftEntryKind.Normal, payload);
            AddTrace($"n{h.Index} installed snapshot index {meta.Index} term {meta.Term}");
        }

        if (ready.HardState is { } hs)
            h.Hard = hs;
        if (ready.EntriesToPersist.Count > 0)
            h.Log.Append(ready.EntriesToPersist);

        if (TryCrashAt(h, RaftCrashPoint.AfterPersistBeforeSend))
            return false;

        foreach (RaftMessage m in ready.Messages)
            Send(h, m);

        if (TryCrashAt(h, RaftCrashPoint.AfterSendBeforeApply))
            return false;

        foreach (RaftEntry e in ready.CommittedEntries)
            ApplyEntry(h, e);
        foreach (RaftReadState rs in ready.ReadStates)
        {
            if (h.ReadFloors.Remove(rs.RequestContext, out ulong floor) && rs.Index < floor)
                Violation($"linearizable read: n{h.Index} confirmed read {rs.RequestContext} at index {rs.Index} below committed index {floor}");
            h.Reads.Add(new SimReadState(rs.Index, rs.RequestContext, h.Raft.Term, _now));
        }
        return true;
    }

    private bool TryCrashAt(SimHost h, RaftCrashPoint point)
    {
        if (h.ArmedCrash != point)
            return false;
        AddTrace($"armed crash n{h.Index} fired at {point}");
        Crash(h.Index);
        return true;
    }

    private void ApplyEntry(SimHost h, RaftEntry e)
    {
        if (e.Index != h.AppliedIndex + 1)
            Violation($"n{h.Index} applied index {e.Index} after {h.AppliedIndex} (gap or repeat)");
        CheckApplied(h, e.Index, e.Term, e.Kind, e.Data.ToArray());
        switch (e.Kind)
        {
            case RaftEntryKind.Normal:
                h.StateMachine.Add((e.Index, e.Data.ToArray()));
                break;
            case RaftEntryKind.ConfChange when RaftConfChange.TryDecode(e.Data.Span, out RaftConfChange? change):
                h.AppliedConf = h.AppliedConf.Apply(change);
                break;
        }

        h.AppliedIndex = e.Index;
    }

    /// <summary>State-machine safety: every application at an index must be the same entry.</summary>
    private void CheckApplied(SimHost h, ulong index, ulong? term, RaftEntryKind kind, byte[] data)
    {
        if (h.Raft.Term > _maxTermObserved)
            _maxTermObserved = h.Raft.Term;
        if (_applied.TryGetValue(index, out AppliedRecord? existing))
        {
            bool same = existing.Kind == kind && existing.Data.AsSpan().SequenceEqual(data)
                && (term is null || existing.Term is null || existing.Term == term);
            if (!same)
            {
                Violation($"state machine safety: n{h.Index} applied {kind} term {term?.ToString() ?? "?"} at index {index} " +
                    $"but n{existing.FirstNode} applied {existing.Kind} term {existing.Term?.ToString() ?? "?"}");
            }
            else if (existing.Term is null && term is not null)
            {
                _applied[index] = existing with { Term = term };
            }

            return;
        }

        _applied[index] = new AppliedRecord(term, kind, data, h.Index, _maxTermObserved);
        if (index > _maxAppliedIndex)
            _maxAppliedIndex = index;
    }

    private void Send(SimHost from, RaftMessage m)
    {
        MessagesSent++;
        int to = IndexOf(m.To);
        if (to < 0)
        {
            MessagesDropped++;
            return;
        }

        if (_dropProbability > 0 && _net.NextDouble() < _dropProbability)
        {
            MessagesDropped++;
            if (_options.TraceMessages)
                AddTrace($"drop {Describe(m)}");
            return;
        }

        Enqueue(from.Index, to, m);
        if (_duplicateProbability > 0 && _net.NextDouble() < _duplicateProbability)
        {
            MessagesDuplicated++;
            if (_options.TraceMessages)
                AddTrace($"duplicate {Describe(m)}");
            Enqueue(from.Index, to, m);
        }
    }

    private void Enqueue(int from, int to, RaftMessage m)
    {
        long delay = _reorder && _options.MaxReorderDelayTicks > 0 ? _net.Next(_options.MaxReorderDelayTicks + 1) : 0;
        _inflight.Add(new Envelope(_now + delay, ++_seq, from, to, m));
    }

    private void DeliverDue()
    {
        for (int round = 0; round < MaxDeliveryRoundsPerTick; round++)
        {
            List<Envelope> due = [];
            List<Envelope> later = [];
            foreach (Envelope e in _inflight)
                (e.DeliverAt <= _now ? due : later).Add(e);
            if (due.Count == 0)
                return;
            _inflight.Clear();
            _inflight.AddRange(later);
            if (_reorder)
                _net.Shuffle(due);

            foreach (Envelope e in due)
            {
                SimHost target = _hosts[e.To];
                if (!target.Up || !Connected(e.From, e.To))
                {
                    MessagesDropped++;
                    if (_options.TraceMessages)
                        AddTrace($"lost {Describe(e.Message)}");
                    continue;
                }

                MessagesDelivered++;
                if (_options.TraceMessages)
                    AddTrace($"deliver {Describe(e.Message)}");
                target.Raft.Step(e.Message);
                Drive(target);
            }
        }
    }

    private bool Connected(int from, int to)
        => _groups[from] == _groups[to] && !_cuts.Contains((from, to));

    private void CompactHost(SimHost h)
    {
        if (h.AppliedIndex <= h.Log.Snapshot.Index || h.AppliedIndex > h.Log.LastIndex)
            return;
        h.SnapshotData = SerializeStateMachine(h.StateMachine);
        h.Log.Compact(h.AppliedIndex, h.AppliedConf);
    }

    /// <summary>Records role/term changes and runs the online election-safety and leader-completeness checks.</summary>
    private void Observe(SimHost h)
    {
        RaftNode r = h.Raft;
        if (r.Term > _maxTermObserved)
            _maxTermObserved = r.Term;
        if (r.CommitIndex < h.LastCommit)
            Violation($"n{h.Index} commit index went backwards from {h.LastCommit} to {r.CommitIndex}");
        h.LastCommit = r.CommitIndex;

        if (r.Role != h.LastRole || r.Term != h.LastTerm)
        {
            AddTrace($"n{h.Index} {r.Role} term {r.Term} (last {r.LastIndex}, commit {r.CommitIndex})");
            h.LastRole = r.Role;
            h.LastTerm = r.Term;
        }

        if (r.Role != RaftRole.Leader)
            return;
        if (_leaderOfTerm.TryGetValue(r.Term, out int existing))
        {
            if (existing != h.Index && _splitBrainTerms.Add(r.Term))
                Violation($"election safety: n{existing} and n{h.Index} are both leader of term {r.Term}");
            return;
        }

        _leaderOfTerm[r.Term] = h.Index;
        string? missing = FindMissingCommitted(h, r.Term);
        if (missing is not null)
            Violation($"leader completeness: new leader n{h.Index} of term {r.Term} {missing}");
    }

    /// <summary>Returns a description of a committed entry missing from the node's log, or null.</summary>
    private string? FindMissingCommitted(SimHost h, ulong leaderTerm)
    {
        InMemoryRaftLog log = h.Log;
        foreach ((ulong index, AppliedRecord rec) in _applied)
        {
            if (rec.CommitTermBound >= leaderTerm || index <= log.Snapshot.Index)
                continue;
            if (index > log.LastIndex)
                return $"lacks committed index {index}";
            RaftEntry e = log.EntryAt(index);
            if (e.Kind != rec.Kind || !e.Data.Span.SequenceEqual(rec.Data) || (rec.Term is { } t && t != e.Term))
                return $"has a different entry at committed index {index}";
        }

        return null;
    }

    private void Violation(string message)
    {
        string text = $"[{_now}] {message}";
        _violations.Add(text);
        _trace.Add("VIOLATION " + text);
    }

    /// <summary>Checks election safety, log matching, leader completeness and state-machine safety; null when OK.</summary>
    public string? CheckSafety()
    {
        if (_violations.Count > 0)
            return _violations[0];

        for (int a = 0; a < _hosts.Count; a++)
        {
            for (int b = a + 1; b < _hosts.Count; b++)
            {
                string? mismatch = CheckLogMatching(_hosts[a], _hosts[b]);
                if (mismatch is not null)
                    return $"log matching: {mismatch}";
            }
        }

        foreach (SimHost h in _hosts)
        {
            if (!h.Up || h.Raft.Role != RaftRole.Leader)
                continue;
            string? missing = FindMissingCommitted(h, h.Raft.Term);
            if (missing is not null)
                return $"leader completeness: leader n{h.Index} of term {h.Raft.Term} {missing}";
        }

        return null;
    }

    /// <summary>If two logs hold an entry with the same index and term, all preceding retained entries are identical.</summary>
    private static string? CheckLogMatching(SimHost a, SimHost b)
    {
        InMemoryRaftLog la = a.Log;
        InMemoryRaftLog lb = b.Log;
        ulong lo = Math.Max(la.FirstIndex, lb.FirstIndex);
        ulong hi = Math.Min(la.LastIndex, lb.LastIndex);
        if (hi < lo)
            return null;
        ulong anchor = 0;
        for (ulong i = hi; i >= lo; i--)
        {
            if (la.TermAt(i) == lb.TermAt(i))
            {
                anchor = i;
                break;
            }

            if (i == lo)
                break;
        }

        if (anchor == 0)
            return null;
        for (ulong i = lo; i <= anchor; i++)
        {
            RaftEntry ea = la.EntryAt(i);
            RaftEntry eb = lb.EntryAt(i);
            if (ea.Term != eb.Term || ea.Kind != eb.Kind || !ea.Data.Span.SequenceEqual(eb.Data.Span))
                return $"n{a.Index} and n{b.Index} agree on index {anchor} term {la.TermAt(anchor)} but differ at index {i}";
        }

        return null;
    }

    private string Describe(RaftMessage m)
    {
        string name = m.GetType().Name;
        string detail = m switch
        {
            AppendEntries ae => $" prev {ae.PrevLogIndex}/{ae.PrevLogTerm} n={ae.Entries.Count} commit {ae.Commit}",
            AppendResponse ar => $" reject={ar.Reject} match {ar.MatchIndex} hint {ar.RejectHint}/{ar.RejectHintTerm}",
            VoteRequest vr => $" pre={vr.PreVote} last {vr.LastLogIndex}/{vr.LastLogTerm}",
            VoteResponse vp => $" pre={vp.PreVote} reject={vp.Reject}",
            Heartbeat hb => $" commit {hb.Commit} ctx {hb.ReadContext}",
            InstallSnapshot snap => $" index {snap.Meta.Index} offset {snap.Offset} len {snap.Data.Length} done={snap.Done}",
            InstallSnapshotResponse sr => $" index {sr.SnapshotIndex} next {sr.NextOffset} reject={sr.Reject}",
            _ => "",
        };
        return $"{name} n{IndexOf(m.From)}->n{IndexOf(m.To)} term {m.Term}{detail}";
    }

    /// <summary>Serializes the applied normal entries: count, then (u64 index, i32 length, bytes) per entry.</summary>
    private static byte[] SerializeStateMachine(List<(ulong Index, byte[] Data)> entries)
    {
        int size = 4 + entries.Sum(e => 12 + e.Data.Length);
        var buffer = new byte[size];
        Span<byte> span = buffer;
        BinaryPrimitives.WriteInt32LittleEndian(span, entries.Count);
        int pos = 4;
        foreach ((ulong index, byte[] data) in entries)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span[pos..], index);
            BinaryPrimitives.WriteInt32LittleEndian(span[(pos + 8)..], data.Length);
            data.CopyTo(span[(pos + 12)..]);
            pos += 12 + data.Length;
        }

        return buffer;
    }

    private static List<(ulong Index, byte[] Data)> DeserializeStateMachine(byte[] data)
    {
        var result = new List<(ulong, byte[])>();
        if (data.Length == 0)
            return result;
        ReadOnlySpan<byte> span = data;
        int count = BinaryPrimitives.ReadInt32LittleEndian(span);
        int pos = 4;
        for (int i = 0; i < count; i++)
        {
            ulong index = BinaryPrimitives.ReadUInt64LittleEndian(span[pos..]);
            int length = BinaryPrimitives.ReadInt32LittleEndian(span[(pos + 8)..]);
            result.Add((index, span.Slice(pos + 12, length).ToArray()));
            pos += 12 + length;
        }

        if (pos != data.Length)
            throw new InvalidOperationException("Corrupt simulated state-machine image.");
        return result;
    }

    private sealed record Envelope(long DeliverAt, long Seq, int From, int To, RaftMessage Message);

    /// <param name="Term">Entry term (null when only known from a snapshot image).</param>
    /// <param name="Kind">Entry kind.</param>
    /// <param name="Data">Entry payload.</param>
    /// <param name="FirstNode">First node that applied it.</param>
    /// <param name="CommitTermBound">Upper bound of the term in which it was committed (highest term observed when first applied).</param>
    private sealed record AppliedRecord(ulong? Term, RaftEntryKind Kind, byte[] Data, int FirstNode, ulong CommitTermBound);

    private sealed class SimHost(int index, NodeId id, RaftConfState baseConf)
    {
        public int Index { get; } = index;
        public NodeId Id { get; } = id;
        public RaftConfState BaseConf { get; } = baseConf;

        // Durable state.
        public InMemoryRaftLog Log { get; } = new();
        public RaftHardState Hard { get; set; }
        public byte[] SnapshotData { get; set; } = [];

        // Volatile state.
        public RaftNode Raft { get; set; } = null!;
        public bool Up { get; set; }
        public int Incarnation { get; set; }
        public List<(ulong Index, byte[] Data)> StateMachine { get; } = [];
        public ulong AppliedIndex { get; set; }
        public RaftConfState AppliedConf { get; set; } = baseConf;
        public List<SimReadState> Reads { get; } = [];
        public Dictionary<ulong, ulong> ReadFloors { get; } = [];
        public RaftCrashPoint? ArmedCrash { get; set; }
        public ulong LastCommit { get; set; }
        public RaftRole LastRole { get; set; }
        public ulong LastTerm { get; set; }
    }
}
