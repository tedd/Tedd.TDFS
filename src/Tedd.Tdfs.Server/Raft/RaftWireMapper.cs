using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// Maps <see cref="Consensus"/> message records to and from the neutral <see cref="RaftWireMessage"/> (PROTOCOL.md §8.7,
/// field use per kind documented on <see cref="RaftWireMessage"/>).
/// </summary>
internal static class RaftWireMapper
{
    private static readonly IReadOnlyList<RaftWireEntry> NoEntries = [];

    /// <summary>Converts a consensus message to its wire form.</summary>
    public static RaftWireMessage ToWire(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return message switch
        {
            VoteRequest m => Wire(RaftWireKind.VoteRequest, m) with
            {
                LogIndex = m.LastLogIndex, LogTerm = m.LastLogTerm, PreVote = m.PreVote, LeadershipTransfer = m.LeadershipTransfer,
            },
            VoteResponse m => Wire(RaftWireKind.VoteResponse, m) with { PreVote = m.PreVote, Reject = m.Reject },
            AppendEntries m => Wire(RaftWireKind.AppendEntries, m) with
            {
                LogIndex = m.PrevLogIndex, LogTerm = m.PrevLogTerm, Entries = ToWire(m.Entries), Commit = m.Commit, ReadContext = m.ReadContext,
            },
            AppendResponse m => Wire(RaftWireKind.AppendResponse, m) with
            {
                Reject = m.Reject, LogIndex = m.MatchIndex, RejectHint = m.RejectHint, RejectHintTerm = m.RejectHintTerm, ReadContext = m.ReadContext,
            },
            InstallSnapshot m => Wire(RaftWireKind.InstallSnapshot, m) with
            {
                Snapshot = ToWire(m.Meta), Offset = m.Offset, Data = m.Data, Done = m.Done,
            },
            InstallSnapshotResponse m => Wire(RaftWireKind.InstallSnapshotResponse, m) with
            {
                LogIndex = m.SnapshotIndex, Offset = m.NextOffset, Reject = m.Reject,
            },
            TimeoutNow m => Wire(RaftWireKind.TimeoutNow, m),
            Heartbeat m => Wire(RaftWireKind.Heartbeat, m) with { Commit = m.Commit, ReadContext = m.ReadContext },
            HeartbeatResponse m => Wire(RaftWireKind.HeartbeatResponse, m) with { ReadContext = m.ReadContext },
            _ => throw new ArgumentException($"Unknown Raft message type {message.GetType().Name}.", nameof(message)),
        };
    }

    /// <summary>
    /// Converts a wire message to its consensus form; null when it is not well-formed for its kind (unknown entry kind,
    /// missing snapshot metadata). Structural Raft validation is left to <see cref="RaftNode.Step"/>.
    /// </summary>
    public static RaftMessage? FromWire(RaftWireMessage m)
    {
        ArgumentNullException.ThrowIfNull(m);
        switch (m.Kind)
        {
            case RaftWireKind.VoteRequest:
                return new VoteRequest(m.From, m.To, m.Term, m.PreVote, m.LogIndex, m.LogTerm, m.LeadershipTransfer);
            case RaftWireKind.VoteResponse:
                return new VoteResponse(m.From, m.To, m.Term, m.PreVote, m.Reject);
            case RaftWireKind.AppendEntries:
            {
                var entries = new RaftEntry[m.Entries.Count];
                for (int i = 0; i < entries.Length; i++)
                {
                    RaftWireEntry e = m.Entries[i];
                    var kind = (RaftEntryKind)e.Kind;
                    if (kind is not (RaftEntryKind.Normal or RaftEntryKind.ConfChange or RaftEntryKind.Noop))
                        return null;
                    entries[i] = new RaftEntry(e.Index, e.Term, kind, e.Data);
                }

                return new AppendEntries(m.From, m.To, m.Term, m.LogIndex, m.LogTerm, entries, m.Commit, m.ReadContext);
            }

            case RaftWireKind.AppendResponse:
                return new AppendResponse(m.From, m.To, m.Term, m.Reject, m.LogIndex, m.RejectHint, m.ReadContext, m.RejectHintTerm);
            case RaftWireKind.InstallSnapshot:
                return m.Snapshot is null ? null : new InstallSnapshot(m.From, m.To, m.Term, FromWire(m.Snapshot), m.Offset, m.Data, m.Done);
            case RaftWireKind.InstallSnapshotResponse:
                return new InstallSnapshotResponse(m.From, m.To, m.Term, m.LogIndex, m.Offset, m.Reject);
            case RaftWireKind.TimeoutNow:
                return new TimeoutNow(m.From, m.To, m.Term);
            case RaftWireKind.Heartbeat:
                return new Heartbeat(m.From, m.To, m.Term, m.Commit, m.ReadContext);
            case RaftWireKind.HeartbeatResponse:
                return new HeartbeatResponse(m.From, m.To, m.Term, m.ReadContext);
            default:
                return null;
        }
    }

    /// <summary>Converts snapshot metadata to its wire form.</summary>
    public static RaftWireSnapshotMeta ToWire(RaftSnapshotMeta meta)
        => new(meta.Index, meta.Term, meta.ConfState.Voters.ToArray(), meta.ConfState.Learners.ToArray());

    /// <summary>Converts wire snapshot metadata to its consensus form.</summary>
    public static RaftSnapshotMeta FromWire(RaftWireSnapshotMeta meta)
        => new(meta.Index, meta.Term, new RaftConfState(meta.Voters.ToArray(), meta.Learners.ToArray()));

    private static IReadOnlyList<RaftWireEntry> ToWire(IReadOnlyList<RaftEntry> entries)
    {
        if (entries.Count == 0)
            return NoEntries;
        var result = new RaftWireEntry[entries.Count];
        for (int i = 0; i < result.Length; i++)
        {
            RaftEntry e = entries[i];
            result[i] = new RaftWireEntry(e.Index, e.Term, (byte)e.Kind, e.Data);
        }

        return result;
    }

    private static RaftWireMessage Wire(RaftWireKind kind, RaftMessage m)
        => new(kind, m.From, m.To, m.Term, 0, 0, 0, 0, 0, 0, false, false, false, NoEntries, null, 0, ReadOnlyMemory<byte>.Empty, false);
}
