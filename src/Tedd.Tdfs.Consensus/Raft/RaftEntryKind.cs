namespace Tedd.Tdfs.Consensus;

/// <summary>Kind of a Raft log entry.</summary>
public enum RaftEntryKind : byte
{
    /// <summary>Application command for the replicated state machine.</summary>
    Normal,

    /// <summary>Membership change (<see cref="RaftConfChange"/> payload).</summary>
    ConfChange,

    /// <summary>Empty entry appended by a new leader to commit entries from earlier terms.</summary>
    Noop,
}
