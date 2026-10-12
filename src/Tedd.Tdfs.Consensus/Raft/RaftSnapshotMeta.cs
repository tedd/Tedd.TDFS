namespace Tedd.Tdfs.Consensus;

/// <summary>Metadata of a state-machine snapshot that replaces the log prefix up to <paramref name="Index"/>.</summary>
/// <param name="Index">Last log index included in the snapshot (0 = no snapshot).</param>
/// <param name="Term">Term of the entry at <paramref name="Index"/>.</param>
/// <param name="ConfState">Membership as of <paramref name="Index"/>.</param>
public sealed record RaftSnapshotMeta(ulong Index, ulong Term, RaftConfState ConfState)
{
    /// <summary>The empty snapshot (index 0, term 0, no members).</summary>
    public static RaftSnapshotMeta Empty { get; } = new(0, 0, RaftConfState.Empty);
}
