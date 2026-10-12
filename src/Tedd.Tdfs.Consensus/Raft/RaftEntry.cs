namespace Tedd.Tdfs.Consensus;

/// <summary>A Raft log entry.</summary>
/// <param name="Index">Log index (first entry is 1).</param>
/// <param name="Term">Term in which the entry was proposed.</param>
/// <param name="Kind">Entry kind.</param>
/// <param name="Data">Payload (command bytes or encoded <see cref="RaftConfChange"/>).</param>
public sealed record RaftEntry(ulong Index, ulong Term, RaftEntryKind Kind, ReadOnlyMemory<byte> Data);
