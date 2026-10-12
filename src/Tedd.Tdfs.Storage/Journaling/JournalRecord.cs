namespace Tedd.Tdfs.Storage;

/// <summary>A record replayed from a <see cref="Journal"/>.</summary>
/// <param name="Index">Monotonic record index assigned at append time.</param>
/// <param name="Payload">Record payload (decrypted).</param>
public sealed record JournalRecord(long Index, ReadOnlyMemory<byte> Payload);
