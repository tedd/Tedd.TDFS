namespace Tedd.Tdfs.Storage;

/// <summary>
/// Append-only journal of CRC-protected records in segment files (used by the Raft log store and backups).
/// Opening replays the records, truncates a torn tail and rejects corrupt interior records.
/// </summary>
public sealed class Journal : IAsyncDisposable
{
    private Journal()
    {
    }

    /// <summary>Records replayed at open time (index, payload).</summary>
    public IReadOnlyList<JournalRecord> Records => throw new NotImplementedException("Journal.Records: implemented in a later phase");

    /// <summary>Opens (or creates) the journal in <paramref name="directory"/> and replays it. A corrupt non-tail record throws <see cref="DfsIntegrityException"/>.</summary>
    public static ValueTask<Journal> OpenAsync(IStorageMedium medium, string directory, JournalOptions options, CancellationToken ct = default)
        => throw new NotImplementedException("Journal.OpenAsync: implemented in a later phase");

    /// <summary>Appends a record and returns its index. Not durable until <see cref="FlushAsync"/>.</summary>
    public ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default)
        => throw new NotImplementedException("Journal.AppendAsync: implemented in a later phase");

    /// <summary>Durability barrier for every appended record.</summary>
    public ValueTask FlushAsync(CancellationToken ct = default)
        => throw new NotImplementedException("Journal.FlushAsync: implemented in a later phase");

    /// <summary>Removes every record with an index greater than <paramref name="index"/>.</summary>
    public ValueTask TruncateAfterAsync(long index, CancellationToken ct = default)
        => throw new NotImplementedException("Journal.TruncateAfterAsync: implemented in a later phase");

    /// <summary>Drops records with an index lower than <paramref name="index"/> (segment granularity).</summary>
    public ValueTask DropBeforeAsync(long index, CancellationToken ct = default)
        => throw new NotImplementedException("Journal.DropBeforeAsync: implemented in a later phase");

    /// <summary>Closes the journal files.</summary>
    public ValueTask DisposeAsync()
        => throw new NotImplementedException("Journal.DisposeAsync: implemented in a later phase");
}
