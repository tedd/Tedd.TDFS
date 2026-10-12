namespace Tedd.Tdfs.Storage;

/// <summary>Handle to a file of a <see cref="SimulatedMedium"/>. Invalidated by <see cref="SimulatedMedium.Crash"/>.</summary>
internal sealed class SimulatedStorageFile : IStorageFile
{
    internal SimulatedStorageFile(SimulatedMedium medium, SimulatedNode node, string path, long generation)
    {
        Medium = medium;
        Node = node;
        Path = path;
        Generation = generation;
    }

    internal SimulatedMedium Medium { get; }

    internal SimulatedNode Node { get; }

    internal long Generation { get; }

    internal bool IsDisposed { get; set; }

    /// <inheritdoc />
    public string Path { get; }

    /// <inheritdoc />
    public long Length => Medium.GetLength(this);

    /// <inheritdoc />
    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) => Medium.ReadAsync(this, offset, buffer, ct);

    /// <inheritdoc />
    public ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default) => Medium.WriteAsync(this, offset, data, ct);

    /// <inheritdoc />
    public ValueTask FlushAsync(CancellationToken ct = default) => Medium.FlushFileAsync(this, ct);

    /// <inheritdoc />
    public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => Medium.SetLengthAsync(this, length, ct);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Medium.Close(this);
        return default;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Medium.Name}:{Path}";
}
