using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>
/// Models the write-back error semantics of real kernels: when a file flush fails, the dirty pages written since the
/// previous flush are dropped (their bytes read back as zeroes) and the error is reported to that one flush only; a
/// later flush of the same file succeeds. A component that acknowledges a write because "its" flush succeeded, although
/// another flush of the same file failed after the write, loses acknowledged data under these semantics (DSK-004).
/// </summary>
public sealed class LossyFlushMedium(SimulatedMedium inner) : IStorageMedium
{
    private readonly Dictionary<string, List<(long Offset, int Length)>> _dirty = new(StringComparer.Ordinal);

    public SimulatedMedium Inner { get; } = inner;

    /// <summary>Number of further file flushes that fail (dropping the file's dirty bytes).</summary>
    public int FailNextFlushes { get; set; }

    /// <summary>Runs once before the next file flush whose path matches (then clears itself).</summary>
    public (Func<string, bool> Match, Func<Task> Action)? BeforeFlush { get; set; }

    public string Name => Inner.Name;

    public long CapacityBytes => Inner.CapacityBytes;

    public long UsedBytes => Inner.UsedBytes;

    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
        => new LossyFile(this, await Inner.OpenAsync(path, mode, ct));

    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default) => Inner.ExistsAsync(path, ct);

    public ValueTask DeleteAsync(string path, CancellationToken ct = default) => Inner.DeleteAsync(path, ct);

    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default) => Inner.RenameAsync(from, to, overwrite, ct);

    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default) => Inner.ListAsync(directory, ct);

    public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default) => Inner.CreateDirectoryAsync(directory, ct);

    public ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default) => Inner.FlushDirectoryAsync(directory, ct);

    private List<(long Offset, int Length)> DirtyOf(string path)
    {
        if (!_dirty.TryGetValue(path, out List<(long Offset, int Length)>? list))
            _dirty[path] = list = [];
        return list;
    }

    private sealed class LossyFile(LossyFlushMedium medium, IStorageFile inner) : IStorageFile
    {
        public string Path => inner.Path;

        public long Length => inner.Length;

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) => inner.ReadAsync(offset, buffer, ct);

        public async ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            int n = await inner.WriteAsync(offset, data, ct);
            medium.DirtyOf(Path).Add((offset, n));
            return n;
        }

        public async ValueTask FlushAsync(CancellationToken ct = default)
        {
            if (medium.BeforeFlush is { } hook && hook.Match(Path))
            {
                medium.BeforeFlush = null;
                await hook.Action();
            }

            List<(long Offset, int Length)> dirty = medium.DirtyOf(Path);
            if (medium.FailNextFlushes > 0)
            {
                medium.FailNextFlushes--;

                // The device lost the pages; the cache marks them clean, so they read back as what the medium holds.
                foreach ((long offset, int length) in dirty)
                {
                    long end = Math.Min(offset + length, inner.Length);
                    if (end > offset)
                        await inner.WriteAsync(offset, new byte[end - offset], ct);
                }

                dirty.Clear();
                await inner.FlushAsync(ct);
                throw new IOException($"I/O error during write-back of '{Path}'.");
            }

            await inner.FlushAsync(ct);
            dirty.Clear();
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => inner.SetLengthAsync(length, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
