using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>
/// Models a file system that does not persist unflushed unlinks in order: deletes are deferred until the directory is
/// flushed, and a crash (before mutating step <see cref="CrashAtStep"/>) persists only the newest deferred delete of
/// each directory, dropping the older ones. A component that deletes several files and relies on them disappearing in
/// the order it deleted them (rather than flushing the directory in between) is exposed by this medium.
/// </summary>
public sealed class UnorderedUnlinkMedium(SimulatedMedium inner, int crashAtStep) : IStorageMedium
{
    private readonly List<string> _deferred = [];

    public SimulatedMedium Inner { get; } = inner;

    public int CrashAtStep { get; } = crashAtStep;

    public int Steps { get; private set; }

    public bool Crashed { get; private set; }

    public string Name => Inner.Name;

    public long CapacityBytes => Inner.CapacityBytes;

    public long UsedBytes => Inner.UsedBytes;

    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        await StepAsync();
        if (_deferred.Contains(StoragePath.Normalize(path)))
            throw new FileNotFoundException($"'{path}' was deleted.", path);
        return new File(this, await Inner.OpenAsync(path, mode, ct));
    }

    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ThrowIfCrashed();
        return _deferred.Contains(StoragePath.Normalize(path)) ? new ValueTask<bool>(false) : Inner.ExistsAsync(path, ct);
    }

    public async ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        await StepAsync();
        string p = StoragePath.Normalize(path);
        if (await Inner.ExistsAsync(p, ct) && !_deferred.Contains(p))
            _deferred.Add(p);
    }

    public async ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        await StepAsync();
        await Inner.RenameAsync(from, to, overwrite, ct);
    }

    public async ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfCrashed();
        string dir = StoragePath.Normalize(directory);
        IReadOnlyList<string> names = await Inner.ListAsync(directory, ct);
        return [.. names.Where(n => !_deferred.Contains(StoragePath.Combine(dir, n)))];
    }

    public async ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        await StepAsync();
        await Inner.CreateDirectoryAsync(directory, ct);
    }

    public async ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        await StepAsync();
        string dir = StoragePath.Normalize(directory);
        foreach (string p in _deferred.Where(p => StoragePath.GetParent(p) == dir).ToList())
        {
            await Inner.DeleteAsync(p, ct);
            _deferred.Remove(p);
        }

        await Inner.FlushDirectoryAsync(directory, ct);
    }

    private async ValueTask StepAsync()
    {
        ThrowIfCrashed();
        if (Steps == CrashAtStep)
        {
            Crashed = true;
            foreach (IGrouping<string, string> group in _deferred.GroupBy(StoragePath.GetParent))
            {
                // Only the newest unflushed unlink of the directory reached the device.
                await Inner.DeleteAsync(group.Last());
                await Inner.FlushDirectoryAsync(group.Key);
            }

            _deferred.Clear();
            Inner.Crash(CrashMode.LoseUnflushed);
            throw new SimulatedCrashException($"Simulated crash before step {Steps}.");
        }

        Steps++;
    }

    private void ThrowIfCrashed()
    {
        if (Crashed)
            throw new SimulatedCrashException("The medium crashed earlier.");
    }

    private sealed class File(UnorderedUnlinkMedium medium, IStorageFile inner) : IStorageFile
    {
        public string Path => inner.Path;

        public long Length => inner.Length;

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) => inner.ReadAsync(offset, buffer, ct);

        public async ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            await medium.StepAsync();
            return await inner.WriteAsync(offset, data, ct);
        }

        public async ValueTask FlushAsync(CancellationToken ct = default)
        {
            await medium.StepAsync();
            await inner.FlushAsync(ct);
        }

        public async ValueTask SetLengthAsync(long length, CancellationToken ct = default)
        {
            await medium.StepAsync();
            await inner.SetLengthAsync(length, ct);
        }

        public ValueTask DisposeAsync() => medium.Crashed ? default : inner.DisposeAsync();
    }
}
