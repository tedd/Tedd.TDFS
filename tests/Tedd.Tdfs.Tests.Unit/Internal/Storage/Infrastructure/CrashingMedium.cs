using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>Thrown by <see cref="CrashingMedium"/> when the simulated power loss happens (the "process" stops here).</summary>
public sealed class SimulatedCrashException : Exception
{
    public SimulatedCrashException()
        : base("Simulated crash.")
    {
    }

    public SimulatedCrashException(string message)
        : base(message)
    {
    }

    public SimulatedCrashException(string message, Exception inner)
        : base(message, inner)
    {
    }
}

/// <summary>
/// Wraps a <see cref="SimulatedMedium"/> and crashes it just before the N-th mutating I/O step (write, flush,
/// length change, open, rename, delete, directory creation or directory flush). After the crash every call throws
/// <see cref="SimulatedCrashException"/>, so the component under test cannot do anything further.
/// </summary>
public sealed class CrashingMedium : IStorageMedium
{
    private readonly SimulatedMedium _inner;

    public CrashingMedium(SimulatedMedium inner, int crashAtStep, CrashMode mode)
    {
        _inner = inner;
        CrashAtStep = crashAtStep;
        Mode = mode;
    }

    /// <summary>Zero-based mutating step before which the crash happens (-1 = never).</summary>
    public int CrashAtStep { get; }

    public CrashMode Mode { get; }

    /// <summary>Mutating steps executed so far.</summary>
    public int Steps { get; private set; }

    public bool Crashed { get; private set; }

    /// <summary>Description of the step at which the crash happened.</summary>
    public string? CrashedAt { get; private set; }

    public string Name => _inner.Name;

    public long CapacityBytes => _inner.CapacityBytes;

    public long UsedBytes
    {
        get
        {
            ThrowIfCrashed();
            return _inner.UsedBytes;
        }
    }

    internal void Step(string what)
    {
        ThrowIfCrashed();
        if (Steps == CrashAtStep)
        {
            Crashed = true;
            CrashedAt = what;
            _inner.Crash(Mode);
            throw new SimulatedCrashException($"Simulated crash before step {Steps}: {what}.");
        }

        Steps++;
    }

    internal void ThrowIfCrashed()
    {
        if (Crashed)
            throw new SimulatedCrashException("The medium crashed earlier.");
    }

    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        Step($"open {path} ({mode})");
        IStorageFile file = await _inner.OpenAsync(path, mode, ct);
        return new CrashingFile(this, file);
    }

    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ThrowIfCrashed();
        return _inner.ExistsAsync(path, ct);
    }

    public ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        Step($"delete {path}");
        return _inner.DeleteAsync(path, ct);
    }

    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        Step($"rename {from} -> {to}");
        return _inner.RenameAsync(from, to, overwrite, ct);
    }

    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfCrashed();
        return _inner.ListAsync(directory, ct);
    }

    public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        Step($"mkdir {directory}");
        return _inner.CreateDirectoryAsync(directory, ct);
    }

    public ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        Step($"flush dir {directory}");
        return _inner.FlushDirectoryAsync(directory, ct);
    }

    private sealed class CrashingFile(CrashingMedium medium, IStorageFile inner) : IStorageFile
    {
        public string Path => inner.Path;

        public long Length
        {
            get
            {
                medium.ThrowIfCrashed();
                return inner.Length;
            }
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
        {
            medium.ThrowIfCrashed();
            return inner.ReadAsync(offset, buffer, ct);
        }

        public ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            medium.Step($"write {data.Length} bytes at {offset} of {Path}");
            return inner.WriteAsync(offset, data, ct);
        }

        public ValueTask FlushAsync(CancellationToken ct = default)
        {
            medium.Step($"flush {Path}");
            return inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
        {
            medium.Step($"set length {length} of {Path}");
            return inner.SetLengthAsync(length, ct);
        }

        public ValueTask DisposeAsync() => medium.Crashed ? default : inner.DisposeAsync();
    }
}
