using Microsoft.Win32.SafeHandles;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// A real-directory <see cref="IStorageMedium"/> (<see cref="StorageKind.Disk"/>) with the fault switches of
/// <see cref="SimulatedMediumFaults"/> and crash fidelity: it keeps an undo log of every write, length change and namespace
/// change since the last durability barrier, so <see cref="Crash"/> can reset the directory to exactly what a power loss in
/// the chosen <see cref="CrashMode"/> would leave (the operating system's page cache survives a process kill, a power cut
/// does not). Namespace changes become durable by prefix on a directory flush, like a journaling file system and like
/// <see cref="SimulatedMedium"/>. Deleted and overwritten files are parked outside the root until the deletion is durable.
/// </summary>
/// <remarks>
/// Used only in realtime mode (deterministic mode requires simulated storage). Every operation runs synchronously under
/// one lock (blocking file I/O on the caller's thread), so a crash requested from another thread waits for an operation in
/// progress and then invalidates every handle.
/// </remarks>
internal sealed class FaultingFileSystemMedium : IStorageMedium
{
    private readonly object _gate = new();
    private readonly string _root;
    private readonly string _trash;
    private readonly IRandomSource _random;
    private readonly long _capacity;
    private readonly Dictionary<string, FileState> _states = new(StringComparer.Ordinal);
    private readonly List<NamespaceOp> _pendingNamespace = [];
    private readonly HashSet<FsFile> _open = new(ReferenceEqualityComparer.Instance);
    private long _used = -1;
    private long _generation;
    private long _sequence;
    private long _trashCounter;
    private SimulatedLossReport _lastCrashLoss = SimulatedLossReport.None;

    /// <summary>Creates the medium over <paramref name="rootDirectory"/> (created if missing); parked files live in <paramref name="trashDirectory"/>.</summary>
    public FaultingFileSystemMedium(string rootDirectory, string trashDirectory, long capacityBytes, IRandomSource random, TimeProvider timeProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(rootDirectory);
        ArgumentException.ThrowIfNullOrEmpty(trashDirectory);
        _root = System.IO.Path.GetFullPath(rootDirectory);
        _trash = System.IO.Path.GetFullPath(trashDirectory);
        Directory.CreateDirectory(_root);
        Directory.CreateDirectory(_trash);
        _capacity = capacityBytes;
        _random = random;
        TimeProvider = timeProvider;
        Name = _root;
    }

    /// <summary>Root directory of the medium (full path).</summary>
    public string RootDirectory => _root;

    /// <summary>Fault switches (same semantics as for <see cref="SimulatedMedium"/>).</summary>
    public SimulatedMediumFaults Faults { get; } = new();

    /// <summary>Clock of <see cref="SimulatedMediumFaults.IoDelay"/>.</summary>
    public TimeProvider TimeProvider { get; set; }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public long CapacityBytes => Faults.CapacityOverride ?? _capacity;

    /// <inheritdoc />
    public long UsedBytes
    {
        get
        {
            lock (_gate)
                return UsedLocked();
        }
    }

    /// <summary>Acknowledged-but-volatile changes the last crash discarded (dishonest flushes).</summary>
    public SimulatedLossReport LastCrashLoss
    {
        get
        {
            lock (_gate)
                return _lastCrashLoss;
        }
    }

    /// <summary>Changes a dishonest flush reported durable that are still volatile.</summary>
    public SimulatedLossReport GetAcknowledgedVolatileState()
    {
        lock (_gate)
        {
            var paths = new SortedSet<string>(StringComparer.Ordinal);
            int writes = 0;
            long bytes = 0;
            foreach (FileState state in _states.Values)
            {
                foreach (PendingWrite w in state.Pending)
                {
                    if (!w.Acknowledged)
                        continue;
                    writes++;
                    bytes += w.Data?.Length ?? 0;
                    paths.Add(state.Path);
                }
            }

            int ns = 0;
            foreach (NamespaceOp op in _pendingNamespace)
            {
                if (!op.Acknowledged)
                    continue;
                ns++;
                paths.Add(op.Path);
            }

            return writes == 0 && ns == 0 ? SimulatedLossReport.None : new SimulatedLossReport(writes, bytes, ns, [.. paths]);
        }
    }

    /// <summary>Every file under the root (medium-relative, '/' separated, ordinal).</summary>
    public IReadOnlyList<string> AllFiles()
    {
        lock (_gate)
        {
            if (!Directory.Exists(_root))
                return [];
            var files = new List<string>();
            foreach (string file in Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories))
                files.Add(System.IO.Path.GetRelativePath(_root, file).Replace(System.IO.Path.DirectorySeparatorChar, '/'));
            files.Sort(StringComparer.Ordinal);
            return files;
        }
    }

    /// <summary>The bytes of a file as they are on disk now.</summary>
    public byte[] ReadRaw(string path)
    {
        lock (_gate)
            return File.ReadAllBytes(Full(StoragePath.Normalize(path)));
    }

    /// <summary>XORs one stored byte of a file (media corruption).</summary>
    public void CorruptByte(string path, long offset, byte xorMask)
    {
        lock (_gate)
        {
            using SafeFileHandle handle = File.OpenHandle(Full(StoragePath.Normalize(path)), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            long length = RandomAccess.GetLength(handle);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, length);
            Span<byte> one = stackalloc byte[1];
            RandomAccess.Read(handle, one, offset);
            one[0] ^= xorMask;
            RandomAccess.Write(handle, one, offset);
            RandomAccess.FlushToDisk(handle);
        }
    }

    /// <summary>
    /// Power loss: decides what unflushed state survives according to <paramref name="mode"/>, rolls the directory back to it
    /// and invalidates every open handle.
    /// </summary>
    public void Crash(CrashMode mode)
    {
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode));
        lock (_gate)
        {
            _generation++;
            foreach (FsFile file in _open)
                file.Invalidate();
            _open.Clear();

            var lostPaths = new SortedSet<string>(StringComparer.Ordinal);
            int lostWrites = 0;
            long lostBytes = 0;
            int lostNamespace = 0;

            // 1. Namespace: a prefix of the unflushed changes survives; the rest is undone newest first.
            int count = _pendingNamespace.Count;
            int keep = mode switch
            {
                CrashMode.LoseUnflushed => 0,
                CrashMode.RandomSubset => _random.Next(count + 1),
                _ => count,
            };
            for (int i = count - 1; i >= keep; i--)
            {
                NamespaceOp op = _pendingNamespace[i];
                if (op.Acknowledged)
                {
                    lostNamespace++;
                    lostPaths.Add(op.Path);
                }

                UndoNamespace(op);
            }

            for (int i = 0; i < keep; i++)
                MakeNamespaceDurable(_pendingNamespace[i]);
            _pendingNamespace.Clear();

            // 2. File data of files that still exist (or are parked): per mode.
            long tornSequence = -1;
            if (mode == CrashMode.TearLastWrite)
            {
                foreach (FileState state in _states.Values)
                {
                    foreach (PendingWrite w in state.Pending)
                    {
                        if (w.Data is not null && w.Sequence > tornSequence)
                            tornSequence = w.Sequence;
                    }
                }
            }

            foreach (FileState state in _states.Values.OrderBy(s => s.Path, StringComparer.Ordinal))
            {
                // A file that no longer exists was removed by an undone creation (counted as a lost namespace change) or by
                // a durable deletion; its unflushed bytes are gone either way.
                if (state.Pending.Count == 0 || !File.Exists(state.Path))
                    continue;

                List<PendingWrite> lost = ApplyCrash(state, mode, tornSequence);
                CountLost(state, lost, ref lostWrites, ref lostBytes, lostPaths);
            }

            _states.Clear();
            _used = -1;
            _lastCrashLoss = lostWrites == 0 && lostNamespace == 0
                ? SimulatedLossReport.None
                : new SimulatedLossReport(lostWrites, lostBytes, lostNamespace, [.. lostPaths]);
        }
    }

    /// <summary>Removes every file and parked file (a replaced, empty disk).</summary>
    public void Wipe()
    {
        lock (_gate)
        {
            _generation++;
            foreach (FsFile file in _open)
                file.Invalidate();
            _open.Clear();
            _states.Clear();
            _pendingNamespace.Clear();
            if (Directory.Exists(_root))
                Directory.Delete(_root, recursive: true);
            if (Directory.Exists(_trash))
                Directory.Delete(_trash, recursive: true);
            Directory.CreateDirectory(_root);
            Directory.CreateDirectory(_trash);
            _used = 0;
        }
    }

    /// <summary>Replaces the content of this medium with a copy of <paramref name="source"/>'s directory (a disk clone).</summary>
    public void CopyFrom(FaultingFileSystemMedium source)
    {
        ArgumentNullException.ThrowIfNull(source);
        Wipe();
        lock (source._gate)
        {
            lock (_gate)
            {
                foreach (string dir in Directory.EnumerateDirectories(source._root, "*", SearchOption.AllDirectories))
                    Directory.CreateDirectory(System.IO.Path.Combine(_root, System.IO.Path.GetRelativePath(source._root, dir)));
                foreach (string file in Directory.EnumerateFiles(source._root, "*", SearchOption.AllDirectories))
                    File.Copy(file, System.IO.Path.Combine(_root, System.IO.Path.GetRelativePath(source._root, file)), overwrite: true);
                _used = -1;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string p = FilePath(path);
        await DelayAsync(p, ct);
        lock (_gate)
        {
            string full = Full(p);
            if (Directory.Exists(full))
                throw new UnauthorizedAccessException($"Access to the path '{p}' is denied: it is a directory.");
            if (!Directory.Exists(System.IO.Path.GetDirectoryName(full)!))
                throw new DirectoryNotFoundException($"Could not find a part of the path '{p}'.");
            bool exists = File.Exists(full);
            switch (mode)
            {
                case StorageOpenMode.CreateNew:
                    if (exists)
                        throw StorageErrors.FileExists(p);
                    CreateFileLocked(p, full);
                    break;
                case StorageOpenMode.OpenExisting:
                    if (!exists)
                        throw new FileNotFoundException($"Could not find file '{p}'.", p);
                    break;
                case StorageOpenMode.OpenOrCreate:
                    if (!exists)
                        CreateFileLocked(p, full);
                    break;
                case StorageOpenMode.Truncate:
                    if (!exists)
                    {
                        CreateFileLocked(p, full);
                    }
                    else
                    {
                        ThrowIfReadOnly(p);
                        FileState truncated = StateOf(full);
                        using SafeFileHandle h = File.OpenHandle(full, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                        RecordAndSetLength(truncated, h, 0);
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode));
            }

            SafeFileHandle handle = File.OpenHandle(full, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            var file = new FsFile(this, handle, p, StateOf(full), _generation);
            _open.Add(file);
            return file;
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string full = Full(StoragePath.Normalize(path));
        lock (_gate)
            return new ValueTask<bool>(File.Exists(full) || Directory.Exists(full));
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string p = StoragePath.Normalize(path);
        if (p.Length == 0)
            throw new ArgumentException("The root directory cannot be deleted.", nameof(path));
        lock (_gate)
        {
            string full = Full(p);
            if (File.Exists(full))
            {
                ThrowIfReadOnly(p);
                long length = new FileInfo(full).Length;
                string parked = Park(full);
                _pendingNamespace.Add(new NamespaceOp(OpKind.DeleteFile, p, null, parked));
                AdjustUsed(-length);
            }
            else if (Directory.Exists(full))
            {
                if (Directory.EnumerateFileSystemEntries(full).Any())
                    throw new IOException($"The directory '{p}' is not empty.");
                ThrowIfReadOnly(p);
                Directory.Delete(full);
                _pendingNamespace.Add(new NamespaceOp(OpKind.DeleteDirectory, p, null, null));
            }
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string f = FilePath(from);
        string t = FilePath(to);
        lock (_gate)
        {
            string fromFull = Full(f);
            string toFull = Full(t);
            if (!File.Exists(fromFull))
            {
                if (Directory.Exists(fromFull))
                    throw new IOException($"Renaming directories ('{f}') is not supported by this medium.");
                throw new FileNotFoundException($"Could not find file '{f}'.", f);
            }

            if (string.Equals(f, t, StringComparison.Ordinal))
                return default;
            if (Directory.Exists(toFull))
                throw new IOException($"Cannot rename '{f}' over directory '{t}'.");
            if (!Directory.Exists(System.IO.Path.GetDirectoryName(toFull)!))
                throw new DirectoryNotFoundException($"Could not find a part of the path '{t}'.");
            if (!overwrite && File.Exists(toFull))
                throw StorageErrors.FileExists(t);
            ThrowIfReadOnly(f);
            ThrowIfReadOnly(t);

            string? parked = null;
            if (File.Exists(toFull))
            {
                AdjustUsed(-new FileInfo(toFull).Length);
                parked = Park(toFull);
            }

            File.Move(fromFull, toFull);
            Rekey(fromFull, toFull);
            _pendingNamespace.Add(new NamespaceOp(OpKind.Rename, f, t, parked));
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string full = Full(StoragePath.Normalize(directory));
        lock (_gate)
        {
            if (!Directory.Exists(full))
                throw new DirectoryNotFoundException($"Could not find directory '{directory}'.");
            var names = new List<string>();
            foreach (string entry in Directory.EnumerateFileSystemEntries(full))
                names.Add(System.IO.Path.GetFileName(entry));
            names.Sort(StringComparer.Ordinal);
            return new ValueTask<IReadOnlyList<string>>(names);
        }
    }

    /// <inheritdoc />
    public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string d = StoragePath.Normalize(directory);
        if (d.Length == 0)
            return default;
        lock (_gate)
        {
            string current = StoragePath.Root;
            foreach (string component in d.Split('/'))
            {
                current = current.Length == 0 ? component : current + "/" + component;
                string full = Full(current);
                if (File.Exists(full))
                    throw new IOException($"Cannot create directory '{d}': '{current}' is a file.");
                if (Directory.Exists(full))
                    continue;
                ThrowIfReadOnly(current);
                Directory.CreateDirectory(full);
                _pendingNamespace.Add(new NamespaceOp(OpKind.CreateDirectory, current, null, null));
            }
        }

        return default;
    }

    /// <inheritdoc />
    public async ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string d = StoragePath.Normalize(directory);
        await DelayAsync(d, ct);
        lock (_gate)
        {
            if (!Directory.Exists(Full(d)))
                throw new DirectoryNotFoundException($"Could not find directory '{directory}'.");
            int last = _pendingNamespace.FindLastIndex(op => op.Touches(d));
            if (last >= 0 && Faults.ReadOnly && MatchesFilter(d))
                throw StorageErrors.ReadOnlyDevice(d);
            if (MatchesFilter(d) && TryConsume(() => Faults.FailNextFlushes, v => Faults.FailNextFlushes = v))
                throw StorageErrors.DeviceError("directory flush", d);
            if (Faults.DishonestFlush)
            {
                for (int i = 0; i <= last; i++)
                    _pendingNamespace[i].Acknowledged = true;
                return;
            }

            for (int i = 0; i <= last; i++)
                MakeNamespaceDurable(_pendingNamespace[i]);
            if (last >= 0)
                _pendingNamespace.RemoveRange(0, last + 1);
            if (!OperatingSystem.IsWindows())
                DirectorySyncHelper.TryFsync(Full(d));
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"FaultingFileSystemMedium({_root})";

    // -------------------------------------------------------------------------------------------- file operations

    internal async ValueTask<int> ReadAsync(FsFile file, long offset, Memory<byte> buffer, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        await DelayAsync(file.Path, ct);
        lock (_gate)
        {
            file.EnsureValid(_generation);
            if (MatchesFilter(file.Path) && TryConsume(() => Faults.FailNextReads, v => Faults.FailNextReads = v))
                throw StorageErrors.DeviceError("read", file.Path);
            return RandomAccess.Read(file.Handle, buffer.Span, offset);
        }
    }

    internal async ValueTask<int> WriteAsync(FsFile file, long offset, ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        await DelayAsync(file.Path, ct);
        lock (_gate)
        {
            file.EnsureValid(_generation);
            ThrowIfReadOnly(file.Path);
            if (MatchesFilter(file.Path) && TryConsume(() => Faults.FailNextWrites, v => Faults.FailNextWrites = v))
                throw StorageErrors.DeviceError("write", file.Path);
            if (data.IsEmpty)
                return 0;
            int count = data.Length;
            if (MatchesFilter(file.Path) && TryConsume(() => Faults.ShortNextWrites, v => Faults.ShortNextWrites = v))
                count = (count + 1) / 2;

            long lengthBefore = RandomAccess.GetLength(file.Handle);
            long growth = Math.Max(0, offset + count - lengthBefore);
            if (growth > 0 && UsedLocked() + growth > CapacityBytes)
                throw StorageErrors.DiskFull(file.Path);

            ReadOnlySpan<byte> written = data.Span[..count];
            long preEnd = Math.Min(offset + count, lengthBefore);
            byte[] pre = preEnd > offset ? new byte[preEnd - offset] : [];
            if (pre.Length > 0)
                RandomAccess.Read(file.Handle, pre, offset);
            RandomAccess.Write(file.Handle, written, offset);
            file.State.Pending.Add(new PendingWrite(++_sequence, offset, written.ToArray(), -1, offset, pre, lengthBefore));
            AdjustUsed(growth);
            return count;
        }
    }

    internal ValueTask SetLengthAsync(FsFile file, long length, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        lock (_gate)
        {
            file.EnsureValid(_generation);
            ThrowIfReadOnly(file.Path);
            long current = RandomAccess.GetLength(file.Handle);
            if (length > current && UsedLocked() + (length - current) > CapacityBytes)
                throw StorageErrors.DiskFull(file.Path);
            RecordAndSetLength(file.State, file.Handle, length);
        }

        return default;
    }

    internal async ValueTask FlushAsync(FsFile file, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await DelayAsync(file.Path, ct);
        lock (_gate)
        {
            file.EnsureValid(_generation);
            if (file.State.Pending.Count > 0 && Faults.ReadOnly && MatchesFilter(file.Path))
                throw StorageErrors.ReadOnlyDevice(file.Path);
            if (MatchesFilter(file.Path) && TryConsume(() => Faults.FailNextFlushes, v => Faults.FailNextFlushes = v))
                throw StorageErrors.DeviceError("flush", file.Path);
            if (Faults.DishonestFlush)
            {
                for (int i = 0; i < file.State.Pending.Count; i++)
                    file.State.Pending[i] = file.State.Pending[i] with { Acknowledged = true };
                return;
            }

            RandomAccess.FlushToDisk(file.Handle);
            file.State.Pending.Clear();
        }
    }

    internal long GetLength(FsFile file)
    {
        lock (_gate)
        {
            file.EnsureValid(_generation);
            return RandomAccess.GetLength(file.Handle);
        }
    }

    internal void Close(FsFile file)
    {
        lock (_gate)
        {
            _open.Remove(file);
            file.Handle.Dispose();
        }
    }

    // -------------------------------------------------------------------------------------------- helpers

    private static bool TryConsume(Func<int> get, Action<int> set)
    {
        int value = get();
        if (value <= 0)
            return false;
        set(value - 1);
        return true;
    }

    private static string FilePath(string path)
    {
        string p = StoragePath.Normalize(path);
        if (p.Length == 0)
            throw new ArgumentException("A file path is required (the root directory is not a file).", nameof(path));
        return p;
    }

    private async ValueTask DelayAsync(string path, CancellationToken ct)
    {
        if (Faults.IoDelay is { } delay && delay > TimeSpan.Zero && MatchesFilter(path))
            await Task.Delay(delay, TimeProvider, ct).ConfigureAwait(false);
    }

    private bool MatchesFilter(string path) => Faults.PathFilter is not { } filter || filter(path);

    private void ThrowIfReadOnly(string path)
    {
        if (Faults.ReadOnly && MatchesFilter(path))
            throw StorageErrors.ReadOnlyDevice(path);
    }

    private string Full(string normalized) =>
        normalized.Length == 0 ? _root : System.IO.Path.Combine(_root, normalized.Replace('/', System.IO.Path.DirectorySeparatorChar));

    private FileState StateOf(string full)
    {
        if (!_states.TryGetValue(full, out FileState? state))
            _states[full] = state = new FileState(full);
        return state;
    }

    private void CreateFileLocked(string relative, string full)
    {
        ThrowIfReadOnly(relative);
        using (File.Create(full))
        {
        }

        StateOf(full);
        _pendingNamespace.Add(new NamespaceOp(OpKind.CreateFile, relative, null, null));
    }

    private void RecordAndSetLength(FileState state, SafeFileHandle handle, long length)
    {
        long before = RandomAccess.GetLength(handle);
        byte[] tail = length < before ? new byte[before - length] : [];
        if (tail.Length > 0)
            RandomAccess.Read(handle, tail, length);
        RandomAccess.SetLength(handle, length);
        state.Pending.Add(new PendingWrite(++_sequence, 0, null, length, length, tail, before));
        AdjustUsed(length - before);
    }

    /// <summary>Moves a file out of the root (until its deletion or replacement is durable); returns the parked full path.</summary>
    private string Park(string full)
    {
        string parked = System.IO.Path.Combine(_trash, (++_trashCounter).ToString(System.Globalization.CultureInfo.InvariantCulture));
        File.Move(full, parked);
        Rekey(full, parked);
        return parked;
    }

    private void Rekey(string fromFull, string toFull)
    {
        // Open handles reference the same state object, so they follow the file.
        if (_states.Remove(fromFull, out FileState? state))
        {
            state.Path = toFull;
            _states[toFull] = state;
        }
    }

    private void MakeNamespaceDurable(NamespaceOp op)
    {
        if (op.Parked is { } parked && File.Exists(parked))
        {
            _states.Remove(parked);
            File.Delete(parked);
        }
    }

    private void UndoNamespace(NamespaceOp op)
    {
        switch (op.Kind)
        {
            case OpKind.CreateFile:
            {
                string full = Full(op.Path);
                if (File.Exists(full))
                {
                    _states.Remove(full);
                    File.Delete(full);
                }

                break;
            }

            case OpKind.CreateDirectory:
            {
                string full = Full(op.Path);
                if (Directory.Exists(full))
                    Directory.Delete(full, recursive: true);
                break;
            }

            case OpKind.DeleteFile:
                if (op.Parked is { } parkedFile && File.Exists(parkedFile))
                {
                    File.Move(parkedFile, Full(op.Path), overwrite: true);
                    Rekey(parkedFile, Full(op.Path));
                }

                break;
            case OpKind.DeleteDirectory:
                Directory.CreateDirectory(Full(op.Path));
                break;
            case OpKind.Rename:
            {
                string from = Full(op.Path);
                string to = Full(op.Target!);
                if (File.Exists(to))
                {
                    File.Move(to, from, overwrite: true);
                    Rekey(to, from);
                }

                if (op.Parked is { } parked && File.Exists(parked))
                {
                    File.Move(parked, to, overwrite: true);
                    Rekey(parked, to);
                }

                break;
            }
        }
    }

    /// <summary>Applies the crash mode to one file's unflushed changes; returns the changes that did not survive.</summary>
    private List<PendingWrite> ApplyCrash(FileState state, CrashMode mode, long tornSequence)
    {
        List<PendingWrite> pending = state.Pending;
        if (mode == CrashMode.KeepUnflushed)
            return [];
        using SafeFileHandle handle = File.OpenHandle(state.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        if (mode == CrashMode.TearLastWrite && !pending.Any(w => w.Sequence == tornSequence))
            return [];

        // Roll back to the durable image (newest change first)...
        for (int i = pending.Count - 1; i >= 0; i--)
        {
            PendingWrite w = pending[i];
            if (w.PreImage.Length > 0)
                RandomAccess.Write(handle, w.PreImage, w.PreOffset);
            RandomAccess.SetLength(handle, w.LengthBefore);
        }

        // ...then replay the changes that survive.
        var lost = new List<PendingWrite>();
        foreach (PendingWrite w in pending)
        {
            switch (mode)
            {
                case CrashMode.LoseUnflushed:
                    lost.Add(w);
                    break;
                case CrashMode.RandomSubset:
                    if (_random.Next(2) == 1)
                        Redo(handle, w, w.Data?.Length ?? 0);
                    else
                        lost.Add(w);
                    break;
                case CrashMode.TearLastWrite:
                    if (w.Sequence == tornSequence)
                    {
                        Redo(handle, w, _random.Next(w.Data!.Length));
                        lost.Add(w);
                    }
                    else
                    {
                        Redo(handle, w, w.Data?.Length ?? 0);
                    }

                    break;
            }
        }

        RandomAccess.FlushToDisk(handle);
        return lost;
    }

    private static void Redo(SafeFileHandle handle, PendingWrite w, int keep)
    {
        if (w.Data is null)
        {
            RandomAccess.SetLength(handle, w.NewLength);
            return;
        }

        if (keep > 0)
            RandomAccess.Write(handle, w.Data.AsSpan(0, keep), w.Offset);
    }

    private static void CountLost(FileState state, IEnumerable<PendingWrite> lost, ref int writes, ref long bytes, SortedSet<string> paths)
    {
        foreach (PendingWrite w in lost)
        {
            if (!w.Acknowledged)
                continue;
            writes++;
            bytes += w.Data?.Length ?? 0;
            paths.Add(state.Path);
        }
    }

    private long UsedLocked()
    {
        if (_used < 0)
        {
            long total = 0;
            foreach (string file in Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories))
            {
                try
                {
                    total += new FileInfo(file).Length;
                }
                catch (IOException)
                {
                    // Vanished during the scan.
                }
            }

            _used = total;
        }

        return _used;
    }

    private void AdjustUsed(long delta)
    {
        if (_used >= 0)
            _used = Math.Max(0, _used + delta);
    }

    private enum OpKind
    {
        CreateFile,
        CreateDirectory,
        DeleteFile,
        DeleteDirectory,
        Rename,
    }

    /// <summary>One unflushed namespace change; <see cref="Parked"/> holds a deleted or overwritten file until durable.</summary>
    private sealed class NamespaceOp(OpKind kind, string path, string? target, string? parked)
    {
        public OpKind Kind { get; } = kind;

        public string Path { get; } = path;

        public string? Target { get; } = target;

        public string? Parked { get; } = parked;

        public bool Acknowledged { get; set; }

        public bool Touches(string directory)
            => (Path.Length != 0 && string.Equals(StoragePath.GetParent(Path), directory, StringComparison.Ordinal))
            || (Target is not null && string.Equals(StoragePath.GetParent(Target), directory, StringComparison.Ordinal));
    }

    /// <summary>One file identity (follows renames and parking) with its unflushed changes.</summary>
    internal sealed class FileState(string path)
    {
        public string Path { get; set; } = path;

        public List<PendingWrite> Pending { get; } = [];
    }

    /// <summary>One unflushed change with the bytes it replaced (undo) and the bytes it wrote (redo).</summary>
    internal readonly record struct PendingWrite(long Sequence, long Offset, byte[]? Data, long NewLength, long PreOffset, byte[] PreImage,
        long LengthBefore, bool Acknowledged = false);

    /// <summary>An open file of the medium (invalidated by <see cref="Crash"/>).</summary>
    internal sealed class FsFile : IStorageFile
    {
        private readonly FaultingFileSystemMedium _medium;
        private readonly long _generation;
        private bool _invalid;

        public FsFile(FaultingFileSystemMedium medium, SafeFileHandle handle, string path, FileState state, long generation)
        {
            _medium = medium;
            Handle = handle;
            Path = path;
            State = state;
            _generation = generation;
        }

        public SafeFileHandle Handle { get; }

        public string Path { get; }

        public FileState State { get; }

        public long Length => _medium.GetLength(this);

        public void Invalidate()
        {
            _invalid = true;
            Handle.Dispose();
        }

        public void EnsureValid(long generation)
        {
            if (_invalid || _generation != generation)
                throw new ObjectDisposedException(Path, $"Medium '{_medium.Name}' crashed; the handle for '{Path}' is no longer valid.");
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default) => _medium.ReadAsync(this, offset, buffer, ct);

        public ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default) => _medium.WriteAsync(this, offset, data, ct);

        public ValueTask FlushAsync(CancellationToken ct = default) => _medium.FlushAsync(this, ct);

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default) => _medium.SetLengthAsync(this, length, ct);

        public ValueTask DisposeAsync()
        {
            _medium.Close(this);
            return default;
        }
    }

    /// <summary>Best-effort directory fsync on Unix-like systems (NTFS journals directory metadata).</summary>
    private static class DirectorySyncHelper
    {
        public static void TryFsync(string directory)
        {
            try
            {
                using SafeFileHandle handle = File.OpenHandle(directory, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                RandomAccess.FlushToDisk(handle);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Not every platform can open a directory for flushing.
            }
        }
    }
}
