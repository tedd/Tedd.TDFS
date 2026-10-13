namespace Tedd.Tdfs.Storage;

/// <summary>
/// Crash-faithful in-memory disk. Each file keeps a durable image plus an ordered list of unflushed writes;
/// <see cref="IStorageFile.FlushAsync"/> makes that file's writes durable. Namespace changes (file creation, rename,
/// delete, directory creation/removal) are kept in an ordered journal and become durable when
/// <see cref="FlushDirectoryAsync"/> is called on a directory they touch; like a journaling file system, such a flush
/// also persists every earlier namespace change (prefix semantics). <see cref="Crash"/> decides what survives
/// according to a <see cref="CrashMode"/> (seeded) and invalidates every open handle.
/// </summary>
public sealed class SimulatedMedium : IStorageMedium
{
    private readonly Lock _gate = new();
    private readonly IRandomSource _random;
    private readonly long _capacityBytes;

    // Current (volatile) namespace.
    private readonly SortedDictionary<string, SimulatedNode> _files = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _dirs = new(StringComparer.Ordinal) { StoragePath.Root };

    // Durable namespace.
    private readonly SortedDictionary<string, SimulatedNode> _durableFiles = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _durableDirs = new(StringComparer.Ordinal) { StoragePath.Root };

    // Unflushed namespace changes, oldest first.
    private readonly List<NamespaceOp> _pendingNamespace = [];

    // Nodes with open handles (unlinked files keep consuming space while open).
    private readonly HashSet<SimulatedNode> _openNodes = [];

    private TimeProvider _time;
    private long _generation;
    private long _writeSequence;
    private long _flushCount;
    private long _crashCount;
    private SimulatedLossReport _lastCrashLoss = SimulatedLossReport.None;

    /// <summary>Creates an empty medium.</summary>
    /// <param name="name">Diagnostic name.</param>
    /// <param name="capacityBytes">Capacity; writes beyond it fail with ENOSPC.</param>
    /// <param name="random">Seeded source used for <see cref="CrashMode.RandomSubset"/> and <see cref="CrashMode.TearLastWrite"/>.</param>
    /// <param name="timeProvider">Clock used for <see cref="SimulatedMediumFaults.IoDelay"/> (default <see cref="TimeProvider.System"/>).</param>
    public SimulatedMedium(string name, long capacityBytes, IRandomSource random, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentOutOfRangeException.ThrowIfNegative(capacityBytes);
        ArgumentNullException.ThrowIfNull(random);
        Name = name;
        _capacityBytes = capacityBytes;
        _random = random;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <summary>Effective capacity: <see cref="SimulatedMediumFaults.CapacityOverride"/> when set, otherwise the configured capacity.</summary>
    public long CapacityBytes => Faults.CapacityOverride ?? _capacityBytes;

    /// <summary>Sum of the current lengths of all linked files and of unlinked files that are still open.</summary>
    public long UsedBytes
    {
        get
        {
            lock (_gate)
                return ComputeUsedLocked();
        }
    }

    /// <summary>Fault switches.</summary>
    public SimulatedMediumFaults Faults { get; } = new();

    /// <summary>Clock used for <see cref="SimulatedMediumFaults.IoDelay"/>. May be replaced (for example by a node's simulated clock).</summary>
    public TimeProvider TimeProvider
    {
        get => _time;
        set => _time = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>Number of successful durability barriers (file flushes plus directory flushes).</summary>
    public long FlushCount => Interlocked.Read(ref _flushCount);

    /// <summary>Number of <see cref="Crash"/> calls so far.</summary>
    public long CrashCount => Interlocked.Read(ref _crashCount);

    /// <summary>
    /// Acknowledged-but-volatile changes the most recent <see cref="Crash"/> discarded: changes a dishonest flush
    /// (<see cref="SimulatedMediumFaults.DishonestFlush"/>) reported durable that did not survive. Empty after an honest crash.
    /// </summary>
    public SimulatedLossReport LastCrashLoss
    {
        get
        {
            lock (_gate)
                return _lastCrashLoss;
        }
    }

    /// <summary>Changes a dishonest flush reported durable that are still volatile (a crash or disk loss now would lose them).</summary>
    public SimulatedLossReport GetAcknowledgedVolatileState()
    {
        lock (_gate)
        {
            Dictionary<SimulatedNode, string> nodePaths = NodePathMapLocked();
            var paths = new SortedSet<string>(StringComparer.Ordinal);
            int writes = 0;
            long bytes = 0;
            foreach (SimulatedNode node in AllNodesLocked())
            {
                foreach (PendingWrite w in node.Pending)
                {
                    if (!w.Acknowledged)
                        continue;
                    writes++;
                    bytes += w.Data?.Length ?? 0;
                    if (nodePaths.TryGetValue(node, out string? path))
                        paths.Add(path);
                }
            }

            int namespaceChanges = 0;
            foreach (NamespaceOp op in _pendingNamespace)
            {
                if (!op.Acknowledged)
                    continue;
                namespaceChanges++;
                paths.Add(op.Path);
            }

            return writes == 0 && namespaceChanges == 0
                ? SimulatedLossReport.None
                : new SimulatedLossReport(writes, bytes, namespaceChanges, [.. paths]);
        }
    }

    /// <summary>Number of unflushed file changes plus unflushed namespace changes (diagnostics).</summary>
    public int UnflushedOperationCount
    {
        get
        {
            lock (_gate)
            {
                int count = _pendingNamespace.Count;
                foreach (SimulatedNode node in AllNodesLocked())
                    count += node.Pending.Count;
                return count;
            }
        }
    }

    /// <summary>
    /// Simulates a power loss: decides what unflushed state survives according to <paramref name="mode"/>, resets the
    /// visible state to the surviving durable image, and invalidates every handle opened before the crash (their
    /// operations then throw <see cref="ObjectDisposedException"/>). Directory entries whose parent directory did not
    /// survive are dropped.
    /// </summary>
    public void Crash(CrashMode mode)
    {
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode));

        lock (_gate)
        {
            // Acknowledged-but-volatile changes (dishonest flushes) that this crash discards are reported (HAR-018).
            Dictionary<SimulatedNode, string> nodePaths = NodePathMapLocked();
            var lostPaths = new SortedSet<string>(StringComparer.Ordinal);
            int lostWrites = 0;
            long lostBytes = 0;
            int lostNamespace = 0;

            // 1. Namespace: a prefix of the ordered unflushed changes survives.
            int namespaceCount = _pendingNamespace.Count;
            int keepNamespace = mode switch
            {
                CrashMode.LoseUnflushed => 0,
                CrashMode.RandomSubset => _random.Next(namespaceCount + 1),
                _ => namespaceCount,
            };
            for (int i = 0; i < keepNamespace; i++)
                ApplyNamespaceOp(_pendingNamespace[i], _durableFiles, _durableDirs);
            for (int i = keepNamespace; i < namespaceCount; i++)
            {
                if (_pendingNamespace[i].Acknowledged)
                {
                    lostNamespace++;
                    lostPaths.Add(_pendingNamespace[i].Path);
                }
            }

            _pendingNamespace.Clear();
            RemoveOrphans(_durableFiles, _durableDirs);

            // 2. File data.
            long tornSequence = -1;
            if (mode == CrashMode.TearLastWrite)
            {
                foreach (SimulatedNode node in AllNodesLocked())
                {
                    foreach (PendingWrite w in node.Pending)
                    {
                        if (w.Data is not null && w.Sequence > tornSequence)
                            tornSequence = w.Sequence;
                    }
                }
            }

            var processed = new HashSet<SimulatedNode>();
            var keptWrites = new HashSet<long>();
            foreach (KeyValuePair<string, SimulatedNode> entry in _durableFiles)
            {
                SimulatedNode node = entry.Value;
                if (!processed.Add(node))
                    continue;

                foreach (PendingWrite w in node.Pending)
                {
                    bool kept = true;
                    switch (mode)
                    {
                        case CrashMode.LoseUnflushed:
                            kept = false;
                            break;
                        case CrashMode.KeepUnflushed:
                            SimulatedNode.Apply(node.Durable, w);
                            break;
                        case CrashMode.RandomSubset:
                            if (_random.Next(2) == 1)
                                SimulatedNode.Apply(node.Durable, w);
                            else
                                kept = false;
                            break;
                        case CrashMode.TearLastWrite:
                            if (w.Sequence == tornSequence && w.Data is not null)
                            {
                                int keep = _random.Next(w.Data.Length); // strict prefix: 0 .. Length-1 bytes
                                node.Durable.Write(w.Offset, w.Data.AsSpan(0, keep));
                                kept = false;
                            }
                            else
                            {
                                SimulatedNode.Apply(node.Durable, w);
                            }

                            break;
                    }

                    if (kept)
                        keptWrites.Add(w.Sequence);
                }
            }

            // Every node (reachable or not) loses its volatile state; unreachable nodes are simply dropped.
            foreach (SimulatedNode node in AllNodesLocked())
            {
                foreach (PendingWrite w in node.Pending)
                {
                    if (w.Acknowledged && !keptWrites.Contains(w.Sequence))
                    {
                        lostWrites++;
                        lostBytes += w.Data?.Length ?? 0;
                        if (nodePaths.TryGetValue(node, out string? path))
                            lostPaths.Add(path);
                    }
                }

                node.Pending.Clear();
                node.OpenHandles = 0;
            }

            _lastCrashLoss = lostWrites == 0 && lostNamespace == 0
                ? SimulatedLossReport.None
                : new SimulatedLossReport(lostWrites, lostBytes, lostNamespace, [.. lostPaths]);

            foreach (SimulatedNode node in processed)
                node.Current = node.Durable.Clone();

            // 3. The visible namespace restarts from the durable one.
            _files.Clear();
            foreach (KeyValuePair<string, SimulatedNode> entry in _durableFiles)
                _files.Add(entry.Key, entry.Value);
            _dirs.Clear();
            foreach (string dir in _durableDirs)
                _dirs.Add(dir);
            _openNodes.Clear();

            _generation++;
            Interlocked.Increment(ref _crashCount);
        }
    }

    /// <summary>All file paths of the current view and of the durable view (union), sorted ordinally.</summary>
    public IReadOnlyList<string> AllFiles()
    {
        lock (_gate)
        {
            var all = new SortedSet<string>(_files.Keys, StringComparer.Ordinal);
            all.UnionWith(_durableFiles.Keys);
            return [.. all];
        }
    }

    /// <summary>All directory paths of the current view (excluding the root), sorted ordinally.</summary>
    public IReadOnlyList<string> AllDirectories()
    {
        lock (_gate)
            return [.. _dirs.Where(d => d.Length != 0)];
    }

    /// <summary>Returns the durable image of a file whose directory entry is durable.</summary>
    /// <exception cref="FileNotFoundException">The file has no durable directory entry.</exception>
    public byte[] ReadDurable(string path)
    {
        string p = NormalizeFilePath(path);
        lock (_gate)
        {
            if (!_durableFiles.TryGetValue(p, out SimulatedNode? node))
                throw new FileNotFoundException($"'{p}' has no durable directory entry on medium '{Name}'.", p);
            return node.Durable.ToArray();
        }
    }

    /// <summary>Returns the current (volatile) content of a file.</summary>
    /// <exception cref="FileNotFoundException">The file does not exist in the current view.</exception>
    public byte[] ReadCurrent(string path)
    {
        string p = NormalizeFilePath(path);
        lock (_gate)
        {
            if (!_files.TryGetValue(p, out SimulatedNode? node))
                throw new FileNotFoundException($"'{p}' does not exist on medium '{Name}'.", p);
            return node.Current.ToArray();
        }
    }

    /// <summary>
    /// Flips stored bits of a file (media corruption): XORs the byte at <paramref name="offset"/> with
    /// <paramref name="xorMask"/> in the durable image, the current image and any unflushed write covering it.
    /// </summary>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The offset is beyond both the durable and the current length.</exception>
    public void CorruptByte(string path, long offset, byte xorMask = 0xFF)
    {
        string p = NormalizeFilePath(path);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        lock (_gate)
        {
            SimulatedNode? node = _files.TryGetValue(p, out SimulatedNode? current) ? current
                : _durableFiles.TryGetValue(p, out SimulatedNode? durable) ? durable : null;
            if (node is null)
                throw new FileNotFoundException($"'{p}' does not exist on medium '{Name}'.", p);

            bool touched = false;
            if (offset < node.Durable.Length)
            {
                node.Durable.XorByte(offset, xorMask);
                touched = true;
            }

            if (offset < node.Current.Length)
            {
                node.Current.XorByte(offset, xorMask);
                touched = true;
            }

            foreach (PendingWrite w in node.Pending)
            {
                if (w.Data is not null && offset >= w.Offset && offset < w.Offset + w.Data.Length)
                    w.Data[offset - w.Offset] ^= xorMask;
            }

            if (!touched)
                throw new ArgumentOutOfRangeException(nameof(offset), $"Offset {offset} is beyond the end of '{p}'.");
        }
    }

    /// <summary>Creates an independent medium holding a copy of this medium's durable image (a disk clone, RSY-021).</summary>
    public SimulatedMedium CloneDurable(string newName)
    {
        ArgumentNullException.ThrowIfNull(newName);
        lock (_gate)
        {
            var clone = new SimulatedMedium(newName, _capacityBytes, _random.Fork("clone:" + newName), _time);
            foreach (string dir in _durableDirs)
            {
                clone._dirs.Add(dir);
                clone._durableDirs.Add(dir);
            }

            foreach (KeyValuePair<string, SimulatedNode> entry in _durableFiles)
            {
                var node = new SimulatedNode
                {
                    Durable = entry.Value.Durable.Clone(),
                    Current = entry.Value.Durable.Clone(),
                };
                clone._files.Add(entry.Key, node);
                clone._durableFiles.Add(entry.Key, node);
            }

            return clone;
        }
    }

    /// <inheritdoc />
    public ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string p = NormalizeFilePath(path);
        lock (_gate)
        {
            if (_dirs.Contains(p))
                throw new UnauthorizedAccessException($"Access to the path '{p}' is denied: it is a directory.");
            string parent = StoragePath.GetParent(p);
            if (!_dirs.Contains(parent))
                throw new DirectoryNotFoundException($"Could not find a part of the path '{p}'.");

            _files.TryGetValue(p, out SimulatedNode? node);
            switch (mode)
            {
                case StorageOpenMode.CreateNew:
                    if (node is not null)
                        throw StorageErrors.FileExists(p);
                    node = CreateFileLocked(p);
                    break;
                case StorageOpenMode.OpenExisting:
                    if (node is null)
                        throw new FileNotFoundException($"Could not find file '{p}'.", p);
                    break;
                case StorageOpenMode.OpenOrCreate:
                    node ??= CreateFileLocked(p);
                    break;
                case StorageOpenMode.Truncate:
                    if (node is null)
                    {
                        node = CreateFileLocked(p);
                    }
                    else
                    {
                        ThrowIfReadOnlyLocked(p);
                        AppendSetLengthLocked(node, 0);
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode));
            }

            node.OpenHandles++;
            _openNodes.Add(node);
            return new ValueTask<IStorageFile>(new SimulatedStorageFile(this, node, p, _generation));
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string p = StoragePath.Normalize(path);
        lock (_gate)
            return new ValueTask<bool>(_dirs.Contains(p) || _files.ContainsKey(p));
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
            if (_files.TryGetValue(p, out SimulatedNode? node))
            {
                ThrowIfReadOnlyLocked(p);
                _files.Remove(p);
                _pendingNamespace.Add(new NamespaceOp(NamespaceOpKind.DeleteFile, p, null, node));
            }
            else if (_dirs.Contains(p))
            {
                if (HasChildrenLocked(p))
                    throw new IOException($"The directory '{p}' is not empty.");
                ThrowIfReadOnlyLocked(p);
                _dirs.Remove(p);
                _pendingNamespace.Add(new NamespaceOp(NamespaceOpKind.DeleteDirectory, p, null, null));
            }
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string f = NormalizeFilePath(from);
        string t = NormalizeFilePath(to);
        lock (_gate)
        {
            if (!_files.TryGetValue(f, out SimulatedNode? node))
            {
                if (_dirs.Contains(f))
                    throw new IOException($"Renaming directories ('{f}') is not supported by this medium.");
                throw new FileNotFoundException($"Could not find file '{f}'.", f);
            }

            if (string.Equals(f, t, StringComparison.Ordinal))
                return default;
            if (_dirs.Contains(t))
                throw new IOException($"Cannot rename '{f}' over directory '{t}'.");
            if (!_dirs.Contains(StoragePath.GetParent(t)))
                throw new DirectoryNotFoundException($"Could not find a part of the path '{t}'.");
            if (!overwrite && _files.ContainsKey(t))
                throw StorageErrors.FileExists(t);
            ThrowIfReadOnlyLocked(f);
            ThrowIfReadOnlyLocked(t);

            _files.Remove(f);
            _files[t] = node;
            _pendingNamespace.Add(new NamespaceOp(NamespaceOpKind.Rename, f, t, node));
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string d = StoragePath.Normalize(directory);
        lock (_gate)
        {
            if (!_dirs.Contains(d))
                throw new DirectoryNotFoundException($"Could not find directory '{d}'.");
            var names = new List<string>();
            foreach (string file in _files.Keys)
            {
                if (StoragePath.IsDirectChild(d, file))
                    names.Add(StoragePath.GetName(file));
            }

            foreach (string dir in _dirs)
            {
                if (StoragePath.IsDirectChild(d, dir))
                    names.Add(StoragePath.GetName(dir));
            }

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
                if (_files.ContainsKey(current))
                    throw new IOException($"Cannot create directory '{d}': '{current}' is a file.");
                if (_dirs.Contains(current))
                    continue;
                ThrowIfReadOnlyLocked(current);
                _dirs.Add(current);
                _pendingNamespace.Add(new NamespaceOp(NamespaceOpKind.CreateDirectory, current, null, null));
            }
        }

        return default;
    }

    /// <inheritdoc />
    public async ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string d = StoragePath.Normalize(directory);
        if (Faults.DelayFor(d) is { } delay)
            await Task.Delay(delay, _time, ct);

        lock (_gate)
        {
            if (!_dirs.Contains(d))
                throw new DirectoryNotFoundException($"Could not find directory '{d}'.");
            int last = _pendingNamespace.FindLastIndex(op => op.Touches(d));
            if (last >= 0 && Faults.IsReadOnlyFor(d))
                throw StorageErrors.ReadOnlyDevice(d);
            if (Faults.TryConsumeFailFlush(d))
                throw StorageErrors.DeviceError("directory flush", d);
            if (Faults.DishonestFlush)
            {
                // Out-of-model device: report success, persist nothing.
                for (int i = 0; i <= last; i++)
                    _pendingNamespace[i].Acknowledged = true;
                return;
            }

            for (int i = 0; i <= last; i++)
                ApplyNamespaceOp(_pendingNamespace[i], _durableFiles, _durableDirs);
            if (last >= 0)
                _pendingNamespace.RemoveRange(0, last + 1);
            Interlocked.Increment(ref _flushCount);
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"SimulatedMedium({Name})";

    internal long GetLength(SimulatedStorageFile handle)
    {
        lock (_gate)
        {
            EnsureValidLocked(handle);
            return handle.Node.Current.Length;
        }
    }

    internal async ValueTask<int> ReadAsync(SimulatedStorageFile handle, long offset, Memory<byte> buffer, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        EnsureValid(handle);
        if (Faults.DelayFor(handle.Path) is { } delay)
            await Task.Delay(delay, _time, ct);

        lock (_gate)
        {
            EnsureValidLocked(handle);
            if (Faults.TryConsumeFailRead(handle.Path))
                throw StorageErrors.DeviceError("read", handle.Path);
            return handle.Node.Current.Read(offset, buffer.Span);
        }
    }

    internal async ValueTask<int> WriteAsync(SimulatedStorageFile handle, long offset, ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        EnsureValid(handle);
        if (Faults.DelayFor(handle.Path) is { } delay)
            await Task.Delay(delay, _time, ct);

        lock (_gate)
        {
            EnsureValidLocked(handle);
            ThrowIfReadOnlyLocked(handle.Path);
            if (Faults.TryConsumeFailWrite(handle.Path))
                throw StorageErrors.DeviceError("write", handle.Path);
            if (data.IsEmpty)
                return 0;

            int count = data.Length;
            if (Faults.TryConsumeShortWrite(handle.Path))
                count = (count + 1) / 2;

            SimulatedNode node = handle.Node;
            long growth = Math.Max(0, offset + count - node.Current.Length);
            if (growth > 0 && ComputeUsedLocked() + growth > CapacityBytes)
                throw StorageErrors.DiskFull(handle.Path);

            ReadOnlySpan<byte> written = data.Span[..count];
            node.Current.Write(offset, written);
            node.Pending.Add(new PendingWrite(++_writeSequence, offset, written.ToArray(), -1));
            return count;
        }
    }

    internal ValueTask SetLengthAsync(SimulatedStorageFile handle, long length, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        lock (_gate)
        {
            EnsureValidLocked(handle);
            ThrowIfReadOnlyLocked(handle.Path);
            SimulatedNode node = handle.Node;
            long growth = Math.Max(0, length - node.Current.Length);
            if (growth > 0 && ComputeUsedLocked() + growth > CapacityBytes)
                throw StorageErrors.DiskFull(handle.Path);
            AppendSetLengthLocked(node, length);
        }

        return default;
    }

    internal async ValueTask FlushFileAsync(SimulatedStorageFile handle, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        EnsureValid(handle);
        if (Faults.DelayFor(handle.Path) is { } delay)
            await Task.Delay(delay, _time, ct);

        lock (_gate)
        {
            EnsureValidLocked(handle);
            SimulatedNode node = handle.Node;
            if (node.Pending.Count > 0 && Faults.IsReadOnlyFor(handle.Path))
                throw StorageErrors.ReadOnlyDevice(handle.Path);
            if (Faults.TryConsumeFailFlush(handle.Path))
                throw StorageErrors.DeviceError("flush", handle.Path);
            if (Faults.DishonestFlush)
            {
                // Out-of-model device: report success, keep the changes volatile.
                for (int i = 0; i < node.Pending.Count; i++)
                    node.Pending[i] = node.Pending[i] with { Acknowledged = true };
                return;
            }

            foreach (PendingWrite w in node.Pending)
                SimulatedNode.Apply(node.Durable, w);
            node.Pending.Clear();
            Interlocked.Increment(ref _flushCount);
        }
    }

    internal void Close(SimulatedStorageFile handle)
    {
        lock (_gate)
        {
            if (handle.IsDisposed)
                return;
            handle.IsDisposed = true;
            if (handle.Generation != _generation)
                return;
            SimulatedNode node = handle.Node;
            node.OpenHandles--;
            if (node.OpenHandles <= 0)
            {
                node.OpenHandles = 0;
                _openNodes.Remove(node);
            }
        }
    }

    private static string NormalizeFilePath(string path)
    {
        string p = StoragePath.Normalize(path);
        if (p.Length == 0)
            throw new ArgumentException("A file path is required (the root directory is not a file).", nameof(path));
        return p;
    }

    private static void ApplyNamespaceOp(NamespaceOp op, SortedDictionary<string, SimulatedNode> files, SortedSet<string> dirs)
    {
        switch (op.Kind)
        {
            case NamespaceOpKind.CreateFile:
                files[op.Path] = op.Node!;
                break;
            case NamespaceOpKind.CreateDirectory:
                dirs.Add(op.Path);
                break;
            case NamespaceOpKind.DeleteFile:
                files.Remove(op.Path);
                break;
            case NamespaceOpKind.DeleteDirectory:
                dirs.Remove(op.Path);
                break;
            case NamespaceOpKind.Rename:
                files.Remove(op.Path);
                files[op.Target!] = op.Node!;
                break;
        }
    }

    private static void RemoveOrphans(SortedDictionary<string, SimulatedNode> files, SortedSet<string> dirs)
    {
        // Ordinal order visits a parent before its children (a parent path is a strict prefix of its children).
        var orphanDirs = new List<string>();
        var kept = new HashSet<string>(StringComparer.Ordinal) { StoragePath.Root };
        foreach (string dir in dirs)
        {
            if (dir.Length == 0)
                continue;
            if (kept.Contains(StoragePath.GetParent(dir)))
                kept.Add(dir);
            else
                orphanDirs.Add(dir);
        }

        foreach (string dir in orphanDirs)
            dirs.Remove(dir);

        var orphanFiles = files.Keys.Where(f => !kept.Contains(StoragePath.GetParent(f))).ToList();
        foreach (string file in orphanFiles)
            files.Remove(file);
    }

    private SimulatedNode CreateFileLocked(string path)
    {
        ThrowIfReadOnlyLocked(path);
        var node = new SimulatedNode();
        _files[path] = node;
        _pendingNamespace.Add(new NamespaceOp(NamespaceOpKind.CreateFile, path, null, node));
        return node;
    }

    private void AppendSetLengthLocked(SimulatedNode node, long length)
    {
        node.Current.SetLength(length);
        node.Pending.Add(new PendingWrite(++_writeSequence, 0, null, length));
    }

    private bool HasChildrenLocked(string directory)
    {
        foreach (string file in _files.Keys)
        {
            if (StoragePath.IsDescendant(directory, file))
                return true;
        }

        foreach (string dir in _dirs)
        {
            if (StoragePath.IsDescendant(directory, dir))
                return true;
        }

        return false;
    }

    private void ThrowIfReadOnlyLocked(string path)
    {
        if (Faults.IsReadOnlyFor(path))
            throw StorageErrors.ReadOnlyDevice(path);
    }

    private void EnsureValid(SimulatedStorageFile handle)
    {
        lock (_gate)
            EnsureValidLocked(handle);
    }

    private void EnsureValidLocked(SimulatedStorageFile handle)
    {
        if (handle.IsDisposed)
            throw new ObjectDisposedException(handle.Path, $"The file handle for '{handle.Path}' has been disposed.");
        if (handle.Generation != _generation)
            throw new ObjectDisposedException(handle.Path, $"Medium '{Name}' crashed; the handle for '{handle.Path}' is no longer valid.");
    }

    private long ComputeUsedLocked()
    {
        long used = 0;
        foreach (SimulatedNode node in LiveNodesLocked())
            used += node.Current.Length;
        return used;
    }

    private HashSet<SimulatedNode> LiveNodesLocked()
    {
        var live = new HashSet<SimulatedNode>(_files.Values);
        live.UnionWith(_openNodes);
        return live;
    }

    private HashSet<SimulatedNode> AllNodesLocked()
    {
        HashSet<SimulatedNode> all = LiveNodesLocked();
        all.UnionWith(_durableFiles.Values);
        foreach (NamespaceOp op in _pendingNamespace)
        {
            if (op.Node is not null)
                all.Add(op.Node);
        }

        return all;
    }

    private enum NamespaceOpKind
    {
        CreateFile,
        CreateDirectory,
        DeleteFile,
        DeleteDirectory,
        Rename,
    }

    /// <summary>Maps every known file node to one path naming it (current view first, then durable view, then pending operations).</summary>
    private Dictionary<SimulatedNode, string> NodePathMapLocked()
    {
        var map = new Dictionary<SimulatedNode, string>();
        foreach (KeyValuePair<string, SimulatedNode> entry in _files)
            map.TryAdd(entry.Value, entry.Key);
        foreach (KeyValuePair<string, SimulatedNode> entry in _durableFiles)
            map.TryAdd(entry.Value, entry.Key);
        foreach (NamespaceOp op in _pendingNamespace)
        {
            if (op.Node is not null)
                map.TryAdd(op.Node, op.Target ?? op.Path);
        }

        return map;
    }

    private sealed record NamespaceOp(NamespaceOpKind Kind, string Path, string? Target, SimulatedNode? Node)
    {
        /// <summary>True once a dishonest directory flush reported this change durable although it is still volatile.</summary>
        public bool Acknowledged { get; set; }

        public bool Touches(string directory)
            => (Path.Length != 0 && string.Equals(StoragePath.GetParent(Path), directory, StringComparison.Ordinal))
            || (Target is not null && string.Equals(StoragePath.GetParent(Target), directory, StringComparison.Ordinal));
    }
}
