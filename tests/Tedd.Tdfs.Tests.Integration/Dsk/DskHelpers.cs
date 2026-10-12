using System.Globalization;
using System.Security.Cryptography;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Dsk;

/// <summary>Local medium a storage-engine test runs on.</summary>
public enum DskMediumKind
{
    /// <summary>Crash-faithful in-memory <see cref="SimulatedMedium"/> (power loss emulated with every <see cref="CrashMode"/>).</summary>
    Simulated,

    /// <summary>Real directory under <c>TDFS_TEST_ROOT</c> via <see cref="FileSystemMedium"/> (process-kill crashes only).</summary>
    FileSystem,
}

/// <summary>Storage-engine component exercised by a DSK family.</summary>
public enum DskComponent
{
    /// <summary><see cref="Tedd.Tdfs.Storage.Journal"/>.</summary>
    Journal,

    /// <summary><see cref="Tedd.Tdfs.Storage.ObjectStore"/>.</summary>
    ObjectStore,

    /// <summary><see cref="Tedd.Tdfs.Storage.AtomicFile"/> (checkpoints, superblock, pointers).</summary>
    AtomicFile,
}

/// <summary>Kind of on-disk damage applied by DSK-002.</summary>
public enum DskDamage
{
    /// <summary>Only a strict prefix of a record's bytes reached the device.</summary>
    TornPrefix,

    /// <summary>One byte XOR 0xFF (bit rot / misdirected write).</summary>
    BitFlip,

    /// <summary>An 8-byte run zeroed (lost sector fragment).</summary>
    ZeroRun,
}

/// <summary>Thrown by the interposed medium / fault injector when the simulated process "loses power".</summary>
internal sealed class SimulatedPowerLossException : Exception
{
    public SimulatedPowerLossException(string message)
        : base(message)
    {
    }

    public SimulatedPowerLossException()
        : base("simulated power loss")
    {
    }

    public SimulatedPowerLossException(string message, Exception inner)
        : base(message, inner)
    {
    }
}

/// <summary>Mutating medium operations that delimit crash points (every write and durability barrier).</summary>
internal enum BoundaryKind
{
    Open,
    Write,
    SetLength,
    FlushFile,
    FlushDirectory,
    Rename,
    Delete,
    CreateDirectory,
}

/// <summary>One attempted mutating medium operation.</summary>
internal readonly record struct Boundary(int Index, BoundaryKind Kind, string Path, long Offset, long Length)
{
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"#{Index} {Kind} '{Path}' @{Offset}+{Length}");
}

/// <summary>
/// <see cref="IStorageMedium"/> interposer used by the DSK crash matrices: it logs every mutating operation as a crash
/// boundary, can "lose power" immediately before boundary <see cref="CrashAt"/> (after which every call fails, as for a
/// dead process), can inject an exception at any boundary, and can emulate short / zero-progress writes on media that
/// have no native switch for them (the file-system medium).
/// </summary>
internal sealed class InterposedMedium : IStorageMedium
{
    private readonly Lock _gate = new();
    private readonly List<Boundary> _log = [];
    private readonly HashSet<InterposedFile> _open = [];
    private readonly Dictionary<string, int> _unflushedWrites = new(StringComparer.Ordinal);
    private int _unflushedNamespace;

    public InterposedMedium(IStorageMedium inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        Inner = inner;
    }

    public IStorageMedium Inner { get; }

    /// <summary>0-based boundary index before which power is lost (null = never).</summary>
    public int? CrashAt { get; set; }

    /// <summary>Optional error injected at a boundary (after logging it, before performing it).</summary>
    public Func<Boundary, Exception?>? Inject { get; set; }

    /// <summary>Emulated: the next N writes only write (and report) the first half of their bytes.</summary>
    public int ShortWrites { get; set; }

    /// <summary>Emulated: the next N writes write nothing and report 0 bytes.</summary>
    public int ZeroProgressWrites { get; set; }

    /// <summary>Emulated: the next N reads fail with a device I/O error (EIO).</summary>
    public int FailReads { get; set; }

    /// <summary>Reads failed through <see cref="FailReads"/>.</summary>
    public int ReadFailuresInjected { get; private set; }

    /// <summary>Writes that reported fewer bytes than requested (native or emulated).</summary>
    public int ShortWritesObserved { get; private set; }

    public bool Crashed { get; private set; }

    public Boundary? CrashedBefore { get; private set; }

    /// <summary>Unflushed file writes + unflushed namespace changes outstanding when power was lost.</summary>
    public int UnflushedAtCrash { get; private set; }

    public IReadOnlyList<Boundary> Log
    {
        get
        {
            lock (_gate)
                return [.. _log];
        }
    }

    /// <summary>Boundaries attempted so far (excluding the one that lost power).</summary>
    public int BoundaryCount
    {
        get
        {
            lock (_gate)
                return _log.Count;
        }
    }

    public int UnflushedNow
    {
        get
        {
            lock (_gate)
                return _unflushedWrites.Values.Sum() + _unflushedNamespace;
        }
    }

    public string Name => Inner.Name;

    public long CapacityBytes => Inner.CapacityBytes;

    public long UsedBytes => Inner.UsedBytes;

    /// <summary>Closes every handle opened through this interposer without flushing (process death).</summary>
    public async ValueTask AbandonHandlesAsync()
    {
        InterposedFile[] open;
        lock (_gate)
        {
            open = [.. _open];
            _open.Clear();
        }

        foreach (InterposedFile file in open)
        {
            try
            {
                await file.Inner.DisposeAsync();
            }
            catch (ObjectDisposedException)
            {
                // Already invalidated by SimulatedMedium.Crash.
            }
        }
    }

    public void PowerLossNow(string reason)
    {
        lock (_gate)
        {
            if (Crashed)
                return;
            Crashed = true;
            UnflushedAtCrash = _unflushedWrites.Values.Sum() + _unflushedNamespace;
        }

        _ = reason;
    }

    internal void Enter(BoundaryKind kind, string path, long offset = 0, long length = 0)
    {
        Exception? inject;
        lock (_gate)
        {
            if (Crashed)
                throw new SimulatedPowerLossException($"process lost power before {kind} '{path}'");
            var boundary = new Boundary(_log.Count, kind, path, offset, length);
            if (CrashAt == boundary.Index)
            {
                Crashed = true;
                CrashedBefore = boundary;
                UnflushedAtCrash = _unflushedWrites.Values.Sum() + _unflushedNamespace;
                throw new SimulatedPowerLossException($"power lost before {boundary}");
            }

            _log.Add(boundary);
            inject = Inject?.Invoke(boundary);
        }

        if (inject is not null)
            throw inject;
    }

    internal void ThrowIfDead()
    {
        lock (_gate)
        {
            if (Crashed)
                throw new SimulatedPowerLossException("process lost power");
        }
    }

    internal void OnWritten(string path)
    {
        lock (_gate)
            _unflushedWrites[path] = _unflushedWrites.GetValueOrDefault(path) + 1;
    }

    internal void OnFlushed(string path)
    {
        lock (_gate)
            _unflushedWrites[path] = 0;
    }

    internal void OnNamespaceChange()
    {
        lock (_gate)
            _unflushedNamespace++;
    }

    internal void OnDirectoryFlushed()
    {
        lock (_gate)
            _unflushedNamespace = 0;
    }

    internal (bool Zero, bool Short) TakeWriteFault()
    {
        lock (_gate)
        {
            if (ZeroProgressWrites > 0)
            {
                ZeroProgressWrites--;
                return (true, false);
            }

            if (ShortWrites > 0)
            {
                ShortWrites--;
                return (false, true);
            }

            return (false, false);
        }
    }

    internal bool TakeReadFault()
    {
        lock (_gate)
        {
            if (FailReads <= 0)
                return false;
            FailReads--;
            ReadFailuresInjected++;
            return true;
        }
    }

    internal void CountShortWrite()
    {
        lock (_gate)
            ShortWritesObserved++;
    }

    internal void Untrack(InterposedFile file)
    {
        lock (_gate)
            _open.Remove(file);
    }

    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        if (mode == StorageOpenMode.OpenExisting)
        {
            ThrowIfDead();
        }
        else
        {
            Enter(BoundaryKind.Open, path, (long)mode);
            OnNamespaceChange();
        }

        IStorageFile inner = await Inner.OpenAsync(path, mode, ct);
        var file = new InterposedFile(this, inner);
        lock (_gate)
            _open.Add(file);
        return file;
    }

    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ThrowIfDead();
        return Inner.ExistsAsync(path, ct);
    }

    public async ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        Enter(BoundaryKind.Delete, path);
        await Inner.DeleteAsync(path, ct);
        OnNamespaceChange();
    }

    public async ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        Enter(BoundaryKind.Rename, from + " -> " + to);
        await Inner.RenameAsync(from, to, overwrite, ct);
        OnNamespaceChange();
    }

    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfDead();
        return Inner.ListAsync(directory, ct);
    }

    public async ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        Enter(BoundaryKind.CreateDirectory, directory);
        await Inner.CreateDirectoryAsync(directory, ct);
        OnNamespaceChange();
    }

    public async ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        Enter(BoundaryKind.FlushDirectory, directory);
        await Inner.FlushDirectoryAsync(directory, ct);
        OnDirectoryFlushed();
    }

    public override string ToString() => $"Interposed({Inner})";
}

/// <summary>File handle of an <see cref="InterposedMedium"/>.</summary>
internal sealed class InterposedFile : IStorageFile
{
    private readonly InterposedMedium _medium;

    public InterposedFile(InterposedMedium medium, IStorageFile inner)
    {
        _medium = medium;
        Inner = inner;
    }

    public IStorageFile Inner { get; }

    public string Path => Inner.Path;

    public long Length
    {
        get
        {
            _medium.ThrowIfDead();
            return Inner.Length;
        }
    }

    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        _medium.ThrowIfDead();
        if (_medium.TakeReadFault())
            throw StorageErrors.DeviceError("read", Path);
        return Inner.ReadAsync(offset, buffer, ct);
    }

    public async ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        _medium.Enter(BoundaryKind.Write, Path, offset, data.Length);
        (bool zero, bool shortWrite) = _medium.TakeWriteFault();
        if (zero && !data.IsEmpty)
        {
            _medium.CountShortWrite();
            return 0;
        }

        ReadOnlyMemory<byte> slice = shortWrite && data.Length > 1 ? data[..((data.Length + 1) / 2)] : data;
        int written = await Inner.WriteAsync(offset, slice, ct);
        if (written < data.Length)
            _medium.CountShortWrite();
        _medium.OnWritten(Path);
        return written;
    }

    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        _medium.Enter(BoundaryKind.FlushFile, Path);
        await Inner.FlushAsync(ct);
        _medium.OnFlushed(Path);
    }

    public async ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        _medium.Enter(BoundaryKind.SetLength, Path, length);
        await Inner.SetLengthAsync(length, ct);
        _medium.OnWritten(Path);
    }

    public async ValueTask DisposeAsync()
    {
        _medium.Untrack(this);
        try
        {
            await Inner.DisposeAsync();
        }
        catch (ObjectDisposedException)
        {
            // Handle invalidated by a simulated crash.
        }
    }
}

/// <summary>
/// One local disk for a storage-engine test: either a seeded <see cref="SimulatedMedium"/> or a fresh directory under
/// the test data root. <see cref="Medium"/> is the interposer every component is opened on; <see cref="PowerLoss"/>
/// ends the "process" and returns a medium as it would be seen after restart.
/// </summary>
internal sealed class DskMedium : IAsyncDisposable
{
    private static long s_counter;

    private DskMedium(DskMediumKind kind, SimulatedMedium? simulated, string? root, long capacity)
    {
        Kind = kind;
        Simulated = simulated;
        Root = root;
        Capacity = capacity;
        Raw = simulated is not null ? simulated : new FileSystemMedium(root!, capacity);
        Medium = new InterposedMedium(Raw);
    }

    public DskMediumKind Kind { get; }

    public SimulatedMedium? Simulated { get; }

    public string? Root { get; }

    public long Capacity { get; }

    /// <summary>The underlying medium (a new <see cref="FileSystemMedium"/> instance after every restart, so no caches survive).</summary>
    public IStorageMedium Raw { get; private set; }

    /// <summary>The interposer components must be opened on.</summary>
    public InterposedMedium Medium { get; private set; }

    /// <summary>Simulated faults (native switches for the in-memory disk).</summary>
    public SimulatedMediumFaults? SimulatedFaults => Simulated?.Faults;

    public static DskMedium Create(DskMediumKind kind, string scope, ulong seed, long capacity = 1L << 30)
    {
        if (kind == DskMediumKind.Simulated)
            return new DskMedium(kind, new SimulatedMedium("dsk-" + scope, capacity, new SeededRandom(seed)), null, capacity);

        long n = Interlocked.Increment(ref s_counter);
        string safe = string.Concat(scope.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
        string root = Path.Combine(DskHelpers.TestRoot, "dsk", safe, string.Create(CultureInfo.InvariantCulture, $"{Environment.ProcessId}-{n}-{seed:x}"));
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
        Directory.CreateDirectory(root);
        return new DskMedium(kind, null, root, capacity);
    }

    /// <summary>
    /// Ends the current process image: the in-memory disk keeps what <paramref name="mode"/> allows; the real directory
    /// keeps everything the process handed to the OS (only <see cref="CrashMode.KeepUnflushed"/> is physically possible
    /// without a power-cut rig). Returns the medium view a restarted process sees.
    /// </summary>
    public async Task<InterposedMedium> PowerLossAsync(CrashMode mode)
    {
        Medium.PowerLossNow("power loss");
        if (Simulated is not null)
        {
            Simulated.Crash(mode);
            await Medium.AbandonHandlesAsync();
            Medium = new InterposedMedium(Simulated);
            return Medium;
        }

        if (mode != CrashMode.KeepUnflushed)
            throw new InvalidOperationException($"A real directory can only model a process kill (KeepUnflushed), not {mode}.");
        await Medium.AbandonHandlesAsync();
        Raw = new FileSystemMedium(Root!, Capacity);
        Medium = new InterposedMedium(Raw);
        return Medium;
    }

    /// <summary>Clean process restart: abandons handles (caller disposed its components) and starts a cold medium view.</summary>
    public async Task<InterposedMedium> RestartAsync()
    {
        await Medium.AbandonHandlesAsync();
        if (Simulated is null)
            Raw = new FileSystemMedium(Root!, Capacity);
        Medium = new InterposedMedium(Raw);
        return Medium;
    }

    public async ValueTask DisposeAsync()
    {
        await Medium.AbandonHandlesAsync();
        if (Root is not null)
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch (IOException)
            {
                // Best effort cleanup of the per-case directory.
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    public override string ToString() => Kind == DskMediumKind.Simulated ? $"Simulated({Simulated!.Name})" : $"FileSystem({Root})";
}

/// <summary>Records every fault-site hit of a storage component and can "lose power" at a chosen site occurrence.</summary>
internal sealed class RecordingFaultInjector : IFaultInjector
{
    private readonly Lock _gate = new();
    private readonly List<FaultContext> _hits = [];
    private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);

    /// <summary>Site at which to throw <see cref="SimulatedPowerLossException"/> (null = never).</summary>
    public string? CrashSite { get; set; }

    /// <summary>1-based occurrence of <see cref="CrashSite"/> that crashes.</summary>
    public int CrashOccurrence { get; set; } = 1;

    public bool Triggered { get; private set; }

    public FaultContext? TriggeredContext { get; private set; }

    public bool IsEnabled => true;

    public IReadOnlyList<FaultContext> Hits
    {
        get
        {
            lock (_gate)
                return [.. _hits];
        }
    }

    public int Count(string site)
    {
        lock (_gate)
            return _counts.GetValueOrDefault(site);
    }

    public ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
    {
        lock (_gate)
        {
            _hits.Add(context);
            int n = _counts.GetValueOrDefault(context.Site) + 1;
            _counts[context.Site] = n;
            if (!Triggered && CrashSite is not null && string.Equals(context.Site, CrashSite, StringComparison.Ordinal) && n == CrashOccurrence)
            {
                Triggered = true;
                TriggeredContext = context;
                throw new SimulatedPowerLossException($"power lost at fault site {context.Site} (occurrence {n})");
            }
        }

        return default;
    }
}

/// <summary>Deterministic AES-GCM record cipher for encrypted-store variants (test key material only).</summary>
internal sealed class TestAesGcmCipher : IRecordCipher
{
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private long _nonceCounter;

    public int Overhead => NonceSize + TagSize;

    public void Encrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> plain, Span<byte> output, uint keyVersion)
    {
        Span<byte> nonce = output[..NonceSize];
        nonce.Clear();
        BitConverter.TryWriteBytes(nonce, Interlocked.Increment(ref _nonceCounter));
        using var gcm = new AesGcm(Key(keyVersion), TagSize);
        gcm.Encrypt(nonce, plain, output.Slice(NonceSize, plain.Length), output.Slice(NonceSize + plain.Length, TagSize), aad);
    }

    public bool TryDecrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, Span<byte> plain, uint keyVersion)
    {
        if (sealedData.Length < Overhead)
            return false;
        int length = sealedData.Length - Overhead;
        using var gcm = new AesGcm(Key(keyVersion), TagSize);
        try
        {
            gcm.Decrypt(sealedData[..NonceSize], sealedData.Slice(NonceSize, length), sealedData.Slice(NonceSize + length, TagSize), plain[..length], aad);
            return true;
        }
        catch (AuthenticationTagMismatchException)
        {
            return false;
        }
    }

    private static byte[] Key(uint keyVersion)
    {
        var key = new byte[32];
        new SeededRandom(0xC1F3_0000UL + keyVersion).Fill(key);
        return key;
    }
}

/// <summary>Reference model of what a <see cref="Journal"/> may legally contain after recovery.</summary>
internal sealed class JournalModel
{
    // Candidate sequences: a recovered journal must be a prefix of one candidate with at least MinLength records.
    private readonly List<Candidate> _candidates = [];

    public JournalModel()
    {
        _candidates.Add(new Candidate([], 0));
    }

    private Candidate Current => _candidates[^1];

    /// <summary>Records currently appended (latest version).</summary>
    public IReadOnlyList<ModelRecord> Records => Current.Records;

    /// <summary>Records whose durability was acknowledged by a completed flush.</summary>
    public int AckedCount => Current.MinLength;

    public int Appends { get; private set; }

    /// <summary>Call before <see cref="Journal.AppendAsync"/>: the record may exist after a crash from now on.</summary>
    public ModelRecord BeginAppend(byte[] payload)
    {
        var record = new ModelRecord(payload) { Index = Current.Records.Count == 0 ? null : Current.Records[^1].Index + 1 };
        Current.Records.Add(record);
        Appends++;
        return record;
    }

    /// <summary>Call after <see cref="Journal.AppendAsync"/> returned.</summary>
    public void Appended(ModelRecord record, long index)
    {
        if (record.Index is { } expected)
            Assert.True(expected == index, $"AppendAsync returned index {index}; the previous record implies {expected} (indices must be consecutive).");
        record.Index = index;
    }

    /// <summary>Call after <see cref="Journal.FlushAsync"/> returned: everything appended is durable; older versions are gone.</summary>
    public void Flushed()
    {
        Candidate current = Current;
        _candidates.Clear();
        _candidates.Add(current with { MinLength = current.Records.Count });
    }

    /// <summary>Call before <see cref="Journal.TruncateAfterAsync"/>.</summary>
    public void BeginTruncateAfter(long index)
    {
        Candidate current = Current;
        var kept = current.Records.Where(r => r.Index is { } i && i <= index).ToList();
        int min = Math.Min(current.MinLength, kept.Count);
        _candidates.Add(new Candidate(kept, min));
    }

    /// <summary>Checks a recovered record list against the model.</summary>
    public void AssertAllowed(IReadOnlyList<JournalRecord> actual, string context)
    {
        for (int i = 1; i < actual.Count; i++)
        {
            Assert.True(actual[i].Index == actual[i - 1].Index + 1,
                $"{context}: recovered indices are not consecutive ({actual[i - 1].Index} then {actual[i].Index}).");
        }

        var reasons = new List<string>();
        foreach (Candidate candidate in _candidates)
        {
            string? why = Matches(candidate, actual);
            if (why is null)
                return;
            reasons.Add(why);
        }

        Assert.Fail($"{context}: recovered journal ({actual.Count} records) is not an allowed complete prefix: {string.Join(" | ", reasons)}");
    }

    private static string? Matches(Candidate candidate, IReadOnlyList<JournalRecord> actual)
    {
        if (actual.Count < candidate.MinLength)
            return $"lost acknowledged records ({actual.Count} < {candidate.MinLength} flushed)";
        if (actual.Count > candidate.Records.Count)
            return $"more records than ever appended ({actual.Count} > {candidate.Records.Count})";
        for (int i = 0; i < actual.Count; i++)
        {
            ModelRecord expected = candidate.Records[i];
            if (expected.Index is { } index && index != actual[i].Index)
                return $"record {i} has index {actual[i].Index}, expected {index}";
            if (!actual[i].Payload.Span.SequenceEqual(expected.Payload))
                return $"record {i} (index {actual[i].Index}) payload differs from what was appended";
        }

        return null;
    }

    internal sealed class ModelRecord(byte[] payload)
    {
        public byte[] Payload { get; } = payload;

        public long? Index { get; set; }
    }

    private sealed record Candidate(List<ModelRecord> Records, int MinLength);
}

/// <summary>Reference model of an <see cref="ObjectStore"/> (what each object id may legally resolve to after recovery).</summary>
internal sealed class ObjectStoreModel
{
    private readonly Dictionary<ObjectId, Entry> _entries = [];

    public IReadOnlyCollection<Entry> Entries => _entries.Values;

    public Entry BeginPut(ObjectId id, VolumeId volume, StagingId staging, byte[] data)
    {
        var entry = new Entry(id, volume, staging, data) { State = ObjectState.PutPending };
        _entries[id] = entry;
        return entry;
    }

    public void PutAcked(Entry entry, ObjectRecordInfo info, string context)
    {
        Assert.Equal(entry.Id, info.Id);
        Assert.Equal(entry.Volume, info.Volume);
        Assert.Equal(entry.Staging, info.Staging);
        Assert.True(info.Length == entry.Data.Length, $"{context}: receipt length {info.Length} != {entry.Data.Length}");
        Assert.True(info.Checksum == ObjectChecksum.Compute(entry.Data), $"{context}: receipt checksum does not match the plaintext");
        entry.State = ObjectState.Acked;
    }

    public void BeginDelete(ObjectId id) => _entries[id].State = ObjectState.DeletePending;

    public void Deleted(ObjectId id) => _entries[id].State = ObjectState.Deleted;

    /// <summary>Verifies a freshly opened store against the model (exact bytes, no resurrection, no phantom records).</summary>
    public async Task AssertAllowedAsync(ObjectStore store, string context)
    {
        IReadOnlyList<ObjectRecordInfo> listed = store.List();
        foreach (ObjectRecordInfo info in listed)
        {
            Assert.True(_entries.TryGetValue(info.Id, out Entry? entry), $"{context}: index lists unknown object {info.Id} (phantom/misdirected record).");
            Assert.True(entry!.State != ObjectState.Deleted, $"{context}: durably deleted object {info.Id} was resurrected in the index.");
            Assert.True(info.Length == entry.Data.Length && info.Checksum == ObjectChecksum.Compute(entry.Data),
                $"{context}: index entry {info.Id} has length {info.Length}/checksum that do not match the stored plaintext.");
            Assert.Equal(entry.Volume, info.Volume);
            Assert.Equal(entry.Staging, info.Staging);
        }

        foreach (Entry entry in _entries.Values)
        {
            bool contains = await store.ContainsAsync(entry.Id);
            switch (entry.State)
            {
                case ObjectState.Acked:
                    Assert.True(contains, $"{context}: acknowledged object {entry.Id} is missing after recovery.");
                    DskHelpers.AssertBytes(entry.Data, await DskHelpers.ReadObjectAsync(store, entry.Id, entry.Data.Length), $"{context}: acknowledged object {entry.Id}");
                    break;
                case ObjectState.Deleted:
                    Assert.False(contains, $"{context}: durably deleted object {entry.Id} was resurrected.");
                    await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => DskHelpers.ReadObjectAsync(store, entry.Id, entry.Data.Length), DfsErrorCode.Integrity);
                    break;
                default:
                    if (contains)
                        DskHelpers.AssertBytes(entry.Data, await DskHelpers.ReadObjectAsync(store, entry.Id, entry.Data.Length), $"{context}: unacknowledged object {entry.Id} is indexed, so it must be complete");
                    break;
            }
        }
    }

    internal sealed class Entry(ObjectId id, VolumeId volume, StagingId staging, byte[] data)
    {
        public ObjectId Id { get; } = id;

        public VolumeId Volume { get; } = volume;

        public StagingId Staging { get; } = staging;

        public byte[] Data { get; } = data;

        public ObjectState State { get; set; }
    }

    internal enum ObjectState
    {
        PutPending,
        Acked,
        DeletePending,
        Deleted,
    }
}

/// <summary>Result of one crash-matrix family.</summary>
internal sealed record CrashMatrixReport(string Name, int Boundaries, int PointsRun, int PointsWithUnflushedData, int CrashesReached, IReadOnlyList<string> BoundaryKinds)
{
    public void Log() =>
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{Name}: {Boundaries} boundaries, {PointsRun} crash points run, {CrashesReached} crashes reached, {PointsWithUnflushedData} with unflushed data at the crash; kinds: {string.Join(",", BoundaryKinds)}");
}

/// <summary>Shared DSK helpers (internal static per test conventions).</summary>
internal static class DskHelpers
{
    public const string Volume = "vol";

    public static readonly VolumeId VolumeA = new(new Guid("6a1f6f63-0000-4000-8000-00000000000a"));
    public static readonly VolumeId VolumeB = new(new Guid("6a1f6f63-0000-4000-8000-00000000000b"));

    /// <summary>Root for real-directory media: $TDFS_TEST_ROOT (D:\Workspaces\AI\tdfs-tests in this repo's setup).</summary>
    public static string TestRoot => new TestClusterOptions().ResolveDataRoot();

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary><see cref="AssertEx.BytesEqual"/> for arrays (usable with awaited arguments).</summary>
    public static void AssertBytes(byte[] expected, byte[]? actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Every (medium, crash mode) combination that is physically meaningful.</summary>
    public static IEnumerable<(DskMediumKind Kind, CrashMode Mode)> CrashCases()
    {
        foreach (CrashMode mode in Enum.GetValues<CrashMode>())
            yield return (DskMediumKind.Simulated, mode);
        yield return (DskMediumKind.FileSystem, CrashMode.KeepUnflushed);
    }

    /// <summary>
    /// Small-segment store options. The object-size limit is kept at half a segment (at most 1 MiB) so a maximal record
    /// (header, payload and per-unit tags) always fits one segment; the API does not define what a store does with an
    /// object larger than its segment size, so no test relies on that.
    /// </summary>
    public static ObjectStoreOptions SmallStoreOptions(long segmentSize = 512 << 10, long reserved = 0, IRecordCipher? cipher = null) =>
        new(SegmentSize: segmentSize, ReservedBytes: reserved, MaxObjectSize: (int)Math.Min(1 << 20, segmentSize / 2), Cipher: cipher);

    /// <summary>True for the typed capacity failure, or a disk-full I/O error (ENOSPC), possibly wrapped.</summary>
    public static bool IsCapacityFailure(Exception? ex) =>
        ex is DfsCapacityException or DfsException { ErrorCode: DfsErrorCode.CapacityExhausted } || StorageErrors.IsDiskFull(ex);

    public static ObjectId Oid(ulong n) => new(((UInt128)0xD5C0_0000_0000_0000UL << 64) | n);

    public static async Task<byte[]> ReadObjectAsync(ObjectStore store, ObjectId id, int length, long offset = 0)
    {
        var buffer = new byte[length];
        int total = 0;
        while (total < length)
        {
            int n = await store.ReadAsync(id, offset + total, buffer.AsMemory(total), Ct);
            if (n == 0)
                break;
            total += n;
        }

        return total == length ? buffer : buffer[..total];
    }

    /// <summary>Opens a store, runs <paramref name="body"/>, disposes it.</summary>
    public static async Task WithStoreAsync(IStorageMedium medium, ObjectStoreOptions options, Func<ObjectStore, Task> body, IFaultInjector? faults = null)
    {
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", options, faults ?? NoFaults.Instance, Ct);
        try
        {
            await body(store);
        }
        finally
        {
            await store.DisposeAsync();
        }
    }

    public static async Task WithJournalAsync(IStorageMedium medium, string directory, JournalOptions options, Func<Journal, Task> body)
    {
        Journal journal = await Journal.OpenAsync(medium, directory, options, Ct);
        try
        {
            await body(journal);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    /// <summary>Recursively lists the files under <paramref name="directory"/> (medium-relative paths).</summary>
    public static async Task<List<string>> ListFilesAsync(IStorageMedium medium, string directory)
    {
        var files = new List<string>();
        if (directory.Length != 0 && !await medium.ExistsAsync(directory))
            return files;
        foreach (string name in await medium.ListAsync(directory))
        {
            string path = StoragePath.Combine(directory, name);
            IReadOnlyList<string>? children = null;
            try
            {
                children = await medium.ListAsync(path);
            }
            catch (IOException)
            {
                // Not a directory.
            }

            if (children is null)
                files.Add(path);
            else
                files.AddRange(await ListFilesAsync(medium, path));
        }

        files.Sort(StringComparer.Ordinal);
        return files;
    }

    public static async Task<byte[]> ReadFileAsync(IStorageMedium medium, string path) =>
        await medium.TryReadAllBytesAsync(path) ?? throw new FileNotFoundException(path);

    /// <summary>Durable image of every file under <paramref name="directory"/>.</summary>
    public static async Task<SortedDictionary<string, byte[]>> SnapshotFilesAsync(IStorageMedium medium, string directory)
    {
        var images = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (string file in await ListFilesAsync(medium, directory))
            images[file] = await ReadFileAsync(medium, file);
        return images;
    }

    /// <summary>Replaces the files under <paramref name="directory"/> with <paramref name="images"/> durably.</summary>
    public static async Task RestoreFilesAsync(IStorageMedium medium, string directory, IReadOnlyDictionary<string, byte[]> images)
    {
        foreach (string file in await ListFilesAsync(medium, directory))
        {
            if (!images.ContainsKey(file))
                await medium.DeleteAsync(file);
        }

        foreach ((string path, byte[] bytes) in images)
            await WriteImageAsync(medium, path, bytes);
    }

    /// <summary>Overwrites one file with exact bytes (truncate, write, flush, directory flush).</summary>
    public static async Task WriteImageAsync(IStorageMedium medium, string path, byte[] bytes)
    {
        await medium.CreateDirectoryAsync(StoragePath.GetParent(StoragePath.Normalize(path)));
        await using (IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.Truncate))
        {
            await file.WriteFullyAsync(0, bytes);
            await file.SetLengthAsync(bytes.Length);
            await file.FlushAsync();
        }

        await medium.FlushDirectoryAsync(StoragePath.GetParent(StoragePath.Normalize(path)));
    }

    /// <summary>Finds every occurrence of <paramref name="needle"/> in <paramref name="haystack"/>.</summary>
    public static List<int> FindAll(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle)
    {
        var hits = new List<int>();
        if (needle.Length == 0)
            return hits;
        int start = 0;
        while (start <= haystack.Length - needle.Length)
        {
            int i = haystack[start..].IndexOf(needle);
            if (i < 0)
                break;
            hits.Add(start + i);
            start += i + 1;
        }

        return hits;
    }

    /// <summary>
    /// Locates where a payload landed on disk: the file and offset of its unique occurrence, searched among the files
    /// written while it was persisted (unencrypted components store plaintext payloads verbatim).
    /// </summary>
    public static (string Path, int Offset)? LocatePayload(IReadOnlyDictionary<string, byte[]> images, byte[] payload)
    {
        (string, int)? found = null;
        foreach ((string path, byte[] image) in images)
        {
            List<int> hits = FindAll(image, payload);
            if (hits.Count > 1)
                return null;
            if (hits.Count == 1)
            {
                if (found is not null)
                    return null;
                found = (path, hits[0]);
            }
        }

        return found;
    }

    /// <summary>The contiguous run of written bytes (from <paramref name="writes"/>) on <paramref name="path"/> that contains <paramref name="anchor"/>.</summary>
    public static (long Start, long End) ContiguousRun(IEnumerable<Boundary> writes, string path, long anchor)
    {
        var intervals = writes
            .Where(w => w.Kind == BoundaryKind.Write && string.Equals(w.Path, path, StringComparison.Ordinal) && w.Length > 0)
            .Select(w => (Start: w.Offset, End: w.Offset + w.Length))
            .OrderBy(i => i.Start)
            .ToList();
        var merged = new List<(long Start, long End)>();
        foreach ((long s, long e) in intervals)
        {
            if (merged.Count > 0 && s <= merged[^1].End)
                merged[^1] = (merged[^1].Start, Math.Max(merged[^1].End, e));
            else
                merged.Add((s, e));
        }

        foreach ((long s, long e) in merged)
        {
            if (anchor >= s && anchor < e)
                return (s, e);
        }

        return merged.Count == 0 ? (anchor, anchor) : merged[^1];
    }

    /// <summary>The file most written in <paramref name="writes"/> and the union extent of those writes.</summary>
    public static (string Path, long Start, long End)? MainWriteRange(IEnumerable<Boundary> writes)
    {
        var byFile = writes.Where(w => w.Kind == BoundaryKind.Write && w.Length > 0)
            .GroupBy(w => w.Path, StringComparer.Ordinal)
            .Select(g => (Path: g.Key, Bytes: g.Sum(w => w.Length), Start: g.Min(w => w.Offset), End: g.Max(w => w.Offset + w.Length)))
            .OrderByDescending(g => g.Bytes)
            .ToList();
        return byFile.Count == 0 ? null : (byFile[0].Path, byFile[0].Start, byFile[0].End);
    }

    /// <summary>
    /// Runs a crash matrix: a dry run counts the medium boundaries of <paramref name="scenario"/>; then, for every
    /// boundary k (and once after the last), a fresh medium runs <paramref name="setup"/> without faults, then the
    /// scenario with power lost immediately before its k-th boundary, applies <paramref name="mode"/>, and calls
    /// <paramref name="verify"/> on the medium a restarted process sees.
    /// </summary>
    public static async Task<CrashMatrixReport> RunCrashMatrixAsync<TModel>(
        string name,
        DskMediumKind kind,
        CrashMode mode,
        ulong seed,
        Func<TModel> newModel,
        Func<InterposedMedium, TModel, Task>? setup,
        Func<InterposedMedium, TModel, Task> scenario,
        Func<InterposedMedium, TModel, string, Task> verify,
        int maxPoints = 4000)
    {
        int boundaries;
        var kinds = new SortedSet<string>(StringComparer.Ordinal);
        await using (DskMedium dry = DskMedium.Create(kind, name + "-dry", seed))
        {
            TModel model = newModel();
            if (setup is not null)
                await setup(dry.Medium, model);
            int before = dry.Medium.BoundaryCount;
            await scenario(dry.Medium, model);
            boundaries = dry.Medium.BoundaryCount - before;
            foreach (Boundary b in dry.Medium.Log.Skip(before))
                kinds.Add(b.Kind.ToString());
        }

        Assert.True(boundaries > 0, $"{name}: the scenario performed no medium writes or durability barriers; nothing to crash.");
        Assert.True(boundaries <= maxPoints, $"{name}: {boundaries} boundaries exceed the bounded matrix size {maxPoints}.");

        int run = 0, withUnflushed = 0, reached = 0;
        for (int k = 0; k <= boundaries; k++)
        {
            await using DskMedium disk = DskMedium.Create(kind, string.Create(CultureInfo.InvariantCulture, $"{name}-{k}"), seed + (ulong)k);
            TModel model = newModel();
            if (setup is not null)
                await setup(disk.Medium, model);
            InterposedMedium medium = disk.Medium;
            medium.CrashAt = medium.BoundaryCount + k;
            bool crashed = false;
            try
            {
                await scenario(medium, model);
            }
            catch (Exception) when (medium.Crashed)
            {
                crashed = true;
            }

            if (k < boundaries)
                Assert.True(crashed, $"{name}: crash point {k}/{boundaries} was not reached (the scenario's boundary sequence is not deterministic).");
            if (crashed)
                reached++;
            string where = crashed ? $"before {medium.CrashedBefore}" : "after the scenario completed";
            if (medium.UnflushedAtCrash > 0 || (!crashed && medium.UnflushedNow > 0))
                withUnflushed++;
            InterposedMedium restarted = await disk.PowerLossAsync(mode);
            await verify(restarted, model, $"{name} [{disk.Kind}/{mode}] crash point {k}/{boundaries} {where}");
            run++;
        }

        var report = new CrashMatrixReport(name, boundaries, run, withUnflushed, reached, [.. kinds]);
        report.Log();
        Assert.Equal(boundaries, reached);
        return report;
    }

    /// <summary>Theory rows: every crash case.</summary>
    public static TheoryData<DskMediumKind, CrashMode> CrashCaseData()
    {
        var data = new TheoryData<DskMediumKind, CrashMode>();
        foreach ((DskMediumKind kind, CrashMode mode) in CrashCases())
            data.Add(kind, mode);
        return data;
    }

    /// <summary>Whole-cluster configuration for I-level node scenarios (real QUIC, real disks).</summary>
    public static TestClusterOptions QuicDisk(int nodeCount = 3) => new()
    {
        NodeCount = nodeCount,
        ControlVoters = Math.Min(3, nodeCount),
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        ScenarioTimeout = TimeSpan.FromSeconds(90),
    };

    /// <summary>Object ids of the extents of a file as seen by the volume leader.</summary>
    public static async Task<IReadOnlyList<ObjectId>> FileObjectsAsync(TestCluster cluster, string path, string volume = Volume)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.True(file is not null, $"'{path}' is not present in the leader's applied state.");
        return file!.Extents.Select(e => e.Object).Distinct().ToList();
    }

    /// <summary>Waits until a condition holds, with an explicit bound and reason.</summary>
    public static Task WaitAsync(TestCluster cluster, Func<Task<bool>> condition, TimeSpan timeout, string because) =>
        cluster.WaitUntilAsync(condition, timeout, because, Ct);

    /// <summary>Reads a whole file through a fresh stream (exercises the live read path rather than helpers).</summary>
    public static async Task<byte[]> ReadViaStreamAsync(DfsVolume volume, string path)
    {
        await using DfsStream stream = await volume.Files.OpenReadAsync(path, Ct);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, Ct);
        return ms.ToArray();
    }

    /// <summary>Restarts every node that is not running (a node may stop itself after a fatal local I/O error).</summary>
    public static async Task RestartStoppedNodesAsync(TestCluster cluster)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            if (node.State is TestNodeState.Crashed or TestNodeState.Stopped)
                await node.RestartAsync();
        }
    }

    /// <summary>Asserts <paramref name="ex"/> is an explicit (typed) failure of a read, never a silent success.</summary>
    public static void AssertExplicitReadFailure(Exception ex, string because)
    {
        Assert.True(ex is DfsIntegrityException or DfsUnavailableException or DfsException { ErrorCode: DfsErrorCode.Integrity or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable },
            $"{because}: expected an explicit integrity/availability failure but got {ex.GetType().Name}: {ex.Message}");
    }
}
