using System.Buffers.Binary;
using System.Security.Cryptography;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Security;

/// <summary>
/// The per-volume data-encryption keys (DEK versions) of this node's object store (ARCHITECTURE §11). The authoritative
/// copies are the wrapped DEKs of each volume's control record (<see cref="VolumeRecord.DataKeys"/>, wrapped by the cluster
/// KEK); this class unwraps them into one <see cref="RecordCipher"/> per volume (record key of version <c>v</c> =
/// HKDF(DEK<sub>v</sub>, <see cref="StorageKeys.ObjectPurpose"/>, v)) and keeps a node-local copy of every wrapped DEK it
/// ever sealed a record with in <see cref="FileName"/>, so the object store can verify (and read) its records before the
/// control state is loaded at start and after a volume's control record is gone.
/// </summary>
/// <remarks>
/// <para>File (inside an <see cref="AtomicFile"/> frame): <c>u32 magic "TDVK" | u16 version | u32 volumeCount |
/// (volumeId(16) | u32 keyCount | (u32 version | bytes wrappedKey)*)*</c>, volumes and versions ascending. Only wrapped
/// keys are stored: without the cluster secret the file reveals nothing.</para>
/// <para>A key version is written to the file before any record is sealed with it (<see cref="PrepareWriteAsync"/>); old
/// versions are never dropped, so every recorded version stays decodable while a record (of an object, snapshot or backup
/// export source) still uses it. Runs on the node loop; the cipher map is additionally guarded for other threads.</para>
/// </remarks>
internal sealed class VolumeDataKeys : IVolumeKeyProvider, IDisposable
{
    /// <summary>Name of the node-local key file inside the data directory.</summary>
    public const string FileName = "volume-keys";

    private const uint Magic = 0x4B564454; // "TDVK"
    private const ushort FormatVersion = 1;
    private const int MaxWrappedKeyLength = 256;

    private readonly IStorageMedium _medium;
    private readonly string _path;
    private readonly byte[] _kek;
    private readonly IRandomSource _random;
    private readonly Func<VolumeId, VolumeRecord?> _lookup;
    private readonly Action<string> _warn;
    private readonly Lock _gate = new();
    private readonly Dictionary<VolumeId, Keys> _volumes = [];
    private readonly SerialGate _persistGate = new();
    private long _generation;
    private long _persisted;
    private bool _disposed;

    private VolumeDataKeys(IStorageMedium medium, string path, byte[] kek, IRandomSource random, Func<VolumeId, VolumeRecord?> lookup, Action<string> warn)
    {
        _medium = medium;
        _path = path;
        _kek = kek;
        _random = random;
        _lookup = lookup;
        _warn = warn;
    }

    /// <summary>
    /// Loads the node-local key file (absent on a new node). A damaged file or a key that does not unwrap is reported
    /// through <paramref name="warn"/> and skipped: the control state supplies every key of an existing volume again, and a
    /// record whose key stays unavailable fails with <see cref="DfsRecoveryKeyMissingException"/> where it is needed.
    /// </summary>
    /// <exception cref="DfsAuthenticationException">The file holds keys and none unwraps with <paramref name="kek"/> (wrong cluster secret).</exception>
    public static async Task<VolumeDataKeys> OpenAsync(IStorageMedium medium, string path, byte[] kek, IRandomSource random,
        Func<VolumeId, VolumeRecord?> lookup, Action<string> warn, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentNullException.ThrowIfNull(kek);
        ArgumentNullException.ThrowIfNull(random);
        ArgumentNullException.ThrowIfNull(lookup);
        ArgumentNullException.ThrowIfNull(warn);
        var keys = new VolumeDataKeys(medium, path, kek, random, lookup, warn);
        byte[]? content;
        try
        {
            content = await AtomicFile.ReadAsync(medium, path, ct);
        }
        catch (Exception ex) when (ex is DfsIntegrityException or DfsProtocolException)
        {
            warn($"The volume key file '{path}' is unreadable ({ex.Message}); keys are taken from the control state.");
            content = null;
        }

        if (content is not null)
            keys.Load(content);
        keys._persisted = keys._generation;
        return keys;
    }

    /// <inheritdoc />
    public IRecordCipher? GetCipher(VolumeId volume)
    {
        VolumeRecord? record = _lookup(volume);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (record is not null)
                MergeLocked(record);
            return _volumes.TryGetValue(volume, out Keys? keys) ? keys.Cipher : null;
        }
    }

    /// <summary>Key versions of <paramref name="volume"/> known to this node, ascending.</summary>
    public IReadOnlyList<uint> VersionsOf(VolumeId volume)
    {
        lock (_gate)
            return _volumes.TryGetValue(volume, out Keys? keys) ? [.. keys.Wrapped.Keys] : [];
    }

    /// <summary>
    /// The key version a new record of <paramref name="record"/>'s volume is sealed with: the newest version this node
    /// knows, which is at least <paramref name="minimum"/> (the version the volume leader required in the staging
    /// capability, 0 for none). When the local control replica has not learned that version yet,
    /// <paramref name="catchUp"/> (a control barrier) runs once. The version is durable in the key file on return.
    /// </summary>
    /// <exception cref="DfsUnavailableException">The required key version is still unknown here (retryable).</exception>
    /// <exception cref="DfsRecoveryKeyMissingException">The volume has no key material on this node.</exception>
    public async Task<uint> PrepareWriteAsync(VolumeRecord record, uint minimum, Func<CancellationToken, Task>? catchUp, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(record);
        uint current = CurrentVersion(record);
        if (current < minimum && catchUp is not null)
        {
            await catchUp(ct);
            current = CurrentVersion(_lookup(record.Id) ?? record);
        }

        if (current == 0)
            throw new DfsRecoveryKeyMissingException($"No data-encryption key of volume '{record.Name}' is available on this node.");
        if (current < minimum)
        {
            throw new DfsUnavailableException(DfsErrorCode.Unavailable,
                $"This node has not learned data-key version {minimum} of volume '{record.Name}' yet (it knows version {current}).");
        }

        await PersistAsync(ct);
        return current;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            foreach (Keys keys in _volumes.Values)
                keys.Cipher.Dispose();
            _volumes.Clear();
        }
    }

    private uint CurrentVersion(VolumeRecord record)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            MergeLocked(record);
            return _volumes.TryGetValue(record.Id, out Keys? keys) ? keys.Ring.CurrentVersion : 0;
        }
    }

    /// <summary>Writes the key file when keys were learned since the last write (serialized; durable on return).</summary>
    private async Task PersistAsync(CancellationToken ct)
    {
        if (Interlocked.Read(ref _persisted) >= Interlocked.Read(ref _generation))
            return;
        using (await _persistGate.EnterAsync(ct))
        {
            long target;
            byte[] content;
            lock (_gate)
            {
                target = _generation;
                if (_persisted >= target)
                    return;
                content = EncodeLocked();
            }

            await AtomicFile.WriteAsync(_medium, _path, content, ct);
            lock (_gate)
                _persisted = Math.Max(_persisted, target);
        }
    }

    /// <summary>Adds the control record's key versions this node does not hold yet.</summary>
    private void MergeLocked(VolumeRecord record)
    {
        Keys? keys = _volumes.GetValueOrDefault(record.Id);
        if (keys is not null && keys.MergedFromControl >= record.DataKeys.Count)
            return;
        foreach (DataKeyVersion version in record.DataKeys)
        {
            if (keys is not null && keys.Wrapped.ContainsKey(version.Version))
                continue;
            keys = AddLocked(record.Id, version.Version, version.WrappedKey.Span, "the control state");
        }

        if (keys is not null)
            keys.MergedFromControl = record.DataKeys.Count;
    }

    private Keys? AddLocked(VolumeId volume, uint version, ReadOnlySpan<byte> wrapped, string source)
    {
        Keys? keys = _volumes.GetValueOrDefault(volume);
        byte[] dataKey;
        try
        {
            dataKey = DataKeys.Unwrap(_kek, volume, version, wrapped);
        }
        catch (Exception ex) when (ex is DfsRecoveryKeyMissingException or DfsIntegrityException)
        {
            _warn($"Data key version {version} of volume {volume} from {source} cannot be unwrapped: {ex.Message}");
            return keys;
        }

        byte[] recordKey = StorageKeys.DeriveKey(dataKey, StorageKeys.ObjectPurpose, version);
        CryptographicOperations.ZeroMemory(dataKey);
        if (keys is null)
        {
            var ring = new KeyRing();
            keys = new Keys(ring, new RecordCipher(ring, _random.Fork("cipher/volume/" + volume.Value.ToString("N"))));
            _volumes.Add(volume, keys);
        }

        try
        {
            keys.Ring.Add(version, recordKey);
            keys.Wrapped[version] = wrapped.ToArray();
            _generation++;
        }
        catch (ArgumentException ex)
        {
            _warn($"Data key version {version} of volume {volume} from {source} conflicts with the key already known: {ex.Message}");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(recordKey);
        }

        return keys;
    }

    private void Load(byte[] content)
    {
        int attempted = 0;
        int unwrapped = 0;
        try
        {
            var reader = new SpanReader(content);
            if (reader.U32() != Magic)
                throw new DfsIntegrityException("bad magic");
            ushort version = reader.U16();
            if (version != FormatVersion)
                throw new DfsIntegrityException($"format version {version} is not supported (this build reads {FormatVersion})");
            uint volumes = reader.U32();
            lock (_gate)
            {
                for (uint i = 0; i < volumes; i++)
                {
                    var volume = new VolumeId(new Guid(reader.Bytes(16)));
                    uint count = reader.U32();
                    for (uint k = 0; k < count; k++)
                    {
                        uint keyVersion = reader.U32();
                        int length = reader.U16();
                        if (length > MaxWrappedKeyLength)
                            throw new DfsIntegrityException("implausible wrapped key length");
                        long before = _generation;
                        attempted++;
                        AddLocked(volume, keyVersion, reader.Bytes(length), "the key file");
                        if (_generation != before)
                            unwrapped++;
                    }
                }

                if (!reader.AtEnd)
                    throw new DfsIntegrityException("trailing bytes");
            }
        }
        catch (Exception ex) when (ex is DfsIntegrityException or ArgumentException or IndexOutOfRangeException)
        {
            _warn($"The volume key file '{_path}' is damaged ({ex.Message}); keys are taken from the control state.");
        }

        // Not one of this node's own data keys unwraps: the configured cluster secret is not the one the node ran with.
        // Refuse to start before the object store could misjudge its records as damaged (SEC-001).
        if (attempted > 0 && unwrapped == 0)
        {
            throw new DfsAuthenticationException(
                $"None of the {attempted} data key(s) in '{_path}' can be unwrapped: the configured cluster secret is not this node's.");
        }
    }

    private byte[] EncodeLocked()
    {
        var volumes = _volumes.OrderBy(v => v.Key.Value).ToArray();
        int size = 4 + 2 + 4;
        foreach ((VolumeId _, Keys keys) in volumes)
        {
            size += 16 + 4;
            foreach (byte[] wrapped in keys.Wrapped.Values)
                size += 4 + 2 + wrapped.Length;
        }

        var output = new byte[size];
        Span<byte> s = output;
        BinaryPrimitives.WriteUInt32LittleEndian(s, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], FormatVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(s[6..], (uint)volumes.Length);
        int pos = 10;
        foreach ((VolumeId volume, Keys keys) in volumes)
        {
            volume.Value.TryWriteBytes(s[pos..]);
            BinaryPrimitives.WriteUInt32LittleEndian(s[(pos + 16)..], (uint)keys.Wrapped.Count);
            pos += 20;
            foreach ((uint version, byte[] wrapped) in keys.Wrapped)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(s[pos..], version);
                BinaryPrimitives.WriteUInt16LittleEndian(s[(pos + 4)..], (ushort)wrapped.Length);
                wrapped.CopyTo(s[(pos + 6)..]);
                pos += 6 + wrapped.Length;
            }
        }

        return output;
    }

    /// <summary>The keys of one volume.</summary>
    private sealed class Keys(KeyRing ring, RecordCipher cipher)
    {
        public KeyRing Ring { get; } = ring;

        public RecordCipher Cipher { get; } = cipher;

        /// <summary>Wrapped DEK by version (what the key file stores).</summary>
        public SortedDictionary<uint, byte[]> Wrapped { get; } = [];

        /// <summary>Number of control-record key versions already merged (the record only grows).</summary>
        public int MergedFromControl { get; set; }
    }

    /// <summary>Bounds-checked little-endian reader over the key file.</summary>
    private ref struct SpanReader(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private int _pos;

        public readonly bool AtEnd => _pos == _data.Length;

        public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Bytes(4));

        public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(Bytes(2));

        public ReadOnlySpan<byte> Bytes(int count)
        {
            if (count < 0 || _data.Length - _pos < count)
                throw new DfsIntegrityException("truncated");
            ReadOnlySpan<byte> slice = _data.Slice(_pos, count);
            _pos += count;
            return slice;
        }
    }
}
