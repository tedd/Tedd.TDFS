using System.Buffers.Binary;
using System.IO.Hashing;

namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of a <see cref="CheckpointStore"/>.</summary>
public sealed record CheckpointStoreOptions
{
    /// <summary>Record cipher for checkpoint contents at rest; null stores plaintext.</summary>
    public IRecordCipher? Cipher { get; init; }

    /// <summary>Fault injector (fault site <see cref="FaultSites.RecoveryCheckpointInstall"/> is hit before a checkpoint becomes current).</summary>
    public IFaultInjector Faults { get; init; } = NoFaults.Instance;

    /// <summary>Node name reported in fault contexts.</summary>
    public string NodeName { get; init; } = "local";

    /// <summary>Raft group name reported in fault contexts.</summary>
    public string? GroupName { get; init; }

    /// <summary>Largest accepted chunk (default 16 MiB).</summary>
    public int MaxChunkSize { get; init; } = 16 << 20;

    /// <summary>Largest accepted metadata blob (default 16 MiB).</summary>
    public int MaxMetadataSize { get; init; } = 16 << 20;
}

/// <summary>Identity of a complete checkpoint.</summary>
/// <param name="Sequence">Store-local sequence number (file name).</param>
/// <param name="Index">Last log index included in the checkpoint.</param>
/// <param name="Term">Term of the entry at <paramref name="Index"/>.</param>
/// <param name="FileLength">Length of the checkpoint file in bytes.</param>
/// <param name="ContentHash">XxHash128 of the concatenated plaintext chunks.</param>
public sealed record CheckpointInfo(ulong Sequence, ulong Index, ulong Term, long FileLength, UInt128 ContentHash);

/// <summary>
/// Per-group state-machine checkpoints written as a stream of chunks. A checkpoint becomes current atomically (via an
/// <see cref="AtomicFile"/> pointer) only after its file is complete and durable; a crash before that leaves the
/// previous checkpoint current. Superseded and incomplete checkpoint files are deleted.
/// </summary>
/// <remarks>
/// <para>File <c>{sequence:x16}.ckpt</c>: header (<c>u32 magic "TCKP" | u16 version | u16 flags | u64 sequence | u64 index |
/// u64 term | u32 keyVersion | u32 metadataLength | metadata | u32 CRC-32C</c>), chunks (<c>u32 magic "TCKC" |
/// u32 chunkIndex | u32 storedLength | u32 0 | stored | u32 CRC-32C</c>), trailer (<c>u32 magic "TCKE" | u32 chunkCount |
/// u64 plaintextBytes | u128 XxHash128 | u32 CRC-32C</c>). With a cipher, the metadata and every chunk are sealed with
/// associated data binding the sequence number and the chunk position.</para>
/// <para>The pointer file <c>CURRENT</c> names the sequence, index, term, file length and content hash. A newer
/// checkpoint may not have a lower index than the current one (an old checkpoint never rolls back newer state).</para>
/// </remarks>
public sealed class CheckpointStore
{
    /// <summary>Name of the pointer file.</summary>
    public const string PointerFileName = "CURRENT";

    /// <summary>File extension of checkpoint files.</summary>
    public const string FileExtension = ".ckpt";

    /// <summary>Checkpoint file format version written by this implementation.</summary>
    public const ushort FormatVersion = 1;

    internal const int ChunkHeaderSize = 16;
    internal const int TrailerSize = 36;
    private const ushort FlagEncrypted = 1;

    private readonly IStorageMedium _medium;
    private readonly string _directory;
    private readonly CheckpointStoreOptions _options;
    private readonly Lock _sync = new();
    private readonly HashSet<ulong> _activeWriters = [];
    private readonly SerialGate _installGate = new();
    private CheckpointInfo? _current;
    private ulong _nextSequence;

    private CheckpointStore(IStorageMedium medium, string directory, CheckpointStoreOptions options)
    {
        _medium = medium;
        _directory = directory;
        _options = options;
    }

    /// <summary>The current checkpoint, or null when none was completed.</summary>
    public CheckpointInfo? Current
    {
        get
        {
            lock (_sync)
                return _current;
        }
    }

    /// <summary>The store directory (medium-relative, normalized).</summary>
    public string Directory => _directory;

    internal CheckpointStoreOptions Options => _options;

    internal IStorageMedium Medium => _medium;

    /// <summary>Opens (or creates) the store in <paramref name="directory"/> and removes incomplete or superseded checkpoint files.</summary>
    /// <exception cref="DfsIntegrityException">The pointer file is corrupt.</exception>
    public static async ValueTask<CheckpointStore> OpenAsync(IStorageMedium medium, string directory, CheckpointStoreOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Faults, nameof(options));
        if (options.MaxChunkSize <= 0 || options.MaxMetadataSize < 0)
            throw new ArgumentOutOfRangeException(nameof(options));
        if (options.Cipher is not null)
            CipherSupport.ValidateOverhead(options.Cipher);

        string dir = StoragePath.Normalize(directory);
        if (!await medium.ExistsAsync(dir, ct))
            await medium.CreateDirectoryDurableAsync(dir, ct);

        var store = new CheckpointStore(medium, dir, options);
        byte[]? pointer = await AtomicFile.ReadAsync(medium, store.PointerPath, ct);
        store._current = pointer is null ? null : DecodePointer(pointer, store.PointerPath);

        ulong maxSequence = store._current?.Sequence ?? 0;
        bool removed = false;
        foreach (string name in await medium.ListAsync(dir, ct))
        {
            if (!TryParseFileName(name, out ulong sequence))
                continue;
            maxSequence = Math.Max(maxSequence, sequence);
            if (sequence != store._current?.Sequence)
            {
                await medium.DeleteAsync(StoragePath.Combine(dir, name), ct);
                removed = true;
            }
        }

        if (removed)
            await medium.FlushDirectoryAsync(dir, ct);
        store._nextSequence = maxSequence + 1;
        return store;
    }

    /// <summary>Starts writing a checkpoint of the state at (<paramref name="index"/>, <paramref name="term"/>).</summary>
    /// <exception cref="InvalidOperationException">The current checkpoint is newer (higher index).</exception>
    public async ValueTask<CheckpointWriter> BeginWriteAsync(ulong index, ulong term, ReadOnlyMemory<byte> metadata, CancellationToken ct = default)
    {
        if (metadata.Length > _options.MaxMetadataSize)
            throw new ArgumentOutOfRangeException(nameof(metadata), $"Metadata of {metadata.Length} bytes exceeds the {_options.MaxMetadataSize}-byte limit.");
        ulong sequence;
        lock (_sync)
        {
            if (_current is { } current && index < current.Index)
                throw new InvalidOperationException($"Checkpoint at index {index} is older than the current checkpoint at index {current.Index}.");
            sequence = _nextSequence++;
            _activeWriters.Add(sequence);
        }

        string path = FilePath(sequence);
        IStorageFile? file = null;
        try
        {
            file = await _medium.OpenAsync(path, StorageOpenMode.Truncate, ct);
            byte[] header = EncodeHeader(sequence, index, term, metadata.Span, out uint keyVersion);
            await file.WriteFullyAsync(0, header, ct);
            return new CheckpointWriter(this, file, path, sequence, index, term, header.Length, keyVersion);
        }
        catch (Exception ex)
        {
            await StorageFormat.CloseQuietlyAsync(file);
            await DeleteQuietlyAsync(path);
            EndWriter(sequence);
            if (StorageErrors.IsDiskFull(ex))
                throw new DfsCapacityException($"Storage is full while writing checkpoint '{path}'.", ex);
            throw;
        }
    }

    /// <summary>Opens the current checkpoint for reading, or returns null when there is none.</summary>
    /// <param name="verifyContent">When true (default) the whole file is verified before the reader is returned, so a
    /// consumer never starts restoring from a damaged checkpoint. Chunks are verified again while streaming.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="DfsIntegrityException">The checkpoint file is missing, truncated or corrupt.</exception>
    public async ValueTask<CheckpointReader?> OpenLatestAsync(bool verifyContent = true, CancellationToken ct = default)
    {
        CheckpointInfo? current;
        string path;
        IStorageFile file;
        while (true)
        {
            current = Current;
            if (current is null)
                return null;
            path = FilePath(current.Sequence);
            try
            {
                file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
                break;
            }
            catch (FileNotFoundException ex)
            {
                // A newer checkpoint may have been installed (and this one deleted) while the file was being opened.
                if (!Equals(Current, current))
                    continue;
                throw new DfsIntegrityException($"Current checkpoint file '{path}' is missing.", ex);
            }
        }

        try
        {
            var reader = await CheckpointReader.OpenAsync(this, file, path, current, ct);
            if (verifyContent)
                await reader.VerifyAsync(ct);
            return reader;
        }
        catch
        {
            await StorageFormat.CloseQuietlyAsync(file);
            throw;
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"CheckpointStore({_medium.Name}:{_directory}, current {_current?.Index.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none"})";

    internal string PointerPath => StoragePath.Combine(_directory, PointerFileName);

    internal string FilePath(ulong sequence) => StoragePath.Combine(_directory, StorageFormat.Hex16(sequence) + FileExtension);

    internal static bool TryParseFileName(string name, out ulong sequence)
    {
        sequence = 0;
        return name.EndsWith(FileExtension, StringComparison.Ordinal)
            && StorageFormat.TryParseHex16(name.AsSpan(0, name.Length - FileExtension.Length), out sequence);
    }

    internal void EndWriter(ulong sequence)
    {
        lock (_sync)
            _activeWriters.Remove(sequence);
    }

    internal async ValueTask DeleteQuietlyAsync(string path)
    {
        try
        {
            await _medium.DeleteAsync(path, CancellationToken.None);
        }
        catch (Exception ex) when (StorageFormat.IsCleanupTolerable(ex))
        {
            // Incomplete checkpoint files are removed at the next open.
        }
    }

    /// <summary>Makes a complete, durable checkpoint file current and removes superseded files.</summary>
    internal async ValueTask InstallAsync(CheckpointWriter writer, CheckpointInfo info, CancellationToken ct)
    {
        using SerialGate.Releaser _ = await _installGate.EnterAsync(ct);
        await _medium.FlushDirectoryAsync(_directory, ct);
        if (_options.Faults.IsEnabled)
        {
            await _options.Faults.HitAsync(new FaultContext(FaultStage.F16, FaultSites.RecoveryCheckpointInstall, _options.NodeName, _options.GroupName,
                DfsOperationKind.RaftSnapshot, Detail: FilePath(info.Sequence)), ct);
        }

        lock (_sync)
        {
            if (_current is { } current && info.Index < current.Index)
                throw new InvalidOperationException($"Checkpoint at index {info.Index} is older than the current checkpoint at index {current.Index}.");
        }

        writer.PointerMayNameThis = true;
        try
        {
            await AtomicFile.WriteAsync(_medium, PointerPath, EncodePointer(info), ct);
        }
        catch
        {
            // The rename may have become visible before the failure: resynchronize with what the pointer says now.
            try
            {
                byte[]? pointer = await AtomicFile.ReadAsync(_medium, PointerPath, CancellationToken.None);
                CheckpointInfo? onDisk = pointer is null ? null : DecodePointer(pointer, PointerPath);
                lock (_sync)
                    _current = onDisk;
                writer.PointerMayNameThis = onDisk?.Sequence == info.Sequence;
            }
            catch (Exception reload) when (StorageFormat.IsCleanupTolerable(reload))
            {
            }

            throw;
        }

        lock (_sync)
            _current = info;

        await RemoveSupersededAsync(info.Sequence);
    }

    private async ValueTask RemoveSupersededAsync(ulong keep)
    {
        try
        {
            bool removed = false;
            foreach (string name in await _medium.ListAsync(_directory))
            {
                if (!TryParseFileName(name, out ulong sequence) || sequence == keep)
                    continue;
                lock (_sync)
                {
                    if (_activeWriters.Contains(sequence))
                        continue;
                }

                await _medium.DeleteAsync(StoragePath.Combine(_directory, name));
                removed = true;
            }

            if (removed)
                await _medium.FlushDirectoryAsync(_directory);
        }
        catch (Exception ex) when (StorageFormat.IsCleanupTolerable(ex))
        {
            // Superseded files are removed at the next open; the new checkpoint is already current.
        }
    }

    private byte[] EncodeHeader(ulong sequence, ulong index, ulong term, ReadOnlySpan<byte> metadata, out uint keyVersion)
    {
        IRecordCipher? cipher = _options.Cipher;
        keyVersion = cipher is null ? 0 : CipherSupport.CurrentVersion(cipher);
        int stored = metadata.Length + (cipher is null ? 0 : cipher.Overhead);
        var header = new byte[40 + stored + 4];
        Span<byte> s = header;
        BinaryPrimitives.WriteUInt32LittleEndian(s, StorageFormat.CheckpointMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], FormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(s[6..], cipher is null ? (ushort)0 : FlagEncrypted);
        BinaryPrimitives.WriteUInt64LittleEndian(s[8..], sequence);
        BinaryPrimitives.WriteUInt64LittleEndian(s[16..], index);
        BinaryPrimitives.WriteUInt64LittleEndian(s[24..], term);
        BinaryPrimitives.WriteUInt32LittleEndian(s[32..], keyVersion);
        BinaryPrimitives.WriteInt32LittleEndian(s[36..], stored);
        if (cipher is null)
        {
            metadata.CopyTo(s[40..]);
        }
        else
        {
            Span<byte> aad = stackalloc byte[12];
            WriteAad(aad, sequence, uint.MaxValue);
            cipher.Encrypt(aad, metadata, s.Slice(40, stored), keyVersion);
        }

        StorageFormat.SealCrc(s, 40 + stored);
        return header;
    }

    /// <summary>Associated data of a sealed checkpoint part: sequence and chunk index (<see cref="uint.MaxValue"/> for the metadata).</summary>
    internal static void WriteAad(Span<byte> aad, ulong sequence, uint chunkIndex)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(aad, sequence);
        BinaryPrimitives.WriteUInt32LittleEndian(aad[8..], chunkIndex);
    }

    private static byte[] EncodePointer(CheckpointInfo info)
    {
        var buffer = new byte[1 + 8 + 8 + 8 + 8 + 16];
        Span<byte> s = buffer;
        s[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(s[1..], info.Sequence);
        BinaryPrimitives.WriteUInt64LittleEndian(s[9..], info.Index);
        BinaryPrimitives.WriteUInt64LittleEndian(s[17..], info.Term);
        BinaryPrimitives.WriteInt64LittleEndian(s[25..], info.FileLength);
        BinaryPrimitives.WriteUInt128LittleEndian(s[33..], info.ContentHash);
        return buffer;
    }

    private static CheckpointInfo DecodePointer(ReadOnlySpan<byte> data, string path)
    {
        if (data.Length != 49 || data[0] != 1)
            throw new DfsIntegrityException($"Checkpoint pointer '{path}' is malformed.");
        long length = BinaryPrimitives.ReadInt64LittleEndian(data[25..]);
        if (length < 0)
            throw new DfsIntegrityException($"Checkpoint pointer '{path}' is malformed.");
        return new CheckpointInfo(BinaryPrimitives.ReadUInt64LittleEndian(data[1..]), BinaryPrimitives.ReadUInt64LittleEndian(data[9..]),
            BinaryPrimitives.ReadUInt64LittleEndian(data[17..]), length, BinaryPrimitives.ReadUInt128LittleEndian(data[33..]));
    }
}

/// <summary>
/// Streams one checkpoint to its file. Dispose without <see cref="CompleteAsync"/> discards it. A failed write or flush
/// makes the writer unusable (every later call throws <see cref="InvalidOperationException"/>); the checkpoint must be
/// aborted and written again.
/// </summary>
public sealed class CheckpointWriter : IAsyncDisposable
{
    private readonly CheckpointStore _store;
    private readonly string _path;
    private readonly XxHash128 _hash = new();
    private readonly uint _keyVersion;
    private IStorageFile? _file;
    private long _offset;
    private uint _chunkIndex;
    private long _plaintextBytes;
    private bool _completed;
    private bool _finished;
    private Exception? _fault;

    internal CheckpointWriter(CheckpointStore store, IStorageFile file, string path, ulong sequence, ulong index, ulong term, int headerLength, uint keyVersion)
    {
        _store = store;
        _keyVersion = keyVersion;
        _file = file;
        _path = path;
        Sequence = sequence;
        Index = index;
        Term = term;
        _offset = headerLength;
    }

    /// <summary>Store-local sequence number of this checkpoint.</summary>
    public ulong Sequence { get; }

    /// <summary>Last log index included in the checkpoint.</summary>
    public ulong Index { get; }

    /// <summary>Term of the entry at <see cref="Index"/>.</summary>
    public ulong Term { get; }

    /// <summary>Plaintext bytes written so far.</summary>
    public long BytesWritten => _plaintextBytes;

    /// <summary>True once the pointer may name this checkpoint (its file must then not be deleted on abort).</summary>
    internal bool PointerMayNameThis { get; set; }

    /// <summary>Appends one chunk (empty chunks are ignored).</summary>
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> chunk, CancellationToken ct = default)
    {
        IStorageFile file = ThrowIfUnusable();
        if (chunk.Length > _store.Options.MaxChunkSize)
            throw new ArgumentOutOfRangeException(nameof(chunk), $"Chunk of {chunk.Length} bytes exceeds the {_store.Options.MaxChunkSize}-byte limit.");
        if (chunk.IsEmpty)
            return;
        if (_chunkIndex == uint.MaxValue - 1)
            throw new InvalidOperationException("Too many checkpoint chunks.");

        IRecordCipher? cipher = _store.Options.Cipher;
        int stored = chunk.Length + (cipher is null ? 0 : cipher.Overhead);
        var buffer = new byte[CheckpointStore.ChunkHeaderSize + stored + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, StorageFormat.CheckpointChunkMagic);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4), _chunkIndex);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(8), stored);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(12), 0);
        if (cipher is null)
        {
            chunk.Span.CopyTo(buffer.AsSpan(CheckpointStore.ChunkHeaderSize));
        }
        else
        {
            var aad = new byte[12];
            CheckpointStore.WriteAad(aad, Sequence, _chunkIndex);
            cipher.Encrypt(aad, chunk.Span, buffer.AsSpan(CheckpointStore.ChunkHeaderSize, stored), _keyVersion);
        }

        StorageFormat.SealCrc(buffer, CheckpointStore.ChunkHeaderSize + stored);
        try
        {
            await file.WriteFullyAsync(_offset, buffer, ct);
        }
        catch (Exception ex)
        {
            // Part of the chunk may be on the device: the checkpoint can no longer be completed consistently.
            _fault = ex;
            if (StorageErrors.IsDiskFull(ex))
                throw new DfsCapacityException($"Storage is full while writing checkpoint '{_path}'.", ex);
            throw;
        }

        _offset += buffer.Length;
        _hash.Append(chunk.Span);
        _plaintextBytes += chunk.Length;
        _chunkIndex++;
    }

    /// <summary>
    /// Writes the trailer, makes the file durable and atomically makes this checkpoint current. Older checkpoint files
    /// are deleted afterwards.
    /// </summary>
    /// <exception cref="InvalidOperationException">A newer checkpoint became current meanwhile.</exception>
    public async ValueTask<CheckpointInfo> CompleteAsync(CancellationToken ct = default)
    {
        IStorageFile file = ThrowIfUnusable();
        UInt128 hash = _hash.GetCurrentHashAsUInt128();
        var trailer = new byte[CheckpointStore.TrailerSize];
        BinaryPrimitives.WriteUInt32LittleEndian(trailer, StorageFormat.CheckpointTrailerMagic);
        BinaryPrimitives.WriteUInt32LittleEndian(trailer.AsSpan(4), _chunkIndex);
        BinaryPrimitives.WriteInt64LittleEndian(trailer.AsSpan(8), _plaintextBytes);
        BinaryPrimitives.WriteUInt128LittleEndian(trailer.AsSpan(16), hash);
        StorageFormat.SealCrc(trailer, 32);
        try
        {
            await file.WriteFullyAsync(_offset, trailer, ct);
            await file.FlushAsync(ct);
        }
        catch (Exception ex)
        {
            // Never retried: after a failed barrier a device may drop the written pages and report success to the next
            // flush, so a retry could install a checkpoint whose content never reached the medium.
            _fault = ex;
            if (StorageErrors.IsDiskFull(ex))
                throw new DfsCapacityException($"Storage is full while writing checkpoint '{_path}'.", ex);
            throw;
        }

        _offset += trailer.Length;
        _file = null;
        await StorageFormat.CloseQuietlyAsync(file);
        var info = new CheckpointInfo(Sequence, Index, Term, _offset, hash);
        await _store.InstallAsync(this, info, ct);
        _completed = true;
        _finished = true;
        _store.EndWriter(Sequence);
        return info;
    }

    /// <summary>Discards the checkpoint (the previous checkpoint stays current).</summary>
    public async ValueTask AbortAsync()
    {
        if (_finished)
            return;
        _finished = true;
        IStorageFile? file = _file;
        _file = null;
        await StorageFormat.CloseQuietlyAsync(file);

        // When the pointer may already name this file it is kept; the next open decides.
        if (!PointerMayNameThis)
            await _store.DeleteQuietlyAsync(_path);
        _store.EndWriter(Sequence);
    }

    /// <summary>Discards the checkpoint unless it was completed.</summary>
    public ValueTask DisposeAsync() => _completed ? default : AbortAsync();

    private IStorageFile ThrowIfUnusable()
    {
        if (_fault is not null && !_finished)
            throw new InvalidOperationException($"Writing checkpoint '{_path}' failed earlier; abort it and write a new one.", _fault);
        if (_finished || _file is null)
            throw new InvalidOperationException(_completed ? "The checkpoint was already completed." : "The checkpoint writer is no longer usable.");
        return _file;
    }
}

/// <summary>Reads one checkpoint: its identity, metadata and verified chunks.</summary>
public sealed class CheckpointReader : IAsyncDisposable
{
    private readonly CheckpointStore _store;
    private readonly string _path;
    private readonly CheckpointInfo _info;
    private readonly int _contentStart;
    private readonly uint _keyVersion;
    private IStorageFile? _file;
    private long _position;
    private uint _nextChunk;
    private XxHash128 _hash = new();
    private long _plaintextBytes;
    private bool _ended;

    private CheckpointReader(CheckpointStore store, IStorageFile file, string path, CheckpointInfo info, ReadOnlyMemory<byte> metadata, int contentStart, uint keyVersion)
    {
        _store = store;
        _keyVersion = keyVersion;
        _file = file;
        _path = path;
        _info = info;
        Metadata = metadata;
        _contentStart = contentStart;
        _position = contentStart;
    }

    /// <summary>Store-local sequence number of this checkpoint.</summary>
    public ulong Sequence => _info.Sequence;

    /// <summary>Last log index included in the checkpoint.</summary>
    public ulong Index => _info.Index;

    /// <summary>Term of the entry at <see cref="Index"/>.</summary>
    public ulong Term => _info.Term;

    /// <summary>Metadata supplied to <see cref="CheckpointStore.BeginWriteAsync"/>.</summary>
    public ReadOnlyMemory<byte> Metadata { get; }

    /// <summary>
    /// Returns the next verified chunk, or null after the last one (the trailer, total length and content hash are
    /// verified before null is returned).
    /// </summary>
    /// <exception cref="DfsIntegrityException">The checkpoint is truncated or corrupt.</exception>
    public async ValueTask<ReadOnlyMemory<byte>?> ReadNextChunkAsync(CancellationToken ct = default)
    {
        IStorageFile file = _file ?? throw new ObjectDisposedException(nameof(CheckpointReader));
        if (_ended)
            return null;
        (byte[]? chunk, long next) = await ReadAtAsync(file, _position, _nextChunk, ct);
        if (chunk is null)
        {
            VerifyTrailer(await ReadTrailerAsync(file, _position, ct));
            _ended = true;
            return null;
        }

        _hash.Append(chunk);
        _plaintextBytes += chunk.Length;
        _position = next;
        _nextChunk++;
        return chunk;
    }

    /// <summary>Streams every chunk (verified) in order.</summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadChunksAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        while (await ReadNextChunkAsync(ct) is { } chunk)
            yield return chunk;
    }

    /// <summary>Reads the whole content into one array (convenience for small checkpoints).</summary>
    public async ValueTask<byte[]> ReadAllAsync(CancellationToken ct = default)
    {
        using var buffer = new MemoryStream();
        while (await ReadNextChunkAsync(ct) is { } chunk)
            buffer.Write(chunk.Span);
        return buffer.ToArray();
    }

    /// <summary>Closes the file.</summary>
    public async ValueTask DisposeAsync()
    {
        IStorageFile? file = _file;
        _file = null;
        await StorageFormat.CloseQuietlyAsync(file);
    }

    internal static async ValueTask<CheckpointReader> OpenAsync(CheckpointStore store, IStorageFile file, string path, CheckpointInfo info, CancellationToken ct)
    {
        long length = file.Length;
        if (length != info.FileLength)
            throw new DfsIntegrityException($"Checkpoint '{path}' has {length} bytes; the pointer records {info.FileLength}.");
        if (length < 44 + CheckpointStore.TrailerSize)
            throw new DfsIntegrityException($"Checkpoint '{path}' is truncated.");
        var fixedPart = new byte[40];
        await file.ReadFullyAsync(0, fixedPart, ct);
        if (BinaryPrimitives.ReadUInt32LittleEndian(fixedPart) != StorageFormat.CheckpointMagic)
            throw new DfsIntegrityException($"Checkpoint '{path}' has an invalid magic number.");
        int stored = BinaryPrimitives.ReadInt32LittleEndian(fixedPart.AsSpan(36));
        if (stored < 0 || stored > store.Options.MaxMetadataSize + CipherSupport.MaxOverhead || 40L + stored + 4 > length)
            throw new DfsIntegrityException($"Checkpoint '{path}' has a damaged header.");
        var header = new byte[40 + stored + 4];
        fixedPart.CopyTo(header, 0);
        await file.ReadFullyAsync(40, header.AsMemory(40), ct);
        if (!StorageFormat.CheckCrc(header, 40 + stored))
            throw new DfsIntegrityException($"Checkpoint '{path}' failed its header CRC check.");
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4));
        ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(6));
        if (version != CheckpointStore.FormatVersion || (flags & ~1) != 0)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Checkpoint '{path}' uses format version {version}; this software supports version {CheckpointStore.FormatVersion}.");
        ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(8));
        ulong index = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(16));
        ulong term = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(24));
        uint keyVersion = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(32));
        if (sequence != info.Sequence || index != info.Index || term != info.Term)
            throw new DfsIntegrityException($"Checkpoint '{path}' does not match the pointer (sequence/index/term).");

        ReadOnlySpan<byte> storedMeta = header.AsSpan(40, stored);
        byte[] metadata;
        if ((flags & 1) == 0)
        {
            metadata = storedMeta.ToArray();
        }
        else
        {
            Span<byte> aad = stackalloc byte[12];
            CheckpointStore.WriteAad(aad, sequence, uint.MaxValue);
            metadata = CipherSupport.TryDecrypt(store.Options.Cipher, aad, storedMeta, keyVersion, $"Checkpoint '{path}'")
                ?? throw new DfsIntegrityException($"Checkpoint '{path}' metadata failed authenticated decryption.");
        }

        if (((flags & 1) != 0) != (keyVersion != 0))
            throw new DfsIntegrityException($"Checkpoint '{path}' has inconsistent encryption flags.");
        return new CheckpointReader(store, file, path, info, metadata, header.Length, keyVersion);
    }

    /// <summary>Reads every chunk and the trailer once, then rewinds.</summary>
    internal async ValueTask VerifyAsync(CancellationToken ct)
    {
        while (await ReadNextChunkAsync(ct) is not null)
        {
        }

        _position = _contentStart;
        _nextChunk = 0;
        _hash = new XxHash128();
        _plaintextBytes = 0;
        _ended = false;
    }

    /// <summary>Reads the chunk at <paramref name="position"/>; returns a null chunk when the trailer starts there.</summary>
    private async ValueTask<(byte[]? Chunk, long Next)> ReadAtAsync(IStorageFile file, long position, uint expectedIndex, CancellationToken ct)
    {
        long length = _info.FileLength;
        if (length - position < 4)
            throw new DfsIntegrityException($"Checkpoint '{_path}' is truncated at offset {position}.");
        var magic = new byte[4];
        await file.ReadFullyAsync(position, magic, ct);
        uint value = BinaryPrimitives.ReadUInt32LittleEndian(magic);
        if (value == StorageFormat.CheckpointTrailerMagic)
            return (null, position);
        if (value != StorageFormat.CheckpointChunkMagic || length - position < CheckpointStore.ChunkHeaderSize + 4)
            throw new DfsIntegrityException($"Checkpoint '{_path}' is damaged at offset {position}.");
        var header = new byte[CheckpointStore.ChunkHeaderSize];
        await file.ReadFullyAsync(position, header, ct);
        uint chunkIndex = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4));
        int stored = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
        if (chunkIndex != expectedIndex || stored < 0 || stored > _store.Options.MaxChunkSize + CipherSupport.MaxOverhead
            || position + CheckpointStore.ChunkHeaderSize + stored + 4 > length)
        {
            throw new DfsIntegrityException($"Checkpoint '{_path}' has a damaged chunk header at offset {position}.");
        }

        var raw = new byte[CheckpointStore.ChunkHeaderSize + stored + 4];
        await file.ReadFullyAsync(position, raw, ct);
        if (!StorageFormat.CheckCrc(raw, CheckpointStore.ChunkHeaderSize + stored))
            throw new DfsIntegrityException($"Checkpoint '{_path}' chunk {chunkIndex} failed its CRC check.");
        ReadOnlySpan<byte> sealedChunk = raw.AsSpan(CheckpointStore.ChunkHeaderSize, stored);
        byte[] chunk;
        IRecordCipher? cipher = _store.Options.Cipher;
        if (_keyVersion == 0)
        {
            chunk = sealedChunk.ToArray();
        }
        else
        {
            Span<byte> aad = stackalloc byte[12];
            CheckpointStore.WriteAad(aad, _info.Sequence, chunkIndex);
            chunk = CipherSupport.TryDecrypt(cipher, aad, sealedChunk, _keyVersion, $"Checkpoint '{_path}'")
                ?? throw new DfsIntegrityException($"Checkpoint '{_path}' chunk {chunkIndex} failed authenticated decryption.");
        }

        return (chunk, position + raw.Length);
    }

    private async ValueTask<byte[]> ReadTrailerAsync(IStorageFile file, long position, CancellationToken ct)
    {
        if (_info.FileLength - position != CheckpointStore.TrailerSize)
            throw new DfsIntegrityException($"Checkpoint '{_path}' has {_info.FileLength - position} bytes after its last chunk; expected a {CheckpointStore.TrailerSize}-byte trailer.");
        var trailer = new byte[CheckpointStore.TrailerSize];
        await file.ReadFullyAsync(position, trailer, ct);
        return trailer;
    }

    private void VerifyTrailer(ReadOnlySpan<byte> trailer)
    {
        if (BinaryPrimitives.ReadUInt32LittleEndian(trailer) != StorageFormat.CheckpointTrailerMagic || !StorageFormat.CheckCrc(trailer, 32))
            throw new DfsIntegrityException($"Checkpoint '{_path}' has a damaged trailer.");
        uint chunks = BinaryPrimitives.ReadUInt32LittleEndian(trailer[4..]);
        long bytes = BinaryPrimitives.ReadInt64LittleEndian(trailer[8..]);
        UInt128 hash = BinaryPrimitives.ReadUInt128LittleEndian(trailer[16..]);
        if (chunks != _nextChunk || bytes != _plaintextBytes || hash != _hash.GetCurrentHashAsUInt128() || hash != _info.ContentHash)
            throw new DfsIntegrityException($"Checkpoint '{_path}' content does not match its trailer or pointer.");
    }
}
