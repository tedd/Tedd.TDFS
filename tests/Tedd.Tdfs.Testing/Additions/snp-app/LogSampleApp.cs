using System.Buffers.Binary;
using System.Text;
using Tedd.Tdfs.Data;

namespace Tedd.Tdfs.Testing.SnpApp;

/// <summary>Options of a <see cref="LogSampleApp"/>.</summary>
public sealed record LogSampleAppOptions
{
    /// <summary>Directory holding the application's files.</summary>
    public string Root { get; init; } = "/app";

    /// <summary>
    /// When true each log record is written through an append stream and published by two flushes (header, then
    /// payload + checksum), so a filesystem-consistent snapshot can capture a torn log tail. When false each record is
    /// appended atomically with <see cref="DfsFileClient.AppendAllBytesAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.
    /// </summary>
    public bool SplitRecordFlush { get; init; }
}

/// <summary>One appended log record and the commit tokens that published it.</summary>
/// <param name="Sequence">Record sequence (1-based, consecutive).</param>
/// <param name="Key">Updated key.</param>
/// <param name="Value">New value.</param>
/// <param name="HeaderToken">Commit that published the record header (equals <paramref name="CompleteToken"/> without split flushes).</param>
/// <param name="CompleteToken">Commit that published the complete, checksummed record.</param>
public sealed record LogRecordInfo(long Sequence, string Key, byte[] Value, CommitToken HeaderToken, CommitToken CompleteToken);

/// <summary>A decoded log record.</summary>
public sealed record LogRecord(long Sequence, string Key, byte[] Value, long Offset, int Length);

/// <summary>Result of scanning a log image.</summary>
/// <param name="Records">Valid records in file order.</param>
/// <param name="ValidBytes">Length of the valid prefix.</param>
/// <param name="TotalBytes">Length of the image.</param>
/// <param name="TornTail">True when bytes after the valid prefix exist (incomplete or checksum-failing record).</param>
public sealed record LogScan(IReadOnlyList<LogRecord> Records, long ValidBytes, long TotalBytes, bool TornTail);

/// <summary>Result of <see cref="LogSampleApp.RecoverAsync"/>.</summary>
/// <param name="DataAppliedSequence">Applied sequence recorded in the data file before recovery (0 when absent).</param>
/// <param name="LastValidSequence">Highest sequence in the valid log prefix (0 when empty).</param>
/// <param name="ReplayedRecords">Records replayed on top of the data file.</param>
/// <param name="TornTail">True when the log had an invalid tail that recovery ignored (and truncated when repairing).</param>
/// <param name="ValidLogBytes">Length of the valid log prefix.</param>
/// <param name="LogBytes">Length of the log before recovery.</param>
/// <param name="State">Recovered key/value state.</param>
public sealed record LogRecoveryResult(long DataAppliedSequence, long LastValidSequence, int ReplayedRecords, bool TornTail,
    long ValidLogBytes, long LogBytes, IReadOnlyDictionary<string, byte[]> State);

/// <summary>
/// A tiny log-based sample application stored on the distributed filesystem (APP-002/003/006/009/012). It keeps a
/// key/value state in a data file and protects updates with a write-ahead log in the documented flush order:
/// <list type="number">
/// <item>append the update to <c>{Root}/wal.log</c> (durable publication, see <see cref="LogSampleAppOptions.SplitRecordFlush"/>);</item>
/// <item>rewrite <c>{Root}/data.bin</c> with the complete state and the applied sequence (atomic WriteAll).</item>
/// </list>
/// A filesystem-consistent snapshot may therefore capture the log ahead of the data file, or a torn log tail; such a
/// snapshot is recoverable by <see cref="RecoverAsync"/>, which replays the valid log suffix. The adapter returned by
/// <see cref="CreateAdapter"/> makes snapshots application-consistent by pausing updates, waiting for the in-flight
/// update and checkpointing the data file during prepare, and unpausing on resume/abort.
/// </summary>
/// <remarks>
/// Single writer: at most one <see cref="UpdateAsync"/>/<see cref="AppendLogAsync(string, byte[], CancellationToken)"/>/<see cref="ApplyAsync"/>
/// may run at a time (concurrent calls throw <see cref="InvalidOperationException"/>); the adapter may run concurrently with
/// them. Uses no clocks, threads or thread-pool APIs, so it runs inside the deterministic simulation.
/// Formats (little-endian): log record = <c>"WAL1"</c> u32, seq u64, keyLen u16, valueLen u32 (18-byte header), key
/// UTF-8, value, CRC-32C u32 over all preceding record bytes. Data file = <c>"DAT1"</c> u32, appliedSeq u64, count u32,
/// entries sorted ordinally (keyLen u16, key, valueLen u32, value), CRC-32C u32 over all preceding bytes.
/// </remarks>
public sealed class LogSampleApp
{
    /// <summary>Log record header length.</summary>
    public const int RecordHeaderLength = 18;

    private const uint LogMagic = 0x314C4157;  // "WAL1"
    private const uint DataMagic = 0x31544144; // "DAT1"

    private readonly object _lock = new();
    private readonly DfsVolume _volume;
    private readonly DfsFileClient _files;
    private readonly List<LogRecordInfo> _records = [];
    private readonly SortedDictionary<string, byte[]> _state = new(StringComparer.Ordinal);
    private TaskCompletionSource? _resumed;      // non-null while paused
    private TaskCompletionSource? _drained;      // completed when no update is in flight
    private bool _busy;
    private long _appliedSequence;
    private CommitToken _lastDataToken;
    private CommitToken? _lastCheckpointToken;

    /// <summary>Creates the application on a live, writable <paramref name="volume"/>.</summary>
    public LogSampleApp(DfsVolume volume, LogSampleAppOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(volume);
        _volume = volume;
        _files = volume.Files;
        Options = options ?? new LogSampleAppOptions();
        Root = DfsPath.Normalize(Options.Root);
    }

    /// <summary>The options.</summary>
    public LogSampleAppOptions Options { get; }

    /// <summary>The volume the application lives on.</summary>
    public DfsVolume Volume => _volume;

    /// <summary>Application directory.</summary>
    public string Root { get; }

    /// <summary>Write-ahead log path.</summary>
    public string LogPath => LogPathOf(Root);

    /// <summary>Data file path.</summary>
    public string DataPath => DataPathOf(Root);

    /// <summary>All records appended so far, in sequence order.</summary>
    public IReadOnlyList<LogRecordInfo> Records
    {
        get
        {
            lock (_lock)
                return _records.ToArray();
        }
    }

    /// <summary>Highest appended (durably logged) sequence.</summary>
    public long LastSequence
    {
        get
        {
            lock (_lock)
                return _records.Count == 0 ? 0 : _records[^1].Sequence;
        }
    }

    /// <summary>Sequence most recently written into the data file.</summary>
    public long AppliedSequence
    {
        get
        {
            lock (_lock)
                return _appliedSequence;
        }
    }

    /// <summary>Commit of the most recent data-file write.</summary>
    public CommitToken LastDataToken
    {
        get
        {
            lock (_lock)
                return _lastDataToken;
        }
    }

    /// <summary>Commit of the data-file checkpoint written by the adapter's most recent prepare, or null.</summary>
    public CommitToken? LastCheckpointToken
    {
        get
        {
            lock (_lock)
                return _lastCheckpointToken;
        }
    }

    /// <summary>True while an adapter holds the application paused.</summary>
    public bool IsPaused
    {
        get
        {
            lock (_lock)
                return _resumed is not null;
        }
    }

    /// <summary>The state after every appended record (what a correct recovery of the latest state yields).</summary>
    public IReadOnlyDictionary<string, byte[]> CommittedState
    {
        get
        {
            lock (_lock)
                return new SortedDictionary<string, byte[]>(_state, StringComparer.Ordinal);
        }
    }

    /// <summary>Log path for an application root.</summary>
    public static string LogPathOf(string root) => DfsPath.Combine(DfsPath.Normalize(root), "wal.log");

    /// <summary>Data path for an application root.</summary>
    public static string DataPathOf(string root) => DfsPath.Combine(DfsPath.Normalize(root), "data.bin");

    /// <summary>Creates the directory, an empty log and an empty data file (applied sequence 0).</summary>
    public async Task<CommitToken> InitializeAsync(CancellationToken ct = default)
    {
        await _volume.Directories.CreateDirectoryAsync(Root, ct);
        await _files.WriteAllBytesAsync(LogPath, ReadOnlyMemory<byte>.Empty, ct);
        CommitToken token = await _files.WriteAllBytesAsync(DataPath, EncodeData(new Dictionary<string, byte[]>(), 0), ct);
        lock (_lock)
        {
            _lastDataToken = token;
            _appliedSequence = 0;
        }

        return token;
    }

    /// <summary>Appends a record and applies it to the data file (waits while the application is paused).</summary>
    public async Task<LogRecordInfo> UpdateAsync(string key, byte[] value, CancellationToken ct = default)
    {
        await EnterAsync(ct);
        try
        {
            LogRecordInfo record = await AppendCoreAsync(key, value, null, ct);
            await ApplyCoreAsync(ct);
            return record;
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>Appends a record to the log only (the data file is not updated; waits while paused).</summary>
    public Task<LogRecordInfo> AppendLogAsync(string key, byte[] value, CancellationToken ct = default) =>
        AppendLogAsync(key, value, null, ct);

    /// <summary>
    /// Appends a record to the log only. With <see cref="LogSampleAppOptions.SplitRecordFlush"/>,
    /// <paramref name="betweenFlushes"/> runs after the header was published and before the payload is written (the
    /// moment at which a snapshot captures a torn tail).
    /// </summary>
    public async Task<LogRecordInfo> AppendLogAsync(string key, byte[] value, Func<CancellationToken, Task>? betweenFlushes, CancellationToken ct = default)
    {
        await EnterAsync(ct);
        try
        {
            return await AppendCoreAsync(key, value, betweenFlushes, ct);
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>Rewrites the data file with the state of every appended record (waits while paused).</summary>
    public async Task<CommitToken> ApplyAsync(CancellationToken ct = default)
    {
        await EnterAsync(ct);
        try
        {
            return await ApplyCoreAsync(ct);
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>
    /// Creates the application's snapshot adapter. Prepare pauses new updates, waits for the in-flight one, then
    /// checkpoints the data file (<see cref="LastCheckpointToken"/>); resume and abort unpause. Restore requirements
    /// record the log and data paths and the recovery rule.
    /// </summary>
    public IDfsSnapshotAdapter CreateAdapter(string id = "log-sample-app", AdapterTimeline? timeline = null, string version = "1.0.0") =>
        new Adapter(this, id, version, timeline);

    /// <summary>
    /// The state a correct recovery of a snapshot with <paramref name="boundary"/> must yield: every record whose
    /// complete publication is at or before the boundary, applied in sequence order.
    /// </summary>
    /// <exception cref="ArgumentException">A record token is not comparable with the boundary.</exception>
    public static IReadOnlyDictionary<string, byte[]> ExpectedStateAt(IEnumerable<LogRecordInfo> records, CommitToken boundary)
    {
        ArgumentNullException.ThrowIfNull(records);
        var state = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (LogRecordInfo record in records.OrderBy(r => r.Sequence))
        {
            if (record.CompleteToken.CompareTo(boundary) <= 0)
                state[record.Key] = record.Value;
        }

        return state;
    }

    /// <summary>Highest record sequence whose complete publication is at or before <paramref name="boundary"/>.</summary>
    public static long ExpectedLastSequenceAt(IEnumerable<LogRecordInfo> records, CommitToken boundary)
    {
        ArgumentNullException.ThrowIfNull(records);
        long last = 0;
        foreach (LogRecordInfo record in records)
        {
            if (record.CompleteToken.CompareTo(boundary) <= 0 && record.Sequence > last)
                last = record.Sequence;
        }

        return last;
    }

    /// <summary>
    /// Recovers the application state stored under <paramref name="root"/>: decodes the data file, scans the log, ignores an
    /// invalid tail and replays valid records newer than the data file's applied sequence. With
    /// <paramref name="repair"/> (writable facades only) it then writes the recovered data file and truncates the log to
    /// its valid prefix.
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// The data file is corrupt, the log is non-consecutive, or the data file is ahead of the valid log (an
    /// application-invalid image that a filesystem-consistent snapshot of a correctly ordered writer can never contain).
    /// </exception>
    public static async Task<LogRecoveryResult> RecoverAsync(DfsFileClient files, string root, bool repair, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(files);
        string logPath = LogPathOf(root);
        string dataPath = DataPathOf(root);

        IReadOnlyDictionary<string, byte[]> dataState = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        long applied = 0;
        if (await files.ExistsAsync(dataPath, ct))
            (dataState, applied) = DecodeData(await files.ReadAllBytesAsync(dataPath, ct));

        byte[] log = await files.ExistsAsync(logPath, ct) ? await files.ReadAllBytesAsync(logPath, ct) : [];
        LogScan scan = ScanLog(log);

        var state = new SortedDictionary<string, byte[]>(dataState.ToDictionary(p => p.Key, p => p.Value, StringComparer.Ordinal), StringComparer.Ordinal);
        long expected = 1;
        long lastValid = 0;
        int replayed = 0;
        foreach (LogRecord record in scan.Records)
        {
            if (record.Sequence != expected)
                throw new InvalidDataException($"Log sequence {record.Sequence} at offset {record.Offset} is not consecutive (expected {expected}).");
            expected++;
            lastValid = record.Sequence;
            if (record.Sequence <= applied)
                continue;
            state[record.Key] = record.Value;
            replayed++;
        }

        if (applied > lastValid)
            throw new InvalidDataException($"Data file applied sequence {applied} is ahead of the valid log ({lastValid}).");

        if (repair)
        {
            await files.WriteAllBytesAsync(dataPath, EncodeData(state, lastValid), ct);
            if (scan.TornTail)
            {
                await using DfsStream stream = await files.OpenAsync(logPath, new DfsOpenOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                }, ct);
                await stream.SetLengthAsync(scan.ValidBytes, ct);
                await stream.CommitAsync(ct);
            }
        }

        return new LogRecoveryResult(applied, lastValid, replayed, scan.TornTail, scan.ValidBytes, scan.TotalBytes, state);
    }

    /// <summary>Encodes one log record.</summary>
    public static byte[] EncodeRecord(long sequence, string key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(key);
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);
        if (keyBytes.Length > ushort.MaxValue)
            throw new ArgumentException("Key too long.", nameof(key));
        var record = new byte[RecordHeaderLength + keyBytes.Length + value.Length + 4];
        Span<byte> span = record;
        BinaryPrimitives.WriteUInt32LittleEndian(span, LogMagic);
        BinaryPrimitives.WriteUInt64LittleEndian(span[4..], (ulong)sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(span[12..], (ushort)keyBytes.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span[14..], (uint)value.Length);
        keyBytes.CopyTo(span[RecordHeaderLength..]);
        value.CopyTo(span[(RecordHeaderLength + keyBytes.Length)..]);
        int bodyLength = record.Length - 4;
        BinaryPrimitives.WriteUInt32LittleEndian(span[bodyLength..], ObjectChecksum.Crc32C(span[..bodyLength]));
        return record;
    }

    /// <summary>Scans a log image; stops at the first incomplete or checksum-failing record.</summary>
    public static LogScan ScanLog(ReadOnlySpan<byte> log)
    {
        var records = new List<LogRecord>();
        int offset = 0;
        while (offset < log.Length)
        {
            ReadOnlySpan<byte> rest = log[offset..];
            if (rest.Length < RecordHeaderLength || BinaryPrimitives.ReadUInt32LittleEndian(rest) != LogMagic)
                break;
            long sequence = (long)BinaryPrimitives.ReadUInt64LittleEndian(rest[4..]);
            int keyLength = BinaryPrimitives.ReadUInt16LittleEndian(rest[12..]);
            long valueLength = BinaryPrimitives.ReadUInt32LittleEndian(rest[14..]);
            long length = RecordHeaderLength + keyLength + valueLength + 4;
            if (length > rest.Length)
                break;
            int bodyLength = (int)length - 4;
            if (BinaryPrimitives.ReadUInt32LittleEndian(rest[bodyLength..]) != ObjectChecksum.Crc32C(rest[..bodyLength]))
                break;
            string key = Encoding.UTF8.GetString(rest.Slice(RecordHeaderLength, keyLength));
            byte[] value = rest.Slice(RecordHeaderLength + keyLength, (int)valueLength).ToArray();
            records.Add(new LogRecord(sequence, key, value, offset, (int)length));
            offset += (int)length;
        }

        return new LogScan(records, offset, log.Length, offset < log.Length);
    }

    /// <summary>Encodes a data file image.</summary>
    public static byte[] EncodeData(IReadOnlyDictionary<string, byte[]> state, long appliedSequence)
    {
        ArgumentNullException.ThrowIfNull(state);
        using var ms = new MemoryStream();
        var scratch = new byte[8];
        void U32(uint v)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(scratch, v);
            ms.Write(scratch, 0, 4);
        }

        U32(DataMagic);
        BinaryPrimitives.WriteUInt64LittleEndian(scratch, (ulong)appliedSequence);
        ms.Write(scratch, 0, 8);
        U32((uint)state.Count);
        foreach (KeyValuePair<string, byte[]> pair in state.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            byte[] key = Encoding.UTF8.GetBytes(pair.Key);
            BinaryPrimitives.WriteUInt16LittleEndian(scratch, (ushort)key.Length);
            ms.Write(scratch, 0, 2);
            ms.Write(key);
            U32((uint)pair.Value.Length);
            ms.Write(pair.Value);
        }

        byte[] body = ms.ToArray();
        var result = new byte[body.Length + 4];
        body.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(body.Length), ObjectChecksum.Crc32C(body));
        return result;
    }

    /// <summary>Decodes a data file image.</summary>
    /// <exception cref="InvalidDataException">Malformed or checksum-failing image.</exception>
    public static (IReadOnlyDictionary<string, byte[]> State, long AppliedSequence) DecodeData(ReadOnlySpan<byte> data)
    {
        if (data.Length < 20 || BinaryPrimitives.ReadUInt32LittleEndian(data) != DataMagic)
            throw new InvalidDataException("Data file is truncated or has a wrong magic.");
        int bodyLength = data.Length - 4;
        if (BinaryPrimitives.ReadUInt32LittleEndian(data[bodyLength..]) != ObjectChecksum.Crc32C(data[..bodyLength]))
            throw new InvalidDataException("Data file checksum mismatch.");
        long applied = (long)BinaryPrimitives.ReadUInt64LittleEndian(data[4..]);
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(data[12..]);
        var state = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        int offset = 16;
        for (uint i = 0; i < count; i++)
        {
            if (offset + 2 > bodyLength)
                throw new InvalidDataException("Data file entry header truncated.");
            int keyLength = BinaryPrimitives.ReadUInt16LittleEndian(data[offset..]);
            offset += 2;
            if (offset + keyLength + 4 > bodyLength)
                throw new InvalidDataException("Data file key truncated.");
            string key = Encoding.UTF8.GetString(data.Slice(offset, keyLength));
            offset += keyLength;
            int valueLength = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(data[offset..]));
            offset += 4;
            if (offset + valueLength > bodyLength)
                throw new InvalidDataException("Data file value truncated.");
            state[key] = data.Slice(offset, valueLength).ToArray();
            offset += valueLength;
        }

        if (offset != bodyLength)
            throw new InvalidDataException("Data file has trailing bytes.");
        return (state, applied);
    }

    /// <summary>True when two states hold the same keys with byte-identical values.</summary>
    public static bool StatesEqual(IReadOnlyDictionary<string, byte[]> a, IReadOnlyDictionary<string, byte[]> b)
    {
        ArgumentNullException.ThrowIfNull(a);
        ArgumentNullException.ThrowIfNull(b);
        if (a.Count != b.Count)
            return false;
        foreach (KeyValuePair<string, byte[]> pair in a)
        {
            if (!b.TryGetValue(pair.Key, out byte[]? other) || !pair.Value.AsSpan().SequenceEqual(other))
                return false;
        }

        return true;
    }

    /// <summary>Fails the test unless the states are equal, describing the first difference.</summary>
    public static void AssertStatesEqual(IReadOnlyDictionary<string, byte[]> expected, IReadOnlyDictionary<string, byte[]> actual, string because)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        foreach (string key in expected.Keys.Union(actual.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal))
        {
            bool inExpected = expected.TryGetValue(key, out byte[]? e);
            bool inActual = actual.TryGetValue(key, out byte[]? a);
            if (!inExpected)
                Assert.Fail($"{because}: unexpected key '{key}' in the recovered state.");
            if (!inActual)
                Assert.Fail($"{because}: key '{key}' missing from the recovered state.");
            AssertEx.BytesEqual(e, a, $"{because}: value of '{key}'");
        }
    }

    private async Task<LogRecordInfo> AppendCoreAsync(string key, byte[] value, Func<CancellationToken, Task>? betweenFlushes, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        long sequence;
        lock (_lock)
            sequence = (_records.Count == 0 ? 0 : _records[^1].Sequence) + 1;
        byte[] record = EncodeRecord(sequence, key, value);
        CommitToken headerToken;
        CommitToken completeToken;
        if (Options.SplitRecordFlush)
        {
            await using DfsStream stream = await _files.OpenAsync(LogPath, new DfsOpenOptions
            {
                Mode = FileMode.Append,
                Access = FileAccess.Write,
                Share = FileShare.Read,
            }, ct);
            await stream.WriteAsync(record.AsMemory(0, RecordHeaderLength), ct);
            headerToken = await stream.CommitAsync(ct);
            if (betweenFlushes is not null)
                await betweenFlushes(ct);
            await stream.WriteAsync(record.AsMemory(RecordHeaderLength), ct);
            completeToken = await stream.CommitAsync(ct);
        }
        else
        {
            completeToken = await _files.AppendAllBytesAsync(LogPath, record, ct);
            headerToken = completeToken;
            if (betweenFlushes is not null)
                await betweenFlushes(ct);
        }

        var info = new LogRecordInfo(sequence, key, (byte[])value.Clone(), headerToken, completeToken);
        lock (_lock)
        {
            _records.Add(info);
            _state[key] = info.Value;
        }

        return info;
    }

    private async Task<CommitToken> ApplyCoreAsync(CancellationToken ct)
    {
        byte[] image;
        long sequence;
        lock (_lock)
        {
            sequence = _records.Count == 0 ? 0 : _records[^1].Sequence;
            image = EncodeData(_state, sequence);
        }

        CommitToken token = await _files.WriteAllBytesAsync(DataPath, image, ct);
        lock (_lock)
        {
            _appliedSequence = sequence;
            _lastDataToken = token;
        }

        return token;
    }

    private async Task EnterAsync(CancellationToken ct)
    {
        while (true)
        {
            Task wait;
            lock (_lock)
            {
                if (_resumed is null)
                {
                    if (_busy)
                        throw new InvalidOperationException("LogSampleApp is single-writer: another update is in flight.");
                    _busy = true;
                    return;
                }

                wait = _resumed.Task;
            }

            await wait.WaitAsync(ct);
        }
    }

    private void Exit()
    {
        TaskCompletionSource? drained;
        lock (_lock)
        {
            _busy = false;
            drained = _drained;
            _drained = null;
        }

        drained?.TrySetResult();
    }

    private async Task PauseAsync(CancellationToken ct)
    {
        Task drained;
        lock (_lock)
        {
            _resumed ??= new TaskCompletionSource();
            if (_busy)
            {
                _drained ??= new TaskCompletionSource();
                drained = _drained.Task;
            }
            else
            {
                drained = Task.CompletedTask;
            }
        }

        await drained.WaitAsync(ct);
    }

    private void Unpause()
    {
        TaskCompletionSource? resumed;
        lock (_lock)
        {
            resumed = _resumed;
            _resumed = null;
        }

        resumed?.TrySetResult();
    }

    private async Task<CommitToken> CheckpointAsync(CancellationToken ct)
    {
        CommitToken token = await ApplyCoreAsync(ct);
        lock (_lock)
            _lastCheckpointToken = token;
        return token;
    }

    private sealed class Adapter : IDfsSnapshotAdapter
    {
        private readonly LogSampleApp _app;
        private readonly AdapterTimeline? _timeline;

        public Adapter(LogSampleApp app, string id, string version, AdapterTimeline? timeline)
        {
            ArgumentException.ThrowIfNullOrEmpty(id);
            _app = app;
            _timeline = timeline;
            Id = id;
            Version = version;
            RestoreRequirements = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["log"] = app.LogPath,
                ["data"] = app.DataPath,
                ["recovery"] = "replay-log-after-data-applied-sequence",
                ["scope"] = app.Root,
            };
        }

        public string Id { get; }

        public string Version { get; }

        public IReadOnlyDictionary<string, string> RestoreRequirements { get; }

        public async Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            _timeline?.Record(Id, "prepare.start", context.SnapshotName);
            try
            {
                await _app.PauseAsync(ct);
                CommitToken token = await _app.CheckpointAsync(ct);
                _timeline?.Record(Id, "prepare.done", token.ToString());
            }
            catch (Exception ex)
            {
                _timeline?.Record(Id, "prepare.failed", ex.GetType().Name);
                throw;
            }
        }

        public Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            _timeline?.Record(Id, "resume.start", context.SnapshotName);
            _app.Unpause();
            _timeline?.Record(Id, "resume.done", context.SnapshotName);
            return Task.CompletedTask;
        }

        public Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            _timeline?.Record(Id, "abort.start", context.SnapshotName);
            _app.Unpause();
            _timeline?.Record(Id, "abort.done", context.SnapshotName);
            return Task.CompletedTask;
        }
    }
}
