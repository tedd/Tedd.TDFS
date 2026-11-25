using System.Buffers;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Server.Volumes.State;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// The read-only client surface of a backup node (ARCHITECTURE §12, FSD §12.1): discovery (a cluster map naming only the
/// backup node and the volumes it holds snapshots of), local sessions and reader pins (no Raft), snapshot listing, views
/// (paths, enumeration, streams, descriptors and ranged reads served from published manifests and the local object store),
/// destination retention under the node's own authorization, and the restore export (<c>BackupExportRestore</c>). A reader
/// stays on the snapshot it pinned while newer ones publish (BAK-013); pinned snapshots are never pruned (BAK-027).
/// </summary>
internal sealed class BackupReader
{
    private const string ContinuationPrefix = "b1.";
    private const int MaxRestoreChunk = 4 << 20;

    private readonly BackupServices _s;
    private readonly Dictionary<ulong, ReaderSession> _sessions = [];
    private readonly Dictionary<ulong, ReaderPin> _pins = [];
    private readonly byte[] _capabilityKey;
    private ulong _nextSession;
    private ulong _nextPin;

    public BackupReader(BackupServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;

        // Sessions and pins live only in this process: random bases make a restarted node never accept a stale id as its own.
        _nextSession = (services.Random.NextUInt64() >> 16) + 1;
        _nextPin = (services.Random.NextUInt64() >> 16) + 1;
        _capabilityKey = new byte[32];
        services.Random.Fill(_capabilityKey);
    }

    /// <summary>Reader pins currently held (diagnostics).</summary>
    public int PinCount => _pins.Count;

    // ------------------------------------------------------------------------------------------------- discovery

    /// <summary>The volume identity the backup host reports for a volume name (stable; the host serves several source authorities under one name).</summary>
    public VolumeId VolumeIdOf(string name)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes("tdfs/backup-volume/v1\0" + _s.Options.SourceClusterId.ToString("N") + "\0" + name));
        return new VolumeId(new Guid(hash.AsSpan(0, 16)));
    }

    /// <summary>The cluster name the backup node reports (the source's, as recorded in its manifests).</summary>
    public string ClusterName
    {
        get
        {
            foreach (BackupVolume volume in _s.Catalog.Volumes)
            {
                if (volume.Pointer?.Manifest is { } manifest)
                    return manifest.SourceClusterName;
                if (volume.Staged is { } staged)
                    return staged.ClusterName;
            }

            return "backup:" + _s.Name;
        }
    }

    /// <summary>Server capabilities announced to clients.</summary>
    public DfsServerCapabilities Capabilities => new()
    {
        ProtocolVersion = ProtocolInfo.CurrentVersion,
        ChunkSize = _s.Timings.ChunkSize,
        MaxObjectSize = _s.Timings.MaxObjectSize,
        LeaseDuration = _s.Timings.LeaseDuration,
    };

    /// <summary><c>GetClusterMap</c>: the backup node itself and the volumes it holds snapshots of.</summary>
    public IWireMessage GetClusterMap(string? principal)
    {
        _s.Principals.RequireRead(principal, "discover volumes");
        ClusterMapData map = BuildMap();
        return new ClusterMapResultMessage(map.ConfigIndex, map);
    }

    /// <summary>The backup node's cluster map.</summary>
    public ClusterMapData BuildMap()
    {
        var self = new NodeEntry(_s.Keys.NodeId, _s.Name, new DfsTopologyLabels("backup", _s.Name, _s.Name, _s.Name, _s.Name), [_s.Endpoint],
            WireNodeStatus.Active, 1, IsBackup: true, IsControlVoter: false, IsReachable: true, PlacementEligible: false, _s.Options.CapacityBytes,
            _s.Medium.UsedBytes);
        var volumes = _s.Catalog.Volumes.Where(v => v.Snapshots.Count > 0).Select(v => EntryOf(v, v.Name)).ToArray();
        return new ClusterMapData(_s.Options.SourceClusterId, ClusterName, _s.Catalog.Version, 0, [self], volumes, null, Capabilities);
    }

    /// <summary><c>GetVolumeRoute</c>: every volume is served by the backup node itself.</summary>
    public IWireMessage GetVolumeRoute(string? principal, GetVolumeRouteMessage m)
    {
        _s.Principals.RequireRead(principal, "discover volumes");
        BackupVolume? volume = _s.Catalog.Find(m.Volume);
        CommitToken last = volume?.Pointer?.Covered ?? CommitToken.None;
        return new VolumeRouteResultMessage(EntryOf(volume, m.Volume), _s.Keys.NodeId, _s.Endpoint, 0, last);
    }

    private VolumeEntry EntryOf(BackupVolume? volume, string name)
    {
        BackupVolumeConfig? config = volume?.Pointer?.Manifest?.Config;
        return new VolumeEntry(VolumeIdOf(name), name, 1, WireVolumeState.Active, [_s.Keys.NodeId], [], _s.Keys.NodeId,
            config?.Metadata ?? new DfsMetadataPolicy(), config?.DefaultPolicy ?? DfsStoragePolicy.Default,
            config?.NamedPolicies ?? new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal), config?.InlineThreshold ?? 0,
            config?.Limits ?? new DfsVolumeLimits());
    }

    /// <summary>Checks a request header against the identity the backup host reports for the volume.</summary>
    private void CheckHeader(RequestHeader header)
    {
        if (string.IsNullOrEmpty(header.Volume))
            throw new ArgumentException("A volume name is required.");
        if (header.ExpectedVolumeId.Value != Guid.Empty && header.ExpectedVolumeId != VolumeIdOf(header.Volume))
            throw new DfsEpochMismatchException($"Volume '{header.Volume}' on this backup host is not the expected volume {header.ExpectedVolumeId}.");
        if (header.ExpectedVolumeEpoch is not (0 or 1))
            throw new DfsEpochMismatchException($"Volume '{header.Volume}' on this backup host is not at epoch {header.ExpectedVolumeEpoch}.");
    }

    // ------------------------------------------------------------------------------------------------- sessions

    /// <summary><c>OpenSession</c>: a local session (renewed by <c>KeepAlive</c>; nothing replicated).</summary>
    public IWireMessage OpenSession(string principal, OpenSessionMessage m)
    {
        _s.Principals.RequireRead(principal, "open a session");
        CheckHeader(m.Header);
        foreach (ReaderSession existing in _sessions.Values)
        {
            if (existing.Nonce == m.Nonce && string.Equals(existing.Principal, principal, StringComparison.Ordinal)
                && string.Equals(existing.Volume, m.Header.Volume, StringComparison.Ordinal))
            {
                existing.LastRenewal = _s.Time.GetTimestamp();
                return SessionReply(existing);
            }
        }

        var session = new ReaderSession(_nextSession++, principal, m.Header.Volume, m.Nonce) { LastRenewal = _s.Time.GetTimestamp() };
        _sessions[session.Id] = session;
        return SessionReply(session);
    }

    private OpenSessionResultMessage SessionReply(ReaderSession session)
        => new(session.Id, 1, _s.Timings.LeaseDuration, _s.Timings.SessionTimeout, _s.Time.GetUtcNow(), _s.Timings.MaxOutcomesPerSession,
            _s.Timings.OutcomeRetention, CommitToken.None);

    /// <summary><c>KeepAlive</c>: renews the session and its reader pins; reports expired ones (handles never exist here).</summary>
    public IWireMessage KeepAlive(string principal, KeepAliveMessage m)
    {
        CheckHeader(m.Header);
        if (!_sessions.TryGetValue(m.Header.Session, out ReaderSession? session) || !string.Equals(session.Principal, principal, StringComparison.Ordinal))
            return new KeepAliveResultMessage(m.Handles, m.ReaderPins, true, _s.Timings.LeaseDuration, _s.Time.GetUtcNow());
        long now = _s.Time.GetTimestamp();
        session.LastRenewal = now;
        var expired = new List<ulong>();
        foreach (ulong pin in m.ReaderPins)
        {
            if (_pins.TryGetValue(pin, out ReaderPin? p) && p.Session == session.Id)
                p.LastRenewal = now;
            else
                expired.Add(pin);
        }

        return new KeepAliveResultMessage(m.Handles, expired, false, _s.Timings.LeaseDuration, _s.Time.GetUtcNow());
    }

    /// <summary><c>ReserveOperationIds</c>: a block of sequences of the caller's local session.</summary>
    public IWireMessage ReserveOperationIds(string principal, ReserveOperationIdsMessage m)
    {
        ReaderSession session = RequireSession(principal, m.Header);
        if (m.Count is < 1 or > 4096)
            throw new ArgumentException("Between 1 and 4096 operation ids can be reserved at once.");
        ulong first = session.NextSequence;
        session.NextSequence += (ulong)m.Count;
        return new ReserveOperationIdsResultMessage(session.Id, first, m.Count);
    }

    /// <summary><c>QueryOperation</c>: answers from the session's retained replies; nothing else ever executes here.</summary>
    public IWireMessage QueryOperation(string principal, QueryOperationMessage m)
    {
        CheckHeader(m.Header);
        if (_sessions.TryGetValue(m.Target.Session, out ReaderSession? session) && string.Equals(session.Principal, principal, StringComparison.Ordinal)
            && session.Outcomes.TryGetValue(m.Target.Sequence, out IWireMessage? reply))
        {
            return new QueryOperationResultMessage(m.Target, WireOutcomeStatus.Committed, CommitToken.None, DfsErrorCode.None, null, DfsOperationKind.Unknown,
                MessageCodec.Encode(reply));
        }

        return new QueryOperationResultMessage(m.Target, m.Resolve ? WireOutcomeStatus.Aborted : WireOutcomeStatus.NotFound, CommitToken.None, DfsErrorCode.None,
            "Backup hosts execute no mutations.", DfsOperationKind.Unknown, ReadOnlyMemory<byte>.Empty);
    }

    private ReaderSession RequireSession(string principal, RequestHeader header)
    {
        CheckHeader(header);
        if (!_sessions.TryGetValue(header.Session, out ReaderSession? session) || !string.Equals(session.Principal, principal, StringComparison.Ordinal))
            throw new DfsLeaseLostException($"Session {header.Session} is not open on this backup host.");
        session.LastRenewal = _s.Time.GetTimestamp();
        return session;
    }

    /// <summary>The retained reply of an operation id of the caller's session (retries answer the same).</summary>
    private static bool TryReplay(ReaderSession? session, RequestHeader header, out IWireMessage reply)
    {
        reply = null!;
        return session is not null && !header.Operation.IsEmpty && header.Operation.Session == session.Id
            && session.Outcomes.TryGetValue(header.Operation.Sequence, out reply!);
    }

    private static void Remember(ReaderSession? session, RequestHeader header, IWireMessage reply)
    {
        if (session is null || header.Operation.IsEmpty || header.Operation.Session != session.Id)
            return;
        session.Outcomes[header.Operation.Sequence] = reply;
        while (session.Outcomes.Count > 256)
            session.Outcomes.Remove(session.Outcomes.Keys.Min());
    }

    // ------------------------------------------------------------------------------------------------- snapshots

    /// <summary><c>ListSnapshots</c>: published snapshots of the volume (publication order) with the retention this node enforces.</summary>
    public IWireMessage ListSnapshots(string principal, ListSnapshotsMessage m)
    {
        _s.Principals.RequireRead(principal, "list snapshots");
        CheckHeader(m.Header);
        _s.Catalog.RequireLoaded("listing snapshots");
        BackupVolume? volume = _s.Catalog.Find(m.Header.Volume);
        SnapshotInfoData[] snapshots = volume is null
            ? []
            : volume.Visible.Where(x => m.Name is null || string.Equals(x.Name, m.Name, StringComparison.Ordinal)).Select(x => x.Describe(volume.Name)).ToArray();
        return new ListSnapshotsResultMessage(snapshots, volume?.Pointer?.Covered ?? CommitToken.None);
    }

    /// <summary><c>OpenSnapshot</c>: a local reader pin on a published snapshot (it keeps it retained until released or expired).</summary>
    public IWireMessage OpenSnapshot(string principal, OpenSnapshotMessage m)
    {
        _s.Principals.RequireRead(principal, "open snapshots");
        ReaderSession session = RequireSession(principal, m.Header);
        if (TryReplay(session, m.Header, out IWireMessage replay))
            return replay;
        _s.Catalog.RequireLoaded($"reading snapshot '{m.Name}'");
        BackupVolume volume = _s.Catalog.Find(m.Header.Volume) ?? throw SnapshotNotFound(m.Name, m.Header.Volume);
        BackupSnapshot snapshot = volume.FindVisible(m.Name) ?? throw SnapshotNotFound(m.Name, volume.Name);
        var pin = new ReaderPin(_nextPin++, session.Id, principal, volume, snapshot) { LastRenewal = _s.Time.GetTimestamp() };
        _pins[pin.Id] = pin;
        snapshot.ReaderPins++;
        var reply = new OpenSnapshotResultMessage(pin.Id, snapshot.Describe(volume.Name), _s.Timings.PinDuration);
        Remember(session, m.Header, reply);
        return reply;
    }

    /// <summary><c>CloseSnapshot</c>: releases a reader pin of the caller (idempotent).</summary>
    public async Task<IWireMessage> CloseSnapshotAsync(string principal, CloseSnapshotMessage m, CancellationToken ct)
    {
        CheckHeader(m.Header);
        if (!_pins.TryGetValue(m.SnapshotPin, out ReaderPin? pin) || !string.Equals(pin.Principal, principal, StringComparison.Ordinal))
            return new CloseSnapshotResultMessage(false);
        await ReleasePinAsync(pin, ct);
        return new CloseSnapshotResultMessage(true);
    }

    private async Task ReleasePinAsync(ReaderPin pin, CancellationToken ct)
    {
        if (!_pins.Remove(pin.Id))
            return;
        pin.Snapshot.ReaderPins = Math.Max(0, pin.Snapshot.ReaderPins - 1);
        if (pin.Snapshot.Condemned && pin.Snapshot.ReaderPins == 0)
        {
            using IDisposable held = await _s.Catalog.Locks.AcquireAsync(pin.Volume.Name, ct);
            await _s.Catalog.ReleaseCondemnedAsync(pin.Volume, ct);
            await _s.Catalog.CollectGarbageAsync(ct);
        }
    }

    /// <summary>
    /// <c>SetRetention</c>: the retention administrator sets retention; the export principal may only extend it; nobody else may
    /// change it (FSD §12.4, BAK-022).
    /// </summary>
    public async Task<IWireMessage> SetRetentionAsync(string principal, SetRetentionMessage m, CancellationToken ct)
    {
        ReaderSession? session = _sessions.GetValueOrDefault(m.Header.Session);
        CheckHeader(m.Header);
        if (TryReplay(session, m.Header, out IWireMessage replay))
            return replay;
        bool admin = _s.Principals.IsRetentionAdmin(principal);
        if (!admin && !_s.Principals.IsExporter(principal))
            throw new UnauthorizedAccessException($"Principal '{principal}' may not change retention on this backup host.");
        ArgumentNullException.ThrowIfNull(m.Retention);
        _s.Catalog.RequireLoaded("changing retention");
        BackupVolume volume = _s.Catalog.Find(m.Header.Volume) ?? throw SnapshotNotFound(m.Name, m.Header.Volume);
        using IDisposable held = await _s.Catalog.Locks.AcquireAsync(volume.Name, ct);

        // A retry that waited for the volume lock while the original still ran is answered by the original's outcome.
        if (TryReplay(session, m.Header, out replay))
            return replay;
        BackupSnapshot snapshot = volume.FindVisible(m.Name) ?? throw SnapshotNotFound(m.Name, volume.Name);
        if (!admin && !BackupReceiver.IsExtension(snapshot.Retention, m.Retention))
        {
            throw new DfsRetentionProtectedException(
                $"Principal '{principal}' may only extend the retention of snapshot '{m.Name}'; shortening it or releasing its hold needs the backup's retention administrator.");
        }

        await _s.Catalog.SetRetentionAsync(volume, snapshot, m.Retention, ct);
        var reply = new SetRetentionResultMessage(snapshot.Describe(volume.Name), CommitToken.None);
        Remember(session, m.Header, reply);
        return reply;
    }

    /// <summary><c>DeleteSnapshot</c>: only the retention administrator deletes, and only snapshots retention does not protect.</summary>
    public async Task<IWireMessage> DeleteSnapshotAsync(string principal, DeleteSnapshotMessage m, CancellationToken ct)
    {
        ReaderSession? session = _sessions.GetValueOrDefault(m.Header.Session);
        CheckHeader(m.Header);
        if (TryReplay(session, m.Header, out IWireMessage replay))
            return replay;
        if (!_s.Principals.IsRetentionAdmin(principal))
            throw new UnauthorizedAccessException($"Principal '{principal}' may not delete snapshots on this backup host.");
        _s.Catalog.RequireLoaded("deleting a snapshot");
        BackupVolume volume = _s.Catalog.Find(m.Header.Volume) ?? throw SnapshotNotFound(m.Name, m.Header.Volume);
        using IDisposable held = await _s.Catalog.Locks.AcquireAsync(volume.Name, ct);

        // A retry that waited for the volume lock while the original still ran (condemning and collecting can take long) is
        // answered by the original's outcome instead of finding the snapshot gone.
        if (TryReplay(session, m.Header, out replay))
            return replay;
        BackupSnapshot snapshot = volume.FindVisible(m.Name) ?? throw SnapshotNotFound(m.Name, volume.Name);
        if (snapshot.IsProtected(_s.Time.GetUtcNow()))
        {
            throw new DfsRetentionProtectedException(snapshot.Retention.Hold
                ? $"Snapshot '{m.Name}' is under a retention hold."
                : $"Snapshot '{m.Name}' is retained until {snapshot.Retention.ExpiresUtc:O}.");
        }

        if (ReferenceEquals(volume.Pointer, snapshot))
            throw new DfsReadOnlyException($"Snapshot '{m.Name}' is the published pointer of volume '{volume.Name}'; the newest recovery point is never deleted.");
        await _s.Catalog.CondemnAsync(volume, snapshot, ct);
        await _s.Catalog.CollectGarbageAsync(ct);
        var reply = new DeleteSnapshotResultMessage(true, CommitToken.None);
        Remember(session, m.Header, reply);
        return reply;
    }

    private static DfsException SnapshotNotFound(string name, string volume)
        => new(DfsErrorCode.SnapshotNotFound, $"Snapshot '{name}' of volume '{volume}' is not published on this backup host.");

    // ------------------------------------------------------------------------------------------------- views

    /// <summary>A reader pin of the caller and its snapshot (PinExpired when gone).</summary>
    private ReaderPin RequirePin(string principal, RequestHeader header, ulong id)
    {
        CheckHeader(header);
        if (!_pins.TryGetValue(id, out ReaderPin? pin) || !string.Equals(pin.Principal, principal, StringComparison.Ordinal)
            || (header.Session != 0 && pin.Session != header.Session))
        {
            throw new DfsPinExpiredException($"Snapshot reader pin {id} is not held on this backup host (expired, released or lost with a restart).");
        }

        if (!string.Equals(pin.Volume.Name, header.Volume, StringComparison.Ordinal))
            throw new DfsPinExpiredException($"Snapshot reader pin {id} belongs to another volume.");
        _s.Keys.RequireUnlocked($"reading snapshot '{pin.Snapshot.Name}'");
        pin.LastRenewal = _s.Time.GetTimestamp();
        return pin;
    }

    private static string NormalizePath(string path)
    {
        if (!DfsPath.TryNormalize(path, out string normalized, out string? error))
            throw new ArgumentException(error ?? $"Invalid path '{path}'.", "path");
        return normalized;
    }

    /// <summary><c>GetInfo</c> inside a snapshot view (backup hosts have no live namespace).</summary>
    public IWireMessage GetInfo(string principal, GetInfoMessage m)
    {
        _s.Principals.RequireRead(principal, "read");
        if (m.SnapshotPin == 0)
            throw new DfsException(DfsErrorCode.Unsupported, "A backup host serves snapshot views only; open a snapshot first.");
        ReaderPin pin = RequirePin(principal, m.Header, m.SnapshotPin);
        string normalized = NormalizePath(m.Path);
        (string filePath, string? stream) = DfsStreamPath.Split(normalized);
        Resolution res = new NamespaceView(pin.Snapshot.Root).Resolve(filePath);
        if (!res.Found || (stream is not null && !res.Target!.Streams.ContainsKey(stream)))
            return new GetInfoResultMessage(false, null, pin.Snapshot.Covered);
        return new GetInfoResultMessage(true, VolumeStateMachine.Describe(null, res.Target!, normalized, stream), pin.Snapshot.Covered);
    }

    /// <summary><c>ListDirectory</c> inside a snapshot view (continuations bind the pin, directory, filter and position).</summary>
    public IWireMessage ListDirectory(string principal, ListDirectoryMessage m)
    {
        _s.Principals.RequireRead(principal, "enumerate");
        if (m.SnapshotPin == 0)
            throw new DfsException(DfsErrorCode.Unsupported, "A backup host serves snapshot views only; open a snapshot first.");
        if (m.PageSize is < 1 or > 10000)
            throw new ArgumentException("The page size must be between 1 and 10000.");
        if (m.Kinds == WireEntryKinds.None || ((int)m.Kinds & ~(int)WireEntryKinds.All) != 0)
            throw new ArgumentException($"Invalid entry kinds {(int)m.Kinds}.");
        if (VolumeService.GetPatternError(m.Pattern) is { } patternError)
            throw new ArgumentException(patternError);
        string path = NormalizePath(m.Path);
        if (DfsStreamPath.HasStream(path))
            throw new ArgumentException($"'{path}' addresses a stream; directories have no stream syntax.", "path");
        ReaderPin pin;
        try
        {
            pin = RequirePin(principal, m.Header, m.SnapshotPin);
        }
        catch (DfsPinExpiredException ex) when (m.Continuation is not null)
        {
            throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, ex.Message);
        }

        ImmutableSortedDictionary<ulong, InodeRecord> root = pin.Snapshot.Root;
        Resolution res = new NamespaceView(root).Resolve(path);
        if (!res.Found)
            throw new DirectoryNotFoundException($"'{path}' does not exist.");
        if (!res.Target!.IsDirectory)
            throw new DfsException(DfsErrorCode.NotADirectory, $"'{path}' is not a directory.");
        InodeRecord directory = res.Target;
        string? after = null;
        if (m.Continuation is { } token)
        {
            if (!TryDecodeContinuation(token, out ulong tokenPin, out ulong tokenDirectory, out string tokenAfter, out string tokenFilter)
                || tokenPin != pin.Id || tokenDirectory != directory.Id || !string.Equals(tokenFilter, Filter(m.Pattern, m.Kinds), StringComparison.Ordinal))
            {
                throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The continuation does not belong to this enumeration.");
            }

            after = tokenAfter;
        }

        var page = new List<DirectoryEntryData>(Math.Min(m.PageSize, 256));
        bool more = false;
        foreach (KeyValuePair<string, ulong> child in directory.Children)
        {
            if (after is not null && directory.Children.KeyComparer.Compare(child.Key, after) <= 0)
                continue;
            if (root.GetValueOrDefault(child.Value) is not { } node)
                continue;
            bool include = node.IsDirectory ? (m.Kinds & WireEntryKinds.Directories) != 0 : (m.Kinds & WireEntryKinds.Files) != 0;
            if (!include || !VolumeService.Matches(m.Pattern, child.Key, directory.ExportProfile == DfsExportProfile.Windows))
                continue;
            if (page.Count == m.PageSize)
            {
                more = true;
                break;
            }

            long length = node.IsDirectory ? 0 : node.DefaultStream?.Length ?? 0;
            page.Add(new DirectoryEntryData(child.Key, node.IsDirectory ? WireEntryKind.Directory : WireEntryKind.File, new InodeId(node.Id), node.ReportedRevision,
                length, node.ModifiedUtc));
        }

        string? next = more ? EncodeContinuation(pin.Id, directory.Id, page[^1].Name, Filter(m.Pattern, m.Kinds)) : null;
        return new ListDirectoryResultMessage(page, next, pin.Snapshot.Covered, 0, new InodeId(directory.Id), directory.Revision);
    }

    /// <summary><c>ReadRange</c> by (snapshot pin, inode): inline bytes, or extents held by this node with a read capability.</summary>
    public IWireMessage ReadRange(string principal, ReadRangeMessage m)
    {
        _s.Principals.RequireRead(principal, "read");
        if (!m.Handle.IsEmpty || m.SnapshotPin == 0)
            throw new DfsReadOnlyException("A backup host grants no handles; read through a snapshot view.");
        if (m.Offset < 0 || m.Length < 0 || m.Offset > long.MaxValue - m.Length)
            throw new ArgumentException($"Invalid read range [{m.Offset}, +{m.Length}).");
        ReaderPin pin = RequirePin(principal, m.Header, m.SnapshotPin);
        InodeRecord inode = pin.Snapshot.Root.GetValueOrDefault(m.Inode.Value)
            ?? throw new FileNotFoundException($"Inode {m.Inode.Value} is not in the snapshot.");
        StreamRecord stream;
        if (m.Stream is { } named)
        {
            if (named.Length == 0 || !DfsPath.TryNormalize("/x:" + named, out string normalizedStream, out _)
                || DfsStreamPath.GetStreamName(normalizedStream) is not { } streamName)
            {
                throw new ArgumentException($"'{named}' is not a valid stream name.");
            }

            stream = inode.GetStream(streamName) ?? throw new FileNotFoundException($"Inode {inode.Id} has no stream '{streamName}' in the snapshot.");
        }
        else
        {
            stream = inode.DefaultStream ?? throw new DfsException(DfsErrorCode.IsADirectory, $"Inode {inode.Id} is a directory.");
        }

        long fileLength = stream.Length;
        long start = Math.Min(m.Offset, fileLength);
        long end = Math.Min(fileLength, m.Offset + m.Length);
        DateTimeOffset now = _s.Time.GetUtcNow();
        if (end <= start)
            return new ReadRangeResultMessage(stream.Revision, fileLength, m.Offset, 0, stream.IsInline, ReadOnlyMemory<byte>.Empty, [], ReadOnlyMemory<byte>.Empty, now,
                pin.Snapshot.Covered);
        if (stream.IsInline)
        {
            return new ReadRangeResultMessage(stream.Revision, fileLength, m.Offset, end - start, true, stream.Inline.Slice((int)start, (int)(end - start)), [],
                ReadOnlyMemory<byte>.Empty, now, pin.Snapshot.Covered);
        }

        List<ExtentRef> extents = ContentAlgebra.Intersecting(stream, start, end, VolumeService.MaxExtentsPerRead + 1);
        if (extents.Count > VolumeService.MaxExtentsPerRead)
        {
            end = Math.Max(start + 1, extents[VolumeService.MaxExtentsPerRead].Offset);
            extents.RemoveRange(VolumeService.MaxExtentsPerRead, extents.Count - VolumeService.MaxExtentsPerRead);
        }

        NodeId[] self = [_s.Keys.NodeId];
        ExtentData[] data = extents.Select(e => new ExtentData(e.Offset, e.Length, e.Object, e.ObjectOffset, e.ObjectLength, e.Checksum, e.Encoding, 0, self))
            .ToArray();
        byte[] capability = [];
        DateTimeOffset expiry = now;
        if (data.Length > 0)
        {
            TimeSpan lifetime = _s.Timings.PinDuration > _s.Timings.LeaseDuration ? _s.Timings.PinDuration : _s.Timings.LeaseDuration;
            expiry = now + lifetime;
            capability = DataCapability.ForRead(_s.Options.SourceClusterId, VolumeIdOf(pin.Volume.Name), 1, principal,
                data.Select(d => d.Object).Distinct().OrderBy(o => o.Value), expiry).Sign(_capabilityKey).Encode();
        }

        return new ReadRangeResultMessage(stream.Revision, fileLength, m.Offset, end - start, false, ReadOnlyMemory<byte>.Empty, data, capability, expiry,
            pin.Snapshot.Covered);
    }

    /// <summary><c>ListStreams</c> inside a snapshot view.</summary>
    public IWireMessage ListStreams(string principal, ListStreamsMessage m)
    {
        (ReaderPin pin, Resolution res, _) = ResolveExisting(principal, m.Header, m.Path, m.SnapshotPin, "list streams");
        var streams = res.Target!.Streams.Where(p => p.Key.Length > 0).Select(p => new StreamInfoData(p.Key, p.Value.Length, p.Value.Revision)).ToArray();
        return new ListStreamsResultMessage(streams, pin.Snapshot.Covered);
    }

    /// <summary><c>GetAccessControl</c> inside a snapshot view: the descriptor as it was at the snapshot.</summary>
    public IWireMessage GetAccessControl(string principal, GetAccessControlMessage m)
    {
        (ReaderPin pin, Resolution res, string path) = ResolveExisting(principal, m.Header, m.Path, m.SnapshotPin, "read permissions");
        InodeRecord target = res.Target!;
        IReadOnlyList<DfsAccessRule> rules = VolumeAccess.EffectiveRules(res.ChainNearestFirst(), DfsStreamPath.GetFilePath(path));
        return new GetAccessControlResultMessage(new DfsSecurityDescriptor(target.Owner, rules, target.Security.AreAccessRulesProtected), new InodeId(target.Id),
            target.ReportedRevision, pin.Snapshot.Covered);
    }

    /// <summary><c>GetEffectiveRights</c> inside a snapshot view (the rights the snapshot's descriptors grant an identity).</summary>
    public IWireMessage GetEffectiveRights(string principal, GetEffectiveRightsMessage m)
    {
        (ReaderPin pin, Resolution res, _) = ResolveExisting(principal, m.Header, m.Path, m.SnapshotPin, "read permissions");
        DfsIdentity identity = m.Identity ?? (DfsIdentity.TryParse("user:" + principal, out DfsIdentity user) ? user : DfsIdentity.Everyone);
        var ids = new HashSet<DfsIdentity> { identity, DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers };
        bool admin = identity == DfsIdentity.Administrators;
        return new GetEffectiveRightsResultMessage(VolumeAccess.Rights(res.ChainNearestFirst(), ids, admin), pin.Snapshot.Covered);
    }

    private (ReaderPin Pin, Resolution Res, string Path) ResolveExisting(string principal, RequestHeader header, string rawPath, ulong snapshotPin, string what)
    {
        _s.Principals.RequireRead(principal, what);
        if (snapshotPin == 0)
            throw new DfsException(DfsErrorCode.Unsupported, "A backup host serves snapshot views only; open a snapshot first.");
        ReaderPin pin = RequirePin(principal, header, snapshotPin);
        string path = NormalizePath(rawPath);
        (string filePath, string? stream) = DfsStreamPath.Split(path);
        Resolution res = new NamespaceView(pin.Snapshot.Root).Resolve(filePath);
        if (!res.Found)
        {
            if (!res.ParentExists)
                throw new DirectoryNotFoundException($"The directory of '{path}' does not exist.");
            throw new FileNotFoundException($"'{path}' does not exist.", path);
        }

        if (stream is not null && !res.Target!.Streams.ContainsKey(stream))
            throw new FileNotFoundException($"Stream '{path}' does not exist.", path);
        return (pin, res, path);
    }

    // ------------------------------------------------------------------------------------------------- data

    /// <summary><c>GetObject</c>: a verified range of an object, authorized by a read capability this node issued.</summary>
    public async Task<IWireMessage> GetObjectAsync(string principal, GetObjectMessage m, CancellationToken ct)
    {
        if (m.Capability.IsEmpty || !DataCapability.TryDecode(m.Capability.Span, out DataCapability? capability) || capability is null
            || !capability.VerifyMac(_capabilityKey) || capability.Kind != DataCapabilityKind.Read || capability.ClusterId != _s.Options.SourceClusterId
            || (m.Volume.Value != Guid.Empty && capability.Volume != m.Volume) || !string.Equals(capability.Principal, principal, StringComparison.Ordinal)
            || capability.IsExpired(_s.Time.GetUtcNow()) || !capability.AllowsRead(m.Object))
        {
            throw new UnauthorizedAccessException("Access denied: a valid read capability of this backup host is required.");
        }

        if (m.Offset < 0 || m.Length < 0)
            throw new ArgumentException("Offset and length must not be negative.");
        ObjectStore store = _s.RequireStore("reading objects");
        ObjectRecordInfo info = store.TryGetInfo(m.Object) ?? throw new DfsObjectMissingException(m.Object);
        long objectLength = info.Length;
        if (m.Offset >= objectLength)
            return new GetObjectResultMessage(objectLength, info.Checksum, ObjectEncodings.Plain, m.Offset, ReadOnlyMemory<byte>.Empty, 0, []);
        long requested = Math.Min(m.Length, objectLength - m.Offset);
        long end = m.Offset + requested;
        int unit = ObjectStore.UnitSize;
        long start = m.Offset / unit * unit;
        long alignedEnd = Math.Min(objectLength, (end + unit - 1) / unit * unit);
        var data = new byte[alignedEnd - start];
        int read = await store.ReadAsync(m.Object, start, data, ct);
        if (read != data.Length)
            throw new DfsIntegrityException(m.Object, $"Object {m.Object} returned {read} of {data.Length} bytes.");
        var checksums = new uint[(data.Length + unit - 1) / unit];
        for (int i = 0; i < checksums.Length; i++)
        {
            int offset = i * unit;
            checksums[i] = ObjectChecksum.Crc32C(data.AsSpan(offset, Math.Min(unit, data.Length - offset)));
        }

        return new GetObjectResultMessage(objectLength, info.Checksum, ObjectEncodings.Plain, start, data, unit, checksums);
    }

    /// <summary><c>LocateObject</c>: which of the listed objects this node holds.</summary>
    public IWireMessage LocateObject(string principal, LocateObjectMessage m)
    {
        _s.Principals.RequireRead(principal, "locate objects");
        ObjectStore store = _s.RequireStore("locating objects");
        var locations = m.Objects.Select(o => store.TryGetInfo(o) is { } info
            ? new ObjectLocation(o, true, info.Length, info.Checksum, default)
            : new ObjectLocation(o, false, 0, UInt128.Zero, default)).ToArray();
        return new LocateObjectResultMessage(locations);
    }

    // ------------------------------------------------------------------------------------------------- restore export

    /// <summary>
    /// <c>BackupExportRestore</c> (restoring cluster, reader credentials): <c>Describe</c> pins a published snapshot, then the
    /// plaintext manifest and objects are read in chunks under the pin, and <c>Release</c> drops it. Without key material the
    /// request fails with RecoveryKeyMissing unless it carries the recovery key (RST-005).
    /// </summary>
    public async Task<IWireMessage> ExportRestoreAsync(string principal, BackupExportRestoreMessage m, CancellationToken ct)
    {
        _s.Principals.RequireRead(principal, "export snapshots for restore");
        if (!_s.Keys.IsUnlocked && !m.RecoveryKey.IsEmpty)
        {
            if (!_s.Keys.TryUnlock(m.RecoveryKey.Span))
                throw new DfsRecoveryKeyMissingException("The supplied recovery key does not unlock this backup node's data.");
        }

        // An unlock (this request's or an earlier one's) completes before anything is served: the store opens and the catalog,
        // sealed with the data key, loads; a failed earlier completion is retried.
        if (_s.Keys.IsUnlocked && (!_s.Catalog.IsLoaded || _s.UnlockCompletion is { IsCompleted: false }))
            await BackupRuntime.OnUnlockedAsync(_s).WaitAsync(ct);

        int max = m.MaxBytes <= 0 ? MaxRestoreChunk : Math.Min(m.MaxBytes, MaxRestoreChunk);
        switch (m.Part)
        {
            case BackupRestorePart.Describe:
            {
                _s.Catalog.RequireLoaded("restoring a snapshot");
                BackupVolume volume = _s.Catalog.Find(m.SourceVolume) ?? throw SnapshotNotFound(m.SnapshotName, m.SourceVolume);
                BackupSnapshot snapshot = volume.FindVisible(m.SnapshotName) ?? throw SnapshotNotFound(m.SnapshotName, m.SourceVolume);
                BackupManifest manifest = snapshot.Manifest ?? throw new DfsRecoveryKeyMissingException("The snapshot's manifest cannot be decrypted.");
                byte[] plain = manifest.Encode();
                var pin = new ReaderPin(_nextPin++, 0, principal, volume, snapshot) { LastRenewal = _s.Time.GetTimestamp(), Manifest = plain };
                _pins[pin.Id] = pin;
                snapshot.ReaderPins++;
                var descriptor = new BackupRestoreDescriptor(snapshot.Describe(volume.Name), manifest.SourceClusterId, manifest.VolumeId, manifest.Epoch, plain.Length,
                    BackupManifest.Checksum(plain), manifest.Closure.Count, manifest.Closure.Sum(c => c.Length));
                return new BackupExportRestoreResultMessage(m.Part, pin.Id, descriptor, 0, plain.Length, BackupManifest.Checksum(plain), ReadOnlyMemory<byte>.Empty,
                    false);
            }

            case BackupRestorePart.Manifest:
            {
                ReaderPin pin = RestorePin(principal, m);
                byte[] plain = pin.Manifest ?? throw new DfsPinExpiredException("The restore pin carries no manifest.");
                if (m.Offset < 0 || m.Offset > plain.Length)
                    throw new ArgumentException("The manifest offset is out of range.");
                int take = (int)Math.Min(max, plain.Length - m.Offset);
                return new BackupExportRestoreResultMessage(m.Part, pin.Id, null, m.Offset, plain.Length, BackupManifest.Checksum(plain),
                    plain.AsMemory((int)m.Offset, take), m.Offset + take >= plain.Length);
            }

            case BackupRestorePart.Object:
            {
                ReaderPin pin = RestorePin(principal, m);
                BackupManifest manifest = pin.Snapshot.Manifest ?? throw new DfsRecoveryKeyMissingException("The snapshot's manifest cannot be decrypted.");
                if (!manifest.Closure.Any(c => c.Id == m.Object))
                    throw new UnauthorizedAccessException($"Object {m.Object} is not part of snapshot '{pin.Snapshot.Name}'.");
                ObjectStore store = _s.RequireStore("restoring a snapshot");
                ObjectRecordInfo info = store.TryGetInfo(m.Object) ?? throw new DfsObjectMissingException(m.Object);
                if (m.Offset < 0 || m.Offset > info.Length)
                    throw new ArgumentException("The object offset is out of range.");
                int take = (int)Math.Min(max, info.Length - m.Offset);
                var data = new byte[take];
                int read = take == 0 ? 0 : await store.ReadAsync(m.Object, m.Offset, data, ct);
                if (read != take)
                    throw new DfsIntegrityException(m.Object, $"Object {m.Object} returned {read} of {take} bytes.");
                return new BackupExportRestoreResultMessage(m.Part, pin.Id, null, m.Offset, info.Length, info.Checksum, data, m.Offset + take >= info.Length);
            }

            case BackupRestorePart.Release:
            {
                if (_pins.TryGetValue(m.ReaderPin, out ReaderPin? pin) && pin.Session == 0 && string.Equals(pin.Principal, principal, StringComparison.Ordinal))
                    await ReleasePinAsync(pin, ct);
                return new BackupExportRestoreResultMessage(m.Part, m.ReaderPin, null, 0, 0, UInt128.Zero, ReadOnlyMemory<byte>.Empty, true);
            }

            default:
                throw new ArgumentException($"Unknown restore part {m.Part}.");
        }
    }

    private ReaderPin RestorePin(string principal, BackupExportRestoreMessage m)
    {
        if (!_pins.TryGetValue(m.ReaderPin, out ReaderPin? pin) || pin.Session != 0 || !string.Equals(pin.Principal, principal, StringComparison.Ordinal)
            || !string.Equals(pin.Volume.Name, m.SourceVolume, StringComparison.Ordinal) || !string.Equals(pin.Snapshot.Name, m.SnapshotName, StringComparison.Ordinal))
        {
            throw new DfsPinExpiredException($"Restore pin {m.ReaderPin} is not held on this backup host.");
        }

        _s.Keys.RequireUnlocked("restoring a snapshot");
        pin.LastRenewal = _s.Time.GetTimestamp();
        return pin;
    }

    // ------------------------------------------------------------------------------------------------- expiry

    /// <summary>Expires sessions and reader pins that were not renewed in time (their snapshots may then be pruned).</summary>
    public async Task ExpireAsync(CancellationToken ct)
    {
        long now = _s.Time.GetTimestamp();
        TimeSpan sessionTimeout = _s.Timings.SessionTimeout;
        TimeSpan pinTimeout = _s.Timings.PinDuration > _s.Timings.LeaseDuration ? _s.Timings.PinDuration : _s.Timings.LeaseDuration;
        foreach (ReaderSession session in _sessions.Values.ToArray())
        {
            if (_s.Time.GetElapsedTime(session.LastRenewal, now) > sessionTimeout)
                _sessions.Remove(session.Id);
        }

        foreach (ReaderPin pin in _pins.Values.ToArray())
        {
            TimeSpan limit = pin.Session == 0 ? sessionTimeout : pinTimeout;
            bool sessionGone = pin.Session != 0 && !_sessions.ContainsKey(pin.Session);
            if (sessionGone || _s.Time.GetElapsedTime(pin.LastRenewal, now) > limit)
                await ReleasePinAsync(pin, ct);
        }
    }

    // ------------------------------------------------------------------------------------------------- continuations

    private static string Filter(string pattern, WireEntryKinds kinds) => pattern + "\0" + ((int)kinds).ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static string EncodeContinuation(ulong pin, ulong directory, string after, string filter)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer);
        w.WriteVarUInt(pin);
        w.WriteVarUInt(directory);
        w.WriteString(after);
        w.WriteString(filter);
        return ContinuationPrefix + Convert.ToBase64String(buffer.WrittenSpan).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static bool TryDecodeContinuation(string token, out ulong pin, out ulong directory, out string after, out string filter)
    {
        pin = 0;
        directory = 0;
        after = string.Empty;
        filter = string.Empty;
        if (!token.StartsWith(ContinuationPrefix, StringComparison.Ordinal) || token.Length > 4096)
            return false;
        string body = token[ContinuationPrefix.Length..].Replace('-', '+').Replace('_', '/');
        body = body.PadRight(body.Length + ((4 - (body.Length % 4)) % 4), '=');
        try
        {
            byte[] bytes = Convert.FromBase64String(body);
            var r = new WireReader(bytes, WireLimits.Default);
            pin = r.ReadVarUInt();
            directory = r.ReadVarUInt();
            after = r.ReadString();
            filter = r.ReadString();
            r.EnsureEnd();
            return true;
        }
        catch (Exception ex) when (ex is FormatException or DfsProtocolException)
        {
            return false;
        }
    }

    private sealed class ReaderSession(ulong id, string principal, string volume, UInt128 nonce)
    {
        public ulong Id { get; } = id;

        public string Principal { get; } = principal;

        public string Volume { get; } = volume;

        public UInt128 Nonce { get; } = nonce;

        public long LastRenewal { get; set; }

        public ulong NextSequence { get; set; } = 1;

        public SortedDictionary<ulong, IWireMessage> Outcomes { get; } = [];
    }

    private sealed class ReaderPin(ulong id, ulong session, string principal, BackupVolume volume, BackupSnapshot snapshot)
    {
        public ulong Id { get; } = id;

        /// <summary>Owning session (0 = a restore export pin).</summary>
        public ulong Session { get; } = session;

        public string Principal { get; } = principal;

        public BackupVolume Volume { get; } = volume;

        public BackupSnapshot Snapshot { get; } = snapshot;

        public long LastRenewal { get; set; }

        /// <summary>The plaintext manifest served to a restore.</summary>
        public byte[]? Manifest { get; init; }
    }
}
