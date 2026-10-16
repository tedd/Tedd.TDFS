using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary>Most extents returned by one <c>ReadRange</c> (the covered range is shortened instead).</summary>
    public const int MaxExtentsPerRead = 512;

    // Leader-local lease tracking (ARCHITECTURE §5.3): monotonic timestamps of the last renewal. A new leader treats every
    // lease as renewed at its election.
    private readonly Dictionary<ulong, long> _handleRenewals = [];
    private readonly Dictionary<ulong, long> _sessionRenewals = [];
    private readonly Dictionary<ulong, long> _pinRenewals = [];
    private ulong _leaseTerm;
    private long _leaderSince;

    private void ResetLeaderState()
    {
        _leaseTerm = Group.Term;
        _leaderSince = _s.Time.GetTimestamp();
        _handleRenewals.Clear();
        _sessionRenewals.Clear();
        _pinRenewals.Clear();
        ClearReplicaTracking();
        _inventories.Clear();
        _staged.Clear();
        _transactions.Clear();
        CancelRestores();
    }

    private void ClearLeaderState()
    {
        _leaseTerm = 0;
        _readyTerm = 0;
        _handleRenewals.Clear();
        _sessionRenewals.Clear();
        _pinRenewals.Clear();
        ClearReplicaTracking();
        _inventories.Clear();
        _staged.Clear();
        _transactions.Clear();
        CancelRestores();
    }

    private void RenewHandle(ulong handle) => _handleRenewals[handle] = _s.Time.GetTimestamp();

    private void RenewSession(ulong session) => _sessionRenewals[session] = _s.Time.GetTimestamp();

    private void RenewPin(ulong pin) => _pinRenewals[pin] = _s.Time.GetTimestamp();

    /// <summary>
    /// Time since the last renewal this leader saw. A lease this leader has not seen yet counts as renewed now: a lease that
    /// existed at the election is renewed by the election (ARCHITECTURE §5.3), and one granted by an entry this leader applied
    /// but whose reply path has not recorded the grant yet (for example while held at F12) must not be expired as if it had
    /// been granted when the leader was elected.
    /// </summary>
    private TimeSpan SinceRenewal(Dictionary<ulong, long> renewals, ulong id, long now)
    {
        if (!renewals.TryGetValue(id, out long at))
        {
            renewals[id] = now;
            return TimeSpan.Zero;
        }

        return _s.Time.GetElapsedTime(Math.Max(at, _leaderSince), now);
    }

    /// <summary>Drops lease-tracking entries of handles, sessions and pins the applied state no longer has.</summary>
    private void PruneRenewals(VolumeState s)
    {
        static void Prune(Dictionary<ulong, long> renewals, Func<ulong, bool> alive)
        {
            foreach (ulong id in renewals.Keys.Where(id => !alive(id)).ToArray())
                renewals.Remove(id);
        }

        Prune(_handleRenewals, s.Handles.ContainsKey);
        Prune(_sessionRenewals, id => s.Sessions.TryGetValue(id, out SessionRecord? session) && session.State == SessionState.Active);
        Prune(_pinRenewals, s.Pins.ContainsKey);
    }

    /// <summary>
    /// Leader maintenance (periodic job): proposes expiry of handles, sessions and reader pins the leader saw not renewed in
    /// time, retires sessions past their outcome retention, prunes the dereference log, closes stale leader staging sessions
    /// and reclaims detached inodes without handles, and resumes accepted restores.
    /// </summary>
    public async Task MaintainAsync()
    {
        if (!IsLeader || _leaseTerm != Group.Term)
            return;
        if (_readyTerm != Group.Term)
        {
            // A new leader acts only after applying every entry committed before its term; establish that here so its
            // housekeeping (expiry, pruning, resumed restores) does not wait for the first client request.
            using CancellationTokenSource deadline = _s.CreateDeadline();
            try
            {
                await EnsureLeaderReadyAsync(deadline.Token);
            }
            catch (Exception ex) when (ex is DfsException or OperationCanceledException or IOException)
            {
                return;
            }

            if (!IsLeader || _readyTerm != Group.Term || _leaseTerm != Group.Term)
                return;
        }

        long now = _s.Time.GetTimestamp();
        VolumeState s = State;
        NodeNow(out DateTimeOffset wall);
        PruneRenewals(s);

        ImmutableArray<ulong> sessions = [.. s.Sessions.Values
            .Where(x => x.State == SessionState.Active && SinceRenewal(_sessionRenewals, x.Id, now) > x.SessionTimeout)
            .Select(x => x.Id)];
        bool retire = s.Sessions.Values.Any(x => x.State == SessionState.Expired && x.ExpiredUtc is { } at && at + x.OutcomeRetention <= wall);
        if (sessions.Length > 0 || retire)
        {
            await ProposeHousekeepingAsync(new ExpireSessionsCommand(sessions));
            foreach (ulong id in sessions)
                _sessionRenewals.Remove(id);
        }

        s = State;
        ImmutableArray<ulong> handles = [.. s.Handles.Values
            .Where(h => SinceRenewal(_handleRenewals, h.Id, now) > _s.Timings.LeaseDuration)
            .Select(h => h.Id)];
        if (handles.Length > 0)
        {
            RememberEndedWriters(s, handles);
            await ProposeHousekeepingAsync(new ExpireHandlesCommand(handles));
            foreach (ulong id in handles)
                _handleRenewals.Remove(id);
            _s.Metrics.LeaseExpired(handles.Length);
        }

        s = State;
        ImmutableArray<ulong> pins = [.. s.Pins.Values
            .Where(p => (p.Kind == PinKind.SnapshotReader && SinceRenewal(_pinRenewals, p.Id, now) > _s.Timings.PinDuration)
                || (p.Kind == PinKind.Enumeration && SinceRenewal(_pinRenewals, p.Id, now) > _s.Timings.EnumerationPinDuration))
            .Select(p => p.Id)];
        if (pins.Length > 0)
        {
            await ProposeHousekeepingAsync(new ExpirePinsCommand(pins));
            foreach (ulong id in pins)
                _pinRenewals.Remove(id);
        }

        s = State;
        ImmutableArray<string> expiredSnapshots = [.. s.Snapshots.Values
            .Where(x => !x.Hold && x.ExpiresUtc is { } expires && expires <= wall)
            .Select(x => x.Name)];
        if (expiredSnapshots.Length > 0)
            await ProposeHousekeepingAsync(new ExpireSnapshotsCommand(expiredSnapshots));

        s = State;
        TimeSpan stale = _s.Timings.SessionTimeout + _s.Timings.SessionTimeout;
        ulong restoreStaging = s.Restore is { Complete: false } restoring ? restoring.Staging : 0;
        ImmutableArray<ulong> leaderStaging = [.. s.Staging.Values
            .Where(x => x.IsOpen && x.Session == 0 && x.Handle == 0 && x.Transaction.IsEmpty && x.Id != restoreStaging && wall - x.CreatedUtc > stale)
            .Select(x => x.Id)];
        if (leaderStaging.Length > 0)
            await ProposeHousekeepingAsync(new CloseStagingCommand(leaderStaging));

        s = State;
        var handled = new HashSet<ulong>(s.Handles.Values.Select(h => h.Inode));
        ImmutableArray<ulong> orphans = [.. s.Detached.Where(id => !handled.Contains(id))];
        if (orphans.Length > 0)
            await ProposeHousekeepingAsync(new ReclaimDetachedCommand(orphans));

        s = State;
        ulong watermark = VolumeStateMachine.Watermark(s, _s.Mutations.IgnoreSnapshotPins);
        bool prunable = (s.DerefLog.Count > 0 && s.DerefLog[0].Index < watermark)
            || s.Staging.Values.Any(x => !x.IsOpen && x.ClosedIndex < watermark && x.ClosedIndex + 64 < s.AppliedIndex);
        if (prunable && DerefPruneDue(now))
            await ProposeHousekeepingAsync(new PruneDerefLogCommand(watermark));

        // Key rotation is committed in the control group (the wrapped DEK); the volume log records the versions in use so
        // its checkpoints and inspection show which data keys the volume's objects may be sealed with.
        s = State;
        uint keyVersion = CurrentDataKeyVersion;
        if (keyVersion > (s.KeyVersions.IsEmpty ? 0 : s.KeyVersions.Max))
            await ProposeHousekeepingAsync(new RotateDataKeyCommand(keyVersion));

        await MaintainTransactionsAsync();

        if (IsLeader && _readyTerm == Group.Term)
            ResumeRestores();
    }

    private long _lastPrune;

    private bool DerefPruneDue(long now)
    {
        if (_lastPrune != 0 && _s.Time.GetElapsedTime(_lastPrune, now) < _s.Timings.GcInterval)
            return false;
        _lastPrune = now;
        return true;
    }

    private void NodeNow(out DateTimeOffset wall) => wall = _s.Time.GetUtcNow();

    // ------------------------------------------------------------------------------------------------- OpenHandle / CloseHandle

    /// <summary><c>OpenHandle</c> (PROTOCOL.md §8.2): one committed entry; the handle's lease starts now.</summary>
    public async Task<IWireMessage> OpenHandleAsync(RpcContext context, OpenHandleMessage request)
    {
        string path = NormalizePath(request.Path);
        if (VolumeStateMachine.ValidateOpen(request.Mode, request.Access, request.Share, request.Options, request.VersionPinned) is { } invalid)
            return new ErrorReplyMessage(invalid.Code, invalid.Message, request.Header.Operation.IsEmpty ? null : request.Header.Operation);
        string principal = context.Principal ?? string.Empty;
        bool writable = (request.Access & FileAccess.Write) != 0;
        RequireGrant(principal, writable ? DfsPermissions.Write : DfsPermissions.Read, "open");
        if ((request.Access & FileAccess.Read) != 0)
            RequireGrant(principal, DfsPermissions.Read, "open");
        if (request.StoragePolicy is { Length: > 0 } policy && !Record.NamedPolicies.ContainsKey(policy))
            throw new DfsException(DfsErrorCode.InvalidPolicy, $"Volume '{Name}' has no storage policy named '{policy}'.");
        bool mutating = request.Mode is FileMode.CreateNew or FileMode.Create or FileMode.OpenOrCreate or FileMode.Truncate or FileMode.Append;
        return await MutateAsync(context, request, DfsOperationKind.Open,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new OpenHandleCommand(path, request.Mode, request.Access, request.Share, request.Options,
                request.VersionPinned, request.StoragePolicy, request.Header.Session, _s.Timings.LeaseDuration), op)),
            result =>
            {
                if (result.Value is OpenValue opened)
                    RenewHandle(opened.Handle.Id);
                return result.Reply!;
            },
            requireOperation: mutating);
    }

    /// <summary><c>CloseHandle</c>: idempotent by handle id.</summary>
    public async Task<IWireMessage> CloseHandleAsync(RpcContext context, CloseHandleMessage request)
    {
        return await MutateAsync(context, request, DfsOperationKind.CloseHandle,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new CloseHandleCommand(request.Handle.Value, request.Generation), op)),
            result =>
            {
                _handleRenewals.Remove(request.Handle.Value);
                return result.Reply!;
            },
            requireOperation: false, cleanup: true);
    }

    // ------------------------------------------------------------------------------------------------- ReadRange

    /// <summary>
    /// <c>ReadRange</c> (PROTOCOL.md §7.4, §9.4): one revision per call; live handles read after a ReadIndex barrier,
    /// version-pinned handles their pinned version, snapshot readers their snapshot root. Inline bytes or the intersecting
    /// extents plus a read capability for exactly those objects.
    /// </summary>
    public async Task<IWireMessage> ReadRangeAsync(RpcContext context, ReadRangeMessage request)
    {
        CancellationToken ct = context.Cancellation;
        BindTransaction(request, context.Principal ?? string.Empty);
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Read, "read");
        if (request.Offset < 0 || request.Length < 0 || request.Offset > long.MaxValue - request.Length)
            throw new ArgumentException($"Invalid read range [{request.Offset}, +{request.Length}).");
        StreamRecord stream;
        ulong revision;
        CommitToken observed;
        if (!request.Handle.IsEmpty)
        {
            if (request.Stream is not null)
                throw new ArgumentException("A handle read addresses the handle's own stream; Stream is only valid for snapshot reads by inode.");
            ulong applied = await ReadBarrierAsync(ct);
            VolumeState s = State;
            HandleRecord h = ReadableHandle(s, request.Handle.Value, principal);
            if (h.View == HandleView.VersionPinned && h.PinnedInode is { } pinned)
            {
                stream = pinned.GetStream(h.Stream) ?? throw new DfsLeaseLostException($"The pinned stream of handle {h.Id} does not exist.");
            }
            else
            {
                InodeRecord inode = s.Find(h.Inode) ?? throw new DfsLeaseLostException($"The inode of handle {h.Id} no longer exists.");
                stream = inode.GetStream(h.Stream) ?? throw new DfsLeaseLostException($"The stream of handle {h.Id} no longer exists.");
            }

            RenewHandle(h.Id);
            revision = stream.Revision;
            observed = Token(applied);
        }
        else if (request.SnapshotPin != 0)
        {
            await ReadBarrierAsync(ct);
            (PinRecord pin, ImmutableSortedDictionary<ulong, InodeRecord> root) = ReaderPin(request.SnapshotPin, principal, request.Header.Session);
            InodeRecord inode = root.GetValueOrDefault(request.Inode.Value) ?? throw new FileNotFoundException($"Inode {request.Inode.Value} is not in the snapshot.");
            IReadOnlyList<InodeRecord> chain = new NamespaceView(root).ChainOf(inode.Id) ?? [inode];
            (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
            if (!VolumeAccess.Has(VolumeAccess.Rights(chain, ids, admin), DfsFileSystemRights.ReadData))
                throw new UnauthorizedAccessException($"Principal '{principal}' may not read inode {inode.Id} of the snapshot.");
            if (request.Stream is { } named)
            {
                // Named streams of files and directories are part of the snapshot (ADS-020); stream names follow component rules.
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

            RenewPin(pin.Id);
            revision = stream.Revision;
            observed = Token(pin.PinIndex);
        }
        else
        {
            throw new ArgumentException("ReadRange addresses a handle or a snapshot reader pin and inode; live reads by inode are not allowed.");
        }

        return BuildReadReply(principal, stream, revision, request.Offset, request.Length, observed);
    }

    private ReadRangeResultMessage BuildReadReply(string principal, StreamRecord stream, ulong revision, long offset, long length, CommitToken observed)
    {
        long fileLength = stream.Length;
        long start = Math.Min(offset, fileLength);
        long end = Math.Min(fileLength, offset + length);
        if (end <= start)
            return new ReadRangeResultMessage(revision, fileLength, offset, 0, stream.IsInline, ReadOnlyMemory<byte>.Empty, [], ReadOnlyMemory<byte>.Empty,
                _s.Time.GetUtcNow(), observed);
        if (stream.IsInline)
        {
            return new ReadRangeResultMessage(revision, fileLength, offset, end - start, true, stream.Inline.Slice((int)start, (int)(end - start)), [],
                ReadOnlyMemory<byte>.Empty, _s.Time.GetUtcNow(), observed);
        }

        List<ExtentRef> extents = ContentAlgebra.Intersecting(stream, start, end, MaxExtentsPerRead + 1);
        if (extents.Count > MaxExtentsPerRead)
        {
            end = Math.Max(start + 1, extents[MaxExtentsPerRead].Offset);
            extents.RemoveRange(MaxExtentsPerRead, extents.Count - MaxExtentsPerRead);
        }

        var data = extents.Select(e => new ExtentData(e.Offset, e.Length, e.Object, e.ObjectOffset, e.ObjectLength, e.Checksum, e.Encoding,
            e.PlacementEpoch, LocationsOf(e))).ToArray();
        byte[] capability = ReadOnlyMemory<byte>.Empty.ToArray();
        DateTimeOffset expiry = _s.Time.GetUtcNow();
        if (data.Length > 0)
        {
            capability = _s.Objects.Capabilities.IssueRead(Record, principal, data.Select(d => d.Object).Distinct().OrderBy(o => o.Value),
                CapabilityLifetime, out expiry);
        }

        return new ReadRangeResultMessage(revision, fileLength, offset, end - start, false, ReadOnlyMemory<byte>.Empty, data, capability, expiry, observed);
    }

    /// <summary>Lifetime of read capabilities.</summary>
    private TimeSpan CapabilityLifetime => _s.Timings.PinDuration > _s.Timings.LeaseDuration ? _s.Timings.PinDuration : _s.Timings.LeaseDuration;

    /// <summary>A readable handle of the caller (InvalidHandle for foreign/never-issued ids, LeaseLost for expired ones).</summary>
    private static HandleRecord ReadableHandle(VolumeState s, ulong id, string principal)
    {
        if (!s.Handles.TryGetValue(id, out HandleRecord? h))
        {
            if (!s.IssuedHandle(id))
                throw new DfsException(DfsErrorCode.InvalidHandle, $"Handle {id} was never issued by this volume.");
            throw new DfsLeaseLostException($"Handle {id} is no longer active (closed or its lease expired).");
        }

        if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
            throw new DfsException(DfsErrorCode.InvalidHandle, $"Handle {id} belongs to another principal.");
        if (!h.CanRead)
            throw new UnauthorizedAccessException($"Handle {id} was not opened for reading.");
        return h;
    }

    /// <summary>A snapshot reader pin of the caller and its root (PinExpired when gone).</summary>
    private (PinRecord Pin, ImmutableSortedDictionary<ulong, InodeRecord> Root) ReaderPin(ulong id, string principal, ulong session)
    {
        if (!State.Pins.TryGetValue(id, out PinRecord? pin) || pin.Kind != PinKind.SnapshotReader || pin.Root is null)
            throw new DfsPinExpiredException($"Snapshot reader pin {id} is not held (expired or released).");
        if (!string.Equals(pin.Principal, principal, StringComparison.Ordinal) || (session != 0 && pin.Session != session))
            throw new DfsPinExpiredException($"Snapshot reader pin {id} is not held by this session.");
        return (pin, pin.Root);
    }

    // ------------------------------------------------------------------------------------------------- CreateStaging

    /// <summary>
    /// <c>CreateStaging</c> (PROTOCOL.md §7.1): a committed staging session plus a staging capability; <c>Renew</c> re-issues the
    /// capability of an existing open session without a log entry.
    /// </summary>
    public async Task<IWireMessage> CreateStagingAsync(RpcContext context, CreateStagingMessage request)
    {
        CancellationToken ct = context.Cancellation;
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Write, "stage uploads");
        if (request.Close)
        {
            // A whole-file helper abandons what it staged (cancelled, failed): close its unbound staging session so the
            // uploads become garbage (FILE-016). Idempotent; only the owner's unbound sessions can be closed this way.
            if (request.Renew.IsEmpty)
                throw new ArgumentException("A staging close names the staging session to close.");
            return await MutateAsync(context, request, DfsOperationKind.CreateStaging,
                (op, _) => Task.FromResult<VolumeCommand>(Stamp(new CloseStagingCommand([request.Renew.Value]), op)),
                requireOperation: false);
        }

        if (!request.Renew.IsEmpty)
        {
            // Renewing the capability of an open staging session (a transaction's included): no log entry.
            await ReadBarrierAsync(ct);
            if (!State.Staging.TryGetValue(request.Renew.Value, out StagingRecord? staging) || !staging.IsOpen)
                throw new DfsLeaseLostException($"Staging session {request.Renew.Value} is closed.");
            if (!string.Equals(staging.Principal, principal, StringComparison.Ordinal))
                throw new UnauthorizedAccessException($"Staging session {request.Renew.Value} belongs to another principal.");
            return StagingReply(new CreateStagingResultMessage(request.Renew, ReadOnlyMemory<byte>.Empty, default, 0, CommitToken.None), principal);
        }

        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.CreateStaging,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new CreateStagingCommand(request.Header.Session, request.Handle.Value, default), op)),
            requireOperation: false);
        if (reply is not CreateStagingResultMessage created)
            return reply;

        // F05: the staging session is registered; no object of it has been written yet.
        await _s.HitAsync(FaultStage.F05, FaultSites.StagingRegistered, Name, DfsOperationKind.CreateStaging,
            request.Header.Operation.IsEmpty ? null : request.Header.Operation, ct: ct);
        return StagingReply(created, principal);
    }

    private CreateStagingResultMessage StagingReply(CreateStagingResultMessage reply, string principal)
    {
        byte[] capability = _s.Objects.Capabilities.IssueStaging(Record, principal, reply.Staging, StagingCapabilityLifetime, out DateTimeOffset expiry,
            CurrentDataKeyVersion);
        return reply with { Capability = capability, CapabilityExpiry = expiry, PlacementEpoch = PlacementEpochForUploads() };
    }

    /// <summary>
    /// The volume's current data-key version in the local control replica (which a proposal's control barrier made reflect
    /// every rotation committed before it): staging capabilities require storage nodes to seal with at least this version.
    /// </summary>
    private uint CurrentDataKeyVersion => Math.Max(Record.CurrentKeyVersion, _s.Control.State.FindVolume(Record.Id)?.CurrentKeyVersion ?? 0);

    /// <summary>Lifetime of staging capabilities (renewable with <c>CreateStaging(Renew)</c>).</summary>
    private TimeSpan StagingCapabilityLifetime => _s.Timings.SessionTimeout;
}
