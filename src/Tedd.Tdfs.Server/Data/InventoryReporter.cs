using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Storage-node side of anti-entropy and garbage collection (ARCHITECTURE §6, §8): periodically reports this node's copies
/// of every volume to the volume's leader (paged <c>InventoryReport</c>) and runs the GC sweep the leader's answer
/// describes. A node reports every volume it holds copies of and every volume whose storage pools it belongs to (an empty
/// report tells the leader the node is up and holds nothing). The decision is the leader's (unreachable from every root,
/// unprotected at the GC watermark, staging session closed); this node only deletes a reported copy whose record still
/// matches the decision's (object, staging) pair, and ignores decisions of a superseded volume epoch. Every completed
/// report is one GC sweep on this node (fault site <c>gc.sweep</c>, hit before anything is deleted). Runs on the node loop.
/// </summary>
internal sealed class InventoryReporter
{
    /// <summary>Items per report page.</summary>
    public const int PageSize = 1000;

    private readonly NodeServices _s;
    private ulong _reports;
    private bool _running;
    private bool _again;

    public InventoryReporter(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>
    /// Requests a report soon (after a scrub or repair quarantined a copy): runs a pass now when none is running, otherwise
    /// once more right after the current one.
    /// </summary>
    public void RequestReport()
    {
        if (_running)
        {
            _again = true;
            return;
        }

        if (!_s.IsStopping)
            _s.Jobs.Run("inventory-now", RunAsync);
    }

    /// <summary>One reporting pass over every volume this node holds copies of or serves storage for.</summary>
    public async Task RunAsync()
    {
        if (_running || !_s.IsServing || _s.IsStopping || !_s.Control.IsSelfRegistered)
            return;
        _running = true;
        try
        {
            do
            {
                _again = false;
                await ReportAllAsync();
            }
            while (_again && !_s.IsStopping);
        }
        finally
        {
            _running = false;
        }
    }

    private async Task ReportAllAsync()
    {
        ControlState control = _s.Control.State;
        IReadOnlyList<ObjectRecordInfo> records = _s.ObjectStore.List();
        var byVolume = new SortedDictionary<Guid, List<ObjectRecordInfo>>();
        foreach (ObjectRecordInfo r in records)
        {
            if (!byVolume.TryGetValue(r.Volume.Value, out List<ObjectRecordInfo>? list))
                byVolume[r.Volume.Value] = list = [];
            list.Add(r);
        }

        foreach (VolumeRecord v in control.Volumes.Values)
        {
            if (!byVolume.ContainsKey(v.Id.Value) && ServesStorageFor(control, v))
                byVolume[v.Id.Value] = [];
        }

        foreach ((Guid id, List<ObjectRecordInfo> list) in byVolume)
        {
            if (_s.IsStopping)
                return;
            if (control.FindVolume(new VolumeId(id)) is not { } volume || volume.State == WireVolumeState.Restoring)
                continue;
            try
            {
                await ReportAsync(volume, [.. list.OrderBy(r => r.Id.Value)]);
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or UnauthorizedAccessException
                                           or ArgumentException)
            {
                _s.Debug("inventory", NodeServices.Inv($"Inventory of volume {volume.Name} was not reported: {ex.GetType().Name}: {ex.Message}"));
            }
        }
    }

    /// <summary>True when this node is a live member of a storage pool the volume places objects in.</summary>
    private bool ServesStorageFor(ControlState control, VolumeRecord volume)
    {
        if (control.FindMember(_s.NodeId) is not { IsLive: true })
            return false;
        if (control.PoolMembers(volume.StoragePool).Any(m => m.Id == _s.NodeId))
            return true;
        foreach (DfsStoragePolicy policy in volume.NamedPolicies.Values.Append(volume.DefaultPolicy))
        {
            if (policy.Pool is { } pool && control.PoolMembers(pool).Any(m => m.Id == _s.NodeId))
                return true;
        }

        return false;
    }

    private async Task ReportAsync(VolumeRecord volume, IReadOnlyList<ObjectRecordInfo> records)
    {
        ulong report = ++_reports;
        InventoryReportResultMessage? last = null;
        int pages = Math.Max(1, (records.Count + PageSize - 1) / PageSize);
        for (int page = 0; page < pages; page++)
        {
            InventoryItem[] items = [.. records.Skip(page * PageSize).Take(PageSize)
                .Select(r => new InventoryItem(r.Id, r.Staging, r.Length, r.Checksum, r.Encoding))];
            var message = new InventoryReportMessage(_s.NodeId, _s.Incarnation, volume.Id, report, page, page == pages - 1, items);
            last = await SendAsync(volume, message);
        }

        if (last is null)
            return;

        // One GC sweep per completed report, whether or not it deletes anything (fault site F20 on the sweeping node).
        await _s.HitAsync(FaultStage.F20, FaultSites.GcSweep, volume.Name, DfsOperationKind.Gc, detail: _s.NodeName);
        if (last.Delete.Count == 0 || _s.Control.State.FindVolume(volume.Id) is not { } current || last.VolumeEpoch < current.Epoch)
            return;

        int deleted = 0;
        long bytes = 0;
        foreach (ObjectStagingRef target in last.Delete)
        {
            if (_s.ObjectStore.TryGetInfo(target.Object) is not { } info || info.Volume != volume.Id || info.Staging != target.Staging)
                continue;
            await _s.ObjectStore.DeleteAsync(target.Object);
            deleted++;
            bytes += info.Length;
        }

        if (deleted > 0)
        {
            _s.Metrics.GcDeleted(deleted, bytes);
            _s.Metrics.SetStored(_s.ObjectStore.ObjectCount, _s.ObjectStore.LiveBytes);
        }
    }

    /// <summary>Sends one page to the volume leader: the local service when this node leads, else the leader or a voter.</summary>
    private async Task<InventoryReportResultMessage> SendAsync(VolumeRecord volume, InventoryReportMessage message)
    {
        using CancellationTokenSource deadline = _s.CreateDeadline();
        if (_s.Volumes.TryGet(volume.Id) is { Service: { IsLeader: true } local })
            return await local.InventoryReportLocalAsync(message, deadline.Token);

        var candidates = new List<NodeId>();
        if (_s.Raft.LeaderOf(volume.GroupName) is { } known)
            candidates.Add(known);
        candidates.AddRange(volume.Voters);
        var tried = new HashSet<NodeId>();
        Exception? lastFailure = null;
        for (int i = 0; i < candidates.Count; i++)
        {
            NodeId node = candidates[i];
            if (node == _s.NodeId || !tried.Add(node))
                continue;
            try
            {
                return (InventoryReportResultMessage)await _s.Peers.CallAsync(node, message, ct: deadline.Token);
            }
            catch (DfsUnavailableException ex) when (ex.ErrorCode == DfsErrorCode.NotLeader)
            {
                lastFailure = ex;
                if (ex.LeaderHint is { } hint && MemberAt(hint) is { } hinted && !tried.Contains(hinted))
                    candidates.Insert(i + 1, hinted);
            }
        }

        throw lastFailure ?? new DfsUnavailableException(DfsErrorCode.NotLeader, $"No leader of volume '{volume.Name}' is known.");
    }

    private NodeId? MemberAt(string endpoint)
    {
        foreach (MemberRecord m in _s.Control.State.Members.Values)
        {
            if (m.IsLive && m.Endpoints.Contains(endpoint, StringComparer.Ordinal))
                return m.Id;
        }

        return null;
    }
}
