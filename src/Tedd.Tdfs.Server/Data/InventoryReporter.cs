using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Storage-node side of anti-entropy and garbage collection (ARCHITECTURE §6, §8): periodically reports this node's copies
/// of every volume to the volume's leader (paged <c>InventoryReport</c>) and deletes the copies the leader classifies as
/// garbage. The decision is the leader's (unreachable from every root, unprotected at the GC watermark, staging session
/// closed); this node only deletes a reported copy whose record still matches the decision's (object, staging) pair, and
/// ignores decisions of a superseded volume epoch. Runs on the node loop.
/// </summary>
internal sealed class InventoryReporter
{
    /// <summary>Items per report page.</summary>
    public const int PageSize = 1000;

    private readonly NodeServices _s;
    private ulong _reports;
    private bool _running;

    public InventoryReporter(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>One reporting pass over every volume this node holds copies of.</summary>
    public async Task RunAsync()
    {
        if (_running || !_s.IsServing || _s.IsStopping || !_s.Control.IsSelfRegistered)
            return;
        _running = true;
        try
        {
            ControlState control = _s.Control.State;
            IReadOnlyList<ObjectRecordInfo> records = _s.ObjectStore.List();
            foreach (IGrouping<VolumeId, ObjectRecordInfo> group in records.GroupBy(r => r.Volume).OrderBy(g => g.Key.Value))
            {
                if (_s.IsStopping)
                    return;
                if (control.FindVolume(group.Key) is not { } volume || volume.State == WireVolumeState.Restoring)
                    continue;
                try
                {
                    await ReportAsync(volume, [.. group.OrderBy(r => r.Id.Value)]);
                }
                catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or UnauthorizedAccessException
                                               or ArgumentException)
                {
                    _s.Debug("inventory", NodeServices.Inv($"Inventory of volume {volume.Name} was not reported: {ex.GetType().Name}: {ex.Message}"));
                }
            }
        }
        finally
        {
            _running = false;
        }
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

        if (last is null || last.Delete.Count == 0 || last.VolumeEpoch < volume.Epoch)
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
