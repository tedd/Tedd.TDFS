using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Sends this node's periodic <c>ReportUsage</c> to the control leader (handled locally when this node leads the control
/// group): device usage and admission headroom, plus a replica-state summary of every volume this node leads. The reply
/// carries the latest scrub request for this node (<c>AdminOp.StartScrub</c>), which starts a full scrub pass. Runs on
/// the node loop.
/// </summary>
internal sealed class UsageReporter
{
    private readonly NodeServices _s;
    private (NodeId Leader, ulong Term) _scrubSource;
    private ulong _scrubSeen;
    private bool _running;

    public UsageReporter(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>One report (periodic job).</summary>
    public async Task RunAsync()
    {
        if (_running || !_s.IsServing || _s.IsStopping || !_s.Control.IsSelfRegistered || _s.Maintenance is not { } plane)
            return;
        _running = true;
        try
        {
            // Migration completions are claimed only on holder-confirmed knowledge (CHA-002).
            foreach (VolumeHost host in _s.Volumes.Hosts)
            {
                if (host.Service is { IsLeader: true } leading && !host.Group.IsClosed)
                {
                    using CancellationTokenSource confirmDeadline = _s.CreateDeadline();
                    try
                    {
                        await leading.ConfirmMigrationAsync(confirmDeadline.Token);
                    }
                    catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException)
                    {
                        _s.Debug("usage", NodeServices.Inv($"Migration confirmation of {host.Name} failed: {ex.GetType().Name}: {ex.Message}"));
                    }
                }
            }

            (ReportUsageMessage report, List<(VolumeService Service, ulong Term)> reported) = Build();
            ReportUsageResultMessage? reply = null;
            if (_s.Control.IsLeader)
            {
                reply = plane.Control.Accept(report);
            }
            else if (_s.Control.Leader is { } leader && leader != _s.NodeId)
            {
                using CancellationTokenSource deadline = _s.CreateDeadline();
                try
                {
                    reply = (ReportUsageResultMessage)await _s.Peers.CallAsync(leader, report, ct: deadline.Token);
                }
                catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException)
                {
                    _s.Debug("usage", NodeServices.Inv($"Usage report to the control leader failed: {ex.GetType().Name}: {ex.Message}"));
                }
            }

            if (reply is not null)
            {
                // The control leader now knows these leaders' replica summaries (they may trim excess copies from now on).
                foreach ((VolumeService service, ulong term) in reported)
                    service.NoteUsageReported(term);
            }

            if (reply is not null && _s.Control.Leader is { } current && _s.Control.Group is { } group)
            {
                if (_scrubSource != (current, group.Term))
                {
                    _scrubSource = (current, group.Term);
                    _scrubSeen = 0;
                }

                if (reply.ScrubRequest > _scrubSeen)
                {
                    _scrubSeen = reply.ScrubRequest;
                    plane.Scrubber.RequestFullPass();
                }
            }
        }
        finally
        {
            _running = false;
        }
    }

    private (ReportUsageMessage Report, List<(VolumeService Service, ulong Term)> Reported) Build()
    {
        ObjectStore store = _s.ObjectStore;
        var volumes = new List<VolumeUsageReport>();
        var reported = new List<(VolumeService Service, ulong Term)>();
        foreach (VolumeHost host in _s.Volumes.Hosts)
        {
            if (host.Service is { IsLeader: true } service && !host.Group.IsClosed && service.BuildUsageReport() is { } summary)
            {
                volumes.Add(summary);
                reported.Add((service, summary.Term));
            }
        }

        return (new ReportUsageMessage(_s.NodeId, _s.Incarnation, store.CapacityBytes, store.MediumUsedBytes, store.AvailableBytes(ObjectAdmission.Normal),
            store.AvailableBytes(ObjectAdmission.Repair), store.LiveBytes, volumes), reported);
    }
}
