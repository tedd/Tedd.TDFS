using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Makes the local control-state replica reflect every control decision committed before the barrier started, so a volume
/// leader's authoritative decision sees revocations (grants, groups, principals) that completed before it (SEC-006,
/// ACL-026, ACL-08). The control leader is current by definition; a follower asks the leader for its applied index
/// (<c>GetClusterMap</c> with its own index, which costs no map when it is current) and waits until it applied that far.
/// Concurrent waiters are coalesced: one run serves every waiter that arrived before it started.
/// </summary>
/// <remarks>
/// Availability: when no control leader is known (no control decision can commit) or it cannot be reached within the
/// barrier timeout, the local state is used as is: a revocation the local replica cannot learn of could only have been
/// committed by a quorum this node is cut off from.
/// </remarks>
internal sealed class ControlBarrier
{
    private readonly NodeServices _s;
    private long _requested;
    private long _completed;
    private Task? _running;

    public ControlBarrier(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Longest time a barrier waits for the control leader before proceeding with the local state.</summary>
    public TimeSpan Timeout => TimeSpan.FromTicks(Math.Max(_s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax * 2, TimeSpan.FromMilliseconds(200).Ticks));

    /// <summary>Completes once a barrier that started after this call completed.</summary>
    public async Task WaitAsync(CancellationToken ct)
    {
        long ticket = ++_requested;
        while (_completed < ticket)
        {
            ct.ThrowIfCancellationRequested();
            Task run = _running ?? Start();
            await run.WaitAsync(ct);
        }
    }

    private Task Start()
    {
        Task run = RunAsync();
        if (!run.IsCompleted)
            _running = run;
        return run;
    }

    private async Task RunAsync()
    {
        long covers = _requested;
        try
        {
            await SynchronizeAsync();
        }
        catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or UnauthorizedAccessException
                                       or Transport.RpcConnectionException or NodeLoopStoppedException)
        {
            _s.Debug("volume", "Control barrier proceeds with the local control state: " + ex.Message);
        }
        finally
        {
            _completed = Math.Max(_completed, covers);
            _running = null;
        }
    }

    private async Task SynchronizeAsync()
    {
        ControlHost control = _s.Control;
        if (control.Group is not { IsClosed: false } group || control.IsLeader || control.Leader is not { } leader || leader == _s.NodeId)
            return;
        ulong local = control.State.AppliedIndex;
        using CancellationTokenSource deadline = _s.CreateDeadline(Timeout);
        IWireMessage reply = await _s.Peers.CallAsync(leader, new GetClusterMapMessage(local), Timeout, deadline.Token);
        if (reply is not ClusterMapResultMessage result || result.ConfigIndex <= control.State.AppliedIndex)
            return;
        await group.WaitForAppliedAsync(result.ConfigIndex, deadline.Token);
    }
}
