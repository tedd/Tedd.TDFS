using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>A process a fault action can crash, pause or delay (live or backup node).</summary>
internal interface IFaultTarget
{
    /// <summary>Node name.</summary>
    string Name { get; }

    /// <summary>The node's clock (delays run on it).</summary>
    TestClock Clock { get; }

    /// <summary>Lifecycle state.</summary>
    TestNodeState State { get; }

    /// <summary>
    /// True when code reporting this node's name can only be a crashed incarnation unwinding: the node is crashed and no
    /// new incarnation is starting (or the starting one was crashed too).
    /// </summary>
    bool IsCrashedIncarnation { get; }

    /// <summary>The simulated process of the running incarnation (deterministic mode), else null.</summary>
    SimProcess? CurrentProcess { get; }

    /// <summary>Crashes the node now (from a fault point); no-op when it is not running or paused.</summary>
    void CrashFromFault(CrashMode mode, string reason);

    /// <summary>Pauses the node now (from a fault point); false when it is not running.</summary>
    bool PauseFromFault(string reason);

    /// <summary>Resumes a node paused by a fault.</summary>
    void ResumeFromFault(string reason);
}

/// <summary>Resolves fault-target names of a cluster.</summary>
internal interface IFaultTargets
{
    /// <summary>The live or backup node named <paramref name="name"/>, or null.</summary>
    IFaultTarget? FindTarget(string name);
}

/// <summary>Thrown at a fault point after a <see cref="FaultAction.CrashNode"/> action crashed the hitting node (realtime mode).</summary>
internal sealed class NodeCrashedAtFaultException : Exception
{
    public NodeCrashedAtFaultException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The <see cref="IFaultInjector"/> of a test cluster: records every hit in <see cref="TestFaults.Coverage"/>, counts it
/// against every armed fault (in arming order, atomically with the recording) and runs the actions of the arms whose
/// trigger window it falls in.
/// </summary>
internal sealed class HarnessFaultInjector : IFaultInjector
{
    private readonly object _gate = new();
    private readonly TestFaults _faults;

    public HarnessFaultInjector(TestFaults faults)
    {
        _faults = faults;
    }

    /// <inheritdoc />
    public bool IsEnabled => true;

    /// <inheritdoc />
    public ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
    {
        IFaultTarget? target = context.Node is { } nodeName ? _faults.Targets?.FindTarget(nodeName) : null;

        // Code of a crashed incarnation that is still unwinding must not count as reaching a fault point.
        if (target is not null && target.IsCrashedIncarnation && IsZombie(target))
            throw CrashedException(target, context);

        List<FaultArm>? triggered = null;
        lock (_gate)
        {
            _faults.Coverage.Record(context);
            foreach (FaultArm arm in _faults.Arms)
            {
                if (arm.OnHit(context))
                    (triggered ??= []).Add(arm);
            }
        }

        if (triggered is null)
            return default;
        if (_faults.Runtime.Scheduler is not { } scheduler)
            return new ValueTask(RunActionsAsync(triggered, context, target, ct));

        // Deterministic mode. SDK code reaches fault points from continuations inlined without a synchronization context;
        // an action that waits (Block, Pause, Delay, a custom action awaiting a test's completion source) would then resume
        // wherever its awaited task completes — on the thread pool when that happens inside a simulation work item. Running
        // the actions on the hitting process's context keeps every resumption on a simulation lane.
        SynchronizationContext? previous = SynchronizationContext.Current;
        SynchronizationContext? home = previous is null ? scheduler.CurrentProcess?.Context : null;
        Task actions;
        if (home is not null)
            SynchronizationContext.SetSynchronizationContext(home);
        try
        {
            actions = RunActionsAsync(triggered, context, target, ct);
        }
        finally
        {
            if (home is not null)
                SynchronizationContext.SetSynchronizationContext(previous);
        }

        return new ValueTask(actions.IsCompleted ? actions : CompleteOutsideContext(actions));
    }

    /// <summary>
    /// A task that completes like <paramref name="actions"/>, but without a synchronization context installed (deterministic
    /// mode). The actions resume on the context of the code that hit the fault point (a node loop or a client context), so
    /// their own task completes inside a simulation work item; SDK code awaits the hit with <c>ConfigureAwait(false)</c>, and
    /// the runtime would queue that continuation to the thread pool (an uncontrolled dependency, CLAUDE.md) instead of
    /// inlining it on the simulation thread.
    /// </summary>
    private Task CompleteOutsideContext(Task actions)
    {
        TaskCompletionSource done = _faults.Runtime.CreateCompletion();
        ClusterRuntime runtime = _faults.Runtime;
        actions.ContinueWith(
            completed => runtime.Complete(() =>
            {
                try
                {
                    completed.GetAwaiter().GetResult();
                    done.TrySetResult();
                }
#pragma warning disable CA1031 // The exact failure (including an OperationCanceledException) is handed to the awaiting code.
                catch (Exception ex)
#pragma warning restore CA1031
                {
                    done.TrySetException(ex);
                }
            }),
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return done.Task;
    }

    private Exception CrashedException(IFaultTarget target, in FaultContext context)
    {
        if (_faults.Runtime.Scheduler?.CurrentProcess is { IsKilled: true } process)
            return new SimProcessKilledException(process.Id);
        return new NodeCrashedAtFaultException($"Node {target.Name} crashed; its code reached fault point {context.Site} while unwinding.");
    }

    private bool IsZombie(IFaultTarget target)
    {
        _ = target;

        // In deterministic mode the hitting code belongs to the current simulated process: a killed process is a zombie.
        // In realtime mode a crashed node without a starting incarnation has no live code, so any code reporting its name is
        // unwinding.
        if (_faults.Runtime.Scheduler is { } scheduler)
            return scheduler.CurrentProcess is { IsKilled: true };
        return true;
    }

    private async Task RunActionsAsync(List<FaultArm> arms, FaultContext context, IFaultTarget? target, CancellationToken ct)
    {
        foreach (FaultArm arm in arms)
        {
            _faults.Trace($"fault triggered: {arm.Describe()} at {Describe(context)}");
            await RunActionAsync(arm, context, target, ct);
            arm.OnActionCompleted();
        }
    }

    private async Task RunActionAsync(FaultArm arm, FaultContext context, IFaultTarget? target, CancellationToken ct)
    {
        switch (arm.Action)
        {
            case FaultAction.CrashNodeFault crash:
            {
                IFaultTarget victim = target ?? throw new InvalidOperationException(
                    $"CrashNode armed at {arm.Describe()} hit at {context.Site} on '{context.Node}', which is not a node of this cluster.");
                SimProcess? process = victim.CurrentProcess;
                victim.CrashFromFault(crash.Mode, $"fault {arm.Number.ToString(CultureInfo.InvariantCulture)} at {context.Site}");

                // Stop the hitting code path right here: a crash never lets the operation continue.
                if (process is not null)
                    throw new SimProcessKilledException(process.Id);
                throw new NodeCrashedAtFaultException($"Node {victim.Name} crashed at fault point {context.Site} (fault {arm.Describe()}).");
            }

            case FaultAction.PauseNodeFault:
            {
                IFaultTarget victim = target ?? throw new InvalidOperationException(
                    $"PauseNode armed at {arm.Describe()} hit at {context.Site} on '{context.Node}', which is not a node of this cluster.");
                if (arm.IsReleased)
                    return;
                if (victim.PauseFromFault($"fault {arm.Number.ToString(CultureInfo.InvariantCulture)} at {context.Site}"))
                    _faults.RecordPause(arm, victim);
                if (arm.IsReleased)
                {
                    // Released while pausing: make sure the pause does not outlive the release.
                    victim.ResumeFromFault($"fault {arm.Number.ToString(CultureInfo.InvariantCulture)} released");
                    return;
                }

                await arm.WhenReleased();
                return;
            }

            case FaultAction.BlockFault:
                await arm.WhenReleased();
                return;

            case FaultAction.ThrowFault thrown:
                throw thrown.Factory() ?? new InvalidOperationException($"The exception factory of {arm.Describe()} returned null.");

            case FaultAction.IoErrorFault:
                throw new IOException($"Injected I/O error at {context.Site} (fault {arm.Describe()}).", StorageErrors.IoDeviceHResult);

            case FaultAction.DiskFullFault:
                throw StorageErrors.DiskFull(context.Path ?? context.Site);

            case FaultAction.DelayFault delay:
                if (delay.Duration > TimeSpan.Zero)
                {
                    TimeProvider clock = target?.Clock ?? _faults.Runtime.Time;
                    await Task.Delay(delay.Duration, clock, CancellationToken.None);
                }

                return;

            case FaultAction.DropReplyFault:
                throw new FaultReplyDroppedException($"The reply was dropped by fault {arm.Describe()} at {context.Site}.");

            case FaultAction.CancelCallerFault:
                throw new OperationCanceledException($"The caller was cancelled by fault {arm.Describe()} at {context.Site}.", ct);

            case FaultAction.CustomFault custom:
                await custom.Action(context);
                return;

            default:
                throw new InvalidOperationException($"Unknown fault action {arm.Action.GetType().Name}.");
        }
    }

    private static string Describe(in FaultContext context)
    {
        var parts = new List<string> { context.Site, "node=" + context.Node };
        if (context.Volume is not null)
            parts.Add("volume=" + context.Volume);
        if (context.Operation != DfsOperationKind.Unknown)
            parts.Add("op=" + context.Operation);
        if (context.Object is { } obj)
            parts.Add("object=" + obj);
        if (context.Path is not null)
            parts.Add("path=" + context.Path);
        if (context.Occurrence != 0)
            parts.Add("occurrence=" + context.Occurrence.ToString(CultureInfo.InvariantCulture));
        if (context.Detail is not null)
            parts.Add("detail=" + context.Detail);
        return string.Join(' ', parts);
    }
}
