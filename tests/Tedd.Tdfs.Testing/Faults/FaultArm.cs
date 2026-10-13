using System.Globalization;

namespace Tedd.Tdfs.Testing;

/// <summary>An armed fault: a match plus the action to run at matching hits.</summary>
/// <remarks>
/// A hit is <i>matching</i> when <see cref="Match"/> (and the optional predicate) accepts its context; matching hits are
/// counted in <see cref="Hits"/>, and the action runs on those whose 1-based number falls in the window
/// [<see cref="FaultMatch.Occurrence"/>, <see cref="FaultMatch.Occurrence"/> + <see cref="FaultMatch.Count"/>).
/// <see cref="Release"/> is permanent: blocks and pauses already holding are released and later triggers no longer hold.
/// </remarks>
public sealed class FaultArm
{
    private readonly object _gate = new();
    private readonly List<FaultContext> _contexts = [];
    private readonly Func<FaultContext, bool>? _predicate;
    private TestFaults? _owner;
    private TaskCompletionSource? _triggered;
    private TaskCompletionSource? _released;
    private int _hits;
    private int _completed;
    private bool _active = true;
    private bool _isReleased;

    internal FaultArm(FaultMatch match, FaultAction action)
        : this(match, null, action)
    {
    }

    internal FaultArm(FaultMatch match, Func<FaultContext, bool>? predicate, FaultAction action)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(action);
        match.Validate();
        Match = match;
        Action = action;
        _predicate = predicate;
    }

    /// <summary>The match.</summary>
    public FaultMatch Match { get; }

    /// <summary>The action.</summary>
    public FaultAction Action { get; }

    /// <summary>Number of matching hits observed.</summary>
    public int Hits
    {
        get
        {
            lock (_gate)
                return _hits;
        }
    }

    /// <summary>True once the action ran at least once.</summary>
    public bool Triggered
    {
        get
        {
            lock (_gate)
                return _contexts.Count > 0;
        }
    }

    /// <summary>Contexts of the hits at which the action ran.</summary>
    public IReadOnlyList<FaultContext> Contexts
    {
        get
        {
            lock (_gate)
                return [.. _contexts];
        }
    }

    /// <summary>True until <see cref="Disarm"/> (or <see cref="TestFaults.DisarmAll"/>).</summary>
    public bool IsArmed
    {
        get
        {
            lock (_gate)
                return _active;
        }
    }

    /// <summary>Number of triggered actions that ran to completion (a blocked or crashing action never completes).</summary>
    internal int CompletedActions
    {
        get
        {
            lock (_gate)
                return _completed;
        }
    }

    /// <summary>Sequence number of the arm in its <see cref="TestFaults"/> (arming order).</summary>
    internal int Number { get; private set; }

    /// <summary>True once <see cref="Release"/> was called.</summary>
    internal bool IsReleased
    {
        get
        {
            lock (_gate)
                return _isReleased;
        }
    }

    private TestFaults Owner => _owner ?? throw new InvalidOperationException("The fault arm is not attached to a cluster.");

    /// <summary>Waits until the action triggered; throws if it is never reached (HAR-013).</summary>
    /// <exception cref="TimeoutException">The arm did not trigger within <paramref name="timeout"/> of cluster time.</exception>
    public async Task WaitTriggeredAsync(TimeSpan timeout)
    {
        TestFaults owner = Owner;
        Task triggered;
        lock (_gate)
        {
            if (_contexts.Count > 0)
                return;
            _triggered ??= owner.Runtime.CreateCompletion();
            triggered = _triggered.Task;
        }

        if (!await owner.Runtime.WaitAsync(triggered, timeout))
        {
            throw new TimeoutException(string.Create(CultureInfo.InvariantCulture,
                $"The fault armed at {Describe()} was not triggered within {timeout}: {Hits} matching hit(s); the site was reached {owner.Coverage.CountHits(Match)} time(s) in total."));
        }
    }

    /// <summary>Releases operations/nodes held by <see cref="FaultAction.Block"/> or <see cref="FaultAction.PauseNode"/>.</summary>
    public void Release()
    {
        TaskCompletionSource? released;
        lock (_gate)
        {
            if (_isReleased)
                return;
            _isReleased = true;
            released = _released;
            _released = null;
        }

        TestFaults? owner = _owner;
        owner?.OnReleased(this);
        if (released is not null && owner is not null)
            owner.Runtime.Complete(() => released.TrySetResult());
    }

    /// <summary>Stops matching further hits.</summary>
    public void Disarm()
    {
        bool wasActive;
        lock (_gate)
        {
            wasActive = _active;
            _active = false;
        }

        if (wasActive)
            _owner?.Trace($"fault disarmed: {Describe()}");
    }

    /// <inheritdoc />
    public override string ToString() => Describe();

    internal string Describe()
    {
        var parts = new List<string> { Match.Site };
        if (Match.Node is not null)
            parts.Add("node=" + Match.Node);
        if (Match.Volume is not null)
            parts.Add("volume=" + Match.Volume);
        if (Match.Operation is not null)
            parts.Add("op=" + Match.Operation.Value);
        if (Match.PathPrefix is not null)
            parts.Add("path=" + Match.PathPrefix);
        if (Match.Occurrence != 1 || Match.Count != 1)
            parts.Add(string.Create(CultureInfo.InvariantCulture, $"occurrence={Match.Occurrence} count={Match.Count}"));
        if (_predicate is not null)
            parts.Add("predicate");
        return $"#{Number.ToString(CultureInfo.InvariantCulture)} {string.Join(' ', parts)} -> {ActionName(Action)}";
    }

    internal static string ActionName(FaultAction action) => action switch
    {
        FaultAction.CrashNodeFault crash => "CrashNode(" + crash.Mode + ")",
        FaultAction.PauseNodeFault => "PauseNode",
        FaultAction.BlockFault => "Block",
        FaultAction.ThrowFault => "Throw",
        FaultAction.IoErrorFault => "IoError",
        FaultAction.DiskFullFault => "DiskFull",
        FaultAction.DelayFault delay => "Delay(" + delay.Duration.ToString("c", CultureInfo.InvariantCulture) + ")",
        FaultAction.DropReplyFault => "DropReply",
        FaultAction.CancelCallerFault => "CancelCaller",
        FaultAction.CustomFault => "Custom",
        _ => action.GetType().Name,
    };

    internal void Attach(TestFaults owner, int number)
    {
        _owner = owner;
        Number = number;
    }

    /// <summary>
    /// Counts <paramref name="context"/> when it matches; returns true when the action must run for it (the context is
    /// then recorded in <see cref="Contexts"/>). Called by the injector in hit order.
    /// </summary>
    internal bool OnHit(in FaultContext context)
    {
        if (!Match.Matches(context))
            return false;
        if (_predicate is not null && !_predicate(context))
            return false;
        TaskCompletionSource? triggered;
        lock (_gate)
        {
            if (!_active)
                return false;
            _hits++;
            if (!Match.TriggersOn(_hits))
                return false;
            _contexts.Add(context);
            triggered = _triggered;
            _triggered = null;
        }

        if (triggered is not null)
            Owner.Runtime.Complete(() => triggered.TrySetResult());
        return true;
    }

    /// <summary>A task that completes once <see cref="Release"/> was called (completed already when it was).</summary>
    internal Task WhenReleased()
    {
        lock (_gate)
        {
            if (_isReleased)
                return Task.CompletedTask;
            _released ??= Owner.Runtime.CreateCompletion();
            return _released.Task;
        }
    }

    internal void OnActionCompleted()
    {
        lock (_gate)
            _completed++;
    }
}
