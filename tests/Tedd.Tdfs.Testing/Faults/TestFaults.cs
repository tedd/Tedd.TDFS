using System.Globalization;
using Xunit.Sdk;

namespace Tedd.Tdfs.Testing;

/// <summary>Fault injection control for a test cluster.</summary>
/// <remarks>
/// Every node, backup node and client of the cluster reports its fault sites to one <see cref="HarnessFaultInjector"/>;
/// arms are evaluated in arming order at every hit. Unknown site names are rejected when arming (a typo would otherwise be
/// an arm that can never trigger).
/// </remarks>
public sealed partial class TestFaults
{
    private readonly object _gate = new();
    private readonly List<FaultArm> _arms = [];
    private readonly Dictionary<FaultArm, List<IFaultTarget>> _pausedBy = new(ReferenceEqualityComparer.Instance);
    private readonly Action<string> _trace;

    internal TestFaults(ClusterRuntime runtime, Action<string> trace)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        ArgumentNullException.ThrowIfNull(trace);
        Runtime = runtime;
        _trace = trace;
        Coverage = new FaultCoverageRegistry();
        Coverage.Attach(this);
        Injector = new HarnessFaultInjector(this);
    }

    /// <summary>Coverage of every fault-point hit in this cluster.</summary>
    public FaultCoverageRegistry Coverage { get; }

    /// <summary>The injector handed to every node, backup node and client of the cluster.</summary>
    internal HarnessFaultInjector Injector { get; }

    /// <summary>The cluster's execution context.</summary>
    internal ClusterRuntime Runtime { get; }

    /// <summary>Resolves node and backup-node names for crash, pause and delay actions (set by the cluster).</summary>
    internal IFaultTargets? Targets { get; set; }

    /// <summary>Registers read-result limits with the network interposition layer (set by the cluster).</summary>
    internal Action<ReadResultLimitRule>? ReadLimitRegistrar { get; set; }

    /// <summary>Every arm in arming order.</summary>
    internal IReadOnlyList<FaultArm> Arms
    {
        get
        {
            lock (_gate)
                return [.. _arms];
        }
    }

    /// <summary>Arms a fault.</summary>
    /// <exception cref="ArgumentException">The match is invalid or names no known fault site.</exception>
    public FaultArm Arm(FaultMatch match, FaultAction action) => ArmCore(match, null, action);

    /// <summary>Disarms every armed fault.</summary>
    public void DisarmAll()
    {
        foreach (FaultArm arm in Arms)
            arm.Disarm();
    }

    /// <summary>Fails the test unless <paramref name="arm"/> triggered.</summary>
    /// <exception cref="XunitException">The arm never triggered.</exception>
    public void AssertReached(FaultArm arm)
    {
        ArgumentNullException.ThrowIfNull(arm);
        if (arm.Triggered)
            return;
        int siteHits = Coverage.CountHits(arm.Match);
        throw new FaultPointNotReachedException(string.Create(CultureInfo.InvariantCulture,
            $"The fault point {arm.Describe()} was never reached: {arm.Hits} matching hit(s), {siteHits} hit(s) of the site and identity in total. An injection that was armed but not reached proves nothing (HAR-013)."));
    }

    /// <summary>Releases every block and pause and disarms every arm (cluster disposal).</summary>
    internal void ReleaseAll()
    {
        foreach (FaultArm arm in Arms)
        {
            arm.Disarm();
            arm.Release();
        }
    }

    internal void Trace(string message) => _trace(message);

    internal FaultArm ArmCore(FaultMatch match, Func<FaultContext, bool>? predicate, FaultAction action)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(action);
        match.Validate();
        ValidateSite(match.Site);
        var arm = new FaultArm(match, predicate, action);
        lock (_gate)
        {
            arm.Attach(this, _arms.Count + 1);
            _arms.Add(arm);
        }

        _trace($"fault armed: {arm.Describe()}");
        return arm;
    }

    /// <summary>Remembers that <paramref name="arm"/> paused <paramref name="target"/> (resumed on release).</summary>
    internal void RecordPause(FaultArm arm, IFaultTarget target)
    {
        lock (_gate)
        {
            if (!_pausedBy.TryGetValue(arm, out List<IFaultTarget>? list))
                _pausedBy[arm] = list = [];
            list.Add(target);
        }
    }

    /// <summary>Resumes every node paused by <paramref name="arm"/> (before its held operations continue).</summary>
    internal void OnReleased(FaultArm arm)
    {
        List<IFaultTarget>? paused;
        lock (_gate)
        {
            if (_pausedBy.Remove(arm, out paused) is false)
                paused = null;
        }

        _trace($"fault released: {arm.Describe()}");
        if (paused is null)
            return;
        foreach (IFaultTarget target in paused)
            target.ResumeFromFault($"fault {arm.Number.ToString(CultureInfo.InvariantCulture)} released");
    }

    private static void ValidateSite(string site)
    {
        if (site.EndsWith('*'))
        {
            string prefix = site[..^1];
            if (!FaultSites.All.Any(s => s.StartsWith(prefix, StringComparison.Ordinal)))
                throw new ArgumentException($"No fault site starts with '{prefix}'.", nameof(site));
        }
        else if (!FaultSites.TryGetStage(site, out _))
        {
            throw new ArgumentException($"Unknown fault site '{site}' (see FaultSites.All).", nameof(site));
        }
    }
}

/// <summary>An assertion failure: an armed fault point was never reached (HAR-013).</summary>
public sealed class FaultPointNotReachedException : XunitException
{
    /// <summary>Creates the exception.</summary>
    public FaultPointNotReachedException(string message)
        : base(message)
    {
    }
}
