using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

// Harness invariants: leadership history (split brain), environment-assumption violations and uncontrolled dependencies.
public sealed partial class TestCluster
{
    private readonly Dictionary<(string Group, ulong Term), string> _leaders = [];
    private readonly List<string> _splitBrain = [];
    private readonly List<AssumptionViolation> _assumptionViolations = [];

    /// <summary>
    /// The uncontrolled dependencies the scheduler recorded (deterministic mode): every post to a simulation
    /// synchronization context from a foreign thread; <c>Component</c> is the addressed process ("n0", ..., "scenario").
    /// </summary>
    private IReadOnlyList<UncontrolledDependency> GetUncontrolledDependencies() =>
        Runtime.Scheduler is { } scheduler ? scheduler.Violations.Select(ToDependency).ToArray() : [];

    /// <summary>The recorded environment-assumption violations, in detection order.</summary>
    private IReadOnlyList<AssumptionViolation> GetAssumptionViolations()
    {
        lock (_assumptionViolations)
            return _assumptionViolations.ToArray();
    }

    /// <summary>Leaderships observed so far: (wire group, term) → node, in no particular order.</summary>
    internal IReadOnlyDictionary<(string Group, ulong Term), string> ObservedLeaders
    {
        get
        {
            lock (_leaders)
                return new Dictionary<(string Group, ulong Term), string>(_leaders);
        }
    }

    /// <summary>Split-brain observations (two leaders of one group in one term), in detection order.</summary>
    internal IReadOnlyList<string> SplitBrainObservations
    {
        get
        {
            lock (_leaders)
                return _splitBrain.ToArray();
        }
    }

    /// <summary>Records an environment-assumption violation (one per node and crash).</summary>
    internal void RecordAssumptionViolation(AssumptionViolation violation)
    {
        ArgumentNullException.ThrowIfNull(violation);
        lock (_assumptionViolations)
            _assumptionViolations.Add(violation);
        Record($"assumption violated on {violation.Node}: {violation.Assumption}: {violation.Detail}");
    }

    /// <summary>
    /// Starts recording every leadership <paramref name="host"/> assumes (for the split-brain invariant). Returns the
    /// subscription (disposed when the incarnation ends), or null when the node went away first.
    /// </summary>
    internal async Task<IDisposable?> ObserveLeadershipAsync(TestNode node, NodeHost host)
    {
        try
        {
            // Only this cluster's leaderships: a node re-bootstrapped under a foreign cluster identity leads that cluster's
            // groups, whose names ("control") and terms collide with this cluster's (SEC-001).
            return await host.Inspect.ObserveLeadershipAsync(OnLeadership, ClusterId);
        }
        catch (Exception ex) when (IsNodeGone(ex))
        {
            Record($"node {node.Name}: leadership observation not attached ({ex.GetType().Name})");
            return null;
        }
    }

    /// <summary>
    /// The end-of-run harness invariants: no group ever had two leaders in one term, and replicas of every hosted group
    /// that reached the same applied index hold identical replicated state (this cluster and its recovery clusters).
    /// </summary>
    /// <exception cref="Xunit.Sdk.XunitException">An invariant does not hold.</exception>
    internal async Task CheckEndOfRunInvariantsAsync()
    {
        CheckNoSplitBrain();
        if (Options.OutOfProcess)
            return;
        var problems = new List<string>();
        var groups = new SortedSet<string>(StringComparer.Ordinal);
        foreach (NodeView view in await SnapshotAsync(groupsOnly: true))
        {
            foreach (GroupInspection group in view.Groups)
                groups.Add(group.Group);
        }

        foreach (string group in groups)
            problems.AddRange(await Inspect.FindDivergenceAsync(group));
        if (problems.Count > 0)
            throw new Xunit.Sdk.XunitException("Replicated state diverged: " + string.Join("; ", problems));

        TestCluster[] recoveries;
        lock (_gate)
            recoveries = [.. _recoveries];
        foreach (TestCluster recovery in recoveries.Where(r => !r.IsDisposed))
            await recovery.CheckEndOfRunInvariantsAsync();
    }

    /// <summary>Asserts that no group ever had two leaders in one term (every observed leadership of this run).</summary>
    internal void CheckNoSplitBrain()
    {
        IReadOnlyList<string> violations = SplitBrainObservations;
        if (violations.Count > 0)
            throw new Xunit.Sdk.XunitException("Split brain: " + string.Join("; ", violations));
    }

    /// <summary>Called on a node loop whenever that node leads a group in some term.</summary>
    private void OnLeadership(string group, string displayName, ulong term, string node)
    {
        string? violation = null;
        bool added = false;
        lock (_leaders)
        {
            if (_leaders.TryGetValue((group, term), out string? existing))
            {
                if (!string.Equals(existing, node, StringComparison.Ordinal))
                {
                    violation = string.Create(CultureInfo.InvariantCulture, $"group {displayName} ({group}) had leaders {existing} and {node} in term {term}");
                    _splitBrain.Add(violation);
                }
            }
            else
            {
                _leaders[(group, term)] = node;
                added = true;
            }
        }

        if (violation is not null)
            Record("INVARIANT VIOLATED: " + violation);
        else if (added)
            Record(string.Create(CultureInfo.InvariantCulture, $"leader {node} of {displayName} in term {term}"));
    }

    private static UncontrolledDependency ToDependency(SimViolation violation)
    {
        string component = violation.Target ?? "simulation";
        string detail = violation.ToString() + (violation.StackTrace is { } stack ? Environment.NewLine + stack : string.Empty);
        return new UncontrolledDependency(UncontrolledDependencyKind.ForeignThreadPost, component, detail);
    }
}
