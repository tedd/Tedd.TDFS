using System.Diagnostics;
using System.Globalization;
using System.Text;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>Explicit exploration bounds of one protocol model (MOD-012 report material).</summary>
/// <param name="MaxDepth">Transition-depth bound passed to <see cref="ModelChecker.Explore{TState}"/>.</param>
/// <param name="MaxStates">Distinct-state bound passed to <see cref="ModelChecker.Explore{TState}"/>.</param>
/// <param name="Parameters">Finite instance parameters (node count, terms, generations, message copies, ...).</param>
internal sealed record SpecBounds(int MaxDepth, long MaxStates, IReadOnlyList<string> Parameters);

/// <summary>A deliberately broken variant of a model (negative control).</summary>
/// <param name="Name">Variant name passed to <see cref="IProtocolSpec.Run"/>.</param>
/// <param name="Description">What safety mechanism was removed.</param>
/// <param name="SafetyMutation">The <see cref="SafetyMutations"/> flag the variant corresponds to, when there is one.</param>
internal sealed record BrokenVariant(string Name, string Description, string? SafetyMutation = null);

/// <summary>Result of exploring one variant of a model.</summary>
/// <param name="Variant">Variant name (<see cref="ProtocolSpecNames.Correct"/> or a broken variant).</param>
/// <param name="Result">The bounded exploration result.</param>
/// <param name="ActionKinds">Every action kind (action name before '(') that was enabled at least once.</param>
/// <param name="Elapsed">Wall-clock duration of the exploration.</param>
/// <param name="Statistics">Model-specific counters (for example states where liveness assumptions did not hold).</param>
internal sealed record SpecRun(string Variant, ModelCheckResult Result, IReadOnlySet<string> ActionKinds, TimeSpan Elapsed,
    IReadOnlyDictionary<string, long> Statistics);

/// <summary>Common variant names.</summary>
internal static class ProtocolSpecNames
{
    /// <summary>The protocol as specified (ARCHITECTURE.md / FSD), which must satisfy every invariant within its bounds.</summary>
    public const string Correct = "correct";
}

/// <summary>
/// An abstract transition-system model of one TDFS protocol, explored with <see cref="ModelChecker"/>. Every model
/// declares its bounds, assumptions, abstractions, refinement gaps, the mapping of its actions to implementation fault
/// sites, and at least one deliberately broken variant that the exploration must reject.
/// </summary>
internal interface IProtocolSpec
{
    /// <summary>Catalog id (MOD-001..MOD-011).</summary>
    string TestId { get; }

    /// <summary>Catalog test name (e.g. FencingAcrossEpochsModel).</summary>
    string Name { get; }

    /// <summary>One-paragraph description of what is modelled.</summary>
    string Summary { get; }

    /// <summary>Explicit exploration bounds.</summary>
    SpecBounds Bounds { get; }

    /// <summary>Safety properties checked as state invariants.</summary>
    IReadOnlyList<string> Properties { get; }

    /// <summary>Environmental/fairness/timing assumptions under which the properties are claimed.</summary>
    IReadOnlyList<string> Assumptions { get; }

    /// <summary>What the model deliberately abstracts away from the implementation.</summary>
    IReadOnlyList<string> Abstractions { get; }

    /// <summary>Known refinement gaps between this model and the implementation (unproved obligations).</summary>
    IReadOnlyList<string> RefinementGaps { get; }

    /// <summary>
    /// Model action kind → implementation fault sites (<see cref="FaultSites"/>) at which the corresponding step can be
    /// interrupted. An empty list means there is no fault site; the action must then be named in <see cref="RefinementGaps"/>.
    /// </summary>
    IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; }

    /// <summary>Deliberately broken variants (negative controls).</summary>
    IReadOnlyList<BrokenVariant> BrokenVariants { get; }

    /// <summary>Explores one variant within <see cref="Bounds"/>.</summary>
    SpecRun Run(string variant);
}

/// <summary>Base class wiring a concrete transition system into <see cref="ModelChecker.Explore{TState}"/>.</summary>
internal abstract class ProtocolSpec<TState> : IProtocolSpec
    where TState : class
{
    private Dictionary<string, long> _statistics = new(StringComparer.Ordinal);

    public abstract string TestId { get; }
    public abstract string Name { get; }
    public abstract string Summary { get; }
    public abstract SpecBounds Bounds { get; }
    public abstract IReadOnlyList<string> Properties { get; }
    public abstract IReadOnlyList<string> Assumptions { get; }
    public abstract IReadOnlyList<string> Abstractions { get; }
    public abstract IReadOnlyList<string> RefinementGaps { get; }
    public abstract IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; }
    public abstract IReadOnlyList<BrokenVariant> BrokenVariants { get; }

    /// <summary>Initial state of <paramref name="variant"/>.</summary>
    protected abstract TState Initial(string variant);

    /// <summary>All enabled transitions of <paramref name="state"/>; action names are "Kind(args)".</summary>
    protected abstract IEnumerable<(string Action, TState Next)> Next(TState state, string variant);

    /// <summary>Null when every invariant holds, otherwise a violation message.</summary>
    protected abstract string? Invariant(TState state, string variant);

    /// <summary>Canonical state key (equal keys ⇔ behaviourally equal states, including ghost variables).</summary>
    protected abstract string Key(TState state);

    /// <summary>Adds <paramref name="delta"/> to a model-specific counter of the current run.</summary>
    protected void Count(string counter, long delta = 1) =>
        _statistics[counter] = _statistics.TryGetValue(counter, out long v) ? v + delta : delta;

    public SpecRun Run(string variant)
    {
        if (!string.Equals(variant, ProtocolSpecNames.Correct, StringComparison.Ordinal)
            && !BrokenVariants.Any(v => string.Equals(v.Name, variant, StringComparison.Ordinal)))
            throw new ArgumentException($"{TestId} has no variant '{variant}'.", nameof(variant));

        _statistics = new Dictionary<string, long>(StringComparer.Ordinal);
        var kinds = new HashSet<string>(StringComparer.Ordinal);
        var watch = Stopwatch.StartNew();
        ModelCheckResult result = ModelChecker.Explore(
            Initial(variant),
            s => Recorded(Next(s, variant), kinds),
            s => Invariant(s, variant),
            Key,
            Bounds.MaxDepth,
            Bounds.MaxStates);
        watch.Stop();
        return new SpecRun(variant, result, kinds, watch.Elapsed, new Dictionary<string, long>(_statistics, StringComparer.Ordinal));
    }

    /// <summary>The action kind of an action name ("Elect(n0,T1)" → "Elect").</summary>
    public static string KindOf(string action)
    {
        int paren = action.IndexOf('(', StringComparison.Ordinal);
        return paren < 0 ? action : action[..paren];
    }

    private static IEnumerable<(string Action, TState Next)> Recorded(IEnumerable<(string Action, TState Next)> transitions, HashSet<string> kinds)
    {
        foreach ((string action, TState next) in transitions)
        {
            kinds.Add(KindOf(action));
            yield return (action, next);
        }
    }
}

/// <summary>Shared assertions and reporting for the MOD suite.</summary>
internal static class ModHelpers
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<SpecRun>> RunCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Explores a variant once per test process (explorations are deterministic, so MOD-012 can reuse the runs of the
    /// individual MOD tests instead of repeating them).
    /// </summary>
    public static SpecRun Run(IProtocolSpec spec, string variant) =>
        RunCache.GetOrAdd(spec.TestId + "/" + variant, _ => new Lazy<SpecRun>(() => spec.Run(variant))).Value;

    /// <summary>Every protocol model of the MOD suite (MOD-001..MOD-011), in catalog order.</summary>
    public static IReadOnlyList<IProtocolSpec> AllSpecs() =>
    [
        new RaftSafetySpec(),
        new FencingSpec(),
        new DataThenMetadataSpec(),
        new IdempotencySpec(),
        new TransactionValidationSpec(),
        new SnapshotBoundarySpec(),
        new BackupPublishSpec(),
        new GcReachabilitySpec(),
        new MembershipSpec(),
        new RecoveryEpochSpec(),
        new ConditionalLivenessSpec(),
    ];

    /// <summary>
    /// Explores the correct variant and asserts that the bounded exploration completed without a violation, that the
    /// model is not vacuous (every declared action kind was enabled at least once) and that it explored more than a
    /// trivial state space.
    /// </summary>
    public static SpecRun AssertCorrectVariantHolds(IProtocolSpec spec, long minimumStates = 50)
    {
        SpecRun run = Run(spec, ProtocolSpecNames.Correct);
        Log($"{spec.TestId} {spec.Name} [{run.Variant}]: {run.Result.StatesExplored} states, depth {run.Result.MaxDepth}, " +
            $"completed={run.Result.Completed}, {run.Elapsed.TotalMilliseconds:F0} ms; bounds: {string.Join("; ", run.Result.Bounds)}");
        foreach ((string counter, long value) in run.Statistics)
            Log($"  {counter} = {value}");
        run.Result.AssertNoViolation();
        run.Result.AssertCompleted();
        Assert.True(run.Result.StatesExplored >= minimumStates,
            $"{spec.TestId}: only {run.Result.StatesExplored} states explored (< {minimumStates}); the model is too small to be evidence.");

        string[] neverEnabled = spec.ActionFaultSites.Keys.Where(k => !run.ActionKinds.Contains(k)).Order(StringComparer.Ordinal).ToArray();
        Assert.True(neverEnabled.Length == 0,
            $"{spec.TestId}: declared actions never enabled in the correct model (vacuous model): {string.Join(", ", neverEnabled)}");
        string[] undeclared = run.ActionKinds.Where(k => !spec.ActionFaultSites.ContainsKey(k)).Order(StringComparer.Ordinal).ToArray();
        Assert.True(undeclared.Length == 0,
            $"{spec.TestId}: actions without a declared fault-site mapping (MOD-012 refinement map incomplete): {string.Join(", ", undeclared)}");
        return run;
    }

    /// <summary>
    /// Explores every broken variant and asserts that each one is rejected with an invariant violation and a nonempty
    /// counterexample trace (negative control: the model and its invariant can detect the removed safety mechanism).
    /// </summary>
    public static IReadOnlyList<SpecRun> AssertNegativeControlsDetected(IProtocolSpec spec)
    {
        Assert.NotEmpty(spec.BrokenVariants);
        var runs = new List<SpecRun>();
        foreach (BrokenVariant broken in spec.BrokenVariants)
        {
            SpecRun run = Run(spec, broken.Name);
            runs.Add(run);
            Assert.True(run.Result.Violation is not null,
                $"{spec.TestId} negative control '{broken.Name}' ({broken.Description}) was NOT detected: {run.Result.StatesExplored} states explored " +
                $"without a violation (completed={run.Result.Completed}). The model or its invariant is too weak.");
            Assert.NotNull(run.Result.CounterexampleTrace);
            Assert.NotEmpty(run.Result.CounterexampleTrace!);
            Assert.False(run.Result.Completed, $"{spec.TestId} '{broken.Name}': a violated exploration must not be reported as completed.");
            Log($"{spec.TestId} negative control '{broken.Name}' detected after {run.Result.StatesExplored} states: {run.Result.Violation}");
            for (int i = 0; i < run.Result.CounterexampleTrace!.Count; i++)
                Log($"    {i + 1}. {run.Result.CounterexampleTrace[i]}");
        }

        return runs;
    }

    /// <summary>Asserts that every mapped fault site exists and every unmapped action is declared as a refinement gap.</summary>
    public static void AssertRefinementMapWellFormed(IProtocolSpec spec)
    {
        var known = new HashSet<string>(FaultSites.All, StringComparer.Ordinal);
        foreach ((string action, IReadOnlyList<string> sites) in spec.ActionFaultSites)
        {
            foreach (string site in sites)
                Assert.True(known.Contains(site), $"{spec.TestId}: action '{action}' maps to unknown fault site '{site}'.");
            if (sites.Count == 0)
            {
                Assert.True(spec.RefinementGaps.Any(g => g.Contains($"'{action}'", StringComparison.Ordinal)),
                    $"{spec.TestId}: action '{action}' has no fault site and no refinement-gap entry naming it.");
            }
        }
    }

    /// <summary>Writes a line to the test output (when running under xunit).</summary>
    public static void Log(string line) => TestContext.Current.TestOutputHelper?.WriteLine(line);

    /// <summary>The disclaimer every MOD-012 report must carry (VER-01, test plan §1).</summary>
    public const string Disclaimer =
        "Completed bounded exploration establishes the listed properties only for the listed finite models, bounds and " +
        "assumptions. It is NOT a proof that the implementation is correct: every refinement gap below is an unproved " +
        "obligation that must be discharged by implementation-level tests (fault matrix, simulation, MUT negative controls).";

    /// <summary>Builds the MOD-012 refinement and bounds report (Markdown).</summary>
    public static string BuildReport(IReadOnlyList<(IProtocolSpec Spec, SpecRun Correct, IReadOnlyList<SpecRun> Broken)> results,
        IReadOnlyList<string> additionalExplorations)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# TDFS bounded protocol models: refinement and bounds report (MOD-012)");
        sb.AppendLine();
        sb.AppendLine("> " + Disclaimer);
        sb.AppendLine();
        sb.AppendLine("| Model | Name | States | Max depth | Completed | Violation | Negative controls detected |");
        sb.AppendLine("|---|---|---:|---:|---|---|---|");
        foreach ((IProtocolSpec spec, SpecRun correct, IReadOnlyList<SpecRun> broken) in results)
        {
            int detected = broken.Count(b => b.Result.Violation is not null);
            sb.AppendLine(CultureInfo.InvariantCulture,
                $"| {spec.TestId} | {spec.Name} | {correct.Result.StatesExplored} | {correct.Result.MaxDepth} | {(correct.Result.Completed ? "yes" : "NO")} | {(correct.Result.Violation ?? "none")} | {detected}/{broken.Count} |");
        }

        sb.AppendLine();
        foreach ((IProtocolSpec spec, SpecRun correct, IReadOnlyList<SpecRun> broken) in results)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"## {spec.TestId} {spec.Name}");
            sb.AppendLine();
            sb.AppendLine(spec.Summary);
            sb.AppendLine();
            sb.AppendLine("### Bounds");
            sb.AppendLine(CultureInfo.InvariantCulture, $"- maxDepth={spec.Bounds.MaxDepth}");
            sb.AppendLine(CultureInfo.InvariantCulture, $"- maxStates={spec.Bounds.MaxStates}");
            foreach (string p in spec.Bounds.Parameters)
                sb.AppendLine("- parameter: " + p);
            foreach (string b in correct.Result.Bounds)
                sb.AppendLine("- checker: " + b);
            sb.AppendLine(CultureInfo.InvariantCulture,
                $"- explored: {correct.Result.StatesExplored} states, deepest {correct.Result.MaxDepth}, completed={(correct.Result.Completed ? "yes" : "NO")}, {correct.Elapsed.TotalMilliseconds:F0} ms");
            foreach ((string counter, long value) in correct.Statistics)
                sb.AppendLine(CultureInfo.InvariantCulture, $"- statistic: {counter}={value}");
            sb.AppendLine();
            AppendList(sb, "Properties (state invariants)", spec.Properties);
            AppendList(sb, "Assumptions", spec.Assumptions);
            AppendList(sb, "Abstractions", spec.Abstractions);
            AppendList(sb, "Refinement gaps (unproved)", spec.RefinementGaps);
            sb.AppendLine("### Action → implementation fault points");
            foreach ((string action, IReadOnlyList<string> sites) in spec.ActionFaultSites.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            {
                string mapped = sites.Count == 0
                    ? "(no fault site — see refinement gaps)"
                    : string.Join(", ", sites.Select(s => $"{s} [{FaultSites.StageOf(s)}]"));
                sb.AppendLine(CultureInfo.InvariantCulture, $"- {action}: {mapped}");
            }

            sb.AppendLine();
            sb.AppendLine("### Negative controls");
            foreach (SpecRun run in broken)
            {
                BrokenVariant variant = spec.BrokenVariants.Single(v => v.Name == run.Variant);
                string mutation = variant.SafetyMutation is null ? string.Empty : $" (SafetyMutations.{variant.SafetyMutation})";
                string outcome = run.Result.Violation is null
                    ? "NOT DETECTED"
                    : $"detected after {run.Result.StatesExplored} states, counterexample of {run.Result.CounterexampleTrace?.Count ?? 0} steps: {run.Result.Violation}";
                sb.AppendLine(CultureInfo.InvariantCulture, $"- {variant.Name}{mutation}: {variant.Description} — {outcome}");
            }

            sb.AppendLine();
        }

        if (additionalExplorations.Count > 0)
        {
            AppendList(sb, "Implementation-level explorations (declared bounds, executed by MOD-001 against RaftNode)", additionalExplorations);
        }

        sb.AppendLine("## Status");
        sb.AppendLine();
        sb.AppendLine("Bounded model checking only. " + Disclaimer);
        return sb.ToString();
    }

    private static void AppendList(StringBuilder sb, string title, IReadOnlyList<string> items)
    {
        sb.AppendLine("### " + title);
        foreach (string item in items)
            sb.AppendLine("- " + item);
        sb.AppendLine();
    }

    /// <summary>Compact canonical rendering of an int array for state keys.</summary>
    public static void AppendInts(StringBuilder sb, ReadOnlySpan<int> values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (i > 0)
                sb.Append(',');
            sb.Append(values[i].ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Compact canonical rendering of a bool array for state keys.</summary>
    public static void AppendBools(StringBuilder sb, ReadOnlySpan<bool> values)
    {
        foreach (bool b in values)
            sb.Append(b ? '1' : '0');
    }

    /// <summary>Renders a bit mask as "{a,b}" with the given element names.</summary>
    public static string Set(int mask, IReadOnlyList<string> names)
    {
        var parts = new List<string>();
        for (int i = 0; i < names.Count; i++)
        {
            if ((mask & (1 << i)) != 0)
                parts.Add(names[i]);
        }

        return "{" + string.Join(",", parts) + "}";
    }

    /// <summary>Number of set bits.</summary>
    public static int Bits(int mask) => System.Numerics.BitOperations.PopCount((uint)mask);

    /// <summary>Convenience for building action → fault-site maps.</summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> Map(params (string Action, string[] Sites)[] entries) =>
        entries.ToDictionary(e => e.Action, e => (IReadOnlyList<string>)e.Sites, StringComparer.Ordinal);
}
