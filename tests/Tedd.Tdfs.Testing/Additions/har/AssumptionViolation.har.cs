namespace Tedd.Tdfs.Testing;

/// <summary>
/// A recorded violation of a stated environment assumption (test plan §3.1: "a successfully completed durability
/// primitive is assumed to meet the declared supported storage-stack contract"). Evidence gathered while an assumption
/// was violated is out-of-model: it can neither prove nor refute the replication protocol (HAR-018).
/// </summary>
/// <param name="Assumption">Stable assumption id, for example <see cref="DurableFlush"/>.</param>
/// <param name="Node">Node (or backup node) whose environment violated the assumption.</param>
/// <param name="Detail">Human-readable evidence (for example which acknowledged files/bytes were dropped).</param>
public sealed record AssumptionViolation(string Assumption, string Node, string Detail)
{
    /// <summary>
    /// "A flush/durability barrier that reported success made the bytes durable." Violated when a dishonest device
    /// (<see cref="TestDisk.SetDishonestFlush"/>) loses bytes whose flush it acknowledged.
    /// </summary>
    public const string DurableFlush = "storage.flush-durability";
}

/// <summary>
/// Thrown by <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/> (and the
/// generic overload) when <see cref="TestCluster.AssumptionViolations"/> is non-empty at the end of the run, whether or
/// not the scenario itself succeeded. It classifies the run as out-of-model instead of reporting a pass or a protocol
/// failure. When the scenario also failed, its exception is the <see cref="Exception.InnerException"/>.
/// </summary>
public sealed class AssumptionViolatedException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="violations">Every recorded violation (at least one).</param>
    /// <param name="inner">The scenario's own failure, if any.</param>
    public AssumptionViolatedException(IReadOnlyList<AssumptionViolation> violations, Exception? inner = null)
        : base(BuildMessage(violations), inner)
    {
        Violations = violations;
    }

    /// <summary>Every recorded violation, in detection order.</summary>
    public IReadOnlyList<AssumptionViolation> Violations { get; }

    private static string BuildMessage(IReadOnlyList<AssumptionViolation> violations)
    {
        ArgumentNullException.ThrowIfNull(violations);
        return "Out-of-model run: environment assumptions were violated (" +
            string.Join("; ", violations.Take(8).Select(v => $"{v.Assumption} on {v.Node}")) + (violations.Count > 8 ? "; ..." : string.Empty) +
            "). The evidence is not a protocol pass or failure.";
    }
}
