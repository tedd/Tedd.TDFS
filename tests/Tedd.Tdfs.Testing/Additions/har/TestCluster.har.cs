namespace Tedd.Tdfs.Testing;

/// <summary>HAR-suite additions to <see cref="TestCluster"/> (see docs/api-additions/har.md).</summary>
public sealed partial class TestCluster
{
    /// <summary>
    /// Every uncontrolled dependency detected so far in this run, in detection order (HAR-002). In
    /// <see cref="ClusterMode.Deterministic"/> mode the simulation records a
    /// <see cref="UncontrolledDependencyKind.ForeignThreadPost"/> entry whenever work is posted to any simulation
    /// <see cref="SynchronizationContext"/> (node loops and the scenario/client context) from a thread other than the
    /// simulation scheduler thread; <see cref="UncontrolledDependency.Component"/> names the target context ("n0", ...,
    /// or "scenario") and <see cref="UncontrolledDependency.Detail"/> contains the posting thread and stack trace. The
    /// posted work is still executed (at a scheduler step chosen by the simulation), so detection never deadlocks the run,
    /// and <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/> then fails
    /// with <see cref="UncontrolledDependencyException"/>. A cluster start that reaches a stable state through the
    /// harness's own facilities records nothing. In <see cref="ClusterMode.Realtime"/> mode the list is always empty.
    /// </summary>
    public IReadOnlyList<UncontrolledDependency> UncontrolledDependencies => GetUncontrolledDependencies();

    /// <summary>
    /// Every environment-assumption violation recorded so far in this run, in detection order (HAR-018). The harness
    /// records an <see cref="AssumptionViolation.DurableFlush"/> entry for a node when its disk was made dishonest with
    /// <see cref="TestDisk.SetDishonestFlush"/> and a crash (<see cref="TestNode.CrashAsync"/>, a
    /// <see cref="FaultAction.CrashNode"/> fault or a disk replacement) then discarded bytes or namespace changes whose
    /// flush had reported success. At most one entry per node and crash. When the list is non-empty at the end of the run,
    /// <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/> throws
    /// <see cref="AssumptionViolatedException"/>.
    /// </summary>
    public IReadOnlyList<AssumptionViolation> AssumptionViolations => GetAssumptionViolations();
}
