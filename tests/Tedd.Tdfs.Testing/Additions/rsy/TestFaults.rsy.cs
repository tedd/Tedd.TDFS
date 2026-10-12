namespace Tedd.Tdfs.Testing;

public sealed partial class TestFaults
{
    /// <summary>
    /// Arms a fault like <see cref="Arm(FaultMatch, FaultAction)"/>, but a hit only counts as matching when
    /// <paramref name="match"/> matches its context <b>and</b> <paramref name="predicate"/> returns true for it (for
    /// example to select a sub-stage by <see cref="FaultContext.Detail"/>). <see cref="FaultMatch.Occurrence"/> and
    /// <see cref="FaultMatch.Count"/> count only such hits, <see cref="FaultArm.Contexts"/> records only such hits, and
    /// <see cref="AssertReached(FaultArm)"/> / <see cref="FaultArm.WaitTriggeredAsync(TimeSpan)"/> behave as for any arm.
    /// The predicate runs on the hitting node's loop, must be pure and must not block. Every hit is still recorded in
    /// <see cref="Coverage"/>. Added for the RSY suite (RSY-012, RSY-013: interrupting checkpoint catch-up at named
    /// sub-stages, see <see cref="ResyncStageDetails"/>; docs/api-additions/rsy.md).
    /// </summary>
    public FaultArm Arm(FaultMatch match, Func<FaultContext, bool> predicate, FaultAction action) =>
        throw Stub.NotYet("TestFaults.Arm(match, predicate, action)");
}
