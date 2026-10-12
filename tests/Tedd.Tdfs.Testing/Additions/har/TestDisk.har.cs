namespace Tedd.Tdfs.Testing;

/// <summary>HAR-suite additions to <see cref="TestDisk"/> (see docs/api-additions/har.md).</summary>
public sealed partial class TestDisk
{
    /// <summary>True while the disk is dishonest (see <see cref="SetDishonestFlush"/>).</summary>
    public bool IsDishonestFlush => throw Stub.NotYet("TestDisk.IsDishonestFlush");

    /// <summary>
    /// Makes the device lie about durability (HAR-018, out-of-model storage): while <paramref name="dishonest"/> is true,
    /// every file flush and directory flush reports success immediately but leaves the affected writes and namespace
    /// changes volatile, so a later crash in any <c>CrashMode</c> (or a disk replacement) loses them exactly as if they had
    /// never been flushed. Turning dishonesty off does not retroactively persist anything; only flushes issued afterwards
    /// are honest again. Whenever a crash discards bytes or namespace changes whose flush reported success, the harness
    /// records an <see cref="AssumptionViolation"/> with <see cref="AssumptionViolation.DurableFlush"/> for this node in
    /// <see cref="TestCluster.AssumptionViolations"/>. Works for simulated and real (wrapped) disks; the node may be running.
    /// </summary>
    public void SetDishonestFlush(bool dishonest) => throw Stub.NotYet("TestDisk.SetDishonestFlush");
}
