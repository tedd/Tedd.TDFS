namespace Tedd.Tdfs.Testing;

/// <summary>Fault injection control for a test cluster.</summary>
public sealed partial class TestFaults
{
    internal TestFaults()
    {
    }

    /// <summary>Coverage of every fault-point hit in this cluster.</summary>
    public FaultCoverageRegistry Coverage => throw Stub.NotYet("TestFaults.Coverage");

    /// <summary>Arms a fault.</summary>
    public FaultArm Arm(FaultMatch match, FaultAction action) => throw Stub.NotYet("TestFaults.Arm");

    /// <summary>Disarms every armed fault.</summary>
    public void DisarmAll() => throw Stub.NotYet("TestFaults.DisarmAll");

    /// <summary>Fails the test unless <paramref name="arm"/> triggered.</summary>
    public void AssertReached(FaultArm arm) => throw Stub.NotYet("TestFaults.AssertReached");
}
