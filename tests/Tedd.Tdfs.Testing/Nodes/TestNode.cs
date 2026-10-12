using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>A live node of a <see cref="TestCluster"/>.</summary>
public sealed partial class TestNode
{
    internal TestNode(string name, int index)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
        Index = index;
    }

    /// <summary>Node name ("n0", ...).</summary>
    public string Name { get; }

    /// <summary>Creation index.</summary>
    public int Index { get; }

    /// <summary>Persistent node id.</summary>
    public NodeId Id => throw Stub.NotYet("TestNode.Id");

    /// <summary>Topology labels.</summary>
    public DfsTopologyLabels Labels => throw Stub.NotYet("TestNode.Labels");

    /// <summary>Bound endpoint.</summary>
    public string Endpoint => throw Stub.NotYet("TestNode.Endpoint");

    /// <summary>Lifecycle state.</summary>
    public TestNodeState State => throw Stub.NotYet("TestNode.State");

    /// <summary>Current incarnation (incremented durably on every start).</summary>
    public ulong Incarnation => throw Stub.NotYet("TestNode.Incarnation");

    /// <summary>This node's clock.</summary>
    public TestClock Clock => throw Stub.NotYet("TestNode.Clock");

    /// <summary>This node's disk.</summary>
    public TestDisk Disk => throw Stub.NotYet("TestNode.Disk");

    /// <summary>Read-only diagnostics; throws <see cref="InvalidOperationException"/> unless <see cref="TestNodeState.Running"/>.</summary>
    public NodeInspector Inspect => throw Stub.NotYet("TestNode.Inspect");

    /// <summary>The in-process host; null unless running or paused.</summary>
    public NodeHost? Host => throw Stub.NotYet("TestNode.Host");

    /// <summary>Crashes the node abruptly; the disk keeps only what <paramref name="mode"/> allows.</summary>
    public Task CrashAsync(CrashMode mode = CrashMode.LoseUnflushed) => throw Stub.NotYet("TestNode.CrashAsync");

    /// <summary>Stops the node gracefully.</summary>
    public Task StopAsync() => throw Stub.NotYet("TestNode.StopAsync");

    /// <summary>Restarts the node from its durable image (incarnation + 1).</summary>
    public Task RestartAsync(Func<NodeOptions, NodeOptions>? reconfigure = null) => throw Stub.NotYet("TestNode.RestartAsync");

    /// <summary>Freezes the node loop.</summary>
    public Task PauseAsync() => throw Stub.NotYet("TestNode.PauseAsync");

    /// <summary>Unfreezes the node loop.</summary>
    public Task ResumeAsync() => throw Stub.NotYet("TestNode.ResumeAsync");

    /// <summary>Replaces the disk with an empty one (node must be stopped or crashed).</summary>
    public Task ReplaceDiskWithEmptyAsync() => throw Stub.NotYet("TestNode.ReplaceDiskWithEmptyAsync");

    /// <summary>Clones another node's durable disk image onto this node (node must be stopped or crashed; RSY-021).</summary>
    public Task CloneDiskFromAsync(TestNode source) => throw Stub.NotYet("TestNode.CloneDiskFromAsync");

    /// <summary>Reads the node counters.</summary>
    public Task<NodeCounters> GetCountersAsync() => throw Stub.NotYet("TestNode.GetCountersAsync");

    /// <inheritdoc />
    public override string ToString() => Name;
}
