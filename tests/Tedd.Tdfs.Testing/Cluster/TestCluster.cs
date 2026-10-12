namespace Tedd.Tdfs.Testing;

/// <summary>A running test cluster: live nodes, backup nodes, network/fault control, inspection and history.</summary>
public sealed partial class TestCluster : IAsyncDisposable
{
    private TestCluster()
    {
    }

    /// <summary>Starts a cluster (prefer <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>).</summary>
    public static Task<TestCluster> StartAsync(TestClusterOptions options, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.StartAsync");

    /// <summary>The options the cluster was started with.</summary>
    public TestClusterOptions Options => throw Stub.NotYet("TestCluster.Options");

    /// <summary>Execution mode.</summary>
    public ClusterMode Mode => throw Stub.NotYet("TestCluster.Mode");

    /// <summary>The resolved master seed (reported on failure).</summary>
    public ulong Seed => throw Stub.NotYet("TestCluster.Seed");

    /// <summary>Cluster id.</summary>
    public Guid ClusterId => throw Stub.NotYet("TestCluster.ClusterId");

    /// <summary>Per-test data root.</summary>
    public string DataRoot => throw Stub.NotYet("TestCluster.DataRoot");

    /// <summary>Live nodes in creation order (n0, n1, ...).</summary>
    public IReadOnlyList<TestNode> Nodes => throw Stub.NotYet("TestCluster.Nodes");

    /// <summary>Backup nodes (b0, b1, ...).</summary>
    public IReadOnlyList<TestBackupNode> BackupNodes => throw Stub.NotYet("TestCluster.BackupNodes");

    /// <summary>Live node by index.</summary>
    public TestNode this[int index] => throw Stub.NotYet("TestCluster.this[int]");

    /// <summary>Network control.</summary>
    public TestNetwork Network => throw Stub.NotYet("TestCluster.Network");

    /// <summary>Fault injection control.</summary>
    public TestFaults Faults => throw Stub.NotYet("TestCluster.Faults");

    /// <summary>Cluster-wide inspection.</summary>
    public ClusterInspector Inspect => throw Stub.NotYet("TestCluster.Inspect");

    /// <summary>Operation history of this scenario.</summary>
    public HistoryRecorder History => throw Stub.NotYet("TestCluster.History");

    /// <summary>Harness/test clock (virtual in deterministic mode).</summary>
    public TimeProvider Time => throw Stub.NotYet("TestCluster.Time");

    /// <summary>Admin client connected as "admin".</summary>
    public DfsAdminClient Admin => throw Stub.NotYet("TestCluster.Admin");

    /// <summary>Credentials of principal "admin" (ClusterAdmin, all grants).</summary>
    public DfsCredentials AdminCredentials => throw Stub.NotYet("TestCluster.AdminCredentials");

    /// <summary>Fault/network/node events (replay artifact).</summary>
    public IReadOnlyList<string> Trace => throw Stub.NotYet("TestCluster.Trace");

    /// <summary>Live node by name.</summary>
    public TestNode Node(string name) => throw Stub.NotYet("TestCluster.Node");

    /// <summary>Connects a client.</summary>
    public Task<DfsClient> ConnectAsync(TestClientOptions? options = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ConnectAsync");

    /// <summary>Builds client options for this cluster without connecting.</summary>
    public DfsClientOptions CreateClientOptions(TestClientOptions? options = null) =>
        throw Stub.NotYet("TestCluster.CreateClientOptions");

    /// <summary>Connects a read-only client to a backup node.</summary>
    public Task<DfsClient> ConnectBackupAsync(int backupIndex = 0, TestClientOptions? options = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ConnectBackupAsync");

    /// <summary>Creates a principal with per-volume grants.</summary>
    public Task<TestPrincipal> CreatePrincipalAsync(string name, IReadOnlyDictionary<string, DfsPermissions> grants, bool clusterAdmin = false) =>
        throw Stub.NotYet("TestCluster.CreatePrincipalAsync");

    /// <summary>Adds a live node (joining the cluster when <paramref name="join"/> is true).</summary>
    public Task<TestNode> AddNodeAsync(TestNodeSpec spec, bool join = true, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.AddNodeAsync");

    /// <summary>Adds a backup node.</summary>
    public Task<TestBackupNode> AddBackupNodeAsync(string? name = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.AddBackupNodeAsync");

    /// <summary>Waits for and returns the leader of a volume group.</summary>
    public Task<TestNode> GetLeaderAsync(string volume, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.GetLeaderAsync");

    /// <summary>Waits for and returns the control-group leader.</summary>
    public Task<TestNode> GetControlLeaderAsync(CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.GetControlLeaderAsync");

    /// <summary>Waits until control and all volume leaders are elected and all reachable nodes caught up.</summary>
    public Task WaitForStableAsync(TimeSpan? timeout = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.WaitForStableAsync");

    /// <summary>Waits until all reachable voters of a volume applied the leader commit index.</summary>
    public Task WaitForReplicationAsync(string volume, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.WaitForReplicationAsync");

    /// <summary>Waits until a volume has no under-replicated objects.</summary>
    public Task WaitForRepairAsync(string volume, TimeSpan? timeout = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.WaitForRepairAsync");

    /// <summary>Polls a condition; throws <see cref="TimeoutException"/> naming <paramref name="because"/>.</summary>
    public Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string because, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.WaitUntilAsync");

    /// <summary>Delays on the harness clock (virtual in deterministic mode).</summary>
    public Task DelayAsync(TimeSpan delay, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.DelayAsync");

    /// <summary>Cluster-wide replicated-state digest.</summary>
    public Task<string> ComputeDigestAsync(CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ComputeDigestAsync");

    /// <summary>Simulates client process death: isolates the client and abandons it without cleanup.</summary>
    public void KillClient(DfsClient client) => throw Stub.NotYet("TestCluster.KillClient");

    /// <summary>Connects a raw protocol client (bypasses SDK validation) to a node.</summary>
    public Task<RawProtocolClient> ConnectRawAsync(string nodeName, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ConnectRawAsync");

    /// <summary>Stops all nodes and releases resources.</summary>
    public ValueTask DisposeAsync() => throw Stub.NotYet("TestCluster.DisposeAsync");
}
