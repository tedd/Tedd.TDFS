namespace Tedd.Tdfs.Server;

/// <summary>
/// A running live node: extent store, control-group member, volume-group member(s), RPC service. All state lives on
/// the node's event loop.
/// </summary>
public sealed class NodeHost : IAsyncDisposable
{
    private NodeHost()
    {
    }

    /// <summary>Node name.</summary>
    public string Name => throw new NotImplementedException("NodeHost.Name: implemented in a later phase");

    /// <summary>Node id (persisted in the superblock).</summary>
    public NodeId Id => throw new NotImplementedException("NodeHost.Id: implemented in a later phase");

    /// <summary>Incarnation of this process (durably incremented on every start).</summary>
    public ulong Incarnation => throw new NotImplementedException("NodeHost.Incarnation: implemented in a later phase");

    /// <summary>Actual bound endpoint.</summary>
    public string Endpoint => throw new NotImplementedException("NodeHost.Endpoint: implemented in a later phase");

    /// <summary>Read-only diagnostics (executed on the node loop).</summary>
    public NodeInspector Inspect => throw new NotImplementedException("NodeHost.Inspect: implemented in a later phase");

    /// <summary>Starts a node: recovers or bootstraps local state, then starts serving.</summary>
    public static Task<NodeHost> StartAsync(NodeOptions options, NodeEnvironment environment, CancellationToken ct = default)
        => throw new NotImplementedException("NodeHost.StartAsync: implemented in a later phase");

    /// <summary>Graceful stop: stop accepting, drain, flush, close.</summary>
    public Task StopAsync(CancellationToken ct = default)
        => throw new NotImplementedException("NodeHost.StopAsync: implemented in a later phase");

    /// <summary>Abrupt stop (crash): the loop stops immediately; no flush and no cleanup.</summary>
    public void Kill() => throw new NotImplementedException("NodeHost.Kill: implemented in a later phase");

    /// <summary>Freezes the node loop (memory retained).</summary>
    public void Pause() => throw new NotImplementedException("NodeHost.Pause: implemented in a later phase");

    /// <summary>Unfreezes the node loop.</summary>
    public void Resume() => throw new NotImplementedException("NodeHost.Resume: implemented in a later phase");

    /// <summary>Stops the node gracefully if still running.</summary>
    public ValueTask DisposeAsync() => throw new NotImplementedException("NodeHost.DisposeAsync: implemented in a later phase");
}
