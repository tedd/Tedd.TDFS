using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server;

/// <summary>
/// A running live node: extent store, control-group member, volume-group member(s), RPC service. All state lives on
/// the node's event loop.
/// </summary>
public sealed class NodeHost : IAsyncDisposable
{
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(5);

    private readonly INodeLoop _loop;
    private readonly NodeRuntime _runtime;
    private int _state; // 0 running, 1 stopping/stopped, 2 killed
    private Task? _stop;

    private NodeHost(INodeLoop loop, NodeRuntime runtime)
    {
        _loop = loop;
        _runtime = runtime;
        NodeServices s = runtime.Services;
        Name = s.NodeName;
        Id = s.NodeId;
        Incarnation = s.Incarnation;
        Endpoint = s.Endpoint;
        Inspect = new NodeInspector(loop, runtime);
    }

    /// <summary>Node name.</summary>
    public string Name { get; }

    /// <summary>Node id (persisted in the superblock).</summary>
    public NodeId Id { get; }

    /// <summary>Incarnation of this process (durably incremented on every start).</summary>
    public ulong Incarnation { get; }

    /// <summary>Actual bound endpoint.</summary>
    public string Endpoint { get; }

    /// <summary>Read-only diagnostics (executed on the node loop).</summary>
    public NodeInspector Inspect { get; }

    /// <summary>The node loop (test and harness use).</summary>
    internal INodeLoop Loop => _loop;

    /// <summary>The loop-side runtime (test and harness use; touch only on <see cref="Loop"/>).</summary>
    internal NodeRuntime Runtime => _runtime;

    /// <summary>True until the node was stopped or killed.</summary>
    internal bool IsRunning => Volatile.Read(ref _state) == 0;

    /// <summary>Starts a node: recovers or bootstraps local state, then starts serving.</summary>
    /// <remarks>
    /// The returned task is completed by the node loop (never on the caller's synchronization context), so callers may
    /// await it with <c>ConfigureAwait(false)</c>, also under the deterministic simulation. A failed start stops the loop.
    /// </remarks>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    public static Task<NodeHost> StartAsync(NodeOptions options, NodeEnvironment environment, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentException.ThrowIfNullOrEmpty(options.Name);
        INodeLoop loop = environment.LoopFactory.Create(options.Name);
        Task<NodeHost> start = loop.InvokeAsync(async () =>
        {
            NodeRuntime runtime = await NodeRuntime.StartAsync(options, environment, loop, ct);
            return new NodeHost(loop, runtime);
        });
        _ = start.ContinueWith(static (_, state) => ((INodeLoop)state!).Stop(), loop, CancellationToken.None,
            TaskContinuationOptions.NotOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return start;
    }

    /// <summary>Graceful stop: stop accepting, drain, flush, close.</summary>
    /// <remarks>Completed by the node loop (see <see cref="StartAsync"/>); repeated calls return the same stop.</remarks>
    public Task StopAsync(CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return Volatile.Read(ref _stop) ?? Task.CompletedTask;
        Task stop = _loop.InvokeAsync(() => _runtime.StopAsync(DrainTimeout));
        Task drain = stop.ContinueWith(_ => _loop.DrainAsync(ct), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default).Unwrap();
        Task result = drain.ContinueWith(d =>
        {
            if (stop.Exception?.InnerException is { } failure and not NodeLoopStoppedException)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
            d.GetAwaiter().GetResult();
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        Volatile.Write(ref _stop, result);
        return result;
    }

    /// <summary>Abrupt stop (crash): the loop stops immediately; no flush and no cleanup.</summary>
    public void Kill()
    {
        if (Interlocked.Exchange(ref _state, 2) == 2)
            return;
        _loop.Stop();
        _runtime.Kill();
    }

    /// <summary>Freezes the node loop (memory retained).</summary>
    public void Pause() => _loop.Pause();

    /// <summary>Unfreezes the node loop.</summary>
    public void Resume() => _loop.Resume();

    /// <summary>Runs <paramref name="work"/> on the node loop (test and harness use).</summary>
    internal Task<T> InvokeAsync<T>(Func<NodeServices, T> work) => _loop.InvokeAsync(() => work(_runtime.Services));

    /// <summary>Runs asynchronous <paramref name="work"/> on the node loop (test and harness use).</summary>
    internal Task<T> InvokeAsync<T>(Func<NodeServices, Task<T>> work) => _loop.InvokeAsync(() => work(_runtime.Services));

    /// <summary>Stops the node gracefully if still running.</summary>
    public ValueTask DisposeAsync() => Volatile.Read(ref _state) == 0 ? new ValueTask(StopAsync()) : default;
}
