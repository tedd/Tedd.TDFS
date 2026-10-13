using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server;

/// <summary>
/// A running late-sync backup node: receives snapshot exports, stores and publishes them, and serves read-only
/// snapshot views. Never a member of any Raft group.
/// </summary>
public sealed class BackupNodeHost : IAsyncDisposable
{
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(5);

    private readonly INodeLoop _loop;
    private readonly BackupRuntime _runtime;
    private int _state; // 0 running, 1 stopping/stopped, 2 killed
    private Task? _stop;

    private BackupNodeHost(INodeLoop loop, BackupRuntime runtime)
    {
        _loop = loop;
        _runtime = runtime;
        BackupServices s = runtime.Services;
        Name = s.Name;
        Endpoint = s.Endpoint;
        Inspect = new BackupInspector(loop, runtime);
    }

    /// <summary>Backup node name.</summary>
    public string Name { get; }

    /// <summary>Actual bound endpoint.</summary>
    public string Endpoint { get; }

    /// <summary>Read-only diagnostics.</summary>
    public BackupInspector Inspect { get; }

    /// <summary>The node loop (harness use).</summary>
    internal INodeLoop Loop => _loop;

    /// <summary>The loop-side runtime (harness use; touch only on <see cref="Loop"/>).</summary>
    internal BackupRuntime Runtime => _runtime;

    /// <summary>Starts a backup node (fails with <see cref="DfsRecoveryKeyMissingException"/> when key material is missing).</summary>
    /// <remarks>
    /// An empty data directory needs <see cref="BackupNodeOptions.BackupSecret"/> to create its key material. A node whose
    /// secret was removed starts locked: it lists its catalog but refuses to present snapshot contents
    /// (<see cref="DfsRecoveryKeyMissingException"/>) until recovery key material is supplied with a restore request. A secret
    /// that does not unlock the stored key material fails the start. The returned task is completed by the node loop.
    /// </remarks>
    public static Task<BackupNodeHost> StartAsync(BackupNodeOptions options, NodeEnvironment environment, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentException.ThrowIfNullOrEmpty(options.Name);
        INodeLoop loop = environment.LoopFactory.Create(options.Name);
        Task<BackupNodeHost> start = loop.InvokeAsync(async () =>
        {
            BackupRuntime runtime = await BackupRuntime.StartAsync(options, environment, loop, ct);
            return new BackupNodeHost(loop, runtime);
        });
        _ = start.ContinueWith(static (_, state) => ((INodeLoop)state!).Stop(), loop, CancellationToken.None,
            TaskContinuationOptions.NotOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return start;
    }

    /// <summary>Graceful stop.</summary>
    /// <remarks>Completed by the node loop; repeated calls return the same stop.</remarks>
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

    /// <summary>Freezes the node loop.</summary>
    public void Pause() => _loop.Pause();

    /// <summary>Unfreezes the node loop.</summary>
    public void Resume() => _loop.Resume();

    /// <summary>Runs <paramref name="work"/> on the node loop (harness use).</summary>
    internal Task<T> InvokeAsync<T>(Func<BackupServices, Task<T>> work) => _loop.InvokeAsync(() => work(_runtime.Services));

    /// <summary>Stops the node gracefully if still running.</summary>
    public ValueTask DisposeAsync() => Volatile.Read(ref _state) == 0 ? new ValueTask(StopAsync()) : default;
}
