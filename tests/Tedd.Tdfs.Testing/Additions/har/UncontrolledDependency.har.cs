namespace Tedd.Tdfs.Testing;

/// <summary>
/// Category of a nondeterministic dependency that escaped the harness's control (HAR-002, VER-03). Used both by the
/// runtime detector of the deterministic simulation and by static (IL) audits of simulated components.
/// </summary>
public enum UncontrolledDependencyKind
{
    /// <summary>
    /// Work was posted to a simulation <see cref="SynchronizationContext"/> (a node loop or the scenario/client context)
    /// from a thread that is not the simulation scheduler thread: a thread-pool continuation (<c>Task.Run</c>,
    /// <c>ThreadPool</c>), an OS timer (<c>System.Threading.Timer</c>, <c>Task.Delay</c> without a
    /// <see cref="TimeProvider"/>), or an OS I/O completion (socket, file). This is what the runtime detector reports.
    /// </summary>
    ForeignThreadPost,

    /// <summary>Direct wall-clock or monotonic clock read (<c>DateTime.UtcNow</c>, <c>DateTimeOffset.UtcNow</c>,
    /// <c>Environment.TickCount</c>, <c>Stopwatch</c>) instead of the injected <see cref="TimeProvider"/>.</summary>
    WallClock,

    /// <summary>Direct randomness (<c>Random.Shared</c>, <c>new Random()</c>, <c>Guid.NewGuid()</c>,
    /// <c>RandomNumberGenerator</c>) instead of the injected <see cref="IRandomSource"/>.</summary>
    Random,

    /// <summary>Raw timer (<c>System.Threading.Timer</c>, <c>System.Timers.Timer</c>, <c>Task.Delay</c>/<c>WaitAsync</c>/
    /// <c>CancellationTokenSource</c> timeouts without a <see cref="TimeProvider"/>).</summary>
    Timer,

    /// <summary>Thread-pool scheduling (<c>Task.Run</c>, <c>TaskFactory.StartNew</c>, <c>ThreadPool</c>, <c>Parallel</c>).</summary>
    ThreadPool,

    /// <summary>Raw threads (<c>new Thread</c>, <c>Thread.Start</c>, <c>Thread.Sleep</c>).</summary>
    Thread,

    /// <summary>Direct network access (<c>System.Net.Sockets</c>, <c>HttpClient</c>, <c>Dns</c>) instead of the injected transport.</summary>
    Socket,

    /// <summary>Direct disk access (<c>File</c>, <c>FileStream</c>, <c>Directory</c>, <c>RandomAccess</c>, path-based
    /// readers/writers) instead of the injected <c>IStorageMedium</c>.</summary>
    Disk,

    /// <summary>
    /// Leaving the owning loop/simulation context: <c>ConfigureAwait(false)</c> in server code, <c>Task.Yield</c>,
    /// <c>SemaphoreSlim</c> or <c>RunContinuationsAsynchronously</c> on the SDK request path (ARCHITECTURE §3.1, §9.6).
    /// </summary>
    ContextEscape,
}

/// <summary>One detected uncontrolled dependency.</summary>
/// <param name="Kind">What escaped control.</param>
/// <param name="Component">
/// Where it was detected: a node name ("n0"), "scenario" for the scenario/client context of a deterministic run, or a
/// type/method name for static audits.
/// </param>
/// <param name="Detail">Human-readable evidence (for runtime detection: the posting thread and its stack trace).</param>
public sealed record UncontrolledDependency(UncontrolledDependencyKind Kind, string Component, string Detail);

/// <summary>
/// Thrown by <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/> (and the
/// generic overload) when a deterministic run observed at least one <see cref="UncontrolledDependency"/>. The run is
/// never reported as a pass in that case, even when the scenario itself completed (HAR-002). When the scenario also
/// failed, its exception is the <see cref="Exception.InnerException"/>.
/// </summary>
public sealed class UncontrolledDependencyException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="dependencies">Every dependency observed during the run, in detection order (at least one).</param>
    /// <param name="inner">The scenario's own failure, if any.</param>
    public UncontrolledDependencyException(IReadOnlyList<UncontrolledDependency> dependencies, Exception? inner = null)
        : base(BuildMessage(dependencies), inner)
    {
        Dependencies = dependencies;
    }

    /// <summary>Every dependency observed during the run, in detection order.</summary>
    public IReadOnlyList<UncontrolledDependency> Dependencies { get; }

    private static string BuildMessage(IReadOnlyList<UncontrolledDependency> dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        return $"The deterministic run observed {dependencies.Count} uncontrolled dependenc{(dependencies.Count == 1 ? "y" : "ies")}: " +
            string.Join("; ", dependencies.Take(8).Select(d => $"{d.Kind} in {d.Component}")) + (dependencies.Count > 8 ? "; ..." : string.Empty);
    }
}
