namespace Tedd.Tdfs.Server;

/// <summary>
/// Production loop factory: one dedicated (background) thread per node running a FIFO work queue (ARCHITECTURE §3.3).
/// Exceptions escaping work items are logged to <see cref="Log"/> and raised through
/// <see cref="INodeLoop.UnhandledException"/>; they never terminate the loop or the process.
/// </summary>
public sealed class ThreadNodeLoopFactory : INodeLoopFactory
{
    /// <summary>The shared instance (exceptions are reported through the loop event only).</summary>
    public static readonly ThreadNodeLoopFactory Instance = new();

    private ThreadNodeLoopFactory()
    {
        Log = NullNodeLog.Instance;
    }

    /// <summary>Creates a factory whose loops log unhandled work-item exceptions to <paramref name="log"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="log"/> is null.</exception>
    public ThreadNodeLoopFactory(INodeLog log)
    {
        ArgumentNullException.ThrowIfNull(log);
        Log = log;
    }

    /// <summary>Log sink for unhandled work-item exceptions (category "loop").</summary>
    public INodeLog Log { get; }

    /// <summary>Creates and starts a loop thread named <c>tdfs-loop:{nodeName}</c>.</summary>
    /// <exception cref="ArgumentException"><paramref name="nodeName"/> is null or empty.</exception>
    public INodeLoop Create(string nodeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeName);
        return new ThreadNodeLoop(nodeName, Log);
    }
}
