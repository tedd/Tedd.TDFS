using System.Collections.Concurrent;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Sdk;

/// <summary>Shared helpers for the SDK integration suite.</summary>
internal static class SdkHelpers
{
    /// <summary>The default volume created by the harness.</summary>
    public const string Volume = "vol";

    /// <summary>Cancellation token of the running test.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Three nodes, one per zone, on real QUIC transport and real disk storage.</summary>
    public static TestClusterOptions QuicOnDisk => TestClusterOptions.ThreeZones with { Transport = TransportKind.Quic, Storage = StorageKind.Disk };

    /// <summary>Writes a line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}

/// <summary>
/// A <see cref="SynchronizationContext"/> with exactly one thread (like a UI thread). Work posted to it runs only when
/// that thread pumps; while the thread is blocked in a synchronous call nothing posted to the context can run, so any
/// SDK continuation that needs it would deadlock. <see cref="PostCount"/> counts every post, which lets a test prove that
/// no continuation was even scheduled onto the blocked context (SDK-010).
/// </summary>
internal sealed class SingleThreadedSynchronizationContext : SynchronizationContext
{
    private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> _queue = new();
    private int _posts;
    private int _sends;

    private SingleThreadedSynchronizationContext()
    {
    }

    /// <summary>Number of callbacks posted to this context so far.</summary>
    public int PostCount => Volatile.Read(ref _posts);

    /// <summary>Number of synchronous <see cref="Send"/> attempts (always rejected).</summary>
    public int SendCount => Volatile.Read(ref _sends);

    /// <summary>The managed thread id of the context's only thread.</summary>
    public int ThreadId { get; private set; }

    /// <inheritdoc />
    public override void Post(SendOrPostCallback d, object? state)
    {
        Interlocked.Increment(ref _posts);
        _queue.Add((d, state));
    }

    /// <inheritdoc />
    public override void Send(SendOrPostCallback d, object? state)
    {
        Interlocked.Increment(ref _sends);
        throw new InvalidOperationException("Synchronous Send to the single-threaded context is not allowed.");
    }

    /// <inheritdoc />
    public override SynchronizationContext CreateCopy() => this;

    /// <summary>
    /// Starts a dedicated thread, installs a new context on it, runs <paramref name="body"/> there and pumps posted work
    /// until the body's task completes. The returned task completes with the body's result (or exception); if the body
    /// deadlocks it never completes (the thread is a background thread), so callers await it with a timeout.
    /// </summary>
    public static Task<T> Run<T>(Func<SingleThreadedSynchronizationContext, Task<T>> body, string name)
    {
        var context = new SingleThreadedSynchronizationContext();
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            context.ThreadId = Environment.CurrentManagedThreadId;
            SetSynchronizationContext(context);
            Task<T> task;
            try
            {
                task = body(context);
            }
            catch (Exception ex)
            {
                completion.TrySetException(ex);
                return;
            }

            task.ContinueWith(_ => context._queue.CompleteAdding(), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            foreach ((SendOrPostCallback callback, object? state) in context._queue.GetConsumingEnumerable())
                callback(state);

            if (task.IsFaulted)
                completion.TrySetException(task.Exception!.InnerExceptions);
            else if (task.IsCanceled)
                completion.TrySetCanceled();
            else
                completion.TrySetResult(task.Result);
        })
        {
            IsBackground = true,
            Name = name,
        };
        thread.Start();
        return completion.Task;
    }
}
