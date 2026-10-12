namespace Tedd.Tdfs.Server;

/// <summary>
/// Single-threaded event loop that owns all state of one node (ARCHITECTURE.md §3). Work posted to the loop runs one
/// item at a time in FIFO order; <see cref="Context"/> is installed while work runs so every <c>await</c> in node code
/// resumes on the loop. The production loop is a dedicated thread; the deterministic simulation supplies its own
/// implementation driven by the simulation scheduler.
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle: a loop starts running when it is created. <see cref="Pause"/> holds queued work (nothing is lost) until
/// <see cref="Resume"/>. <see cref="Stop"/> is the crash path: queued work is discarded, later posts are discarded and no
/// cleanup runs. <see cref="DrainAsync"/> is the graceful path: queued work (and work it posts) keeps running until the
/// queue is observed empty, then the loop stops. Either way <see cref="Completion"/> completes once the loop has
/// stopped.
/// </para>
/// <para>
/// A work item that is executing when the loop is paused or stopped runs to completion (a loop cannot preempt code);
/// the pause or stop takes effect before the next item.
/// </para>
/// </remarks>
public interface INodeLoop
{
    /// <summary>Name of the node owning the loop.</summary>
    string Name { get; }

    /// <summary>Synchronization context that posts continuations to this loop.</summary>
    SynchronizationContext Context { get; }

    /// <summary>True when the caller is currently executing on this loop.</summary>
    bool IsOnLoop { get; }

    /// <summary>True while the loop is paused (queued work is held, not discarded).</summary>
    bool IsPaused { get; }

    /// <summary>True once the loop was stopped (by <see cref="Stop"/> or at the end of <see cref="DrainAsync"/>).</summary>
    bool IsStopped { get; }

    /// <summary>
    /// Completes when the loop has stopped and will run no further work (for the production loop: when its thread has
    /// exited). Never faults.
    /// </summary>
    Task Completion { get; }

    /// <summary>
    /// Raised when a posted work item throws. The loop keeps running: an exception escaping a work item never
    /// terminates the loop or the process. Handlers run on the loop; exceptions thrown by handlers are swallowed (the
    /// production loop logs both).
    /// </summary>
    event Action<INodeLoop, Exception>? UnhandledException;

    /// <summary>
    /// Queues work. Work posted after the loop stopped is discarded. Work items should handle their own exceptions;
    /// an exception escaping one is reported through <see cref="UnhandledException"/> (and the production loop logs
    /// it) instead of terminating the loop.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="work"/> is null.</exception>
    void Post(Action work);

    /// <summary>
    /// Runs <paramref name="work"/> on the loop (always queued, never inlined, even when called on the loop) and
    /// completes the returned task with its outcome. If the loop stops before the work ran or finished, the task faults
    /// with <see cref="NodeLoopStoppedException"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="work"/> is null.</exception>
    Task InvokeAsync(Action work);

    /// <summary>As <see cref="InvokeAsync(Action)"/>, returning the work's result.</summary>
    Task<T> InvokeAsync<T>(Func<T> work);

    /// <summary>
    /// Starts asynchronous <paramref name="work"/> on the loop (its awaits resume on the loop) and completes the
    /// returned task when the work's task completes. If the loop stops first, the task faults with
    /// <see cref="NodeLoopStoppedException"/>.
    /// </summary>
    Task InvokeAsync(Func<Task> work);

    /// <summary>As <see cref="InvokeAsync(Func{Task})"/>, returning the work's result.</summary>
    Task<T> InvokeAsync<T>(Func<Task<T>> work);

    /// <summary>Freezes the loop: queued and newly posted work is held until <see cref="Resume"/> (memory is retained).</summary>
    void Pause();

    /// <summary>Unfreezes a paused loop.</summary>
    void Resume();

    /// <summary>Stops the loop immediately and discards queued work (crash semantics; no cleanup runs).</summary>
    void Stop();

    /// <summary>
    /// Graceful stop: resumes the loop if it is paused, keeps running queued work and work posted meanwhile (for example
    /// continuations of that work) until the queue is observed empty, then stops the loop. Work posted afterwards is
    /// discarded, so callers should first quiesce whatever still posts to the loop (timers, transports, I/O).
    /// </summary>
    /// <param name="ct">
    /// Cancelling it stops the loop abruptly (as <see cref="Stop"/>); the returned task then completes as cancelled once
    /// the loop has stopped.
    /// </param>
    /// <returns>A task that completes when the loop has stopped. Do not wait for it on the loop itself.</returns>
    Task DrainAsync(CancellationToken ct = default);
}
