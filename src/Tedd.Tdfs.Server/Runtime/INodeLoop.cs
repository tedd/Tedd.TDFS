namespace Tedd.Tdfs.Server;

/// <summary>
/// Single-threaded event loop that owns all state of one node (ARCHITECTURE.md §3). Work posted to the loop runs one
/// item at a time in FIFO order; <see cref="Context"/> is installed while work runs so every <c>await</c> in node code
/// resumes on the loop. The production loop is a dedicated thread; the deterministic simulation supplies its own
/// implementation driven by the simulation scheduler.
/// </summary>
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

    /// <summary>True once <see cref="Stop"/> was called.</summary>
    bool IsStopped { get; }

    /// <summary>
    /// Queues work. Work posted after <see cref="Stop"/> is discarded. Work items should handle their own exceptions;
    /// how a loop reports an unhandled exception is implementation-defined (the production loop logs it).
    /// </summary>
    void Post(Action work);

    /// <summary>Freezes the loop: queued and newly posted work is held until <see cref="Resume"/> (memory is retained).</summary>
    void Pause();

    /// <summary>Unfreezes a paused loop.</summary>
    void Resume();

    /// <summary>Stops the loop immediately and discards queued work (crash semantics; no cleanup runs).</summary>
    void Stop();
}
