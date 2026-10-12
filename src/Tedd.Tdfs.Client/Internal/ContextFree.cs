namespace Tedd.Tdfs.Client.Internal;

/// <summary>
/// Runs completions (TaskCompletionSource results, CancellationTokenSource cancellation) with no
/// <see cref="SynchronizationContext"/> installed (ARCHITECTURE §9.6, CLAUDE.md simulation gotchas).
/// </summary>
/// <remarks>
/// The runtime inlines <c>ConfigureAwait(false)</c> continuations of a completed task only when the completing thread has
/// no custom synchronization context; otherwise it queues them to the thread pool. Inside the deterministic simulation
/// that escape is an uncontrolled dependency (HAR-002), so every completion the SDK triggers from code that may run on an
/// application or simulation context (cancellation callbacks registered on caller tokens, timer callbacks, lock
/// hand-over) goes through this helper. Continuations that captured a context are still posted to it.
/// </remarks>
internal static class ContextFree
{
    /// <summary>Runs <paramref name="completion"/> with the synchronization context cleared, restoring it afterwards.</summary>
    public static void Run(Action completion)
    {
        SynchronizationContext? previous = SynchronizationContext.Current;
        if (previous is null)
        {
            completion();
            return;
        }

        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            completion();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    /// <summary>Runs <paramref name="completion"/> with <paramref name="state"/> and the synchronization context cleared.</summary>
    public static void Run<TState>(Action<TState> completion, TState state)
    {
        SynchronizationContext? previous = SynchronizationContext.Current;
        if (previous is null)
        {
            completion(state);
            return;
        }

        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            completion(state);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    /// <summary>Cancels <paramref name="source"/> without a synchronization context; a disposed source is ignored.</summary>
    public static void Cancel(CancellationTokenSource source)
    {
        Run(static s =>
        {
            try
            {
                s.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The owner already finished.
            }
        }, source);
    }
}
