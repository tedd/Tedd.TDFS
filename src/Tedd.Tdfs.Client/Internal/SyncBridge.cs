namespace Tedd.Tdfs.Client.Internal;

/// <summary>
/// Runs the SDK's asynchronous implementations for the synchronous API surface (SDK-010, ARCHITECTURE §9.6): the
/// operation is started with no <see cref="SynchronizationContext"/> so none of its continuations can be posted back
/// to the (blocked) calling context, and the caller then blocks on the result. Exceptions propagate unwrapped.
/// </summary>
internal static class SyncBridge
{
    /// <summary>Runs <paramref name="operation"/> and blocks until it completes.</summary>
    public static void Run(Func<Task> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Task task = Start(operation);
        task.ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>Runs <paramref name="operation"/> and blocks until it completes.</summary>
    public static T Run<T>(Func<Task<T>> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Task<T> task = Start(operation);
        return task.ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>Runs a <see cref="ValueTask"/>-returning <paramref name="operation"/> and blocks until it completes.</summary>
    /// <remarks>Named differently from <c>Run</c> so async lambdas are never ambiguous.</remarks>
    public static void RunValueTask(Func<ValueTask> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ValueTask task = Start(operation);
        if (task.IsCompleted)
        {
            task.GetAwaiter().GetResult();
            return;
        }

        // A pending ValueTask may be backed by an IValueTaskSource, which does not support blocking: go via Task.
        task.AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>Runs a <see cref="ValueTask{TResult}"/>-returning <paramref name="operation"/> and blocks until it completes.</summary>
    /// <remarks>Named differently from <c>Run</c> so async lambdas are never ambiguous.</remarks>
    public static T RunValueTask<T>(Func<ValueTask<T>> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ValueTask<T> task = Start(operation);
        if (task.IsCompleted)
            return task.GetAwaiter().GetResult();

        return task.AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Exposes an async sequence as a blocking sequence. <paramref name="source"/> is invoked immediately (so eager
    /// argument validation surfaces at the call); each element is then fetched on demand, and disposing the
    /// enumerator (including breaking out of a <c>foreach</c>) disposes the async enumerator.
    /// </summary>
    public static IEnumerable<T> Enumerate<T>(Func<IAsyncEnumerable<T>> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        IAsyncEnumerable<T> sequence = Start(source);
        return EnumerateCore(sequence);
    }

    private static IEnumerable<T> EnumerateCore<T>(IAsyncEnumerable<T> sequence)
    {
        IAsyncEnumerator<T> enumerator = Start(() => sequence.GetAsyncEnumerator(CancellationToken.None));
        try
        {
            while (RunValueTask(enumerator.MoveNextAsync))
                yield return enumerator.Current;
        }
        finally
        {
            RunValueTask(enumerator.DisposeAsync);
        }
    }

    // Invokes start with SynchronizationContext.Current cleared, so awaits inside the started operation do not
    // capture the caller's context; the caller's context is restored before blocking.
    private static TResult Start<TResult>(Func<TResult> start)
    {
        SynchronizationContext? previous = SynchronizationContext.Current;
        if (previous is null)
            return start();

        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            return start();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }
}
