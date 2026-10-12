namespace Tedd.Tdfs.Server;

/// <summary>
/// Shared implementation of <see cref="INodeLoop.InvokeAsync(Action)"/> and its overloads: runs work on a loop,
/// completes a <see cref="TaskCompletionSource{TResult}"/> with its outcome and faults every outstanding invocation with
/// <see cref="NodeLoopStoppedException"/> when the loop stops (<see cref="Close"/>).
/// </summary>
/// <remarks>
/// <para>
/// Outstanding invocations are tracked in invocation order (a linked list, never a hash-ordered collection) so that
/// failing them on stop is deterministic under simulation. The completion-source options are chosen by the loop: the
/// production loop runs external continuations asynchronously (callers never execute on the loop thread), the
/// deterministic simulation completes them inline (no thread pool involvement).
/// </para>
/// <para>
/// When completing inline, the runtime only inlines <c>ConfigureAwait(false)</c> continuations if the completing thread
/// has no custom <see cref="SynchronizationContext"/>; otherwise it queues them to the thread pool. With
/// <c>completeWithoutSynchronizationContext</c> the loop's context is therefore removed for the duration of each
/// completion (continuations that captured a context are still posted to it).
/// </para>
/// </remarks>
internal sealed class NodeLoopInvoker
{
    private readonly object _lock = new();
    private readonly LinkedList<IPendingInvocation> _pending = new();
    private readonly INodeLoop _loop;
    private readonly TaskCreationOptions _options;
    private readonly bool _completeWithoutContext;
    private bool _closed;

    /// <summary>Creates the invoker for <paramref name="loop"/>.</summary>
    /// <param name="loop">The loop work is posted to.</param>
    /// <param name="completionOptions">Options for the completion sources handed to callers.</param>
    /// <param name="completeWithoutSynchronizationContext">Complete sources with no synchronization context installed.</param>
    public NodeLoopInvoker(INodeLoop loop, TaskCreationOptions completionOptions, bool completeWithoutSynchronizationContext = false)
    {
        ArgumentNullException.ThrowIfNull(loop);
        _loop = loop;
        _options = completionOptions;
        _completeWithoutContext = completeWithoutSynchronizationContext;
    }

    /// <summary>Runs <paramref name="completion"/> with no synchronization context installed (restored afterwards).</summary>
    internal static void RunWithoutSynchronizationContext(Action completion)
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

    /// <summary>Number of invocations not yet completed (diagnostics).</summary>
    public int PendingCount
    {
        get
        {
            lock (_lock)
                return _pending.Count;
        }
    }

    /// <summary>Runs synchronous work.</summary>
    public Task InvokeAsync(Action work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return Start<object?>(invocation =>
        {
            work();
            invocation.SetResult(null);
        });
    }

    /// <summary>Runs synchronous work returning a value.</summary>
    public Task<T> InvokeAsync<T>(Func<T> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return Start<T>(invocation => invocation.SetResult(work()));
    }

    /// <summary>Runs asynchronous work.</summary>
    public Task InvokeAsync(Func<Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return Start<object?>(invocation =>
        {
            Task task = work() ?? throw new InvalidOperationException("The work delegate returned a null task.");
            invocation.Follow(task);
        });
    }

    /// <summary>Runs asynchronous work returning a value.</summary>
    public Task<T> InvokeAsync<T>(Func<Task<T>> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return Start<T>(invocation =>
        {
            Task<T> task = work() ?? throw new InvalidOperationException("The work delegate returned a null task.");
            invocation.Follow(task);
        });
    }

    /// <summary>
    /// Marks the loop as stopped: every outstanding invocation (queued or awaiting) faults with
    /// <see cref="NodeLoopStoppedException"/>, in invocation order, and later invocations fault immediately. Idempotent.
    /// </summary>
    public void Close()
    {
        IPendingInvocation[] outstanding;
        lock (_lock)
        {
            _closed = true;
            if (_pending.Count == 0)
                return;
            outstanding = new IPendingInvocation[_pending.Count];
            _pending.CopyTo(outstanding, 0);
            _pending.Clear();
            foreach (IPendingInvocation invocation in outstanding)
                invocation.Detach();
        }

        foreach (IPendingInvocation invocation in outstanding)
            invocation.Fail(new NodeLoopStoppedException(_loop.Name));
    }

    private Task<T> Start<T>(Action<Invocation<T>> body)
    {
        var invocation = new Invocation<T>(this, _options);
        lock (_lock)
        {
            if (_closed)
                return Task.FromException<T>(new NodeLoopStoppedException(_loop.Name));
            invocation.Node = _pending.AddLast(invocation);
        }

        // If the loop stops before the item runs, Post discards it and Close() faults the invocation.
        _loop.Post(() =>
        {
            if (invocation.IsCompleted)
                return;
            try
            {
                body(invocation);
            }
            catch (OperationCanceledException ex)
            {
                invocation.SetCanceled(ex.CancellationToken);
            }
            catch (Exception ex)
            {
                invocation.SetException(ex);
            }
        });
        return invocation.Task;
    }

    private void Remove(LinkedListNode<IPendingInvocation>? node)
    {
        if (node is null)
            return;
        lock (_lock)
        {
            if (node.List is not null)
                _pending.Remove(node);
        }
    }

    private interface IPendingInvocation
    {
        void Detach();

        void Fail(Exception exception);
    }

    private sealed class Invocation<T> : IPendingInvocation
    {
        private readonly NodeLoopInvoker _owner;
        private readonly TaskCompletionSource<T> _source;

        public Invocation(NodeLoopInvoker owner, TaskCreationOptions options)
        {
            _owner = owner;
            _source = new TaskCompletionSource<T>(options);
        }

        public LinkedListNode<IPendingInvocation>? Node { get; set; }

        public Task<T> Task => _source.Task;

        public bool IsCompleted => _source.Task.IsCompleted;

        public void Detach() => Node = null;

        public void Fail(Exception exception) => Complete(() => _source.TrySetException(exception));

        public void SetResult(T value)
        {
            _owner.Remove(Node);
            Complete(() => _source.TrySetResult(value));
        }

        public void SetException(Exception exception)
        {
            _owner.Remove(Node);
            Complete(() => _source.TrySetException(exception));
        }

        public void SetCanceled(CancellationToken token)
        {
            _owner.Remove(Node);
            Complete(() => _source.TrySetCanceled(token));
        }

        private void Complete(Action completion)
        {
            if (_owner._completeWithoutContext)
                RunWithoutSynchronizationContext(completion);
            else
                completion();
        }

        /// <summary>Completes from <paramref name="task"/>, awaiting it on the loop when it is still running.</summary>
        public void Follow(Task task)
        {
            if (task.IsCompleted)
                CompleteFrom(task);
            else
                _ = AwaitOnLoopAsync(task);
        }

        // Runs on the loop: the await captures the loop's synchronization context, so the continuation is posted back
        // to the loop (and is discarded if the loop stops first; Close() then faults the invocation).
        private async Task AwaitOnLoopAsync(Task task)
        {
            try
            {
                await task;
            }
            catch
            {
                // Observed and propagated by CompleteFrom.
            }

            CompleteFrom(task);
        }

        private void CompleteFrom(Task task)
        {
            if (task.IsCanceled)
            {
                CancellationToken token = CancellationToken.None;
                try
                {
                    task.GetAwaiter().GetResult();
                }
                catch (OperationCanceledException ex)
                {
                    token = ex.CancellationToken;
                }

                SetCanceled(token);
            }
            else if (task.IsFaulted)
            {
                _owner.Remove(Node);
                Complete(() => _source.TrySetException(task.Exception!.InnerExceptions));
            }
            else if (task is Task<T> typed)
            {
                SetResult(typed.Result);
            }
            else
            {
                SetResult(default!);
            }
        }
    }
}
