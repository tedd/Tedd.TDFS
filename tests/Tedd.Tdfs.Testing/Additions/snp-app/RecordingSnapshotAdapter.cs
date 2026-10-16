namespace Tedd.Tdfs.Testing.SnpApp;

/// <summary>How a <see cref="RecordingSnapshotAdapter"/> behaves in one callback phase.</summary>
public enum AdapterPhaseBehavior
{
    /// <summary>Completes successfully (after running the phase's work delegate, if any).</summary>
    Succeed,

    /// <summary>Returns a task faulted with <see cref="AdapterInjectedFailureException"/>.</summary>
    ThrowAsync,

    /// <summary>Throws <see cref="AdapterInjectedFailureException"/> synchronously from the interface method (no task is returned).</summary>
    ThrowSynchronously,

    /// <summary>Returns a canceled task although the caller's token was not canceled.</summary>
    ReturnCanceledTask,

    /// <summary>Never completes on its own; completes as canceled when the caller's token is canceled.</summary>
    HangUntilCancelled,

    /// <summary>Never completes on its own and ignores cancellation; only <see cref="RecordingSnapshotAdapter.ReleaseHangs"/> ends it.</summary>
    HangIgnoringCancellation,
}

/// <summary>The failure injected by a <see cref="RecordingSnapshotAdapter"/>; lets tests find it in exception chains.</summary>
public sealed class AdapterInjectedFailureException : Exception
{
    /// <summary>Creates the exception.</summary>
    public AdapterInjectedFailureException(string adapterId, string phase)
        : base($"Injected failure of adapter '{adapterId}' in phase '{phase}'.")
    {
        AdapterId = adapterId;
        Phase = phase;
    }

    /// <summary>Creates the exception (serialization-style constructor kept for analyzers).</summary>
    public AdapterInjectedFailureException()
        : this(string.Empty, string.Empty)
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    public AdapterInjectedFailureException(string message)
        : base(message)
    {
        AdapterId = string.Empty;
        Phase = string.Empty;
    }

    /// <summary>Creates the exception with a message and inner exception.</summary>
    public AdapterInjectedFailureException(string message, Exception innerException)
        : base(message, innerException)
    {
        AdapterId = string.Empty;
        Phase = string.Empty;
    }

    /// <summary>Adapter that failed.</summary>
    public string AdapterId { get; }

    /// <summary>Phase that failed ("prepare", "resume" or "abort").</summary>
    public string Phase { get; }
}

/// <summary>
/// A test double for <see cref="IDfsSnapshotAdapter"/> that records every callback (order, timing, context and
/// cancellation) into an <see cref="AdapterTimeline"/> and can be told to succeed, fail or hang per phase.
/// </summary>
/// <remarks>
/// Timeline events use the adapter id as source: <c>prepare.start</c>, <c>prepare.done</c>, <c>prepare.failed</c>,
/// <c>prepare.cancelled</c>, and the same for <c>resume</c> and <c>abort</c>; <c>cancel.observed</c> when the caller's
/// token fires during a callback. The application this adapter stands for is considered paused from the start of a
/// prepare call until a resume or abort call starts (<see cref="IsPaused"/>). No real-time APIs are used, so the
/// adapter is usable inside the deterministic simulation.
/// </remarks>
public sealed class RecordingSnapshotAdapter : IDfsSnapshotAdapter
{
    private readonly object _lock = new();
    private readonly List<DfsSnapshotAdapterContext> _contexts = [];
    private readonly TaskCompletionSource _hangRelease = new();
    private int _prepareCalls;
    private int _resumeCalls;
    private int _abortCalls;
    private bool _prepareStarted;
    private bool _prepareSucceeded;
    private bool _resumeSucceeded;
    private bool _abortSucceeded;
    private bool _released;
    private bool _cancellationObserved;

    /// <summary>Creates the adapter.</summary>
    public RecordingSnapshotAdapter(string id, AdapterTimeline timeline, string version = "1.0.0",
        IReadOnlyDictionary<string, string>? restoreRequirements = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentNullException.ThrowIfNull(timeline);
        ArgumentNullException.ThrowIfNull(version);
        Id = id;
        Timeline = timeline;
        Version = version;
        RestoreRequirements = restoreRequirements is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(restoreRequirements, StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public string Id { get; }

    /// <inheritdoc />
    public string Version { get; }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, string> RestoreRequirements { get; }

    /// <summary>The timeline receiving this adapter's events.</summary>
    public AdapterTimeline Timeline { get; }

    /// <summary>Behaviour of <see cref="IDfsSnapshotAdapter.PrepareAsync"/>.</summary>
    public AdapterPhaseBehavior PrepareBehavior { get; set; } = AdapterPhaseBehavior.Succeed;

    /// <summary>Behaviour of <see cref="IDfsSnapshotAdapter.ResumeAsync"/>.</summary>
    public AdapterPhaseBehavior ResumeBehavior { get; set; } = AdapterPhaseBehavior.Succeed;

    /// <summary>Behaviour of <see cref="IDfsSnapshotAdapter.AbortAsync"/>.</summary>
    public AdapterPhaseBehavior AbortBehavior { get; set; } = AdapterPhaseBehavior.Succeed;

    /// <summary>Work run at the start of prepare (for example an application checkpoint written through the SDK).</summary>
    public Func<DfsSnapshotAdapterContext, CancellationToken, Task>? PrepareWork { get; set; }

    /// <summary>Work run at the start of resume.</summary>
    public Func<DfsSnapshotAdapterContext, CancellationToken, Task>? ResumeWork { get; set; }

    /// <summary>Work run at the start of abort.</summary>
    public Func<DfsSnapshotAdapterContext, CancellationToken, Task>? AbortWork { get; set; }

    /// <summary>Number of prepare calls.</summary>
    public int PrepareCalls => Volatile.Read(ref _prepareCalls);

    /// <summary>Number of resume calls (attempts, including failed ones).</summary>
    public int ResumeCalls => Volatile.Read(ref _resumeCalls);

    /// <summary>Number of abort calls (attempts, including failed ones).</summary>
    public int AbortCalls => Volatile.Read(ref _abortCalls);

    /// <summary>True once a prepare call started.</summary>
    public bool PrepareStarted
    {
        get
        {
            lock (_lock)
                return _prepareStarted;
        }
    }

    /// <summary>True once a prepare call completed successfully.</summary>
    public bool PrepareSucceeded
    {
        get
        {
            lock (_lock)
                return _prepareSucceeded;
        }
    }

    /// <summary>True once a resume call completed successfully.</summary>
    public bool ResumeSucceeded
    {
        get
        {
            lock (_lock)
                return _resumeSucceeded;
        }
    }

    /// <summary>True once an abort call completed successfully.</summary>
    public bool AbortSucceeded
    {
        get
        {
            lock (_lock)
                return _abortSucceeded;
        }
    }

    /// <summary>True when the caller's token was canceled while a callback was running.</summary>
    public bool CancellationObserved
    {
        get
        {
            lock (_lock)
                return _cancellationObserved;
        }
    }

    /// <summary>True while the application is paused: a prepare started and no resume/abort has been attempted since.</summary>
    public bool IsPaused
    {
        get
        {
            lock (_lock)
                return _prepareStarted && !_released;
        }
    }

    /// <summary>True when a resume or abort was attempted after the last prepare (the pause was released).</summary>
    public bool PauseReleased
    {
        get
        {
            lock (_lock)
                return _released;
        }
    }

    /// <summary>Contexts passed to all callbacks, in call order.</summary>
    public IReadOnlyList<DfsSnapshotAdapterContext> Contexts
    {
        get
        {
            lock (_lock)
                return _contexts.ToArray();
        }
    }

    /// <summary>Ends every <see cref="AdapterPhaseBehavior.HangIgnoringCancellation"/> wait (call during test cleanup).</summary>
    public void ReleaseHangs() => _hangRelease.TrySetResult();

    /// <inheritdoc />
    public Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
    {
        Interlocked.Increment(ref _prepareCalls);
        lock (_lock)
        {
            _prepareStarted = true;
            _released = false;
        }

        return RunPhase("prepare", PrepareBehavior, PrepareWork, context, ct, () =>
        {
            lock (_lock)
                _prepareSucceeded = true;
        });
    }

    /// <inheritdoc />
    public Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
    {
        Interlocked.Increment(ref _resumeCalls);
        lock (_lock)
            _released = true;
        return RunPhase("resume", ResumeBehavior, ResumeWork, context, ct, () =>
        {
            lock (_lock)
                _resumeSucceeded = true;
        });
    }

    /// <inheritdoc />
    public Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
    {
        Interlocked.Increment(ref _abortCalls);
        lock (_lock)
            _released = true;
        return RunPhase("abort", AbortBehavior, AbortWork, context, ct, () =>
        {
            lock (_lock)
                _abortSucceeded = true;
        });
    }

    /// <inheritdoc />
    public override string ToString() =>
        $"{Id} (prepare {PrepareCalls}{(PrepareSucceeded ? " ok" : string.Empty)}, resume {ResumeCalls}, abort {AbortCalls})";

    private Task RunPhase(string phase, AdapterPhaseBehavior behavior, Func<DfsSnapshotAdapterContext, CancellationToken, Task>? work,
        DfsSnapshotAdapterContext context, CancellationToken ct, Action onSuccess)
    {
        ArgumentNullException.ThrowIfNull(context);
        lock (_lock)
            _contexts.Add(context);
        Timeline.Record(Id, phase + ".start", context.SnapshotName);
        if (behavior == AdapterPhaseBehavior.ThrowSynchronously)
        {
            Timeline.Record(Id, phase + ".failed", "synchronous");
            throw new AdapterInjectedFailureException(Id, phase);
        }

        return RunPhaseAsync(phase, behavior, work, context, ct, onSuccess);
    }

    private async Task RunPhaseAsync(string phase, AdapterPhaseBehavior behavior, Func<DfsSnapshotAdapterContext, CancellationToken, Task>? work,
        DfsSnapshotAdapterContext context, CancellationToken ct, Action onSuccess)
    {
        using CancellationTokenRegistration registration = ct.Register(() =>
        {
            lock (_lock)
                _cancellationObserved = true;
            Timeline.Record(Id, "cancel.observed", phase);
        });
        try
        {
            if (work is not null)
                await work(context, ct);

            switch (behavior)
            {
                case AdapterPhaseBehavior.Succeed:
                    break;
                case AdapterPhaseBehavior.ThrowAsync:
                    await Task.CompletedTask;
                    throw new AdapterInjectedFailureException(Id, phase);
                case AdapterPhaseBehavior.ReturnCanceledTask:
                    throw new OperationCanceledException($"Adapter '{Id}' canceled its {phase} on its own.");
                case AdapterPhaseBehavior.HangUntilCancelled:
                    {
                        var waiter = new TaskCompletionSource();
                        using (ct.Register(() => waiter.TrySetCanceled(ct)))
                            await waiter.Task;
                        break;
                    }

                case AdapterPhaseBehavior.HangIgnoringCancellation:
                    await _hangRelease.Task;
                    break;
                default:
                    throw new InvalidOperationException($"Unknown behaviour {behavior}.");
            }
        }
        catch (OperationCanceledException ex)
        {
            // Cancellation callbacks run last-registered first: a phase that ends because of the cancellation can complete
            // (inline) and dispose the observing registration above before it ran, so the observation is recorded here too.
            if (ct.IsCancellationRequested)
            {
                lock (_lock)
                    _cancellationObserved = true;
            }

            Timeline.Record(Id, phase + ".cancelled", ex.GetType().Name);
            throw;
        }
        catch (Exception ex)
        {
            Timeline.Record(Id, phase + ".failed", ex.GetType().Name);
            throw;
        }

        onSuccess();
        Timeline.Record(Id, phase + ".done", context.SnapshotName);
    }
}
