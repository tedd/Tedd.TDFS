namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// One public SDK operation in flight: the caller's token, the operation deadline (on the injected clock), the client's
/// shutdown token, fault-site hits and the classification of failures that happen before anything was transmitted.
/// Created by <see cref="ClientRuntime.BeginOperation"/>; disposing it ends the in-flight registration.
/// </summary>
internal sealed class OperationScope : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly CancellationTokenRegistration _userRegistration;
    private readonly CancellationTokenRegistration _killRegistration;
    private readonly bool _tracked;
    private int _disposed;

    internal OperationScope(ClientRuntime runtime, DfsOperationKind kind, string? volume, string? path, CancellationToken userToken,
        TimeSpan timeout, bool tracked)
    {
        Runtime = runtime;
        Kind = kind;
        Volume = volume;
        Path = path;
        UserToken = userToken;
        Timeout = timeout;
        StartTimestamp = runtime.Time.GetTimestamp();
        _tracked = tracked;
        _cts = new CancellationTokenSource(timeout, runtime.Time);
        if (userToken.CanBeCanceled)
            _userRegistration = userToken.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), _cts);
        _killRegistration = runtime.KillToken.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), _cts);
    }

    /// <summary>The client runtime.</summary>
    public ClientRuntime Runtime { get; }

    /// <summary>Public (API-level) operation kind, used for F00/F01 and client-side contexts.</summary>
    public DfsOperationKind Kind { get; }

    /// <summary>Volume the operation addresses, if any.</summary>
    public string? Volume { get; }

    /// <summary>Normalized path the operation addresses, if any.</summary>
    public string? Path { get; set; }

    /// <summary>The caller's token.</summary>
    public CancellationToken UserToken { get; }

    /// <summary>Operation deadline measured from <see cref="StartTimestamp"/>.</summary>
    public TimeSpan Timeout { get; }

    /// <summary>Start of the operation on the injected clock.</summary>
    public long StartTimestamp { get; }

    /// <summary>Cancelled by the caller's token, the deadline or client shutdown.</summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>The caller cancelled.</summary>
    public bool IsUserCancelled => UserToken.IsCancellationRequested;

    /// <summary>The client was disposed past its shutdown deadline (or abandoned) while the operation ran.</summary>
    public bool IsKilled => Runtime.KillToken.IsCancellationRequested;

    /// <summary>Time since the operation started.</summary>
    public TimeSpan Elapsed => Runtime.Time.GetElapsedTime(StartTimestamp);

    /// <summary>Time left until the deadline (never negative).</summary>
    public TimeSpan Remaining
    {
        get
        {
            TimeSpan left = Timeout - Elapsed;
            return left > TimeSpan.Zero ? left : TimeSpan.Zero;
        }
    }

    /// <summary>The deadline passed.</summary>
    public bool IsDeadlineExpired => Elapsed >= Timeout;

    /// <summary>Operation id of the mutation this scope currently performs (fault contexts), if any.</summary>
    public DfsOperationId? OperationId { get; set; }

    /// <summary>
    /// Fraction of the deadline after which mutation retries stop so that an uncertain outcome can still be resolved
    /// (fenced) within the deadline.
    /// </summary>
    public TimeSpan MutationRetryBudget
    {
        get
        {
            TimeSpan reserve = TimeSpan.FromTicks(Math.Clamp(Timeout.Ticks / 6, TimeSpan.FromMilliseconds(100).Ticks, TimeSpan.FromSeconds(3).Ticks));
            TimeSpan budget = Timeout - reserve;
            return budget > TimeSpan.Zero ? budget : TimeSpan.FromTicks(Timeout.Ticks / 2);
        }
    }

    /// <summary>Hits a client fault site (no-op unless the injector is enabled).</summary>
    public ValueTask HitAsync(FaultStage stage, string site, DfsOperationKind? operation = null, DfsOperationId? operationId = null, int occurrence = 0,
        ObjectId? obj = null, string? detail = null)
    {
        IFaultInjector faults = Runtime.Faults;
        if (!faults.IsEnabled)
            return ValueTask.CompletedTask;
        var context = new FaultContext(stage, site, Runtime.NetworkName, Volume, operation ?? Kind, operationId ?? OperationId, Path, obj, occurrence, detail);
        return faults.HitAsync(context, Token);
    }

    /// <summary>Hits the operation-start site (F00) of a public operation.</summary>
    public ValueTask StartedAsync() => HitAsync(FaultStage.F00, FaultSites.ClientOperationStart);

    /// <summary>Hits the validated site (F01) after local validation, then honours cancellation (nothing was sent yet).</summary>
    public async ValueTask ValidatedAsync()
    {
        await HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
        ThrowIfCancelled();
    }

    /// <summary>Throws the classified cancellation failure when <see cref="Token"/> fired (nothing transmitted).</summary>
    public void ThrowIfCancelled()
    {
        if (Token.IsCancellationRequested)
            throw NotTransmittedFailure(null);
    }

    /// <summary>
    /// The failure to report when the operation ends without a result and nothing that could take effect was transmitted:
    /// <see cref="OperationCanceledException"/> for caller cancellation, <see cref="ObjectDisposedException"/> for client
    /// shutdown, and otherwise the last availability failure for the deadline: always a typed <see cref="DfsException"/> (FSD §13),
    /// a timeout as <see cref="DfsUnavailableException"/> with <see cref="DfsErrorCode.Timeout"/> carrying the
    /// <see cref="TimeoutException"/> as its cause, never a bare <see cref="TimeoutException"/>.
    /// </summary>
    public Exception NotTransmittedFailure(Exception? lastFailure)
    {
        if (IsUserCancelled)
            return new OperationCanceledException($"{Kind} was cancelled by the caller.", lastFailure, UserToken);
        if (IsKilled)
            return new ObjectDisposedException($"{Kind} was cancelled because the DfsClient was disposed.", lastFailure);
        if (lastFailure is DfsException or ObjectDisposedException or DfsBackupNotReachedException)
            return lastFailure;
        if (lastFailure is TimeoutException timeout)
            return new DfsUnavailableException(DfsErrorCode.Timeout, $"{Kind} failed: {timeout.Message}", timeout);
        if (lastFailure is not null && lastFailure is not OperationCanceledException)
        {
            // Transport-level failures surface as the typed availability error (never as end of stream or success).
            return new DfsUnavailableException(DfsErrorCode.Unavailable, $"{Kind} failed: the cluster could not be reached ({lastFailure.Message})", lastFailure);
        }

        string message = $"{Kind} did not complete within its {Timeout} deadline.";
        return new DfsUnavailableException(DfsErrorCode.Timeout, message, new TimeoutException(message, lastFailure));
    }

    /// <summary>True when the scope's token fired for a reason other than the deadline (caller or shutdown).</summary>
    public bool IsAbortedByCaller => IsUserCancelled || IsKilled;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _userRegistration.Unregister();
        _killRegistration.Unregister();
        _cts.Dispose();
        if (_tracked)
            Runtime.EndOperation();
    }
}
