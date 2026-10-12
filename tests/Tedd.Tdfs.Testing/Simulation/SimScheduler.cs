using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// Deterministic discrete-event scheduler for whole-cluster simulation (ARCHITECTURE §3.2). One OS thread (the thread
/// calling a <c>Run*</c> method) executes every work item; virtual time only moves when nothing is runnable, jumping to
/// the next pending timer or delivery. Given the same seed, inputs and code, every run produces the same interleaving,
/// the same virtual timestamps and the same <see cref="Trace"/> (HAR-001).
/// </summary>
/// <remarks>
/// <para>
/// <b>Processes and lanes.</b> Work belongs to a <see cref="SimProcess"/> (a node incarnation, a client, the built-in
/// <see cref="ClientProcess"/>) or to the harness (global). Each process has a main lane — its
/// <see cref="SimProcess.Context"/>, used by <see cref="SimNodeLoop"/> — plus background lanes for timer callbacks and
/// network deliveries. Items of one lane always run in FIFO order. Among runnable lanes the next one is chosen by a seeded
/// random draw (<see cref="SimScheduleMode.Random"/>, exploring interleavings) or by global FIFO order. Pausing a process
/// holds all of its lanes; killing it discards their items and drops later posts.
/// </para>
/// <para>
/// <b>Uncontrolled dependencies (HAR-002).</b> Any interaction with the simulation (posting to a simulated
/// synchronization context, creating or changing a timer, sending a message, ...) from a thread other than the driving
/// thread is recorded in <see cref="Violations"/>. Typical causes are real timers (<c>Task.Delay</c> without the injected
/// provider), <c>Task.Run</c>/thread-pool work, real I/O completions and continuations that escaped to the thread pool.
/// With <see cref="SimSchedulerOptions.ThrowOnViolation"/> the driving run then fails with
/// <see cref="SimulationViolationException"/>.
/// </para>
/// <para>
/// <b>Client code.</b> <see cref="RunAsync(Func{Task}, TimeSpan?)"/> runs a test body on the client process's
/// synchronization context. SDK code that uses <c>ConfigureAwait(false)</c> continues inline on the simulation thread
/// wherever its awaited task is completed (for example inside a network delivery), which stays deterministic because
/// completion sources are completed from simulated work without <c>RunContinuationsAsynchronously</c> and with no
/// synchronization context installed (see <see cref="CompleteWithoutContext"/>: the runtime refuses to inline such
/// continuations on a thread with a custom context and would queue them to the thread pool instead).
/// </para>
/// <para>
/// Not supported inside the simulation: blocking waits (<c>.Result</c>, <c>Wait()</c>, the SDK's synchronous wrappers)
/// on work that the simulation itself must run — the single simulation thread would block forever.
/// </para>
/// </remarks>
public sealed class SimScheduler
{
    private const string GlobalOwnerId = "global";

    private readonly object _lock = new();
    private readonly SeededRandom _root;
    private readonly IRandomSource _choices;
    private readonly List<SimLane> _runnable = [];
    private readonly PriorityQueue<SimTimedEntry, (long Due, long Seq)> _timed = new();
    private readonly Dictionary<string, SimProcess> _live = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _incarnations = new(StringComparer.Ordinal);
    private readonly List<SimProcess> _processes = [];
    private readonly Dictionary<string, SimTimeProvider> _clocks = new(StringComparer.Ordinal);
    private readonly List<SimViolation> _violations = [];
    private readonly List<SimUnhandledError> _errors = [];
    private readonly List<string> _trace = [];
    private readonly ManualResetEventSlim _foreignWork = new(false);
    private readonly SimLane _globalLane;
    private SimNodeLoopFactory? _loopFactory;
    private long _now;
    private long _seq;
    private long _laneIds;
    private long _steps;
    private long _stepsAtInstant;
    private ulong _fingerprint = 0xCBF29CE484222325UL;
    private int _networkCount;
    private int _driverThreadId;
    private int _homeThreadId;
    private bool _hasRun;
    private bool _traceTruncated;
    private SimLane? _currentLane;
    private ExecutionContext? _driverExecutionContext;
    private Exception? _pendingFailure;

    /// <summary>Creates a scheduler for <paramref name="seed"/>.</summary>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    public SimScheduler(ulong seed, SimSchedulerOptions? options = null)
    {
        Options = options ?? new SimSchedulerOptions();
        Options.Validate();
        Seed = seed;
        _root = new SeededRandom(seed);
        _choices = _root.Fork("sim/scheduler");
        Clock = new SimTimeProvider(this, "global", isGlobal: true, processName: null, Options.StartTimeUtc, Options.StartTimestamp, rate: 1.0);
        _globalLane = CreateLane(owner: null, "global/main", context: null, transient: false);
        ClientProcess = CreateProcess(Options.ClientProcessName);
    }

    /// <summary>The master seed.</summary>
    public ulong Seed { get; }

    /// <summary>The options.</summary>
    public SimSchedulerOptions Options { get; }

    /// <summary>
    /// The global virtual clock (rate 1, never frozen). Timers created on it belong to the process executing at creation
    /// time. Use <see cref="CreateClock"/> for per-node clocks with offsets, drift and freezing.
    /// </summary>
    public SimTimeProvider Clock { get; }

    /// <summary>Virtual time elapsed since the simulation start.</summary>
    public TimeSpan Now
    {
        get
        {
            lock (_lock)
                return TimeSpan.FromTicks(_now);
        }
    }

    /// <summary>Global virtual wall-clock reading.</summary>
    public DateTimeOffset UtcNow
    {
        get
        {
            lock (_lock)
                return Options.StartTimeUtc.AddTicks(_now);
        }
    }

    /// <summary>The built-in client process; <see cref="RunAsync(Func{Task}, TimeSpan?)"/> runs bodies on its context.</summary>
    public SimProcess ClientProcess { get; }

    /// <summary>The synchronization context of <see cref="ClientProcess"/>.</summary>
    public SynchronizationContext ClientContext => ClientProcess.Context;

    /// <summary>A loop factory bound to this scheduler (for <c>NodeEnvironment.LoopFactory</c>).</summary>
    public SimNodeLoopFactory LoopFactory
    {
        get
        {
            lock (_lock)
                return _loopFactory ??= new SimNodeLoopFactory(this);
        }
    }

    /// <summary>Every process created so far (live and dead), in creation order.</summary>
    public IReadOnlyList<SimProcess> Processes
    {
        get
        {
            lock (_lock)
                return [.. _processes];
        }
    }

    /// <summary>Detected uncontrolled dependencies, in detection order.</summary>
    public IReadOnlyList<SimViolation> Violations
    {
        get
        {
            lock (_lock)
                return [.. _violations];
        }
    }

    /// <summary>Exceptions that escaped work items of live processes, in order.</summary>
    public IReadOnlyList<SimUnhandledError> UnhandledExceptions
    {
        get
        {
            lock (_lock)
                return [.. _errors];
        }
    }

    /// <summary>
    /// Trace lines (<c>[time] message</c>): explicit <see cref="AddTrace"/> events, process/clock/network lifecycle events,
    /// violations, unhandled exceptions and — with <see cref="SimSchedulerOptions.RecordSteps"/> — one line per executed
    /// item. Identical for identical seeds and inputs.
    /// </summary>
    public IReadOnlyList<string> Trace
    {
        get
        {
            lock (_lock)
                return [.. _trace];
        }
    }

    /// <summary>True when trace recording stopped at <see cref="SimSchedulerOptions.MaxTraceEntries"/>.</summary>
    public bool TraceTruncated
    {
        get
        {
            lock (_lock)
                return _traceTruncated;
        }
    }

    /// <summary>Number of work items executed so far.</summary>
    public long StepCount
    {
        get
        {
            lock (_lock)
                return _steps;
        }
    }

    /// <summary>
    /// Hash over the exact schedule executed so far (which item ran on which lane at which virtual time). Equal
    /// fingerprints mean equal interleavings.
    /// </summary>
    public ulong ScheduleFingerprint
    {
        get
        {
            lock (_lock)
                return _fingerprint;
        }
    }

    /// <summary>The process whose work item is executing on the driving thread, or null.</summary>
    public SimProcess? CurrentProcess => CurrentLane?.Owner;

    /// <summary>True while a <c>Run*</c> call is in progress.</summary>
    public bool IsDriving => Volatile.Read(ref _driverThreadId) != 0;

    /// <summary>True when the calling thread is the one currently driving the simulation.</summary>
    public bool IsOnSimulationThread => Volatile.Read(ref _driverThreadId) == Environment.CurrentManagedThreadId;

    /// <summary>Number of runnable lanes (diagnostics).</summary>
    public int RunnableLaneCount
    {
        get
        {
            lock (_lock)
                return _runnable.Count;
        }
    }

    /// <summary>Number of live (not cancelled) entries in the virtual-time queue (diagnostics; may include entries of dead processes).</summary>
    public int PendingTimedCount
    {
        get
        {
            lock (_lock)
            {
                PurgeCancelledHeadsLocked();
                int count = 0;
                foreach ((SimTimedEntry entry, _) in _timed.UnorderedItems)
                {
                    if (!entry.Cancelled)
                        count++;
                }

                return count;
            }
        }
    }

    internal object SyncRoot => _lock;

    /// <summary>Virtual elapsed ticks. Caller holds the lock (or is the driver).</summary>
    internal long NowTicksLocked => _now;

    internal long NowTicks
    {
        get
        {
            lock (_lock)
                return _now;
        }
    }

    internal SimLane? CurrentLane => IsOnSimulationThread ? _currentLane : null;

    /// <summary>Raised on the simulation thread after a process was killed (its work already discarded).</summary>
    public event Action<SimProcess>? ProcessKilled;

    /// <summary>Raised on the simulation thread for every recorded unhandled work-item exception.</summary>
    public event Action<SimUnhandledError>? UnhandledException;

    /// <summary>Formats a virtual time as seconds with 7 decimals (invariant).</summary>
    public static string FormatTime(TimeSpan time) => FormatTicks(time.Ticks);

    /// <summary>
    /// Runs <paramref name="completion"/> (typically <c>TaskCompletionSource.TrySet*</c> or <c>CancellationTokenSource.Cancel</c>)
    /// with no <see cref="SynchronizationContext"/> installed, restoring the current one afterwards.
    /// </summary>
    /// <remarks>
    /// The runtime inlines <c>ConfigureAwait(false)</c> continuations of a completed task only when the completing thread has
    /// no custom synchronization context; otherwise it queues them to the thread pool, which escapes the simulation (and is
    /// reported as a violation). Code that completes tasks awaited by SDK-style code while running on a simulated main lane
    /// (a node loop or the client context) should therefore complete them through this method. Continuations that captured
    /// a synchronization context are still posted to it.
    /// </remarks>
    public static void CompleteWithoutContext(Action completion)
    {
        ArgumentNullException.ThrowIfNull(completion);
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

    /// <summary>Creates an independent deterministic random stream derived from the seed and <paramref name="purpose"/>.</summary>
    public IRandomSource ForkRandom(string purpose)
    {
        ArgumentNullException.ThrowIfNull(purpose);
        return _root.Fork(purpose);
    }

    /// <summary>Creates a process (incarnation 1 for a new name, previous incarnation + 1 otherwise).</summary>
    /// <exception cref="InvalidOperationException">A live process with that name exists.</exception>
    public SimProcess CreateProcess(string name)
    {
        ValidateName(name);
        CheckThread("create process");
        lock (_lock)
        {
            if (_live.TryGetValue(name, out SimProcess? existing))
                throw new InvalidOperationException($"A live process named '{name}' already exists ({existing.Id}); kill it before creating a new incarnation.");
            return CreateProcessLocked(name);
        }
    }

    /// <summary>Returns the live process named <paramref name="name"/>, creating a new incarnation when there is none.</summary>
    public SimProcess GetOrCreateProcess(string name)
    {
        ValidateName(name);
        CheckThread("create process");
        lock (_lock)
            return _live.TryGetValue(name, out SimProcess? existing) ? existing : CreateProcessLocked(name);
    }

    /// <summary>Returns the live process named <paramref name="name"/>, or null.</summary>
    public SimProcess? FindProcess(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        lock (_lock)
            return _live.TryGetValue(name, out SimProcess? existing) ? existing : null;
    }

    /// <summary>
    /// Creates a per-node (or per-client) clock derived from global virtual time. With
    /// <see cref="SimClockOptions.BindToProcess"/> its timers belong to the live process of the same name, so they are held
    /// while that process is paused and die with it.
    /// </summary>
    /// <exception cref="InvalidOperationException">A clock with that name exists.</exception>
    public SimTimeProvider CreateClock(string name, SimClockOptions? options = null)
    {
        ValidateName(name);
        options ??= SimClockOptions.Default;
        options.Validate();
        CheckThread("create clock");
        lock (_lock)
        {
            if (_clocks.ContainsKey(name) || string.Equals(name, Clock.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"A simulated clock named '{name}' already exists.");
            long monotonic = checked(Options.StartTimestamp + _now + options.MonotonicOffset.Ticks);
            DateTimeOffset wall = Options.StartTimeUtc.AddTicks(_now).Add(options.WallOffset);
            var clock = new SimTimeProvider(this, name, isGlobal: false, options.BindToProcess ? name : null, wall, monotonic, options.Rate);
            _clocks[name] = clock;
            TraceLocked($"clock {name} created (wall offset {options.WallOffset}, rate {options.Rate.ToString("R", CultureInfo.InvariantCulture)})");
            return clock;
        }
    }

    /// <summary>Returns the clock named <paramref name="name"/>, creating it with default options when missing.</summary>
    public SimTimeProvider GetOrCreateClock(string name)
    {
        ValidateName(name);
        lock (_lock)
        {
            if (_clocks.TryGetValue(name, out SimTimeProvider? existing))
                return existing;
        }

        return CreateClock(name);
    }

    /// <summary>Returns the clock named <paramref name="name"/>, or null.</summary>
    public SimTimeProvider? FindClock(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        lock (_lock)
            return _clocks.TryGetValue(name, out SimTimeProvider? clock) ? clock : null;
    }

    /// <summary>Appends an event to <see cref="Trace"/> (prefixed with the virtual time).</summary>
    public void AddTrace(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        CheckThread("add trace");
        lock (_lock)
            TraceLocked(message);
    }

    /// <summary>Queues work on the global (harness) lane: it never pauses and never dies.</summary>
    public void Post(Action work, string? description = null)
    {
        ArgumentNullException.ThrowIfNull(work);
        Enqueue(_globalLane, new SimWorkItem(work, description), "post work");
    }

    /// <summary>
    /// Runs <paramref name="work"/> after <paramref name="delay"/> of global virtual time, on a fresh lane of
    /// <paramref name="owner"/> (or the harness when null). Dispose the result to cancel.
    /// </summary>
    public IDisposable Schedule(TimeSpan delay, Action work, SimProcess? owner = null, string? description = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(work);
        if (owner is not null && !ReferenceEquals(owner.Scheduler, this))
            throw new ArgumentException("The process belongs to another scheduler.", nameof(owner));
        CheckThread("schedule work");
        var entry = new SimActionEntry(owner, (owner?.Id ?? GlobalOwnerId) + "/scheduled", new SimWorkItem(work, description));
        lock (_lock)
            ScheduleLocked(entry, SaturatingAdd(_now, delay.Ticks));
        return new SimScheduledHandle(this, entry);
    }

    /// <summary>A delay on the global virtual clock.</summary>
    public Task DelayAsync(TimeSpan delay, CancellationToken ct = default) => Task.Delay(delay, Clock, ct);

    /// <summary>Records an uncontrolled dependency found by another detector (treated like a foreign-thread violation).</summary>
    public void ReportViolation(string description)
    {
        ArgumentException.ThrowIfNullOrEmpty(description);
        Thread thread = Thread.CurrentThread;
        AddViolation(new SimViolation(SimViolationKind.Reported, "report", description, thread.ManagedThreadId, thread.Name,
            thread.IsThreadPoolThread, Now, Environment.StackTrace));
    }

    /// <summary>
    /// Records an exception that simulated code caught but could not handle (for example a failing RPC handler),
    /// attributed to the currently executing process. Subject to <see cref="SimSchedulerOptions.ThrowOnUnhandledException"/>.
    /// </summary>
    public void ReportUnhandledException(Exception exception, string? where = null)
    {
        ArgumentNullException.ThrowIfNull(exception);
        SimLane? lane = CurrentLane;
        RecordUnhandled(lane?.Owner, where ?? lane?.Name ?? "global", exception, raiseLoopEvent: false);
    }

    /// <summary>Runs one work item, first advancing virtual time to the next due timer if nothing is runnable.</summary>
    /// <returns>False when the simulation is idle (nothing runnable, nothing pending).</returns>
    public bool RunOne()
    {
        EnterDriver();
        try
        {
            ThrowPendingFailure();
            while (true)
            {
                if (TryRunOne())
                {
                    ThrowPendingFailure();
                    return true;
                }

                if (TryAdvanceTime(long.MaxValue) == AdvanceResult.NoTimers)
                    return false;
            }
        }
        finally
        {
            ExitDriver();
        }
    }

    /// <summary>Runs until nothing is runnable and nothing is pending in virtual time.</summary>
    /// <returns>The number of work items executed.</returns>
    /// <exception cref="SimulationTimeoutException">Not idle within <paramref name="virtualTimeout"/> (for example periodic timers).</exception>
    public long RunUntilIdle(TimeSpan? virtualTimeout = null)
    {
        long before = StepCount;
        TimeSpan timeout = virtualTimeout ?? Options.DefaultRunTimeout;
        DriveResult result = Drive(null, DeadlineAfter(timeout), stopWhenIdle: true, waitForForeignWork: false);
        if (result == DriveResult.Deadline)
            throw new SimulationTimeoutException($"The simulation did not become idle within {timeout} of virtual time.{Environment.NewLine}{DescribeState()}", Now);
        return StepCount - before;
    }

    /// <summary>Runs until <paramref name="condition"/> (evaluated before every step) is true.</summary>
    /// <exception cref="SimulationTimeoutException">The virtual-time budget ran out.</exception>
    /// <exception cref="SimulationStalledException">Nothing can run any more and the condition is still false.</exception>
    public void RunUntil(Func<bool> condition, TimeSpan? virtualTimeout = null, string? because = null)
    {
        ArgumentNullException.ThrowIfNull(condition);
        TimeSpan timeout = virtualTimeout ?? Options.DefaultRunTimeout;
        DriveResult result = Drive(condition, DeadlineAfter(timeout), stopWhenIdle: false, waitForForeignWork: true);
        string what = because ?? "the run condition";
        switch (result)
        {
            case DriveResult.ConditionMet:
                return;
            case DriveResult.Deadline:
                throw new SimulationTimeoutException($"Timed out after {timeout} of virtual time waiting for {what}.{Environment.NewLine}{DescribeState()}", Now);
            default:
                throw new SimulationStalledException($"The simulation stalled waiting for {what}: nothing is runnable and no timer is pending.{Environment.NewLine}{DescribeState()}");
        }
    }

    /// <summary>Runs until <paramref name="task"/> completes (it is not awaited: its exception is not thrown here).</summary>
    public void RunUntil(Task task, TimeSpan? virtualTimeout = null, string? because = null)
    {
        ArgumentNullException.ThrowIfNull(task);
        RunUntil(() => task.IsCompleted, virtualTimeout, because ?? "a task to complete");
    }

    /// <summary>Runs everything due within <paramref name="duration"/> of virtual time, then sets the clock to its end.</summary>
    public void RunFor(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
        long deadline = DeadlineAfter(duration);
        DriveResult result = Drive(null, deadline, stopWhenIdle: true, waitForForeignWork: false);
        if (result == DriveResult.Idle)
        {
            lock (_lock)
            {
                if (_now < deadline)
                    SetNowLocked(deadline);
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="body"/> on the client process's synchronization context and drives the simulation until the
    /// task it returns completes. The returned task is already completed: it is the body's task (so awaiting it yields its
    /// result or exception), or a faulted task when the simulation failed first (timeout, stall, violation, unhandled
    /// exception).
    /// </summary>
    public Task RunAsync(Func<Task> body, TimeSpan? virtualTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(body);
        Task? task = null;
        try
        {
            ClientProcess.Post(() => task = StartBody(body), "RunAsync body");
            RunUntil(() => task is { IsCompleted: true }, virtualTimeout, "the RunAsync body to complete");
        }
        catch (Exception ex)
        {
            return Task.FromException(ex);
        }

        return task!;
    }

    /// <summary>As <see cref="RunAsync(Func{Task}, TimeSpan?)"/>, returning the body's result.</summary>
    public Task<T> RunAsync<T>(Func<Task<T>> body, TimeSpan? virtualTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(body);
        Task<T>? task = null;
        try
        {
            ClientProcess.Post(() => task = StartBody(body), "RunAsync body");
            RunUntil(() => task is { IsCompleted: true }, virtualTimeout, "the RunAsync body to complete");
        }
        catch (Exception ex)
        {
            return Task.FromException<T>(ex);
        }

        return task!;
    }

    /// <summary>Synchronous form of <see cref="RunAsync(Func{Task}, TimeSpan?)"/>: throws the body's (or the simulation's) exception.</summary>
    public void Run(Func<Task> body, TimeSpan? virtualTimeout = null) =>
        RunAsync(body, virtualTimeout).GetAwaiter().GetResult();

    /// <summary>Synchronous form of <see cref="RunAsync{T}(Func{Task{T}}, TimeSpan?)"/>.</summary>
    public T Run<T>(Func<Task<T>> body, TimeSpan? virtualTimeout = null) =>
        RunAsync(body, virtualTimeout).GetAwaiter().GetResult();

    /// <summary>Multi-line description of the scheduler state for failure messages.</summary>
    public string DescribeState()
    {
        var sb = new StringBuilder();
        lock (_lock)
        {
            sb.Append(CultureInfo.InvariantCulture, $"Simulation state at t={FormatTicks(_now)} (seed {Seed}, steps {_steps}):");
            sb.AppendLine();
            sb.Append(CultureInfo.InvariantCulture, $"  runnable lanes: {_runnable.Count}");
            int shown = 0;
            foreach (SimLane lane in _runnable)
            {
                if (shown++ == 16)
                {
                    sb.Append(" ...");
                    break;
                }

                sb.Append(CultureInfo.InvariantCulture, $" [{lane.Name}: {lane.Items.Count}]");
            }

            sb.AppendLine();
            PurgeCancelledHeadsLocked();
            sb.Append(CultureInfo.InvariantCulture, $"  pending timed entries: {_timed.Count}");
            if (_timed.TryPeek(out SimTimedEntry? next, out _))
                sb.Append(CultureInfo.InvariantCulture, $" (next due at t={FormatTicks(next.Due)})");
            sb.AppendLine();
            sb.Append("  processes:");
            foreach (SimProcess process in _processes)
            {
                int queued = CountQueuedLocked(process);
                sb.Append(CultureInfo.InvariantCulture, $" [{process.Id} {process.StateLocked}{(queued > 0 ? $" queued={queued}" : string.Empty)}]");
            }

            sb.AppendLine();
            sb.Append(CultureInfo.InvariantCulture, $"  violations: {_violations.Count}, unhandled exceptions: {_errors.Count}");
            if (_violations.Count > 0)
            {
                sb.AppendLine();
                sb.Append(CultureInfo.InvariantCulture, $"  first violation: {_violations[0]}");
            }
        }

        return sb.ToString();
    }

    /// <inheritdoc />
    public override string ToString() => $"SimScheduler(seed {Seed}, t={FormatTime(Now)})";

    internal static string FormatTicks(long ticks)
    {
        long seconds = Math.DivRem(ticks, TimeSpan.TicksPerSecond, out long fraction);
        return string.Create(CultureInfo.InvariantCulture, $"{seconds}.{Math.Abs(fraction):D7}s");
    }

    internal int NextNetworkIndex() => Interlocked.Increment(ref _networkCount);

    internal SimLane CreateLane(SimProcess? owner, string name, SynchronizationContext? context, bool transient)
    {
        lock (_lock)
            return new SimLane(++_laneIds, name, owner, context, transient);
    }

    /// <summary>Queues an item on a lane (from simulated or harness code).</summary>
    internal void Enqueue(SimLane lane, SimWorkItem item, string operation)
    {
        bool foreign = CheckThread(operation);
        EnqueueCore(lane, item);
        if (foreign)
            _foreignWork.Set();
    }

    /// <summary>Queues an item from virtual-time processing (driving thread).</summary>
    internal void EnqueueFromTimer(SimLane lane, SimWorkItem item) => EnqueueCore(lane, item);

    /// <summary>Schedules a work item onto an existing lane at an absolute virtual time (FIFO network deliveries).</summary>
    internal void ScheduleOnLane(SimLane lane, long dueTicks, SimWorkItem item, string operation)
    {
        bool foreign = CheckThread(operation);
        lock (_lock)
            ScheduleLocked(new SimLaneEntry(lane, item), dueTicks);
        if (foreign)
            _foreignWork.Set();
    }

    /// <summary>Schedules a work item onto a fresh transient lane of <paramref name="owner"/> at an absolute virtual time.</summary>
    internal void ScheduleOnNewLane(SimProcess? owner, string laneName, long dueTicks, SimWorkItem item, string operation)
    {
        bool foreign = CheckThread(operation);
        lock (_lock)
            ScheduleLocked(new SimActionEntry(owner, laneName, item), dueTicks);
        if (foreign)
            _foreignWork.Set();
    }

    /// <summary>Adds an entry to the virtual-time queue. Caller holds the lock.</summary>
    internal void ScheduleLocked(SimTimedEntry entry, long dueTicks)
    {
        if (dueTicks < _now)
            dueTicks = _now;
        entry.Due = dueTicks;
        entry.Seq = ++_seq;
        entry.Cancelled = false;
        _timed.Enqueue(entry, (dueTicks, entry.Seq));
    }

    internal void Cancel(SimTimedEntry entry)
    {
        lock (_lock)
            entry.Cancelled = true;
    }

    internal void TraceLocked(string message)
    {
        if (_traceTruncated)
            return;
        if (_trace.Count >= Options.MaxTraceEntries)
        {
            _traceTruncated = true;
            return;
        }

        _trace.Add(string.Concat("[", FormatTicks(_now), "] ", message));
    }

    /// <summary>
    /// Verifies that the caller is allowed to touch the simulation; records a violation otherwise. Returns true when the
    /// caller is a foreign thread.
    /// </summary>
    internal bool CheckThread(string operation)
    {
        int id = Environment.CurrentManagedThreadId;
        int driver = Volatile.Read(ref _driverThreadId);
        if (driver != 0)
        {
            if (id == driver)
                return false;
            RecordForeign(operation, $"the simulation is being driven by thread {driver}");
            return true;
        }

        if (!Volatile.Read(ref _hasRun) || !Options.StrictThreadAffinity)
            return false;
        int home = Volatile.Read(ref _homeThreadId);
        if (id == home)
            return false;
        RecordForeign(operation, $"the simulation belongs to thread {home}, which last drove it");
        return true;
    }

    internal void PauseProcess(SimProcess process)
    {
        CheckThread("pause process");
        lock (_lock)
        {
            if (process.StateLocked != SimProcessState.Running)
                return;
            process.StateLocked = SimProcessState.Paused;
            for (int i = 0; i < _runnable.Count;)
            {
                SimLane lane = _runnable[i];
                if (ReferenceEquals(lane.Owner, process))
                {
                    RemoveRunnableLocked(lane); // swaps the last lane into slot i: examine i again
                    lane.Held = true;
                    process.HeldLanes.Add(lane);
                }
                else
                {
                    i++;
                }
            }

            TraceLocked($"process {process.Id} paused");
        }
    }

    internal void ResumeProcess(SimProcess process)
    {
        CheckThread("resume process");
        (SimTimeProvider.SimTimer Timer, long Version)[]? parked = null;
        lock (_lock)
        {
            if (process.StateLocked != SimProcessState.Paused)
                return;
            process.StateLocked = SimProcessState.Running;
            foreach (SimLane lane in process.HeldLanes)
            {
                lane.Held = false;
                if (lane.Items.Count > 0 && lane.RunnableIndex < 0)
                    AddRunnableLocked(lane);
            }

            process.HeldLanes.Clear();
            if (process.ParkedTimers.Count > 0)
            {
                parked = [.. process.ParkedTimers];
                process.ParkedTimers.Clear();
            }

            TraceLocked($"process {process.Id} resumed");
        }

        // Timers that came due while the process was frozen fire once now (missed periods are not replayed).
        if (parked is not null)
        {
            foreach ((SimTimeProvider.SimTimer timer, long version) in parked)
                timer.Clock.FireParked(timer, version);
        }
    }

    internal void KillProcess(SimProcess process, string reason)
    {
        CheckThread("kill process");
        List<Action>? hooks = null;
        lock (_lock)
        {
            if (process.StateLocked == SimProcessState.Killed)
                return;
            process.StateLocked = SimProcessState.Killed;
            for (int i = 0; i < _runnable.Count;)
            {
                SimLane lane = _runnable[i];
                if (ReferenceEquals(lane.Owner, process))
                {
                    RemoveRunnableLocked(lane);
                    DiscardLaneLocked(process, lane, ref hooks);
                }
                else
                {
                    i++;
                }
            }

            foreach (SimLane lane in process.HeldLanes)
            {
                lane.Held = false;
                DiscardLaneLocked(process, lane, ref hooks);
            }

            process.HeldLanes.Clear();
            process.ParkedTimers.Clear();

            // Its timers die with it (they would be dropped when due anyway; disarming keeps the clocks accurate).
            Clock.DisarmOwnedByLocked(process);
            foreach (SimTimeProvider clock in _clocks.Values)
                clock.DisarmOwnedByLocked(process);

            if (_live.TryGetValue(process.Name, out SimProcess? current) && ReferenceEquals(current, process))
                _live.Remove(process.Name);
            TraceLocked($"process {process.Id} {reason}");
        }

        if (hooks is not null)
        {
            foreach (Action hook in hooks)
                RunHook(hook);
        }

        Action<SimProcess>? killed = ProcessKilled;
        if (killed is not null)
        {
            foreach (Delegate handler in killed.GetInvocationList())
            {
                try
                {
                    ((Action<SimProcess>)handler)(process);
                }
                catch (Exception ex)
                {
                    RecordUnhandled(null, "ProcessKilled handler", ex, raiseLoopEvent: false);
                }
            }
        }

        process.OnKilled();
    }

    /// <summary>Kills a draining process whose main lane became empty.</summary>
    internal void CompleteDrainIfEmpty(SimProcess process)
    {
        lock (_lock)
        {
            if (process.StateLocked == SimProcessState.Killed || process.MainLane.Items.Count > 0)
                return;
        }

        KillProcess(process, "stopped (drained)");
    }

    internal int CountQueuedLocked(SimProcess process)
    {
        int count = 0;
        foreach (SimLane lane in _runnable)
        {
            if (ReferenceEquals(lane.Owner, process))
                count += lane.Items.Count;
        }

        foreach (SimLane lane in process.HeldLanes)
            count += lane.Items.Count;
        return count;
    }

    /// <summary>Runs <paramref name="callback"/> synchronously "on" <paramref name="lane"/> (SynchronizationContext.Send).</summary>
    internal void SendInline(SimLane lane, SendOrPostCallback callback, object? state)
    {
        if (CheckThread("send to synchronization context"))
        {
            SimViolation? first;
            lock (_lock)
                first = _violations.Count > 0 ? _violations[^1] : null;
            throw first is null
                ? new InvalidOperationException("SynchronizationContext.Send from a foreign thread is not supported by the simulation.")
                : new SimulationViolationException(first);
        }

        SimProcess? owner = lane.Owner;
        if (owner is not null)
        {
            SimProcessState state1 = owner.State;
            if (state1 == SimProcessState.Killed)
                throw new InvalidOperationException($"Cannot Send to the synchronization context of killed process {owner.Id}.");
            if (state1 == SimProcessState.Paused)
                throw new InvalidOperationException($"Cannot Send to the synchronization context of paused process {owner.Id}: the single simulation thread would deadlock.");
        }

        bool onDriver = IsOnSimulationThread;
        if (onDriver && ReferenceEquals(_currentLane, lane))
        {
            callback(state);
            return;
        }

        SimLane? previousLane = onDriver ? _currentLane : null;
        SynchronizationContext? previousContext = SynchronizationContext.Current;
        if (onDriver)
            _currentLane = lane;
        SynchronizationContext.SetSynchronizationContext(lane.Context);
        try
        {
            callback(state);
        }
        finally
        {
            if (onDriver)
                _currentLane = previousLane;
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }
    }

    private static Task StartBody(Func<Task> body)
    {
        try
        {
            return body() ?? Task.FromException(new InvalidOperationException("The simulation body returned a null task."));
        }
        catch (Exception ex)
        {
            return Task.FromException(ex);
        }
    }

    private static Task<T> StartBody<T>(Func<Task<T>> body)
    {
        try
        {
            return body() ?? Task.FromException<T>(new InvalidOperationException("The simulation body returned a null task."));
        }
        catch (Exception ex)
        {
            return Task.FromException<T>(ex);
        }
    }

    private static void ValidateName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        foreach (char c in name)
        {
            if (char.IsControl(c))
                throw new ArgumentException("Names must not contain control characters.", nameof(name));
        }
    }

    private static long SaturatingAdd(long a, long b)
    {
        long r = unchecked(a + b);
        if (b > 0 && r < a)
            return long.MaxValue;
        if (b < 0 && r > a)
            return long.MinValue;
        return r;
    }

    private static ulong Mix(ulong hash, ulong value) =>
        BitOperations.RotateLeft((hash ^ value) * 0x9E3779B97F4A7C15UL, 27) ^ (value >> 17);

    private static void RunHook(Action hook)
    {
        try
        {
            hook();
        }
        catch
        {
            // Discard hooks only complete internal completion sources; a failure must not break the kill.
        }
    }

    private SimProcess CreateProcessLocked(string name)
    {
        int incarnation = _incarnations.TryGetValue(name, out int prior) ? prior + 1 : 1;
        _incarnations[name] = incarnation;
        var process = new SimProcess(this, name, incarnation);
        _live[name] = process;
        _processes.Add(process);
        TraceLocked($"process {process.Id} created");
        return process;
    }

    private long DeadlineAfter(TimeSpan timeout)
    {
        lock (_lock)
            return SaturatingAdd(_now, timeout.Ticks);
    }

    private void SetNowLocked(long ticks)
    {
        if (ticks != _now)
        {
            _now = ticks;
            _stepsAtInstant = 0;
        }
    }

    private void EnqueueCore(SimLane lane, SimWorkItem item)
    {
        Action? discard = null;
        lock (_lock)
        {
            SimProcess? owner = lane.Owner;
            if (owner is not null && owner.StateLocked == SimProcessState.Killed)
            {
                owner.DiscardedLocked++;
                discard = item.OnDiscard;
            }
            else
            {
                item.Seq = ++_seq;
                lane.Items.Enqueue(item);
                if (lane.RunnableIndex < 0 && !lane.Held)
                {
                    if (owner is not null && owner.StateLocked == SimProcessState.Paused)
                    {
                        lane.Held = true;
                        owner.HeldLanes.Add(lane);
                    }
                    else
                    {
                        AddRunnableLocked(lane);
                    }
                }
            }
        }

        if (discard is not null)
            RunHook(discard);
    }

    private void AddRunnableLocked(SimLane lane)
    {
        lane.RunnableIndex = _runnable.Count;
        _runnable.Add(lane);
    }

    private void RemoveRunnableLocked(SimLane lane)
    {
        int index = lane.RunnableIndex;
        int last = _runnable.Count - 1;
        if (index != last)
        {
            SimLane moved = _runnable[last];
            _runnable[index] = moved;
            moved.RunnableIndex = index;
        }

        _runnable.RemoveAt(last);
        lane.RunnableIndex = -1;
    }

    private void DiscardLaneLocked(SimProcess process, SimLane lane, ref List<Action>? hooks)
    {
        while (lane.Items.Count > 0)
        {
            SimWorkItem item = lane.Items.Dequeue();
            process.DiscardedLocked++;
            if (item.OnDiscard is not null)
                (hooks ??= []).Add(item.OnDiscard);
        }
    }

    private int OldestRunnableIndexLocked()
    {
        int best = 0;
        long bestSeq = long.MaxValue;
        for (int i = 0; i < _runnable.Count; i++)
        {
            long seq = _runnable[i].Items.Peek().Seq;
            if (seq < bestSeq)
            {
                bestSeq = seq;
                best = i;
            }
        }

        return best;
    }

    private bool TryRunOne()
    {
        SimLane lane;
        SimWorkItem item;
        lock (_lock)
        {
            int count = _runnable.Count;
            if (count == 0)
                return false;
            int index = Options.Mode == SimScheduleMode.Random
                ? count == 1 ? 0 : _choices.Next(count)
                : OldestRunnableIndexLocked();
            lane = _runnable[index];
            item = lane.Items.Dequeue();
            if (lane.Items.Count == 0)
                RemoveRunnableLocked(lane);
            _steps++;
            _stepsAtInstant++;
            _fingerprint = Mix(Mix(Mix(_fingerprint, (ulong)item.Seq), (ulong)lane.Id), (ulong)_now);
            if (Options.RecordSteps)
                TraceLocked(item.Description is null ? $"step #{item.Seq} {lane.Name}" : $"step #{item.Seq} {lane.Name}: {item.Description}");
        }

        Execute(lane, item);

        SimProcess? owner = lane.Owner;
        if (owner?.Loop is { DrainRequested: true } && ReferenceEquals(lane, owner.MainLane))
            CompleteDrainIfEmpty(owner);

        if (_stepsAtInstant > Options.MaxStepsPerInstant)
        {
            throw new SimulationException(
                $"Livelock: more than {Options.MaxStepsPerInstant} work items ran at t={FormatTicks(_now)} without virtual time advancing.{Environment.NewLine}{DescribeState()}");
        }

        return true;
    }

    private void Execute(SimLane lane, in SimWorkItem item)
    {
        SynchronizationContext? previous = SynchronizationContext.Current;
        _currentLane = lane;
        SynchronizationContext.SetSynchronizationContext(lane.Context);
        try
        {
            item.Invoke();
        }
        catch (Exception ex)
        {
            OnUnhandled(lane, ex);
        }
        finally
        {
            _currentLane = null;
            SynchronizationContext.SetSynchronizationContext(previous);
            if (_driverExecutionContext is { } baseContext)
                ExecutionContext.Restore(baseContext);
        }
    }

    private void OnUnhandled(SimLane lane, Exception exception)
    {
        SimProcess? owner = lane.Owner;
        if (owner is not null && owner.State == SimProcessState.Killed)
        {
            // Zombie code still unwinding after its process was killed (for example SimProcessKilledException).
            lock (_lock)
                TraceLocked($"{owner.Id}: {exception.GetType().Name} after kill ignored");
            return;
        }

        RecordUnhandled(owner, lane.Name, exception, raiseLoopEvent: owner is not null && ReferenceEquals(lane, owner.MainLane));
    }

    private void RecordUnhandled(SimProcess? owner, string where, Exception exception, bool raiseLoopEvent)
    {
        SimUnhandledError error;
        lock (_lock)
        {
            error = new SimUnhandledError(owner?.Id ?? GlobalOwnerId, where, TimeSpan.FromTicks(_now), exception);
            _errors.Add(error);
            TraceLocked($"UNHANDLED {error}");
            if (Options.ThrowOnUnhandledException)
                _pendingFailure ??= new SimulationException($"Unhandled exception in simulated work: {error}", exception);
        }

        if (raiseLoopEvent)
            owner!.Loop?.RaiseUnhandled(exception);

        Action<SimUnhandledError>? handlers = UnhandledException;
        if (handlers is null)
            return;
        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<SimUnhandledError>)handler)(error);
            }
            catch
            {
                // Reporting must not fail the reporter.
            }
        }
    }

    private void RecordForeign(string operation, string detail)
    {
        Thread thread = Thread.CurrentThread;
        AddViolation(new SimViolation(
            SimViolationKind.ForeignThread,
            operation,
            $"{operation} was attempted from a thread outside the simulation ({detail}); the outcome now depends on real scheduling (real timer, thread pool, I/O or an escaped continuation).",
            thread.ManagedThreadId,
            thread.Name,
            thread.IsThreadPoolThread,
            Now,
            Environment.StackTrace));
        _foreignWork.Set();
    }

    private void AddViolation(SimViolation violation)
    {
        lock (_lock)
        {
            _violations.Add(violation);
            TraceLocked($"VIOLATION {violation}");
            if (Options.ThrowOnViolation)
                _pendingFailure ??= new SimulationViolationException(violation);
        }
    }

    private void ThrowPendingFailure()
    {
        Exception? failure;
        lock (_lock)
        {
            failure = _pendingFailure;
            _pendingFailure = null;
        }

        if (failure is not null)
            throw failure;
    }

    private void EnterDriver()
    {
        int id = Environment.CurrentManagedThreadId;
        lock (_lock)
        {
            if (_driverThreadId == id)
                throw new InvalidOperationException("The simulation is already being driven on this thread: Run* methods cannot be called from simulated work or from a run condition.");
            if (_driverThreadId != 0)
            {
                RecordForeign("drive the simulation", $"thread {_driverThreadId} is already driving it");
                throw new InvalidOperationException($"The simulation is already being driven by thread {_driverThreadId}.");
            }

            _driverThreadId = id;
            _homeThreadId = id;
            _hasRun = true;
        }

        _driverExecutionContext = ExecutionContext.Capture();
    }

    private void ExitDriver()
    {
        _driverExecutionContext = null;
        _currentLane = null;
        lock (_lock)
            _driverThreadId = 0;
    }

    private void PurgeCancelledHeadsLocked()
    {
        while (_timed.TryPeek(out SimTimedEntry? head, out _) && head.Cancelled)
            _timed.Dequeue();
    }

    private AdvanceResult TryAdvanceTime(long deadline)
    {
        List<SimTimedEntry> due = [];
        lock (_lock)
        {
            PurgeCancelledHeadsLocked();
            if (!_timed.TryPeek(out SimTimedEntry? head, out _))
                return AdvanceResult.NoTimers;
            long dueTicks = head.Due;
            if (dueTicks > deadline)
            {
                if (deadline > _now)
                    SetNowLocked(deadline);
                return AdvanceResult.PastDeadline;
            }

            if (dueTicks > _now)
                SetNowLocked(dueTicks);
            while (_timed.TryPeek(out SimTimedEntry? entry, out _) && entry.Due == dueTicks)
            {
                _timed.Dequeue();
                if (!entry.Cancelled)
                    due.Add(entry);
            }
        }

        foreach (SimTimedEntry entry in due)
            entry.OnDue(this);
        return AdvanceResult.Advanced;
    }

    private DriveResult Drive(Func<bool>? condition, long deadline, bool stopWhenIdle, bool waitForForeignWork)
    {
        EnterDriver();
        try
        {
            long realStart = Stopwatch.GetTimestamp();
            TimeSpan? realBudget = Options.MaxRealTimePerRun;
            long budgetCheck = 0;
            while (true)
            {
                ThrowPendingFailure();
                if (condition is not null && condition())
                    return DriveResult.ConditionMet;

                if (TryRunOne())
                {
                    if (realBudget is { } budget && (++budgetCheck & 1023) == 0 && Stopwatch.GetElapsedTime(realStart) > budget)
                    {
                        throw new SimulationTimeoutException(
                            $"The run exceeded its real-time budget of {budget}.{Environment.NewLine}{DescribeState()}", Now);
                    }

                    continue;
                }

                AdvanceResult advance = TryAdvanceTime(deadline);
                if (advance == AdvanceResult.Advanced)
                    continue;
                if (advance == AdvanceResult.PastDeadline)
                {
                    ThrowPendingFailure();
                    return DriveResult.Deadline;
                }

                // Nothing runnable and nothing pending in virtual time.
                if (!stopWhenIdle && waitForForeignWork && WaitForForeignWork())
                    continue;
                ThrowPendingFailure();
                if (condition is not null && condition())
                    return DriveResult.ConditionMet;
                return DriveResult.Idle;
            }
        }
        finally
        {
            ExitDriver();
        }
    }

    /// <summary>
    /// Waits up to <see cref="SimSchedulerOptions.StallGracePeriod"/> of real time for foreign threads to post work, so a
    /// stall caused by an uncontrolled dependency is reported as a violation. Returns true when work (or a failure) appeared.
    /// </summary>
    private bool WaitForForeignWork()
    {
        TimeSpan grace = Options.StallGracePeriod;
        if (grace <= TimeSpan.Zero)
            return false;
        long start = Stopwatch.GetTimestamp();
        while (true)
        {
            lock (_lock)
            {
                PurgeCancelledHeadsLocked();
                if (_runnable.Count > 0 || _timed.Count > 0 || _pendingFailure is not null)
                    return true;
                _foreignWork.Reset();
            }

            TimeSpan remaining = grace - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero)
                return false;
            _foreignWork.Wait(remaining);
        }
    }

    private enum AdvanceResult
    {
        Advanced,
        PastDeadline,
        NoTimers,
    }

    private enum DriveResult
    {
        ConditionMet,
        Idle,
        Deadline,
    }
}
