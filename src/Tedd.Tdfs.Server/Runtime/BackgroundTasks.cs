namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Periodic jobs on the node loop (INTERNALS.md §1). Each job runs, then waits its interval on the node's
/// <see cref="TimeProvider"/>; exceptions are logged and never stop the job. Jobs end when the node stops.
/// </summary>
internal sealed class BackgroundTasks
{
    private readonly NodeServices _services;
    private readonly List<Task> _jobs = [];
    private bool _stopped;

    public BackgroundTasks(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <summary>True once <see cref="Stop"/> was called.</summary>
    public bool IsStopped => _stopped || _services.IsStopping;

    /// <summary>Starts a periodic job (first run after <paramref name="initialDelay"/>, default one interval).</summary>
    public void StartPeriodic(string name, TimeSpan interval, Func<Task> job, TimeSpan? initialDelay = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(job);
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval));
        _jobs.Add(RunPeriodicAsync(name, interval, job, initialDelay ?? interval));
    }

    /// <summary>Runs a one-shot background operation and logs its failure.</summary>
    public void Run(string name, Func<Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        _jobs.Add(RunOnceAsync(name, work));
        if (_jobs.Count > 64)
            _jobs.RemoveAll(t => t.IsCompleted);
    }

    /// <summary>Waits <paramref name="delay"/> on the node clock.</summary>
    public Task DelayAsync(TimeSpan delay) => delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, _services.Time);

    /// <summary>Stops every job after its current run.</summary>
    public void Stop() => _stopped = true;

    private async Task RunPeriodicAsync(string name, TimeSpan interval, Func<Task> job, TimeSpan initialDelay)
    {
        await DelayAsync(initialDelay);
        while (!IsStopped)
        {
            try
            {
                await job();
            }
            catch (Exception ex) when (!IsStopped)
            {
                _services.Warn("jobs", NodeServices.Inv($"Background job '{name}' failed: {ex.GetType().Name}: {ex.Message}"));
            }
            catch (Exception)
            {
                return;
            }

            if (IsStopped)
                return;
            await DelayAsync(interval);
        }
    }

    private async Task RunOnceAsync(string name, Func<Task> work)
    {
        try
        {
            await work();
        }
        catch (Exception ex) when (!IsStopped)
        {
            _services.Warn("jobs", NodeServices.Inv($"Background operation '{name}' failed: {ex.GetType().Name}: {ex.Message}"));
        }
        catch (Exception)
        {
            // Stopping: failures of abandoned work are expected.
        }
    }
}
