using Tedd.Tdfs.Client.Internal.Runtime;

namespace Tedd.Tdfs.Client.Internal.Facades;

/// <summary>
/// Drives the application adapters of a snapshot (FSD §11.3, SNP-APP): sequential prepare (each bounded by the adapter
/// timeout; a hung adapter is abandoned, never awaited forever), abort of the failed adapter and resume of the prepared
/// ones when the snapshot cannot be taken, and resume of every prepared adapter after the capture (failures reported,
/// never undoing the snapshot).
/// </summary>
internal sealed class SnapshotAdapters
{
    private readonly ClientRuntime _runtime;
    private readonly DfsSnapshotAdapterContext _context;
    private readonly IReadOnlyList<IDfsSnapshotAdapter> _adapters;
    private readonly TimeSpan _timeout;
    private readonly List<IDfsSnapshotAdapter> _prepared = [];

    public SnapshotAdapters(ClientRuntime runtime, string volume, string snapshot, IReadOnlyList<IDfsSnapshotAdapter> adapters, TimeSpan timeout)
    {
        _runtime = runtime;
        _context = new DfsSnapshotAdapterContext(volume, snapshot);
        _adapters = adapters;
        _timeout = timeout;
    }

    /// <summary>Adapters prepared so far.</summary>
    public int PreparedCount => _prepared.Count;

    /// <summary>Prepares every adapter in order; on a failure the snapshot is abandoned and <see cref="DfsSnapshotAdapterException"/> thrown.</summary>
    public async Task PrepareAsync(OperationScope scope)
    {
        foreach (IDfsSnapshotAdapter adapter in _adapters)
        {
            try
            {
                await RunAsync(adapter, "prepare", (a, c, t) => a.PrepareAsync(c, t), scope.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // The failed adapter may have half-prepared: abort it; every adapter prepared before it resumes.
                await RunQuietlyAsync(adapter, "abort", (a, c, t) => a.AbortAsync(c, t)).ConfigureAwait(false);
                await ResumeAsync().ConfigureAwait(false);
                if (scope.IsAbortedByCaller)
                    throw scope.NotTransmittedFailure(ex);
                throw new DfsSnapshotAdapterException(adapter.Id, "prepare",
                    $"Snapshot adapter '{adapter.Id}' failed to prepare; no snapshot was taken.", ex);
            }

            _prepared.Add(adapter);
        }
    }

    /// <summary>The capture failed: abort then resume every prepared adapter (bounded, best effort).</summary>
    public async Task AbortAndResumeAsync()
    {
        foreach (IDfsSnapshotAdapter adapter in _prepared)
            await RunQuietlyAsync(adapter, "abort", (a, c, t) => a.AbortAsync(c, t)).ConfigureAwait(false);
        await ResumeAsync().ConfigureAwait(false);
    }

    /// <summary>Resumes every prepared adapter (in parallel, each bounded); returns the first failure, if any.</summary>
    public async Task<Exception?> ResumeAsync()
    {
        if (_prepared.Count == 0)
            return null;
        IDfsSnapshotAdapter[] prepared = [.. _prepared];
        _prepared.Clear();
        var resumes = new Task<Exception?>[prepared.Length];
        for (int i = 0; i < prepared.Length; i++)
            resumes[i] = ResumeOneAsync(prepared[i]);
        Exception?[] results = await Task.WhenAll(resumes).ConfigureAwait(false);
        return results.FirstOrDefault(r => r is not null);
    }

    private async Task<Exception?> ResumeOneAsync(IDfsSnapshotAdapter adapter)
    {
        try
        {
            await RunAsync(adapter, "resume", (a, c, t) => a.ResumeAsync(c, t), CancellationToken.None).ConfigureAwait(false);
            return null;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _runtime.Emit("snapshot.adapter-resume-failed", $"Snapshot adapter '{adapter.Id}' failed to resume ({ex.GetType().Name}).", volume: _context.Volume,
                exception: ex);
            return new DfsSnapshotAdapterException(adapter.Id, "resume", $"Snapshot adapter '{adapter.Id}' failed to resume after the snapshot.", ex);
        }
    }

    private async Task RunQuietlyAsync(IDfsSnapshotAdapter adapter, string phase, Func<IDfsSnapshotAdapter, DfsSnapshotAdapterContext, CancellationToken, Task> call)
    {
        try
        {
            await RunAsync(adapter, phase, call, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _runtime.Emit("snapshot.adapter-failed", $"Snapshot adapter '{adapter.Id}' failed to {phase} ({ex.GetType().Name}).", volume: _context.Volume,
                exception: ex);
        }
    }

    /// <summary>Runs one adapter call bounded by the adapter timeout (on the injected clock); a hung call is cancelled and abandoned.</summary>
    private async Task RunAsync(IDfsSnapshotAdapter adapter, string phase, Func<IDfsSnapshotAdapter, DfsSnapshotAdapterContext, CancellationToken, Task> call,
        CancellationToken ct)
    {
        using var cancel = new CancellationTokenSource(_timeout, _runtime.Time);
        using CancellationTokenRegistration link = ct.CanBeCanceled
            ? ct.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), cancel)
            : default;
        Task task;
        try
        {
            task = call(adapter, _context, cancel.Token) ?? Task.CompletedTask;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            task = Task.FromException(ex);
        }

        bool completed = await Waits.CompletesWithinAsync(task, _timeout, _runtime.Time, ct).ConfigureAwait(false);
        if (!completed)
        {
            ContextFree.Cancel(cancel);
            Waits.ObserveQuietly(task);
            throw new TimeoutException($"Snapshot adapter '{adapter.Id}' did not {phase} within {_timeout}.");
        }

        await task.ConfigureAwait(false);
    }
}
