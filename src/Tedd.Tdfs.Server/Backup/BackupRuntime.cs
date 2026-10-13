using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Loop-side composition of a backup node: key material (the node starts locked when its secret is missing and refuses to
/// present snapshot contents until a recovery key unlocks it), the object store, the durable catalog (interrupted
/// publications are cleaned up, staged transfers resume), the transport, background jobs (retention, reader expiry),
/// graceful stop and crash. A backup node never joins a Raft group and never opens connections to the source.
/// </summary>
internal sealed class BackupRuntime
{
    /// <summary>Space the object store leaves free for the catalog (pointers, manifests, staged transfer state).</summary>
    private const long ReservedBytes = 4L << 20;

    private readonly CancellationTokenSource _jobs = new();
    private IRpcTransport? _transport;
    private int _killed;

    private BackupRuntime(BackupServices services)
    {
        Services = services;
        Dispatcher = new BackupDispatcher(services);
    }

    /// <summary>The node's services.</summary>
    public BackupServices Services { get; }

    /// <summary>Inbound request dispatch.</summary>
    public BackupDispatcher Dispatcher { get; }

    /// <summary>Starts a backup node; must run on <paramref name="loop"/>.</summary>
    public static async Task<BackupRuntime> StartAsync(BackupNodeOptions options, NodeEnvironment environment, INodeLoop loop, CancellationToken ct)
    {
        Validate(options);
        var runtime = new BackupRuntime(new BackupServices(options, environment, loop));
        try
        {
            await runtime.StartCoreAsync(ct);
            return runtime;
        }
        catch
        {
            await runtime.AbortStartAsync();
            throw;
        }
    }

    private async Task StartCoreAsync(CancellationToken ct)
    {
        BackupServices s = Services;
        BackupNodeOptions options = s.Options;
        await s.Medium.CreateDirectoryDurableAsync(options.DataDirectory, ct);
        s.Keys = await BackupKeys.LoadOrCreateAsync(s.Medium, options.DataDirectory, options.BackupSecret, s.Random, ct);
        s.Disposables.Add(s.Keys);
        s.Principals = new BackupPrincipals(options, s.Keys.DeriveFakeSaltKey());
        s.Catalog = new BackupCatalog(s);
        s.Receiver = new BackupReceiver(s);
        s.Reader = new BackupReader(s);
        if (s.Keys.IsUnlocked)
            await OpenStoreAsync(s, ct);
        else
            s.Warn("backup", BackupServices.Inv($"Backup node {s.Name} started without key material: snapshot contents stay unavailable until a recovery key is supplied."));
        await s.Catalog.LoadAsync(ct);

        _transport = s.Env.TransportFactory.Create(options.Name, options.ListenEndpoint);
        await _transport.StartAsync(Dispatcher, ct);
        s.Endpoint = _transport.LocalEndpoint;
        s.IsServing = true;
        s.Info("backup", BackupServices.Inv($"Backup node {s.Name} ({s.Keys.NodeId}) serving at {s.Endpoint} for cluster {options.SourceClusterId}."));
        StartJobs();
    }

    /// <summary>Opens the object store (needs the data key: records are encrypted at rest).</summary>
    private static async Task OpenStoreAsync(BackupServices s, CancellationToken ct)
    {
        RecordCipher cipher = s.Keys.ObjectCipher ?? throw new DfsRecoveryKeyMissingException("The backup node's key material is not available.");
        long reserved = Math.Min(ReservedBytes, Math.Max(0, s.Medium.CapacityBytes / 64));
        s.Store = await ObjectStore.OpenAsync(s.Medium, s.DataPath(BackupServices.ObjectsDirectory),
            new ObjectStoreOptions(ReservedBytes: reserved, MaxObjectSize: s.Timings.MaxObjectSize, Cipher: cipher)
            {
                Mutations = s.Mutations,
                NodeName = s.Name,
            }, s.Faults, ct);
    }

    /// <summary>
    /// Completes start-up work that needs the data key after a recovery key unlocked a node started without its secret:
    /// opens the object store and decrypts the retained manifests. Concurrent callers share one run.
    /// </summary>
    public static Task OnUnlockedAsync(BackupServices s, CancellationToken ct)
    {
        if (s.UnlockCompletion is { IsFaulted: false, IsCanceled: false } running)
            return running;
        s.UnlockCompletion = RunAsync();
        return s.UnlockCompletion;

        async Task RunAsync()
        {
            if (s.Store is null)
                await OpenStoreAsync(s, ct);
            await s.Catalog.LoadManifestsAsync(ct);
            s.Catalog.Touch();
            s.Info("backup", BackupServices.Inv($"Backup node {s.Name} unlocked with recovery key material."));
        }
    }

    // ------------------------------------------------------------------------------------------------- jobs

    private void StartJobs()
    {
        BackupServices s = Services;
        TimeSpan tick = s.Timings.Tick;
        TimeSpan expiry = tick * 10;
        TimeSpan retention = tick * 50;
        _ = RunPeriodicAsync("reader-expiry", expiry, ct => s.Reader.ExpireAsync(ct));
        _ = RunPeriodicAsync("retention", retention, ct => s.Catalog.EnforceRetentionAsync(ct));
    }

    private async Task RunPeriodicAsync(string name, TimeSpan period, Func<CancellationToken, Task> work)
    {
        BackupServices s = Services;
        CancellationToken ct = _jobs.Token;
        while (!ct.IsCancellationRequested && !s.IsStopping)
        {
            try
            {
                await Task.Delay(period, s.Time, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (s.IsStopping || ct.IsCancellationRequested)
                return;
            try
            {
                await work(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                if (s.IsStopping)
                    return;
                s.Warn("backup", BackupServices.Inv($"Background job {name} failed: {ex.Message}"));
            }
        }
    }

    // ------------------------------------------------------------------------------------------------- stop and crash

    /// <summary>Graceful stop: refuse new requests, drain in-flight ones, flush and close the store, close the transport.</summary>
    public async Task StopAsync(TimeSpan drainTimeout)
    {
        BackupServices s = Services;
        if (s.IsStopping)
            return;
        s.IsStopping = true;
        _jobs.Cancel();
        await Task.WhenAny(Dispatcher.WaitForDrainAsync(), Task.Delay(drainTimeout, s.Time));
        if (s.Store is not null)
            await s.Store.DisposeAsync();
        await Dispatcher.CloseAllAsync();
        if (_transport is not null)
            await _transport.DisposeAsync();
        DisposeResources();
        s.Info("backup", BackupServices.Inv($"Backup node {s.Name} stopped."));
    }

    /// <summary>Crash: the transport is torn down without any flush (called after the loop was stopped).</summary>
    public void Kill()
    {
        if (Interlocked.Exchange(ref _killed, 1) != 0)
            return;
        Services.IsStopping = true;
        try
        {
            _jobs.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already stopped.
        }

        if (_transport is { } transport)
            _ = transport.DisposeAsync().AsTask();
    }

    private async Task AbortStartAsync()
    {
        BackupServices s = Services;
        s.IsStopping = true;
        _jobs.Cancel();
        try
        {
            if (s.Store is not null)
                await s.Store.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or DfsException or ObjectDisposedException)
        {
            s.Warn("backup", "Cleanup after a failed start failed: " + ex.Message);
        }
        finally
        {
            if (_transport is not null)
                await _transport.DisposeAsync();
            DisposeResources();
        }
    }

    private void DisposeResources()
    {
        foreach (IDisposable disposable in Services.Disposables)
            disposable.Dispose();
        Services.Disposables.Clear();
    }

    private static void Validate(BackupNodeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(options.Name);
        ArgumentException.ThrowIfNullOrEmpty(options.ListenEndpoint);
        ArgumentNullException.ThrowIfNull(options.DataDirectory);
        ArgumentNullException.ThrowIfNull(options.Principals);
        ArgumentNullException.ThrowIfNull(options.Grants);
        ArgumentNullException.ThrowIfNull(options.DefaultRetention);
        ArgumentNullException.ThrowIfNull(options.Timings);
        ArgumentNullException.ThrowIfNull(options.Mutations);
        options.Timings.Validate();
        if (options.SourceClusterId == Guid.Empty)
            throw new ArgumentException("The source cluster id must be set.", nameof(options));
        if (options.KeepLastSnapshots < 1)
            throw new ArgumentException("KeepLastSnapshots must be at least 1.", nameof(options));
        if (options.CapacityBytes <= 0)
            throw new ArgumentException("CapacityBytes must be positive.", nameof(options));
    }
}
