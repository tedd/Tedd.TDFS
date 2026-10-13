using System.Globalization;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Service container of one <see cref="BackupNodeHost"/> (the backup node's counterpart of <c>NodeServices</c>), created on
/// the backup node's loop. Every member is only touched on <see cref="Loop"/> (ARCHITECTURE §3.1): time, randomness, storage,
/// transport and faults come from the injected <see cref="NodeEnvironment"/>.
/// </summary>
internal sealed class BackupServices
{
    /// <summary>Directory (inside the data directory) holding the object store.</summary>
    public const string ObjectsDirectory = "objects";

    public BackupServices(BackupNodeOptions options, NodeEnvironment environment, INodeLoop loop)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(loop);
        Options = options;
        Env = environment;
        Loop = loop;
        Random = environment.Random;
    }

    public BackupNodeOptions Options { get; }

    public NodeEnvironment Env { get; }

    public INodeLoop Loop { get; }

    public TimeProvider Time => Env.Time;

    public IRandomSource Random { get; }

    public IFaultInjector Faults => Env.Faults;

    public IStorageMedium Medium => Env.Medium;

    public NodeTimings Timings => Options.Timings;

    public SafetyMutations Mutations => Options.Mutations;

    public string Name => Options.Name;

    /// <summary>Actual bound endpoint (after the transport started).</summary>
    public string Endpoint { get; set; } = string.Empty;

    /// <summary>Key material.</summary>
    public BackupKeys Keys { get; set; } = null!;

    /// <summary>The object store (null while the node is locked: its records are encrypted with the unavailable data key).</summary>
    public ObjectStore? Store { get; set; }

    /// <summary>The durable catalog of retained snapshots and staged transfers.</summary>
    public BackupCatalog Catalog { get; set; } = null!;

    /// <summary>Client-facing read-only service (sessions, pins, snapshot views).</summary>
    public BackupReader Reader { get; set; } = null!;

    /// <summary>Export protocol receiver.</summary>
    public BackupReceiver Receiver { get; set; } = null!;

    /// <summary>Principal verifiers and grants.</summary>
    public BackupPrincipals Principals { get; set; } = null!;

    /// <summary>Process-local disposables (ciphers).</summary>
    public List<IDisposable> Disposables { get; } = [];

    /// <summary>True once requests are served.</summary>
    public bool IsServing { get; set; }

    /// <summary>True once a graceful stop began.</summary>
    public bool IsStopping { get; set; }

    /// <summary>The work that completes an unlock by recovery key material (shared by concurrent callers).</summary>
    public Task? UnlockCompletion { get; set; }

    /// <summary>Default timeout of calls this node makes (none at the moment) and of bounded internal waits.</summary>
    public TimeSpan CallTimeout
    {
        get
        {
            TimeSpan election = Timings.Tick * Timings.ElectionTicksMin;
            TimeSpan t = election * 20;
            return t < TimeSpan.FromSeconds(2) ? TimeSpan.FromSeconds(2) : t > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : t;
        }
    }

    /// <summary>Path of <paramref name="relative"/> inside the data directory.</summary>
    public string DataPath(string relative) => StoragePath.Combine(Options.DataDirectory, relative);

    /// <summary>The object store, or <see cref="DfsRecoveryKeyMissingException"/> while locked.</summary>
    public ObjectStore RequireStore(string what)
    {
        Keys.RequireUnlocked(what);
        return Store ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"The object store of backup node {Name} is not open.");
    }

    /// <summary>Reports a fault-injection stage boundary (Node = backup name, Volume = source volume name).</summary>
    public ValueTask HitAsync(FaultStage stage, string site, string? volume, DfsOperationKind operation, ObjectId? obj = null, string? detail = null,
        CancellationToken ct = default)
    {
        IFaultInjector faults = Faults;
        if (!faults.IsEnabled)
            return ValueTask.CompletedTask;
        return faults.HitAsync(new FaultContext(stage, site, Name, volume, operation, null, null, obj, 0, detail), ct);
    }

    public void Log(NodeLogLevel level, string category, string message)
    {
        try
        {
            Env.Log.Write(level, Name, category, message);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A failing log sink must never break node code.
        }
    }

    public void Info(string category, string message) => Log(NodeLogLevel.Info, category, message);

    public void Debug(string category, string message) => Log(NodeLogLevel.Debug, category, message);

    public void Warn(string category, string message) => Log(NodeLogLevel.Warn, category, message);

    /// <summary>Formats a value for log messages with the invariant culture.</summary>
    public static string Inv(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);
}
