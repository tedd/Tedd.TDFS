using System.Globalization;
using Tedd.Tdfs.Server.Admin;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Service container of one <see cref="NodeHost"/> (INTERNALS.md §1.1), created on the node loop. Every member is only
/// touched on <see cref="Loop"/>. Components are assigned during start-up in dependency order.
/// </summary>
internal sealed class NodeServices
{
    /// <summary>Directory (inside <see cref="NodeOptions.DataDirectory"/>) holding the object store.</summary>
    public const string ObjectsDirectory = "objects";

    /// <summary>Directory holding one sub-directory per Raft group.</summary>
    public const string GroupsDirectory = "groups";

    public NodeServices(NodeOptions options, NodeEnvironment environment, INodeLoop loop)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(loop);
        Options = options;
        Env = environment;
        Loop = loop;
        Random = environment.Random;
        Metrics = new NodeMetrics(options.Name);
    }

    public NodeOptions Options { get; }

    public NodeEnvironment Env { get; }

    public INodeLoop Loop { get; }

    public TimeProvider Time => Env.Time;

    public IRandomSource Random { get; }

    public IFaultInjector Faults => Env.Faults;

    public INodeLog Log => Env.Log;

    public NodeTimings Timings => Options.Timings;

    public SafetyMutations Mutations => Options.Mutations;

    public string NodeName => Options.Name;

    public NodeMetrics Metrics { get; }

    // ---- identity (set during start-up)

    public NodeId NodeId { get; set; }

    public ulong Incarnation { get; set; }

    /// <summary>Random per process start; registered with the incarnation (RSY-021).</summary>
    public UInt128 BootId { get; set; }

    public Guid ClusterId { get; set; }

    public Superblock? Superblock { get; set; }

    public KeyHierarchy Keys { get; set; } = null!;

    /// <summary>Actual bound endpoint (after the transport started).</summary>
    public string Endpoint { get; set; } = string.Empty;

    // ---- components

    public RpcDispatcher Rpc { get; set; } = null!;

    public PeerConnections Peers { get; set; } = null!;

    public RaftGroupHost Raft { get; set; } = null!;

    public ControlHost Control { get; set; } = null!;

    public VolumeRegistry Volumes { get; set; } = null!;

    public ObjectStore ObjectStore { get; set; } = null!;

    public ObjectService Objects { get; set; } = null!;

    public AdminService Admin { get; set; } = null!;

    public MembershipManager Membership { get; set; } = null!;

    public BackgroundTasks Jobs { get; set; } = null!;

    /// <summary>Late-sync backup exporters of the volumes this node leads (null until the backup area registered).</summary>
    public Backup.BackupExporters? BackupExports { get; set; }

    public IVolumeStateMachineFactory? VolumeStateMachines { get; set; }

    /// <summary>Record ciphers created for this process (disposed at stop).</summary>
    public List<IDisposable> Disposables { get; } = [];

    /// <summary>True once start-up completed and requests are served.</summary>
    public bool IsServing { get; set; }

    /// <summary>True once a graceful stop began (new requests are refused).</summary>
    public bool IsStopping { get; set; }

    /// <summary>The identity this node presents in node handshakes.</summary>
    public NodeCredentials CreateNodeCredentials()
        => new(ClusterId, NodeName, NodeId, Incarnation, BootId, Keys.NodeAuthKey, ProtocolFeatures.Supported);

    /// <summary>Path of <paramref name="relative"/> inside the node data directory.</summary>
    public string DataPath(string relative) => StoragePath.Combine(Options.DataDirectory, relative);

    /// <summary>
    /// A cancellation source firing after <paramref name="timeout"/> (default: the node-to-node call timeout) on the node
    /// clock. Background proposals use it: the fate of an entry appended by a leader that lost leadership is decided only
    /// when some later entry is committed at its index, which can take arbitrarily long in an idle group, and a periodic
    /// job awaiting it would otherwise stop running.
    /// </summary>
    public CancellationTokenSource CreateDeadline(TimeSpan? timeout = null) => new(timeout ?? Peers.DefaultCallTimeout, Time);

    /// <summary>Writes a log entry (never payloads or secrets).</summary>
    public void LogMessage(NodeLogLevel level, string category, string message)
    {
        try
        {
            Log.Write(level, NodeName, category, message);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A failing log sink must never break node code.
        }
    }

    public void Info(string category, string message) => LogMessage(NodeLogLevel.Info, category, message);

    public void Debug(string category, string message) => LogMessage(NodeLogLevel.Debug, category, message);

    public void Warn(string category, string message) => LogMessage(NodeLogLevel.Warn, category, message);

    public void Error(string category, string message) => LogMessage(NodeLogLevel.Error, category, message);

    /// <summary>Reports a fault-injection stage boundary (no-op without an armed injector).</summary>
    public ValueTask HitAsync(FaultStage stage, string site, string? volume = null, DfsOperationKind operation = DfsOperationKind.Unknown,
        DfsOperationId? operationId = null, string? path = null, ObjectId? obj = null, int occurrence = 0, string? detail = null, CancellationToken ct = default)
    {
        IFaultInjector faults = Faults;
        if (!faults.IsEnabled)
            return ValueTask.CompletedTask;
        return faults.HitAsync(new FaultContext(stage, site, NodeName, volume, operation, operationId, path, obj, occurrence, detail), ct);
    }

    /// <summary>Formats a value for log messages with the invariant culture.</summary>
    public static string Inv(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);
}
