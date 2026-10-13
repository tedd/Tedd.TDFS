using System.Globalization;
using System.Runtime.CompilerServices;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

// SDK clients, raw protocol clients, the admin client and principals.
public sealed partial class TestCluster
{
    private readonly Dictionary<string, DfsCredentials> _credentials = new(StringComparer.Ordinal);
    private readonly ConditionalWeakTable<DfsClient, ClientRecord> _clientRecords = [];
    private readonly List<WeakReference<DfsClient>> _clients = [];
    private DfsClient? _adminClient;
    private Exception? _adminClientError;
    private int _clientCount;
    private int _rawCount;

    /// <summary>The SDK admin client connected at start (used by harness helpers such as backup registration).</summary>
    /// <exception cref="InvalidOperationException">It could not be connected (the failure is the inner exception).</exception>
    internal DfsClient AdminClientForHarness
    {
        get
        {
            if (_adminClient is { } client)
                return client;
            if (_adminClientError is { } error)
                throw new InvalidOperationException($"The cluster's admin client could not be connected: {error.Message}", error);
            throw new InvalidOperationException("The cluster's admin client is not connected (the cluster was started without one).");
        }
    }

    /// <summary>Connects a client.</summary>
    public Task<DfsClient> ConnectAsync(TestClientOptions? options = null, CancellationToken ct = default) =>
        ConnectCoreAsync(options ?? new TestClientOptions(), configure: null, harness: false, ct);

    /// <summary>Builds client options for this cluster without connecting.</summary>
    /// <remarks>
    /// Seeds are the endpoints of <see cref="TestClientOptions.SeedNodes"/> (node or backup node names) or of every live node;
    /// the expected cluster id defaults to this cluster's; QUIC clients pin this cluster's certificates; the clock is
    /// <see cref="TestClientOptions.Clock"/> or the client's own independent clock (in deterministic mode bound to the
    /// client's simulated process <c>client:{Name}</c>).
    /// </remarks>
    public DfsClientOptions CreateClientOptions(TestClientOptions? options = null)
    {
        options ??= new TestClientOptions();
        IReadOnlyList<string> seeds = options.SeedNodes is { } names ? names.Select(ResolveSeedEndpoint).ToArray() : Nodes.Select(n => n.Endpoint).ToArray();
        return BuildClientOptions(options, seeds, options.ExpectedClusterId ?? ClusterId, options.Credentials ?? AdminCredentials);
    }

    /// <summary>Connects a read-only client to a backup node.</summary>
    /// <remarks>Credentials default to the backup node's reader principal; the expected cluster id is only checked when given.</remarks>
    public async Task<DfsClient> ConnectBackupAsync(int backupIndex = 0, TestClientOptions? options = null, CancellationToken ct = default)
    {
        TestBackupNode backup = BackupAt(backupIndex);
        options ??= new TestClientOptions { Name = "backup-reader-" + backupIndex.ToString(CultureInfo.InvariantCulture) };
        DfsClientOptions publicOptions = BuildClientOptions(options, [backup.Endpoint], options.ExpectedClusterId, options.Credentials ?? backup.ReaderCredentials);
        return await ConnectWithAsync(options, publicOptions, harness: false, ct);
    }

    /// <summary>Creates a principal with per-volume grants (committed in the control group before the task completes).</summary>
    public async Task<TestPrincipal> CreatePrincipalAsync(string name, IReadOnlyDictionary<string, DfsPermissions> grants, bool clusterAdmin = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(grants);
        var credentials = new DfsCredentials(name, NewSecret(_random.Fork("principal/" + name)));
        await HarnessAdmin.CallAsync(AdminOp.CreatePrincipal, new AdminCreatePrincipalBody(name, credentials.Secret, clusterAdmin));
        lock (_credentials)
            _credentials[name] = credentials;
        foreach (KeyValuePair<string, DfsPermissions> grant in grants.OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            if (grant.Value != DfsPermissions.None)
                await HarnessAdmin.CallAsync(AdminOp.Grant, new AdminGrantBody(grant.Key, name, grant.Value));
        }

        Record($"principal {name}: created ({(clusterAdmin ? "cluster admin, " : string.Empty)}{grants.Count} grant(s))");
        return new TestPrincipal(name, credentials);
    }

    /// <summary>Creates a group and adds <paramref name="members"/> to it (committed before the task completes).</summary>
    private async Task CreateGroupCoreAsync(string group, string[] members)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentNullException.ThrowIfNull(members);
        await HarnessAdmin.CallAsync(AdminOp.CreateGroup, new AdminGroupNameBody(group));
        foreach (string member in members)
            await HarnessAdmin.CallAsync(AdminOp.AddGroupMember, new AdminGroupMemberBody(group, member));
        Record($"group {group}: created ({members.Length} member(s))");
    }

    /// <summary>Creates a non-admin principal with <paramref name="volumeGrants"/> on every test volume and adds it to <paramref name="groups"/>.</summary>
    private async Task<TestPrincipal> CreateUserCoreAsync(string name, DfsPermissions volumeGrants, string[] groups)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(groups);
        var grants = Options.Volumes.ToDictionary(v => v.Name, _ => volumeGrants, StringComparer.Ordinal);
        TestPrincipal principal = await CreatePrincipalAsync(name, grants);
        foreach (string group in groups)
            await HarnessAdmin.CallAsync(AdminOp.AddGroupMember, new AdminGroupMemberBody(group, name));
        return principal;
    }

    /// <summary>Simulates client process death: isolates the client and abandons it without cleanup.</summary>
    /// <remarks>
    /// The client's network identity is isolated (in-flight and later messages of the dead process never arrive), the SDK
    /// client stops without any cleanup traffic, and in deterministic mode the client's simulated process is killed (its
    /// timers and queued work die with it).
    /// </remarks>
    public void KillClient(DfsClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        string name = _clientRecords.TryGetValue(client, out ClientRecord? record) ? record.NetworkName : client.Internals.NetworkName ?? "client:" + client.Options.ClientName;
        Network.Isolate(name);
        Record($"client {name}: killed");
        client.Abandon();
        if (Runtime.Scheduler is { } scheduler && scheduler.FindProcess(name) is { } process)
            scheduler.KillProcess(process, "client killed by the test");
    }

    /// <summary>Connects a raw protocol client (bypasses SDK validation) to a node; network identity <c>client:raw{n}</c>.</summary>
    public Task<RawProtocolClient> ConnectRawAsync(string nodeName, CancellationToken ct = default)
    {
        TestNode node = Node(nodeName);
        int n = Interlocked.Increment(ref _rawCount);
        return RawProtocolClient.ConnectAsync(this, "client:raw" + n.ToString(CultureInfo.InvariantCulture), node.Name, node.Endpoint, ClusterId, ct);
    }


    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>Credentials of a principal created through the harness (any cluster sharing the fabric), or null.</summary>
    internal DfsCredentials? FindCredentials(string principal)
    {
        lock (_credentials)
        {
            if (_credentials.TryGetValue(principal, out DfsCredentials? own))
                return own;
        }

        return _parent?.FindCredentials(principal);
    }

    /// <summary>The backup node with <paramref name="index"/>.</summary>
    private TestBackupNode BackupAt(int index)
    {
        lock (_gate)
        {
            if ((uint)index >= (uint)_backups.Count)
                throw new ArgumentOutOfRangeException(nameof(index), index, $"The cluster has {_backups.Count} backup node(s).");
            return _backups[index];
        }
    }

    /// <summary>The endpoint a seed name resolves to: a live node, a backup node, or any named endpoint of the fabric.</summary>
    private string ResolveSeedEndpoint(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (FindNode(name) is { } node)
            return node.Endpoint;
        if (FindBackup(name) is { } backup)
            return backup.Endpoint;
        return Fabric.EndpointOfName(name) ?? throw new ArgumentException($"Seed '{name}' is not a node of this cluster.", nameof(name));
    }

    private DfsClientOptions BuildClientOptions(TestClientOptions options, IReadOnlyList<string> seeds, Guid? expectedClusterId, DfsCredentials credentials)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Name);
        TimeProvider clock = options.Clock ?? (TimeProvider)Runtime.CreateProcessClock(options.NetworkName);
        var result = new DfsClientOptions
        {
            SeedEndpoints = seeds,
            ExpectedClusterId = expectedClusterId,
            Credentials = credentials,
            ClientName = options.Name,
            OperationTimeout = options.OperationTimeout ?? TimeSpan.FromSeconds(10),
            Security = new DfsTransportSecurity { PinnedSpkiSha256 = _trustedPins },
            TimeProvider = clock,
            Diagnostics = options.Diagnostics ?? HarnessNodeLog.ClientEcho(options.Name, clock),
        };
        if (options.ConnectTimeout is { } connect)
            result = result with { ConnectTimeout = connect };
        if (options.BufferSize is { } buffer)
            result = result with { DefaultBufferSize = buffer };
        if (options.MaxMaterializeBytes is { } materialize)
            result = result with { MaxMaterializeBytes = materialize };
        return result;
    }

    private Task<DfsClient> ConnectCoreAsync(TestClientOptions options, Func<DfsClientOptions, DfsClientOptions>? configure, bool harness, CancellationToken ct)
    {
        DfsClientOptions publicOptions = CreateClientOptions(options);
        if (configure is not null)
            publicOptions = configure(publicOptions) ?? throw new InvalidOperationException("The client options callback returned null.");
        return ConnectWithAsync(options, publicOptions, harness, ct);
    }

    private async Task<DfsClient> ConnectWithAsync(TestClientOptions options, DfsClientOptions publicOptions, bool harness, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        int n = Interlocked.Increment(ref _clientCount);
        string networkName = options.NetworkName;
        TimeProvider clock = publicOptions.TimeProvider ?? Runtime.Time;
        var internals = new DfsClientOptionsInternal
        {
            TransportFactory = Fabric.CreateFactory(new TransportIdentity
            {
                Owner = this,
                Clock = clock,
                IsHarness = harness,
                Credentials = publicOptions.Credentials,
                ClusterId = publicOptions.ExpectedClusterId ?? ClusterId,
                Certificate = null,
                TrustedPins = _trustedPins,
            }),
            Random = _random.Fork(string.Create(CultureInfo.InvariantCulture, $"client:{options.Name}:{n}")),
            Faults = Faults.Injector,
            Mutations = options.Mutations,
            NetworkName = networkName,
        };
        Record($"client {networkName}: connect as {publicOptions.Credentials?.Principal}");
        DfsClient client = await DfsClient.ConnectAsync(publicOptions, internals, ct);
        _clientRecords.AddOrUpdate(client, new ClientRecord(networkName, harness));
        lock (_clients)
        {
            _clients.RemoveAll(static w => !w.TryGetTarget(out _));
            _clients.Add(new WeakReference<DfsClient>(client));
        }

        return client;
    }

    /// <summary>Connects the SDK admin client; an SDK that is not implemented in this build is remembered, not fatal.</summary>
    private async Task ConnectAdminClientAsync(CancellationToken ct)
    {
        try
        {
            _adminClient = await ConnectCoreAsync(new TestClientOptions { Name = _parent is null ? "admin" : _namePrefix + "admin" }, null, harness: true, ct);
        }
        catch (NotImplementedException ex)
        {
            _adminClientError = ex;
            Record("cluster: SDK admin client unavailable (" + ex.Message + ")");
        }
    }

    /// <summary>Releases SDK clients at disposal: the admin client is disposed, clients the test left open are abandoned.</summary>
    private async Task DisposeClientsAsync()
    {
        DfsClient[] open;
        lock (_clients)
        {
            open = _clients.Select(w => w.TryGetTarget(out DfsClient? c) ? c : null).OfType<DfsClient>().ToArray();
            _clients.Clear();
        }

        if (_adminClient is { } admin)
        {
            try
            {
                Task disposal = admin.DisposeAsync().AsTask();
                if (!await Runtime.WaitAsync(disposal, TimeSpan.FromSeconds(10)))
                    admin.Abandon();
            }
            catch (Exception ex) when (ex is NotImplementedException or ObjectDisposedException or DfsException)
            {
                // Nothing left to release.
            }
        }

        foreach (DfsClient client in open)
        {
            if (ReferenceEquals(client, _adminClient))
                continue;
            try
            {
                client.Abandon();
            }
            catch (Exception ex) when (ex is NotImplementedException or ObjectDisposedException)
            {
                // Already disposed by the test, or nothing to release.
            }
        }
    }

    /// <summary>What the harness knows about a client it connected.</summary>
    private sealed record ClientRecord(string NetworkName, bool IsHarness);
}
