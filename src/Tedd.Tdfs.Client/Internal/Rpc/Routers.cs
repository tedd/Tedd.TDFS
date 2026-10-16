using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Rpc;

/// <summary>
/// Routes requests served by the control-group leader (cluster administration, PROTOCOL.md §8.8): the leader named by
/// the cluster map or the latest <c>NotLeader</c> hint, otherwise a control voter (which redirects), refreshing the map
/// when nothing is known. A backup-host client sends everything to its backup node.
/// </summary>
internal sealed class ControlRouter : IRpcRouter
{
    private static readonly TimeSpan SuspectDuration = TimeSpan.FromSeconds(2);
    private readonly ClientRuntime _runtime;
    private readonly Lock _sync = new();
    private readonly Dictionary<string, long> _suspects = new(StringComparer.Ordinal);
    private string? _leader;
    private int _rotation;

    public ControlRouter(ClientRuntime runtime) => _runtime = runtime;

    /// <inheritdoc />
    public bool RoutesToGroupLeader => !_runtime.IsBackupHost;

    /// <inheritdoc />
    public async ValueTask<string?> ResolveAsync(OperationScope scope, TimeSpan attemptTimeout)
    {
        if (_runtime.IsBackupHost)
            return _runtime.BackupHostEndpoint;
        lock (_sync)
        {
            if (_leader is not null)
                return _leader;
        }

        if (MapLeader() is { } fromMap)
            return fromMap;
        await _runtime.RefreshMapAsync(scope).ConfigureAwait(false);
        if (MapLeader() is { } refreshed)
            return refreshed;

        // No leader known: ask a control voter (it redirects with a hint), rotating across attempts.
        ClusterMapData? map = _runtime.Map.Current;
        var candidates = new List<string>();
        if (map is not null)
        {
            foreach (NodeEntry node in map.Nodes)
            {
                if (node.IsControlVoter && node.Endpoints.Count > 0 && node.Status is not WireNodeStatus.Removed)
                    candidates.Add(node.Endpoints[0]);
            }
        }

        if (candidates.Count == 0)
            candidates.AddRange(_runtime.Map.DiscoveryEndpoints());
        if (candidates.Count == 0)
            return null;
        lock (_sync)
        {
            long now = _runtime.Time.GetTimestamp();
            for (int i = 0; i < candidates.Count; i++)
            {
                string candidate = candidates[(_rotation + i) % candidates.Count];
                if (_suspects.TryGetValue(candidate, out long since) && _runtime.Time.GetElapsedTime(since, now) < SuspectDuration)
                    continue;
                _rotation = (_rotation + i + 1) % candidates.Count;
                return candidate;
            }

            _rotation = (_rotation + 1) % candidates.Count;
            return candidates[_rotation];
        }
    }

    private string? MapLeader()
    {
        if (_runtime.Map.Current?.ControlLeader is not { } leader || _runtime.Map.EndpointOf(leader) is not { } endpoint)
            return null;
        lock (_sync)
        {
            if (_suspects.TryGetValue(endpoint, out long since) && _runtime.Time.GetElapsedTime(since) < SuspectDuration)
                return null;
        }

        return endpoint;
    }

    /// <inheritdoc />
    public void OnNotLeader(string endpoint, string? hint)
    {
        lock (_sync)
        {
            if (hint is { Length: > 0 } && !string.Equals(hint, endpoint, StringComparison.Ordinal))
            {
                _leader = hint;
                return;
            }

            if (string.Equals(_leader, endpoint, StringComparison.Ordinal))
                _leader = null;
            _suspects[endpoint] = _runtime.Time.GetTimestamp();
        }
    }

    /// <inheritdoc />
    public void OnUnreachable(string endpoint)
    {
        lock (_sync)
        {
            if (string.Equals(_leader, endpoint, StringComparison.Ordinal))
                _leader = null;
            _suspects[endpoint] = _runtime.Time.GetTimestamp();
        }
    }

    /// <inheritdoc />
    public void OnAnswered(string endpoint)
    {
        lock (_sync)
            _suspects.Remove(endpoint);
        _runtime.Map.MarkGood(endpoint);
    }
}

/// <summary>Routes requests any live node can answer (map, listings): the last good endpoint, then the others in turn.</summary>
internal sealed class AnyNodeRouter : IRpcRouter
{
    private readonly ClientRuntime _runtime;
    private readonly Lock _sync = new();
    private readonly HashSet<string> _failed = new(StringComparer.Ordinal);

    public AnyNodeRouter(ClientRuntime runtime) => _runtime = runtime;

    /// <inheritdoc />
    public bool RoutesToGroupLeader => false;

    /// <inheritdoc />
    public ValueTask<string?> ResolveAsync(OperationScope scope, TimeSpan attemptTimeout)
    {
        if (_runtime.IsBackupHost)
            return ValueTask.FromResult(_runtime.BackupHostEndpoint);
        IReadOnlyList<string> endpoints = _runtime.Map.DiscoveryEndpoints();
        lock (_sync)
        {
            foreach (string endpoint in endpoints)
            {
                if (!_failed.Contains(endpoint))
                    return ValueTask.FromResult<string?>(endpoint);
            }

            // Every endpoint failed recently: start over.
            _failed.Clear();
        }

        return ValueTask.FromResult(endpoints.Count > 0 ? endpoints[0] : null);
    }

    /// <inheritdoc />
    public void OnNotLeader(string endpoint, string? hint)
    {
        lock (_sync)
            _failed.Add(endpoint);
    }

    /// <inheritdoc />
    public void OnUnreachable(string endpoint)
    {
        lock (_sync)
            _failed.Add(endpoint);
    }

    /// <inheritdoc />
    public void OnAnswered(string endpoint)
    {
        lock (_sync)
            _failed.Remove(endpoint);
        _runtime.Map.MarkGood(endpoint);
    }
}
