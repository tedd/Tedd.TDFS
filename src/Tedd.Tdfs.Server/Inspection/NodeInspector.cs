using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

/// <summary>Read-only diagnostics of a live node; every call executes on the node loop.</summary>
public sealed partial class NodeInspector
{
    private readonly INodeLoop _loop;
    private readonly NodeRuntime _runtime;

    internal NodeInspector(INodeLoop loop, NodeRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(loop);
        ArgumentNullException.ThrowIfNull(runtime);
        _loop = loop;
        _runtime = runtime;
    }

    private NodeServices S => _runtime.Services;

    /// <summary>Counter snapshot.</summary>
    public Task<NodeCounters> GetCountersAsync() => _loop.InvokeAsync(() =>
    {
        if (S.ObjectStore is { } store)
            S.Metrics.SetStored(store.ObjectCount, store.LiveBytes);
        return S.Metrics.Snapshot();
    });

    /// <summary>The control group and every volume group hosted on this node.</summary>
    public Task<IReadOnlyList<GroupInspection>> GetGroupsAsync() => _loop.InvokeAsync<IReadOnlyList<GroupInspection>>(() =>
    {
        var result = new List<GroupInspection>();
        if (S.Control.Group is { IsClosed: false } control)
            result.Add(Describe(control));
        foreach (VolumeHost host in S.Volumes.Hosts.OrderBy(h => h.Name, StringComparer.Ordinal))
        {
            if (!host.Group.IsClosed)
                result.Add(Describe(host.Group));
        }

        return result;
    });

    /// <summary>One group ("control" or a volume name), or null when not hosted here.</summary>
    public Task<GroupInspection?> GetGroupAsync(string group) => _loop.InvokeAsync(() =>
    {
        ArgumentNullException.ThrowIfNull(group);
        GroupRuntime? runtime = FindGroup(group);
        return runtime is null || runtime.IsClosed ? null : Describe(runtime);
    });

    /// <summary>Objects held by the local extent store, optionally for one volume.</summary>
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync(string? volume = null) => _loop.InvokeAsync<IReadOnlyList<ObjectRecordInfo>>(() =>
    {
        if (volume is null)
            return S.ObjectStore.List();
        if (S.Control.State.FindVolumeByName(volume) is { } record)
            return S.ObjectStore.List(record.Id);
        return [];
    });

    /// <summary>True when the local extent store holds the object.</summary>
    public Task<bool> HasObjectAsync(ObjectId id) => _loop.InvokeAsync(() => S.ObjectStore.ContainsAsync(id).AsTask());

    /// <summary>This replica's applied state of a volume, or null when the volume is not hosted here.</summary>
    public Task<VolumeStateInspection?> GetVolumeStateAsync(string volume) => _loop.InvokeAsync(() =>
        S.Volumes.TryGet(volume)?.StateMachine is IVolumeStateInspection inspection ? inspection.Inspect() : null);

    /// <summary>
    /// Bytes of unpublished uploads stored on this node: copies of hosted volumes bound to an open staging session and not
    /// referenced by any root of this replica's applied state.
    /// </summary>
    public Task<long> GetStagedBytesAsync() => _loop.InvokeAsync(() =>
    {
        IReadOnlyList<ObjectRecordInfo> records = S.ObjectStore.List();
        return S.Volumes.Hosts.Select(h => h.StateMachine).OfType<VolumeStateMachine>().Sum(sm => sm.LocalStagedBytes(records));
    });

    /// <summary>Bytes the current namespaces no longer reference that snapshots or pins retain.</summary>
    public Task<long> GetPinnedBytesAsync() => _loop.InvokeAsync(() =>
        S.Volumes.Hosts.Select(h => h.StateMachine).OfType<IVolumeStateInspection>().Sum(i => i.PinnedBytes));

    /// <summary>
    /// Hash of the applied replicated state of every group on this node: SHA-256 over, per group in name order, the group
    /// name, its applied index and the SHA-256 of its canonical checkpoint bytes.
    /// </summary>
    public Task<string> ComputeStateDigestAsync() => _loop.InvokeAsync(() =>
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (GroupRuntime group in S.Raft.Groups)
        {
            if (group.IsClosed)
                continue;
            hash.AppendData(Encoding.UTF8.GetBytes(DigestLine(group)));
        }

        return Convert.ToHexStringLower(hash.GetHashAndReset());
    });

    /// <summary>Digest of one group (<c>"name@applied:sha256"</c>), or null when it is not hosted (test and harness use).</summary>
    internal Task<string?> ComputeGroupDigestAsync(string group) => _loop.InvokeAsync(() =>
        FindGroup(group) is { IsClosed: false } runtime ? DigestLine(runtime) : null);

    /// <summary>Catch-up counters of a group (test and harness use; RSY suite).</summary>
    internal Task<GroupStats?> GetGroupStatsAsync(string group) => _loop.InvokeAsync(() => FindGroup(group)?.Stats);

    private GroupRuntime? FindGroup(string group)
    {
        if (string.Equals(group, Protocol.ProtocolInfo.ControlGroup, StringComparison.Ordinal))
            return S.Control.Group;
        return S.Volumes.TryGet(group)?.Group ?? S.Raft.TryGet(group);
    }

    private static string DigestLine(GroupRuntime group)
    {
        var buffer = new ArrayBufferWriter<byte>();
        group.StateMachine.WriteCheckpoint(buffer);
        string state = Convert.ToHexStringLower(SHA256.HashData(buffer.WrittenSpan));
        return string.Create(CultureInfo.InvariantCulture, $"{group.DisplayName}@{group.AppliedIndex}:{state};");
    }

    private GroupInspection Describe(GroupRuntime group)
    {
        RaftConfState conf = group.Conf;
        return new GroupInspection(group.DisplayName, group.Role, group.Term, group.Leader is { } leader ? S.Control.NameOf(leader) : null,
            group.CommitIndex, group.AppliedIndex, group.LastIndex, group.SnapshotIndex,
            conf.Voters.Select(S.Control.NameOf).ToArray(), conf.Learners.Select(S.Control.NameOf).ToArray());
    }
}
