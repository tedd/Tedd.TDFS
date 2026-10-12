using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

/// <summary>Read-only diagnostics of a live node; every call executes on the node loop.</summary>
public sealed partial class NodeInspector
{
    internal NodeInspector()
    {
    }

    /// <summary>Counter snapshot.</summary>
    public Task<NodeCounters> GetCountersAsync()
        => throw new NotImplementedException("NodeInspector.GetCountersAsync: implemented in a later phase");

    /// <summary>The control group and every volume group hosted on this node.</summary>
    public Task<IReadOnlyList<GroupInspection>> GetGroupsAsync()
        => throw new NotImplementedException("NodeInspector.GetGroupsAsync: implemented in a later phase");

    /// <summary>One group ("control" or a volume name), or null when not hosted here.</summary>
    public Task<GroupInspection?> GetGroupAsync(string group)
        => throw new NotImplementedException("NodeInspector.GetGroupAsync: implemented in a later phase");

    /// <summary>Objects held by the local extent store, optionally for one volume.</summary>
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync(string? volume = null)
        => throw new NotImplementedException("NodeInspector.ListObjectsAsync: implemented in a later phase");

    /// <summary>True when the local extent store holds the object.</summary>
    public Task<bool> HasObjectAsync(ObjectId id)
        => throw new NotImplementedException("NodeInspector.HasObjectAsync: implemented in a later phase");

    /// <summary>This replica's applied state of a volume, or null when the volume is not hosted here.</summary>
    public Task<VolumeStateInspection?> GetVolumeStateAsync(string volume)
        => throw new NotImplementedException("NodeInspector.GetVolumeStateAsync: implemented in a later phase");

    /// <summary>Bytes held under open staging sessions.</summary>
    public Task<long> GetStagedBytesAsync()
        => throw new NotImplementedException("NodeInspector.GetStagedBytesAsync: implemented in a later phase");

    /// <summary>Bytes retained only by pins.</summary>
    public Task<long> GetPinnedBytesAsync()
        => throw new NotImplementedException("NodeInspector.GetPinnedBytesAsync: implemented in a later phase");

    /// <summary>Hash of the applied replicated state of every group on this node.</summary>
    public Task<string> ComputeStateDigestAsync()
        => throw new NotImplementedException("NodeInspector.ComputeStateDigestAsync: implemented in a later phase");
}
