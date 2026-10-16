using System.Globalization;
using System.Text.Json;
using Tedd.Tdfs.Host;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Ground truth of an out-of-process node: the queries of Tedd.Tdfs.Host.HostInspection over the host's control channel
// (an out-of-process node has no in-process NodeInspector). Every member throws InvalidOperationException when the node is
// not running (TestCluster.IsNodeGone treats it like a node that went away).
public sealed partial class TestNode
{
    private static readonly TimeSpan RemoteInspectionTimeout = TimeSpan.FromSeconds(30);

    /// <summary>A hosted group as this node's applied state shows it (null when it does not host it).</summary>
    internal async Task<GroupInspection?> InspectGroupAsync(string group)
    {
        if (Process is null)
            return State == TestNodeState.Running ? await Inspect.GetGroupAsync(group) : throw NotRunning();
        return JsonSerializer.Deserialize<GroupInspection?>(await RemoteAsync("group", group), HostInspection.Options);
    }

    /// <summary>Every group this node hosts.</summary>
    internal async Task<IReadOnlyList<GroupInspection>> InspectGroupsAsync()
    {
        if (Process is null)
            return State == TestNodeState.Running ? await Inspect.GetGroupsAsync() : throw NotRunning();
        return JsonSerializer.Deserialize<GroupInspection[]>(await RemoteAsync("groups", string.Empty), HostInspection.Options) ?? [];
    }

    /// <summary>A file's layout in this node's applied volume state (null when the volume is not hosted or the file absent).</summary>
    internal async Task<FileInspection?> InspectFileAsync(string volume, string path)
    {
        if (Process is null)
        {
            if (State != TestNodeState.Running)
                throw NotRunning();
            return (await Inspect.GetVolumeStateAsync(volume))?.GetFile(path);
        }

        InspectedFile? file = JsonSerializer.Deserialize<InspectedFile?>(await RemoteAsync("file", volume + "\n" + path), HostInspection.Options);
        return file?.ToInspection();
    }

    /// <summary>The ids of <paramref name="ids"/> this node holds durably and re-reads successfully.</summary>
    internal async Task<IReadOnlySet<ObjectId>> VerifyObjectsAsync(IReadOnlyCollection<ObjectId> ids)
    {
        if (Process is null)
            return State == TestNodeState.Running ? await Inspect.VerifyObjectsAsync(ids) : throw NotRunning();
        string argument = string.Join(",", ids.Select(i => i.Value.ToString(CultureInfo.InvariantCulture)));
        string[] verified = JsonSerializer.Deserialize<string[]>(await RemoteAsync("verify", argument), HostInspection.Options) ?? [];
        return verified.Select(v => new ObjectId(UInt128.Parse(v, CultureInfo.InvariantCulture))).ToHashSet();
    }

    /// <summary>This node's replicated-state digest line of a hosted group (<c>name@applied:sha256</c>), or null.</summary>
    internal async Task<string?> InspectGroupDigestAsync(string group)
    {
        if (Process is null)
            return State == TestNodeState.Running ? await Inspect.ComputeGroupDigestAsync(group) : throw NotRunning();
        return JsonSerializer.Deserialize<string?>(await RemoteAsync("digest", group), HostInspection.Options);
    }

    private async Task<string> RemoteAsync(string query, string argument)
    {
        HostProcess process = Process ?? throw NotRunning();
        if (State != TestNodeState.Running)
            throw NotRunning();
        return await process.InspectAsync(query, argument, RemoteInspectionTimeout);
    }

    private InvalidOperationException NotRunning() => new($"Node {Name} is {State}; it can only be inspected while it runs.");
}
