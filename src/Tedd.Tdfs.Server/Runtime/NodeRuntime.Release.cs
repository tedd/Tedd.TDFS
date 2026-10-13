using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

// Software releases across the compatibility window (FMT-005..FMT-010, CHA-011): the node runs as one release
// (NodeServices.Release) and never interprets replicated state that needs a feature it does not understand.
internal sealed partial class NodeRuntime
{
    private bool _raisingMarker;
    private bool _excluded;

    /// <summary>
    /// Start-up: replays the control entries committed before the restart from the local journal, then refuses to serve
    /// (<see cref="DfsErrorCode.Unsupported"/>) when they activated a feature this release cannot interpret; otherwise
    /// records the minimum-reader marker of every active irreversible feature and watches later activations.
    /// </summary>
    private async Task ReplayControlAndCheckReleaseAsync(CancellationToken ct)
    {
        NodeServices s = Services;
        if (s.Control.Group is { } group)
            await group.ReplayLocalAsync(ct);
        if (UnsupportedActiveFeature() is { } feature)
        {
            throw new DfsException(DfsErrorCode.Unsupported,
                $"Node {s.NodeName} runs release {s.Release.Name}, which cannot interpret the active cluster feature '{feature}'.");
        }

        await RaiseReaderMarkerAsync();
        s.Control.StateMachine.Changed += OnControlChanged;
    }

    /// <summary>An active cluster feature this node's release does not understand, or null.</summary>
    private string? UnsupportedActiveFeature()
    {
        NodeRelease release = Services.Release;
        foreach (string feature in Services.Control.State.Features)
        {
            if (!release.Supports(feature))
                return feature;
        }

        return null;
    }

    /// <summary>The highest minimum-reader marker the active irreversible features require.</summary>
    private int RequiredReaderMarker()
    {
        int marker = 0;
        foreach (string feature in Services.Control.State.Features)
        {
            if (ProtocolFeatures.Irreversible.Contains(feature))
                marker = Math.Max(marker, ProtocolFeatures.ReaderMarkerOf(feature));
        }

        return marker;
    }

    private void OnControlChanged(ulong index)
    {
        NodeServices s = Services;
        if (s.IsStopping || _excluded)
            return;
        if (UnsupportedActiveFeature() is { } feature)
        {
            // Never apply (or lead, or serve) state committed under a feature this release cannot interpret: the node stops
            // participating, visibly (it becomes unreachable), instead of misreading what follows.
            _excluded = true;
            s.Error("node", NodeServices.Inv(
                $"Release {s.Release.Name} cannot interpret the active cluster feature '{feature}' (control index {index}); {s.NodeName} stops serving."));
            s.Loop.Post(() => _ = StopAsync(TimeSpan.Zero));
            return;
        }

        if (!_raisingMarker && RequiredReaderMarker() > (s.Superblock?.MinSoftwareVersion ?? 0))
            s.Loop.Post(() => _ = RaiseReaderMarkerAsync());
    }

    /// <summary>
    /// Durably raises this node's minimum-reader marker to what the active irreversible features require (never lowers it),
    /// so a release lacking them refuses the image (FMT-006). Retried on the next control change when it fails.
    /// </summary>
    private async Task RaiseReaderMarkerAsync()
    {
        NodeServices s = Services;
        int required = RequiredReaderMarker();
        if (_raisingMarker || s.IsStopping || required <= (s.Superblock?.MinSoftwareVersion ?? 0))
            return;
        if (required > s.Release.MaxReadableFormat)
        {
            s.Error("node", NodeServices.Inv($"Release {s.Release.Name} cannot record minimum-reader marker {required}."));
            return;
        }

        _raisingMarker = true;
        try
        {
            s.Superblock = await Superblock.RaiseMinSoftwareVersionAsync(s.Env.Medium, s.DataPath(SuperblockFile), required, CancellationToken.None,
                s.Release.MaxReadableFormat);
            s.Info("node", NodeServices.Inv($"{s.NodeName} recorded minimum-reader marker {required} (irreversible feature activation)."));
        }
        catch (Exception ex) when (ex is IOException or DfsException)
        {
            s.Warn("node", NodeServices.Inv($"Recording minimum-reader marker {required} failed: {ex.Message}; retrying on the next change."));
        }
        finally
        {
            _raisingMarker = false;
        }
    }
}
