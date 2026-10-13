using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Service;

namespace Tedd.Tdfs.Server.Volumes;

/// <summary>Production state machine factory: the replicated <see cref="VolumeStateMachine"/>.</summary>
internal sealed class VolumeStateMachineFactory : IVolumeStateMachineFactory
{
    public static readonly VolumeStateMachineFactory Instance = new();

    /// <inheritdoc />
    public Raft.IReplicatedStateMachine Create(VolumeHostContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new VolumeStateMachine(context.Creation, context.ClusterId);
    }
}

/// <summary>Attaches the leader-side <see cref="VolumeService"/> to a hosted volume (INTERNALS.md §1.3 hooks).</summary>
internal sealed partial class VolumeHost
{
    /// <summary>The volume service (null when the group hosts another state machine, for example in runtime tests).</summary>
    public VolumeService? Service { get; private set; }

    static partial void CreateDefaultStateMachineFactory(ref IVolumeStateMachineFactory? factory) => factory = VolumeStateMachineFactory.Instance;

    partial void OnOpened()
    {
        if (StateMachine is VolumeStateMachine sm)
        {
            Service = new VolumeService(_s, this, sm);
            Service.OnLeadershipChanged();
        }
    }

    partial void OnClosing()
    {
        Service?.Dispose();
        Service = null;
    }

    partial void OnLeadershipChanged() => Service?.OnLeadershipChanged();

    partial void OnRecordChanged(Control.VolumeRecord previous)
    {
        _ = previous;
        Service?.OnRecordChanged();
    }
}
