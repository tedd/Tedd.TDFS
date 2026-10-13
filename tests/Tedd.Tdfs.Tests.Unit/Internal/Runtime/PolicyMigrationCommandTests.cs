using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>
/// Completion of default-policy migrations (REP-014, CHA-002): a completion established for one migration target must
/// never make a later, different target effective.
/// </summary>
public sealed class PolicyMigrationCommandTests
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 13, 12, 0, 0, TimeSpan.Zero);

    private static DfsStoragePolicy Policy(int copies, int zones, int ack, int ackZones) => new()
    {
        TargetCopies = copies,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ack,
        DurableZonesBeforeAck = ackZones,
    };

    private static (ControlStateMachine Sm, VolumeId Volume) VolumeOnThreeZones()
    {
        var random = new SeededRandom(11);
        NodeId[] ids = Enumerable.Range(0, 3).Select(_ => NodeId.New(random)).ToArray();
        var bootstrap = new ClusterBootstrap(random.NextGuid(), "unit",
            ids.Select((id, i) => new BootstrapMember(id, "n" + i, new DfsTopologyLabels("r1", "z" + i, "rk", "h" + i, "d"), ["mem://n" + i])).ToArray(),
            ["n0"], [new BootstrapPrincipal("admin", "secret-1", true)]);
        var sm = new ControlStateMachine();
        foreach (Consensus.RaftEntry e in ControlBootstrap.BuildEntries(bootstrap, new KeyHierarchy(new byte[] { 1, 2, 3 })).Where(e => e.Kind == Consensus.RaftEntryKind.Normal))
            Assert.True(sm.Apply(e.Index, e.Term, e.Data).IsSuccess);

        var volume = VolumeId.New(random);
        var creation = new VolumeCreation(volume, "vol", 1, "admin", T0, 0, new DfsMetadataPolicy { Voters = 3 }, Policy(1, 1, 1, 1),
            new Dictionary<string, DfsStoragePolicy>(), 4096, new DfsVolumeLimits(), "default", null, false, ids);
        Assert.True(Apply(sm, new CreateVolumeCommand(Admin(), creation, new byte[44])).IsSuccess);
        return (sm, volume);
    }

    private static ApplyOutcome Apply(ControlStateMachine sm, ControlCommand command) => sm.Apply(sm.AppliedIndex + 1, 2, ControlCommandCodec.Encode(command));

    private static CommandHeader Admin() => new(default, "admin", T0);

    [Fact]
    public void CompletionOfASupersededTarget_LeavesTheNewMigrationPending()
    {
        (ControlStateMachine sm, VolumeId volume) = VolumeOnThreeZones();
        DfsStoragePolicy two = Policy(2, 2, 2, 2);
        DfsStoragePolicy three = Policy(3, 3, 3, 3);
        Assert.True(Apply(sm, new SetDefaultPolicyCommand(Admin(), volume, two, true)).IsSuccess);

        // The volume leader reported "two" met; before that completion applies, the administrator starts "three".
        Assert.True(Apply(sm, new SetDefaultPolicyCommand(Admin(), volume, three, true)).IsSuccess);
        Assert.True(Apply(sm, new CompletePolicyMigrationCommand(Admin(), volume, two)).IsSuccess);
        Assert.Equal(three, sm.State.FindVolume(volume)!.MigrationTarget);

        // The completion of the pending target ends the migration.
        Assert.True(Apply(sm, new CompletePolicyMigrationCommand(Admin(), volume, three)).IsSuccess);
        Assert.Null(sm.State.FindVolume(volume)!.MigrationTarget);
    }

    [Fact]
    public void CompletionWithoutTarget_StillCompletes()
    {
        // Entries written before the target extension existed carry no target: they keep their original meaning.
        (ControlStateMachine sm, VolumeId volume) = VolumeOnThreeZones();
        Assert.True(Apply(sm, new SetDefaultPolicyCommand(Admin(), volume, Policy(2, 2, 2, 2), true)).IsSuccess);
        Assert.True(Apply(sm, new CompletePolicyMigrationCommand(Admin(), volume)).IsSuccess);
        Assert.Null(sm.State.FindVolume(volume)!.MigrationTarget);
    }

    [Fact]
    public void CompletionTarget_RoundTripsThroughTheCodec()
    {
        DfsStoragePolicy target = Policy(3, 2, 2, 1) with { Pool = "fast" };
        var volume = VolumeId.New(new SeededRandom(3));
        var command = (CompletePolicyMigrationCommand)ControlCommandCodec.Decode(ControlCommandCodec.Encode(new CompletePolicyMigrationCommand(Admin(), volume, target)));
        Assert.Equal(volume, command.Volume);
        Assert.Equal(target, command.Target);

        var legacy = (CompletePolicyMigrationCommand)ControlCommandCodec.Decode(ControlCommandCodec.Encode(new CompletePolicyMigrationCommand(Admin(), volume)));
        Assert.Null(legacy.Target);
    }
}
