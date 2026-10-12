using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>Deterministic control state machine: commands, validation, deduplication, audit and checkpoints.</summary>
public sealed class ControlStateMachineTests
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 12, 12, 0, 0, TimeSpan.Zero);

    private static (ControlStateMachine Sm, NodeId[] Nodes) Bootstrapped(int members = 3)
    {
        var random = new SeededRandom(5);
        NodeId[] ids = Enumerable.Range(0, members).Select(_ => NodeId.New(random)).ToArray();
        var bootstrap = new ClusterBootstrap(random.NextGuid(), "unit",
            ids.Select((id, i) => new BootstrapMember(id, "n" + i, new DfsTopologyLabels("r1", "z" + i, "rk", "h" + i, "d"), ["mem://n" + i])).ToArray(),
            ["n0"], [new BootstrapPrincipal("admin", "secret-1", true)]);
        IReadOnlyList<Consensus.RaftEntry> entries = ControlBootstrap.BuildEntries(bootstrap, new KeyHierarchy(new byte[] { 1, 2, 3 }));
        var sm = new ControlStateMachine();
        foreach (Consensus.RaftEntry e in entries.Where(e => e.Kind == Consensus.RaftEntryKind.Normal))
            Assert.True(sm.Apply(e.Index, e.Term, e.Data).IsSuccess);
        return (sm, ids);
    }

    private static ApplyOutcome Apply(ControlStateMachine sm, ControlCommand command) => sm.Apply(sm.AppliedIndex + 1, 2, ControlCommandCodec.Encode(command));

    private static CommandHeader Admin(UInt128 requestId = default) => new(requestId, "admin", T0);

    private static VolumeCreation Creation(VolumeId id, string name, NodeId[] voters, string pool = "default") => new(id, name, 1, "admin", T0, 0,
        new DfsMetadataPolicy { Voters = voters.Length }, new DfsStoragePolicy { TargetCopies = 1, TargetDistinctZones = 1, DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1 },
        new Dictionary<string, DfsStoragePolicy>(), 4096, new DfsVolumeLimits(), pool, null, false, voters);

    [Fact]
    public void Bootstrap_IsDeterministicAndCreatesMembersAndPrincipals()
    {
        (ControlStateMachine a, NodeId[] ids) = Bootstrapped();
        (ControlStateMachine b, _) = Bootstrapped();
        Assert.Equal(Checkpoint(a), Checkpoint(b));
        Assert.Equal(3, a.State.Members.Count);
        Assert.True(a.State.IsClusterAdmin("admin"));
        Assert.Equal(1, a.State.ControlVoterTarget);
        Assert.Equal("n1", a.State.FindMember(ids[1])!.Name);
        Assert.Equal(1UL, a.State.PlacementEpoch);
    }

    [Fact]
    public void Commands_AreValidatedDeterministicallyAndAudited()
    {
        (ControlStateMachine sm, NodeId[] ids) = Bootstrapped();
        var random = new SeededRandom(9);
        var volume = VolumeId.New(random);

        Assert.True(Apply(sm, new CreateVolumeCommand(Admin(), Creation(volume, "vol", [ids[0], ids[1], ids[2]]), new byte[44])).IsSuccess);
        Assert.Equal(DfsErrorCode.AlreadyExists, Apply(sm, new CreateVolumeCommand(Admin(), Creation(VolumeId.New(random), "vol", [ids[0]]), new byte[44])).Code);
        Assert.Equal(DfsErrorCode.PoolNotFound, Apply(sm, new CreateVolumeCommand(Admin(), Creation(VolumeId.New(random), "v2", [ids[0]], "fast"), new byte[44])).Code);
        Assert.Equal(DfsPermissions.All, sm.State.GetPermissions("admin", volume));

        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier("pw", new byte[16], 10);
        var alice = new PrincipalData("alice", verifier.Salt, verifier.Iterations, verifier.StoredKey, verifier.ServerKey, false);
        Assert.True(Apply(sm, new CreatePrincipalCommand(Admin(), alice)).IsSuccess);
        Assert.Equal(DfsErrorCode.AlreadyExists, Apply(sm, new CreatePrincipalCommand(Admin(), alice)).Code);
        Assert.Equal(DfsErrorCode.InvalidArgument, Apply(sm, new CreatePrincipalCommand(Admin(), alice with { Name = "bad:name" })).Code);
        Assert.True(Apply(sm, new SetGrantCommand(Admin(), volume, "alice", DfsPermissions.Read | DfsPermissions.Write, false)).IsSuccess);
        Assert.True(Apply(sm, new SetGrantCommand(Admin(), volume, "alice", DfsPermissions.Write, true)).IsSuccess);
        Assert.Equal(DfsPermissions.Read, sm.State.GetPermissions("alice", volume));
        Assert.Equal(DfsErrorCode.PrincipalNotFound, Apply(sm, new SetGrantCommand(Admin(), volume, "ghost", DfsPermissions.Read, false)).Code);

        Assert.True(Apply(sm, new GroupCommand(Admin(), ControlCommandKind.CreateGroup, "eng", null)).IsSuccess);
        Assert.True(Apply(sm, new GroupCommand(Admin(), ControlCommandKind.AddGroupMember, "eng", "alice")).IsSuccess);
        Assert.Equal(["eng"], sm.State.GroupsOf("alice"));
        Assert.Equal(DfsErrorCode.GroupNotFound, Apply(sm, new GroupCommand(Admin(), ControlCommandKind.DeleteGroup, "ops", null)).Code);

        Assert.True(Apply(sm, new PoolCommand(Admin(), ControlCommandKind.CreatePool, "fast", [ids[1], ids[2]])).IsSuccess);
        ulong epoch = sm.State.PlacementEpoch;
        Assert.Equal(DfsErrorCode.InvalidArgument, Apply(sm, new PoolCommand(Admin(), ControlCommandKind.CreatePool, "empty", [])).Code);
        Assert.Equal(DfsErrorCode.InvalidArgument, Apply(sm, new PoolCommand(Admin(), ControlCommandKind.CreatePool, "default", [ids[0]])).Code);
        var fastVolume = VolumeId.New(random);
        Assert.True(Apply(sm, new CreateVolumeCommand(Admin(), Creation(fastVolume, "fastvol", [ids[1]], "fast"), new byte[44])).IsSuccess);
        Assert.Equal(DfsErrorCode.PoolInUse, Apply(sm, new PoolCommand(Admin(), ControlCommandKind.DeletePool, "fast", [])).Code);
        Assert.Equal(["n1", "n2"], sm.State.PoolMembers("fast").Select(m => m.Name).Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(epoch, sm.State.PlacementEpoch);

        // A policy is checked against its pool's eligible nodes (POOL-03).
        var threeZones = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
        Assert.Equal(DfsErrorCode.InvalidPolicy, Apply(sm, new DefinePolicyCommand(Admin(), fastVolume, "three", threeZones)).Code);
        Assert.True(Apply(sm, new DefinePolicyCommand(Admin(), volume, "three", threeZones)).IsSuccess);

        Assert.Equal(DfsErrorCode.Unsupported, Apply(sm, new SetFeatureCommand(Admin(), "unknown-feature", true)).Code);
        Assert.True(Apply(sm, new SetFeatureCommand(Admin(), Server.Runtime.ProtocolFeatures.IrreversibleProbe, true)).IsSuccess);
        Assert.Equal(DfsErrorCode.Unsupported, Apply(sm, new SetFeatureCommand(Admin(), Server.Runtime.ProtocolFeatures.IrreversibleProbe, false)).Code);

        Assert.Equal(DfsErrorCode.InvalidArgument, Apply(sm, new AddDataKeyCommand(Admin(), volume, 3, new byte[44])).Code);
        Assert.Equal(2u, Assert.IsType<uint>(Apply(sm, new AddDataKeyCommand(Admin(), volume, 2, new byte[44])).Value));

        Assert.True(Apply(sm, new DeleteVolumeCommand(Admin(), volume)).IsSuccess);
        Assert.Equal(DfsPermissions.None, sm.State.GetPermissions("alice", volume));
        Assert.Null(sm.State.FindVolumeByName("vol"));

        // Admin commands are audited with principal, operation, outcome and scope, never secrets.
        string audit = string.Join('\n', sm.State.Audit.Select(a => a.ToString()));
        Assert.Contains("op=CreatePrincipal outcome=ok scope=principal=alice", audit, StringComparison.Ordinal);
        Assert.Contains("outcome=failed:AlreadyExists", audit, StringComparison.Ordinal);
        Assert.Contains("op=DeleteVolume outcome=ok scope=volume=vol", audit, StringComparison.Ordinal);
        Assert.DoesNotContain("pw", audit, StringComparison.Ordinal);
    }

    [Fact]
    public void MembershipCommands_FenceRemovedAndStaleIncarnations()
    {
        (ControlStateMachine sm, NodeId[] ids) = Bootstrapped();
        var labels = new DfsTopologyLabels("r1", "z1", "rk", "h1", "d");
        Assert.True(Apply(sm, new RegisterIncarnationCommand(Admin(), ids[1], 3, 77, labels, ["mem://n1b"], 100)).IsSuccess);
        Assert.Equal(DfsErrorCode.StaleIncarnation, Apply(sm, new RegisterIncarnationCommand(Admin(), ids[1], 2, 78, labels, [], 100)).Code);
        Assert.Equal(DfsErrorCode.StaleIncarnation, Apply(sm, new RegisterIncarnationCommand(Admin(), ids[1], 3, 79, labels, [], 100)).Code);
        Assert.True(Apply(sm, new RegisterIncarnationCommand(Admin(), ids[1], 3, 77, labels, [], 100)).IsSuccess);
        Assert.Equal("mem://n1b", sm.State.FindMember(ids[1])!.Endpoints[0]);

        Assert.True(Apply(sm, new SetMemberStatusCommand(Admin(), ids[2], WireNodeStatus.Removed, true)).IsSuccess);
        Assert.Equal(DfsErrorCode.AccessDenied, Apply(sm, new RegisterIncarnationCommand(Admin(), ids[2], 9, 1, labels, [], 1)).Code);
        Assert.Equal(DfsErrorCode.AccessDenied, Apply(sm, new AddMemberCommand(Admin(), ids[2], "n2", labels, [], 9, 1, 1, 1, [])).Code);

        var joiner = NodeId.New(new SeededRandom(44));
        Assert.Equal(DfsErrorCode.AlreadyExists, Apply(sm, new AddMemberCommand(Admin(), joiner, "n1", labels, [], 1, 1, 1, 1, [])).Code);
        ApplyOutcome joined = Apply(sm, new AddMemberCommand(Admin(), joiner, "n9", labels, ["mem://n9"], 1, 5, 1, 1, []));
        Assert.True(joined.IsSuccess);
        Assert.Equal(1UL, Assert.IsType<MemberRecord>(joined.Value).Incarnation);
    }

    [Fact]
    public void AdminRequestIds_AreDeduplicated()
    {
        (ControlStateMachine sm, NodeId[] ids) = Bootstrapped();
        var request = new UInt128(7, 7);
        var volume = VolumeId.New(new SeededRandom(3));
        ApplyOutcome first = Apply(sm, new CreateVolumeCommand(Admin(request), Creation(volume, "vol", [ids[0]]), new byte[44]));
        ApplyOutcome second = Apply(sm, new CreateVolumeCommand(Admin(request), Creation(VolumeId.New(new SeededRandom(4)), "vol", [ids[0]]), new byte[44]));
        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.Equal(volume, Assert.IsType<VolumeRecord>(second.Value).Id);
        Assert.Single(sm.State.Volumes);
    }

    [Fact]
    public void Checkpoint_RoundTripsExactly()
    {
        (ControlStateMachine sm, NodeId[] ids) = Bootstrapped();
        var random = new SeededRandom(12);
        Apply(sm, new CreateVolumeCommand(Admin(), Creation(VolumeId.New(random), "vol", [ids[0], ids[1]]) with
        {
            RootSecurity = DfsSecurityDescriptor.DefaultRoot(DfsIdentity.User("admin")),
        }, new byte[44]));
        Apply(sm, new GroupCommand(Admin(), ControlCommandKind.CreateGroup, "eng", null));
        Apply(sm, new PoolCommand(Admin(), ControlCommandKind.CreatePool, "fast", [ids[0]]));
        Apply(sm, new SetFeatureCommand(Admin(), Server.Runtime.ProtocolFeatures.Probe, true));
        byte[] image = Checkpoint(sm);

        var restored = new ControlStateMachine();
        restored.RestoreCheckpoint(new ReadOnlySequence<byte>(image), sm.AppliedIndex, 2);
        Assert.Equal(image, Checkpoint(restored));
        Assert.Equal(sm.State.ClusterId, restored.State.ClusterId);
        Assert.Equal("vol", restored.State.Volumes.Values.Single().Name);
        Assert.NotNull(restored.State.Volumes.Values.Single().Creation.RootSecurity);
        Assert.Contains(Server.Runtime.ProtocolFeatures.Probe, restored.State.Features);

        // Undecodable commands are deterministic failures with no effect.
        ApplyOutcome garbage = restored.Apply(restored.AppliedIndex + 1, 3, new byte[] { 9, 9, 9 });
        Assert.Equal(DfsErrorCode.ProtocolError, garbage.Code);
    }

    private static byte[] Checkpoint(ControlStateMachine sm)
    {
        var buffer = new ArrayBufferWriter<byte>();
        sm.WriteCheckpoint(buffer);
        return buffer.WrittenSpan.ToArray();
    }
}
