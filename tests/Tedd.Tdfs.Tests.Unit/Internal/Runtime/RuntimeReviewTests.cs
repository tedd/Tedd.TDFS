using System.Diagnostics.Metrics;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>
/// Adversarial review tests of the runtime core: crash points between durable writes, proposal correlation across
/// re-elections, storage failures, log retention of members still counted by the configuration, and identity binding.
/// </summary>
public sealed class RuntimeReviewTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Task<ApplyOutcome> ProposeAsync(NodeHost host, string group, string command, CancellationToken ct)
        => host.InvokeAsync(s => s.Raft.ProposeAsync(group, Encoding.UTF8.GetBytes(command), ProposalContext.None, ct));

    [Fact]
    public async Task HardStateTermBehindTheDurableLog_IsRepairedAtRestart()
    {
        await using var cluster = new RuntimeCluster(3, seed: 41);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode follower = cluster.Nodes.First(n => n != leader);
        GroupInspection leaderView = (await leader.Host!.Inspect.GetGroupAsync("control"))!;
        Assert.True(leaderView.Term > 1);
        await RuntimeCluster.WaitUntilAsync(async () => (await follower.Host!.Inspect.GetGroupAsync("control"))!.AppliedIndex >= leaderView.CommitIndex,
            "the follower holds entries of the current term");
        await cluster.StopAsync(follower);

        // A follower that first hears of a new term through an AppendEntries persists the entries (flush) and then the hard
        // state recording the term. A crash between the two leaves the persisted term behind the last durable entry.
        const string logDirectory = "node/groups/control/log";
        ulong lastTerm;
        using RecordCipher cipher = new KeyHierarchy(cluster.Secret).CreateJournalCipher(new SeededRandom(99));
        await using (RaftLogStore store = await RaftLogStore.OpenAsync(follower.Medium, logDirectory, new RaftLogStoreOptions { Cipher = cipher }, Ct))
        {
            lastTerm = store.LastTerm;
            Assert.True(lastTerm > 1);
            await store.SaveHardStateAsync(new RaftLogHardState(lastTerm - 1, null, store.HardState.Commit), Ct);
        }

        // The node must still start (the term is raised to the last log term, without a vote) and rejoin the group.
        await cluster.StartAsync(follower);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? g = await follower.Host!.Inspect.GetGroupAsync("control");
            GroupInspection? l = await leader.Host!.Inspect.GetGroupAsync("control");
            return g is not null && l is not null && g.Leader == l.Leader && g.Term >= lastTerm && g.AppliedIndex >= l.CommitIndex;
        }, "the restarted follower follows the leader again");
    }

    [Fact]
    public async Task ReelectedLeader_DecidesItsOverwrittenProposalInsteadOfOrphaningIt()
    {
        await using var cluster = new RuntimeCluster(3, seed: 42);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        string group = ProtocolInfo.VolumeGroup(volume.Id);
        RuntimeNode old = await cluster.WaitForLeaderAsync("vol");
        Assert.True((await ProposeAsync(old.Host!, group, "committed", Ct)).IsSuccess);
        GroupInspection before = (await old.Host!.Inspect.GetGroupAsync("vol"))!;

        // Cut the leader off and let it append three proposals (indexes i, i+1, i+2 of its term) that cannot commit.
        cluster.Links.Isolate(old.Name);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        Task<ApplyOutcome> p1 = ProposeAsync(old.Host, group, "lost-1", deadline.Token);
        Task<ApplyOutcome> p2 = ProposeAsync(old.Host, group, "lost-2", deadline.Token);
        Task<ApplyOutcome> p3 = ProposeAsync(old.Host, group, "lost-3", deadline.Token);
        await RuntimeCluster.WaitUntilAsync(async () => (await old.Host.Inspect.GetGroupAsync("vol"))!.LastIndex == before.LastIndex + 3,
            "the isolated leader appended its three proposals");

        // The others elect a leader whose no-op takes index i; nothing else is proposed there.
        RuntimeNode[] others = cluster.Nodes.Where(n => n != old).ToArray();
        RuntimeNode next = await cluster.WaitForLeaderAsync("vol", others);
        await RuntimeCluster.WaitUntilAsync(async () => (await next.Host!.Inspect.GetGroupAsync("vol"))!.CommitIndex == before.LastIndex + 1,
            "the new leader committed its no-op at the first lost index");

        // Healed, the old leader learns the no-op at i: its first proposal lost there.
        cluster.Links.Heal(old.Name);
        DfsUnavailableException lost1 = await Assert.ThrowsAsync<DfsUnavailableException>(() => p1.WaitAsync(TimeSpan.FromSeconds(20), Ct));
        Assert.Equal(DfsErrorCode.NotLeader, lost1.ErrorCode);

        // Re-elected, its own no-op takes i+1 (deciding the second), and a new proposal takes i+2, where the third still waits.
        Assert.True(await next.Host!.InvokeAsync(s => s.Raft.TransferLeadershipAsync(group, old.Id, TimeSpan.FromSeconds(10))));
        await cluster.WaitForLeaderAsync("vol", [old]);
        DfsUnavailableException lost2 = await Assert.ThrowsAsync<DfsUnavailableException>(() => p2.WaitAsync(TimeSpan.FromSeconds(20), Ct));
        Assert.Equal(DfsErrorCode.NotLeader, lost2.ErrorCode);
        ApplyOutcome fresh = await ProposeAsync(old.Host, group, "fresh", Ct);
        Assert.True(fresh.IsSuccess);
        Assert.Equal(before.LastIndex + 3, fresh.Index);

        // The earlier proposal at the same index must be decided as lost, not orphaned until its deadline.
        DfsUnavailableException lost3 = await Assert.ThrowsAsync<DfsUnavailableException>(() => p3.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.Equal(DfsErrorCode.NotLeader, lost3.ErrorCode);
        TestVolumeStateMachine sm = (await RuntimeVolumeTests.VolumeStateAsync(old.Host, "vol"))!;
        Assert.DoesNotContain(await old.Host.InvokeAsync(_ => sm.Applied.ToArray()), a => a.StartsWith("lost-", StringComparison.Ordinal));
    }

    [Fact]
    public async Task UnreachableLearner_IsNotPromotedBeforeItCaughtUp()
    {
        await using var cluster = new RuntimeCluster(4, seed: 48);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode volumeLeader = await cluster.WaitForLeaderAsync("vol");
        RuntimeNode controlLeader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode outsider = cluster.Nodes.Single(n => !volume.Voters.Contains(n.Id));
        RuntimeNode drained = cluster.Nodes.First(n => volume.Voters.Contains(n.Id) && n != volumeLeader && n != controlLeader);

        // The replacement is unreachable: it is added as a learner but has acknowledged nothing. A fresh learner's
        // progress starts "recently active" with match 0, which a lag check against a small commit index accepts.
        cluster.Links.Isolate(outsider.Name);
        RawClient admin = await cluster.ConnectAsync(controlLeader);
        await admin.AdminAsync<AdminNodeNameBody, AdminEmptyBody>(AdminOp.DrainNode, new AdminNodeNameBody(drained.Name));
        await RuntimeCluster.WaitUntilAsync(async () =>
            (await volumeLeader.Host!.Inspect.GetGroupAsync("vol"))?.Learners.Contains(outsider.Name) == true, "the replacement was added as a learner");
        for (int probe = 0; probe < 20; probe++)
        {
            GroupInspection view = (await volumeLeader.Host!.Inspect.GetGroupAsync("vol"))!;
            Assert.DoesNotContain(outsider.Name, view.Voters);
            Assert.Contains(drained.Name, view.Voters);
            await Task.Delay(50, Ct);
        }

        // Reachable again, it catches up, is promoted, and only then the drained voter leaves.
        cluster.Links.Heal(outsider.Name);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? view = await volumeLeader.Host!.Inspect.GetGroupAsync("vol");
            return view is not null && view.Voters.Contains(outsider.Name) && !view.Voters.Contains(drained.Name) && view.Learners.Count == 0;
        }, "the caught-up replacement replaced the drained voter");
    }

    [Fact]
    public async Task UnlistedVoterStillCountedByTheConfiguration_KeepsItsLog()
    {
        await using var cluster = new RuntimeCluster(4, seed: 43);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode volumeLeader = await cluster.WaitForLeaderAsync("vol");
        RuntimeNode controlLeader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode outsider = cluster.Nodes.Single(n => !volume.Voters.Contains(n.Id));
        RuntimeNode drained = cluster.Nodes.First(n => volume.Voters.Contains(n.Id) && n != volumeLeader && n != controlLeader);
        string groupDirectory = "node/groups/vol-" + volume.Id.Value.ToString("N");
        Assert.True(await drained.Medium.ExistsAsync(groupDirectory, Ct));

        // The replacement voter is cut off, so the volume leader can add it as a learner but never promote it, and the
        // drained voter keeps its seat in the group's configuration.
        cluster.Links.Isolate(outsider.Name);
        RawClient admin = await cluster.ConnectAsync(controlLeader);
        await admin.AdminAsync<AdminNodeNameBody, AdminEmptyBody>(AdminOp.DrainNode, new AdminNodeNameBody(drained.Name));
        await RuntimeCluster.WaitUntilAsync(async () => await drained.Host!.InvokeAsync(s =>
            s.Control.State.FindVolume(volume.Id) is { } record && !record.Voters.Contains(drained.Id)), "the record no longer lists the drained voter");

        // The drained node is still a voter of the applied configuration: it keeps hosting the group however long the
        // transition takes, well past the registry's grace period for unlisted learners (leaving would reduce the group's
        // failure tolerance while its quorum still counts this voter, RCV-01; INTERNALS.md §1.3) ...
        TimeSpan grace = await drained.Host!.InvokeAsync(s => s.Timings.Tick * s.Timings.ElectionTicksMax * 10);
        DateTime until = DateTime.UtcNow + grace + grace;
        while (DateTime.UtcNow < until)
        {
            Assert.True(await drained.Host!.Inspect.GetGroupAsync("vol") is not null, "the drained node left the group while it was still a voter");
            await Task.Delay(100, Ct);
        }

        RuntimeNode currentLeader = await cluster.WaitForLeaderAsync("vol", cluster.Nodes.Where(n => n != outsider));
        GroupInspection leaderView = (await currentLeader.Host!.Inspect.GetGroupAsync("vol"))!;
        Assert.Contains(drained.Name, leaderView.Voters);
        Assert.DoesNotContain(outsider.Name, leaderView.Voters);

        // ... and a member the configuration still counts as a voter never loses its log.
        Assert.True(await drained.Medium.ExistsAsync(groupDirectory + "/log", Ct), "the drained voter's log was deleted while it was still a voter");
    }

    [Fact]
    public async Task FollowerJournalFlushFailure_IsRecoveredByReopeningTheLog()
    {
        await using var cluster = new RuntimeCluster(3, seed: 44);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        string group = ProtocolInfo.VolumeGroup(volume.Id);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        RuntimeNode follower = cluster.Nodes.First(n => n != leader);
        await ProposeAsync(leader.Host!, group, "before", Ct);
        await RuntimeCluster.WaitUntilAsync(async () => (await RuntimeVolumeTests.VolumeSummaryAsync(follower.Host!, "vol")).Count == 1,
            "the follower applied the first command");

        // One flush of the follower's volume journal fails: the journal faults and must be reopened.
        follower.Medium.Faults.PathFilter = p => p.Contains("/vol-", StringComparison.Ordinal) && p.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal);
        follower.Medium.Faults.FailNextFlushes = 1;
        for (int i = 0; i < 10; i++)
            Assert.True((await ProposeAsync(leader.Host!, group, "after-" + i, Ct)).IsSuccess);
        Assert.Equal(0, follower.Medium.Faults.FailNextFlushes);
        follower.Medium.Faults.Reset();

        (long count, string hash, _) = await RuntimeVolumeTests.VolumeSummaryAsync(leader.Host!, "vol");
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            (long c, string h, _) = await RuntimeVolumeTests.VolumeSummaryAsync(follower.Host!, "vol");
            return c == count && h == hash;
        }, "the follower recovered from the failed flush and applied every command", TimeSpan.FromSeconds(20));
    }

    [Fact]
    public async Task CrashBetweenSuperblockAndControlLog_SingleNodeClusterRecovers()
    {
        await using var cluster = new RuntimeCluster(1, seed: 45);
        RuntimeNode node = cluster[0];
        var faults = new RecordingFaultInjector();
        int fired = 0;
        faults.OnHit = ctx => ctx.Site == FaultSites.RecoveryJournalReplay && Interlocked.Exchange(ref fired, 1) == 0
            ? throw new IOException("injected crash before the control log was written")
            : ValueTask.CompletedTask;
        cluster.Faults = faults;
        await Assert.ThrowsAsync<IOException>(() => cluster.StartAsync(node));
        node.Medium.Crash(CrashMode.LoseUnflushed);
        Assert.True(await node.Medium.ExistsAsync("node/" + Superblock.DefaultFileName, Ct), "the superblock was created before the crash");

        cluster.Faults = NoFaults.Instance;
        await cluster.StartAsync(node);
        await cluster.WaitForLeaderAsync("control", timeout: TimeSpan.FromSeconds(15));
        await cluster.WaitForRegistrationAsync();
    }

    [Fact]
    public async Task JoinOrRegistration_MustDescribeTheAuthenticatedIncarnation()
    {
        await using var cluster = new RuntimeCluster(3, seed: 46);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode member = cluster.Nodes.First(n => n != leader);
        ulong registered = member.Host!.Incarnation;

        // A process authenticated as a newer incarnation of the member must not register another incarnation through a join.
        var keys = new KeyHierarchy(cluster.Secret);
        IRpcTransport transport = cluster.Network.Factory.Create("impostor", "mem://impostor");
        await using (transport)
        {
            IRpcConnection connection = await transport.ConnectAsync(leader.Endpoint, Ct);
            var credentials = new NodeCredentials(cluster.ClusterId, member.Name, member.Id, registered + 1, 4242, keys.NodeAuthKey, ProtocolFeatures.Supported);
            await HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, new SeededRandom(7), TimeSpan.FromSeconds(10), Ct);
            var join = new JoinClusterMessage(member.Id, member.Name, member.Labels, ["mem://impostor"], registered + 50, 1L << 30, ProtocolInfo.CurrentVersion,
                ProtocolFeatures.Supported, 4242);
            var reply = MessageCodec.Decode((await connection.CallAsync(MessageCodec.Encode(join), TimeSpan.FromSeconds(10), Ct)).Span);
            Assert.Equal(DfsErrorCode.AccessDenied, Assert.IsType<ErrorReplyMessage>(reply).Code);

            var wrongBoot = new RegisterIncarnationMessage(member.Id, registered + 1, member.Name, member.Labels, ["mem://impostor"], 1L << 30, 9999);
            reply = MessageCodec.Decode((await connection.CallAsync(MessageCodec.Encode(wrongBoot), TimeSpan.FromSeconds(10), Ct)).Span);
            Assert.Equal(DfsErrorCode.AccessDenied, Assert.IsType<ErrorReplyMessage>(reply).Code);
        }

        // The real process is still the registered incarnation and keeps working.
        ClusterMapData map = await leader.Host!.InvokeAsync(s => s.Control.BuildClusterMap());
        Assert.Equal(registered, map.Nodes.Single(n => n.Name == member.Name).Incarnation);
        Assert.Equal(member.Endpoint, map.Nodes.Single(n => n.Name == member.Name).Endpoints[0]);
        Assert.True(await member.Host.InvokeAsync(s => s.Control.IsSelfRegistered));
    }

    [Fact]
    public async Task Kill_StopsPublishingTheDeadProcessMetrics()
    {
        await using var cluster = new RuntimeCluster(1, seed: 47);
        await cluster.StartAllAsync();
        await cluster.WaitForLeaderAsync("control");
        RuntimeNode node = cluster[0];
        Meter meter = await node.Host!.InvokeAsync(s => s.Metrics.Meter);
        int completed = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, meter))
                l.EnableMeasurementEvents(instrument);
        };
        listener.MeasurementsCompleted = (instrument, _) =>
        {
            if (ReferenceEquals(instrument.Meter, meter))
                Interlocked.Increment(ref completed);
        };
        listener.Start();

        cluster.Kill(node);
        Assert.True(Volatile.Read(ref completed) > 0, "the killed node's meter still publishes its instruments");
    }
}
