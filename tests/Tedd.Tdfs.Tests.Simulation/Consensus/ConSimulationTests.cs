using System.Globalization;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Con.ConHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Con;

/// <summary>
/// CON — consensus, authority and metadata recovery (level S, deterministic whole-cluster simulation).
/// FSD §§3.2, 7, 8, 13, 14.1; invariants INV-01, INV-03, INV-04, INV-05, INV-11, INV-18.
/// Safety properties are checked during the fault and during recovery, not only after convergence (VER-05); every
/// liveness wait is bounded and states its prerequisite.
/// </summary>
public sealed class ConSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // CON-002 ThreeVoterMajority
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-002", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("isolate-leader")]
    [InlineData("isolate-follower")]
    [InlineData("no-majority-1-1-1")]
    public async Task CON_002_ThreeVoterMajority(string shape)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsVolume setupVolume = setup.GetVolume(Vol);
            byte[] baseline = TestData.Bytes(0x0201, 6000);
            CommitToken baselineToken = await setupVolume.Files.WriteAllBytesAsync("/baseline", baseline, Ct);
            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            TestNode isolated = shape == "isolate-follower" ? followers[0] : leader;
            TestNode[] rest = Others(cluster, isolated);

            await using DfsClient minority = await cluster.ConnectAsync(
                new TestClientOptions { Name = "minority", SeedNodes = [isolated.Name], OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume minorityVolume = minority.GetVolume(Vol);
            DfsOperationId minorityOp = await minorityVolume.NewOperationIdAsync(Ct);

            await using DfsClient majority = await cluster.ConnectAsync(
                new TestClientOptions { Name = "majority", SeedNodes = Names(rest), OperationTimeout = shape == "no-majority-1-1-1" ? ShortOperationTimeout : ElectionBound }, Ct);
            DfsVolume majorityVolume = majority.GetVolume(Vol);
            DfsOperationId majorityOp = await majorityVolume.NewOperationIdAsync(Ct);

            // Every entry proposed after the partition has an index above this bound; in-flight messages may still spread
            // knowledge of earlier commits, so "nothing new committed" is CommitIndex <= lastBefore, not equality.
            ulong lastBefore = await MaxLastIndexAsync(cluster.Nodes);

            if (shape == "no-majority-1-1-1")
                cluster.Network.Partition([isolated.Name, ClientNet("minority")], [rest[0].Name, ClientNet("majority")], [rest[1].Name]);
            else
                cluster.Network.Partition([isolated.Name, ClientNet("minority")], [rest[0].Name, rest[1].Name, ClientNet("majority")]);

            // One voter alone can never commit new metadata, whatever its role was.
            byte[] minorityBytes = TestData.Bytes(0x0202, 3000);
            Exception? minorityError = await CaptureAsync(() => minorityVolume.Files.WriteAllBytesAsync("/minority", minorityBytes, WithId(minorityOp), Ct));
            AssertNoCommitClaimed(minorityError, $"[{shape}] a lone voter ({isolated.Name})");
            Assert.True((await GroupOnAsync(isolated)).CommitIndex <= lastBefore, $"[{shape}] the lone voter {isolated.Name} committed an entry proposed after the partition");
            Assert.Null(await FileOnReplicaAsync(cluster, isolated.Name, "/minority"));

            byte[] majorityBytes = TestData.Bytes(0x0203, 3000);
            if (shape == "no-majority-1-1-1")
            {
                Exception? error = await CaptureAsync(() => majorityVolume.Files.WriteAllBytesAsync("/majority", majorityBytes, WithId(majorityOp), Ct));
                AssertNoCommitClaimed(error, "1+1+1: no group of one voter may commit");
                foreach (TestNode node in cluster.Nodes)
                    Assert.True((await GroupOnAsync(node)).CommitIndex <= lastBefore, $"1+1+1: {node.Name} committed an entry proposed after the partition");
            }
            else
            {
                // Two connected eligible voters progress (re-electing when the leader was the isolated one).
                await WaitForLeaderAmongAsync(cluster, rest, $"[{shape}] the two connected voters must have a leader");
                CommitToken committed = await majorityVolume.Files.WriteAllBytesAsync("/majority", majorityBytes, WithId(majorityOp), Ct);
                Assert.True(committed.IsComparableWith(baselineToken), "tokens of one volume epoch must be comparable");
                Assert.True(committed.CompareTo(baselineToken) > 0, "a later commit must have a later token");
                await AssertContentAsync(majorityVolume, "/majority", majorityBytes, "majority write must be readable");

                // With a quorum of two out of three, both connected voters must hold the committed entry.
                foreach (TestNode node in rest)
                {
                    GroupInspection group = await GroupOnAsync(node);
                    Assert.True(group.LastIndex >= committed.Sequence, $"{node.Name} must persist entry {committed.Sequence} (last {group.LastIndex})");
                }

                Assert.True((await GroupOnAsync(isolated)).CommitIndex < committed.Sequence, "the isolated voter cannot know of the new commit");
                Assert.Null(await FileOnReplicaAsync(cluster, isolated.Name, "/majority"));
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ElectionBound, Ct);

            if (shape == "no-majority-1-1-1")
            {
                // The old leader may have appended the minority write locally. After healing it may win the election (its
                // log is the most up to date) and then commit that entry legitimately, or lose and have it overwritten.
                // Either way the resolution must be definite and match the namespace and what the client was told.
                await AssertResolvedConsistentlyAsync(majorityVolume, minorityOp, minorityError,
                    () => AssertContentAsync(majorityVolume, "/minority", minorityBytes, "a minority write committed after healing"),
                    () => AssertAbsentAsync(majorityVolume, "/minority", "an aborted minority write"),
                    "1+1+1 minority write");
            }
            else
            {
                // The majority committed in a higher term at the overlapping index, so whatever the isolated voter
                // appended is overwritten: resolving the minority request yields a definite abort.
                DfsOperationOutcome minorityOutcome = await majorityVolume.QueryOperationAsync(minorityOp, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, minorityOutcome.Status);
                await AssertAbsentAsync(majorityVolume, "/minority", "an aborted minority write");
            }

            if (shape == "no-majority-1-1-1")
            {
                DfsOperationOutcome outcome = await majorityVolume.QueryOperationAsync(majorityOp, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
                Assert.False(await majorityVolume.Files.ExistsAsync("/majority", Ct));
                await majorityVolume.Files.WriteAllBytesAsync("/majority", majorityBytes, Ct);
            }

            await AssertContentAsync(majorityVolume, "/majority", majorityBytes, "after healing");
            await AssertContentAsync(majorityVolume, "/baseline", baseline, "pre-partition commit must survive");
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-003 FiveVoterMajority
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-003", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("3+2-leader-in-majority")]
    [InlineData("3+2-leader-in-minority")]
    [InlineData("2+2+1")]
    [InlineData("3+2-then-majority-loses-one")]
    public async Task CON_003_FiveVoterMajority(string shape)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.FiveNodes), async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            byte[] baseline = TestData.Bytes(0x0301, 5000);
            await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/baseline", baseline, Ct);
            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] o = Others(cluster, leader);
            (TestNode[] minorityNodes, TestNode[] majorityNodes, TestNode[]? third) = shape switch
            {
                "3+2-leader-in-majority" or "3+2-then-majority-loses-one" => (new[] { o[2], o[3] }, new[] { leader, o[0], o[1] }, (TestNode[]?)null),
                "3+2-leader-in-minority" => (new[] { leader, o[0] }, new[] { o[1], o[2], o[3] }, null),
                "2+2+1" => (new[] { leader, o[0] }, new[] { o[1], o[2] }, new[] { o[3] }),
                _ => throw new ArgumentOutOfRangeException(nameof(shape)),
            };
            bool majorityCanCommit = shape is not "2+2+1";

            await using DfsClient minority = await cluster.ConnectAsync(
                new TestClientOptions { Name = "minority", SeedNodes = Names(minorityNodes), OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume minorityVolume = minority.GetVolume(Vol);
            DfsOperationId minorityOp = await minorityVolume.NewOperationIdAsync(Ct);
            await using DfsClient majority = await cluster.ConnectAsync(
                new TestClientOptions { Name = "majority", SeedNodes = Names(majorityNodes), OperationTimeout = majorityCanCommit ? ElectionBound : ShortOperationTimeout }, Ct);
            DfsVolume majorityVolume = majority.GetVolume(Vol);
            DfsOperationId majorityOp = await majorityVolume.NewOperationIdAsync(Ct);
            DfsOperationId twoOfFiveOp = await majorityVolume.NewOperationIdAsync(Ct);
            byte[] twoOfFiveBytes = TestData.Bytes(0x0304, 100);
            Exception? twoOfFiveError = null;

            // Entries proposed after the partition have indexes above this bound (see CON-002).
            ulong lastBefore = await MaxLastIndexAsync(cluster.Nodes);

            var groups = new List<string[]>();
            groups.Add([.. Names(minorityNodes), ClientNet("minority")]);
            groups.Add([.. Names(majorityNodes), ClientNet("majority")]);
            if (third is not null)
                groups.Add(Names(third));
            cluster.Network.Partition([.. groups]);

            // Two disconnected survivors never advertise a successful authoritative change.
            byte[] minorityBytes = TestData.Bytes(0x0302, 2000);
            Exception? minorityError = await CaptureAsync(() =>
                minorityVolume.Files.WriteAllBytesAsync("/minority", minorityBytes, WithId(minorityOp), Ct));
            AssertNoCommitClaimed(minorityError, $"[{shape}] two of five voters");
            foreach (TestNode node in minorityNodes)
            {
                Assert.True((await GroupOnAsync(node)).CommitIndex <= lastBefore, $"[{shape}] minority voter {node.Name} committed an entry proposed after the partition");
                Assert.Null(await FileOnReplicaAsync(cluster, node.Name, "/minority"));
            }

            // A minority leader steps down (CheckQuorum) rather than keep advertising authority.
            await cluster.WaitUntilAsync(async () => await LeaderAmongAsync(minorityNodes) is null, ElectionBound,
                $"[{shape}] no minority node may keep claiming leadership", Ct);

            byte[] majorityBytes = TestData.Bytes(0x0303, 4000);
            if (majorityCanCommit)
            {
                await WaitForLeaderAmongAsync(cluster, majorityNodes, $"[{shape}] three connected voters must have a leader");
                CommitToken committed = await majorityVolume.Files.WriteAllBytesAsync("/majority", majorityBytes, WithId(majorityOp), Ct);
                await AssertContentAsync(majorityVolume, "/majority", majorityBytes, "majority write");
                int holders = 0;
                foreach (TestNode node in majorityNodes)
                {
                    if ((await GroupOnAsync(node)).LastIndex >= committed.Sequence)
                        holders++;
                }

                Assert.True(holders >= 3, $"a commit with five voters requires three durable copies of the entry; found {holders}");
                foreach (TestNode node in minorityNodes)
                {
                    GroupInspection group = await GroupOnAsync(node);
                    Assert.True(group.CommitIndex < committed.Sequence, $"partitioned minority voter {node.Name} cannot know of the majority's commit {committed.Sequence} (commit {group.CommitIndex})");
                    Assert.Null(await FileOnReplicaAsync(cluster, node.Name, "/majority"));
                }

                if (shape == "3+2-then-majority-loses-one")
                {
                    // Losing one of the three leaves two of five: no further authoritative change is possible.
                    TestNode currentLeader = (await LeaderAmongAsync(majorityNodes))!;
                    TestNode victim = majorityNodes.First(n => n.Name != currentLeader.Name);
                    await victim.CrashAsync(CrashMode.LoseUnflushed);
                    TestNode[] survivors = majorityNodes.Where(n => n.Name != victim.Name).ToArray();
                    ulong survivorLastBefore = await MaxLastIndexAsync(survivors);
                    var shortClient = await cluster.ConnectAsync(new TestClientOptions { Name = "survivor", SeedNodes = Names(survivors), OperationTimeout = ShortOperationTimeout }, Ct);
                    twoOfFiveError = await CaptureAsync(() => shortClient.GetVolume(Vol).Files.WriteAllBytesAsync("/two-of-five", twoOfFiveBytes, WithId(twoOfFiveOp), Ct));
                    AssertNoCommitClaimed(twoOfFiveError, "two surviving voters of five");
                    foreach (TestNode node in survivors)
                        Assert.True((await GroupOnAsync(node)).CommitIndex <= survivorLastBefore, $"survivor {node.Name} committed an entry proposed with two of five voters");
                    await shortClient.DisposeAsync();
                    await victim.RestartAsync();
                }
            }
            else
            {
                Exception? error = await CaptureAsync(() => majorityVolume.Files.WriteAllBytesAsync("/majority", majorityBytes, WithId(majorityOp), Ct));
                AssertNoCommitClaimed(error, "2+2+1: no side holds three voters");
                foreach (TestNode node in cluster.Nodes)
                    Assert.True((await GroupOnAsync(node)).CommitIndex <= lastBefore, $"2+2+1: {node.Name} committed an entry proposed after the partition");
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();

            await RecoverAndConvergeAsync(cluster);
            if (shape == "2+2+1")
            {
                // The old leader and its partner both hold the minority write: after healing either one of them wins the
                // election and commits it, or a voter without it wins and overwrites it. The outcome is resolved, not guessed.
                await AssertResolvedConsistentlyAsync(majorityVolume, minorityOp, minorityError,
                    () => AssertContentAsync(majorityVolume, "/minority", minorityBytes, "a minority write committed after healing"),
                    () => AssertAbsentAsync(majorityVolume, "/minority", "an aborted minority write"),
                    "2+2+1 minority write");
            }
            else
            {
                // A majority committed in a higher term at the overlapping indexes (or the request never reached a
                // leader): the minority request is definitely aborted.
                DfsOperationOutcome minorityOutcome = await majorityVolume.QueryOperationAsync(minorityOp, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, minorityOutcome.Status);
                await AssertAbsentAsync(majorityVolume, "/minority", "an aborted minority write");
            }

            if (shape == "3+2-then-majority-loses-one")
            {
                // The two survivors may both hold the write; whether a later leader commits it depends on who wins after
                // the victim and the minority return. Resolution must be definite and consistent.
                await AssertResolvedConsistentlyAsync(majorityVolume, twoOfFiveOp, twoOfFiveError,
                    () => AssertContentAsync(majorityVolume, "/two-of-five", twoOfFiveBytes, "a two-of-five write committed after recovery"),
                    () => AssertAbsentAsync(majorityVolume, "/two-of-five", "an aborted two-of-five write"),
                    "two-of-five write");
            }

            if (!majorityCanCommit)
            {
                Assert.Equal(DfsOutcomeStatus.Aborted, (await majorityVolume.QueryOperationAsync(majorityOp, resolve: true, Ct)).Status);
                Assert.False(await majorityVolume.Files.ExistsAsync("/majority", Ct));
            }

            await AssertContentAsync(majorityVolume, "/baseline", baseline, "pre-partition commit must survive");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-004 PersistTermAndVoteBeforeReply
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-004", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("vote-flush-fails", CrashMode.LoseUnflushed, 1UL)]
    [InlineData("vote-flush-delayed-then-crash", CrashMode.LoseUnflushed, 1UL)]
    [InlineData("vote-flush-delayed-then-crash", CrashMode.TearLastWrite, 2UL)]
    [InlineData("crash-during-election", CrashMode.LoseUnflushed, 1UL)]
    [InlineData("crash-during-election", CrashMode.LoseUnflushed, 2UL)]
    [InlineData("crash-during-election", CrashMode.LoseUnflushed, 3UL)]
    [InlineData("crash-during-election", CrashMode.TearLastWrite, 4UL)]
    [InlineData("crash-during-election", CrashMode.RandomSubset, 5UL)]
    public async Task CON_004_PersistTermAndVoteBeforeReply(string variant, CrashMode crashMode, ulong seed)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { Seed = 0x0400 + seed }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] initial = TestData.Bytes(0x0401, 3000);
            await volume.Files.WriteAllBytesAsync("/initial", initial, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            if (variant is "vote-flush-fails" or "vote-flush-delayed-then-crash")
            {
                // A vote (or an own candidacy) that is not yet durable must never be sent. With the leader isolated, the
                // other two voters can only elect a leader if the voter whose persistence fails (or is still in progress)
                // grants a vote. In the delayed variant every I/O of that voter takes 3 s (a vote needs at least a write
                // and a flush, so it cannot be durable before 6 s); probing for 2 s therefore observes exactly the window
                // in which the vote exists only in memory, and the crash afterwards discards it.
                bool delayed = variant == "vote-flush-delayed-then-crash";
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                TestNode[] followers = Others(cluster, leader);
                TestNode healthy = followers[0];
                TestNode broken = followers[1];
                ulong lastBefore = await MaxLastIndexAsync(cluster.Nodes);
                if (delayed)
                    broken.Disk.Faults.IoDelay = TimeSpan.FromSeconds(3);
                else
                    broken.Disk.Faults.FailNextFlushes = 1_000_000;
                cluster.Network.Isolate(leader.Name);

                int probes = delayed ? 10 : 15;
                for (int probe = 0; probe < probes; probe++)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct);
                    TestNode? elected = await LeaderAmongAsync(followers);
                    Assert.True(elected is null,
                        $"[{variant}] probe {probe}: {elected?.Name} became leader although {broken.Name} could not yet have persisted any vote or term");
                    Assert.True((await GroupOnAsync(healthy)).CommitIndex <= lastBefore, $"probe {probe}: {healthy.Name} committed a new entry without a leader");
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                }

                // Restore the disk; recover the broken node from its durable image (it may have failed-stop already). In
                // the delayed variant the crash discards the vote that was never made durable: after the restart the node
                // must not behave as if it had voted (no second leader of a term it voted in).
                broken.Disk.Faults.FailNextFlushes = 0;
                broken.Disk.Faults.IoDelay = null;
                if (broken.State == TestNodeState.Running)
                    await broken.CrashAsync(crashMode);
                await broken.RestartAsync();
                await WaitForLeaderAmongAsync(cluster, followers, "once votes can be persisted again an election must succeed");
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await volume.Files.WriteAllBytesAsync("/after", TestData.Bytes(0x0402, 100), Ct);
                await RecoverAndConvergeAsync(cluster);
                await AssertContentAsync(volume, "/initial", initial, "committed data must survive");
                return;
            }

            // The leader crashes, so the two followers must elect; one of them crashes at a seeded point inside that
            // election, and both return from their durable images in a seeded order. The returning old leader is a third
            // voter that has not voted in the new term: if a vote had been sent before it was durable, the restarted
            // voter could campaign in the term it already voted in and win with the old leader's vote, giving that term
            // two leaders. No term may ever have two leaders.
            var random = new SeededRandom(seed);
            var markers = new List<(string Path, byte[] Bytes)>();
            for (int round = 0; round < 4; round++)
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                TestNode[] followers = Others(cluster, leader);
                await leader.CrashAsync(crashMode);
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(400)), Ct);
                TestNode victim = followers[random.Next(followers.Length)];
                await victim.CrashAsync(crashMode);
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(300)), Ct);
                TestNode first = random.Next(2) == 0 ? victim : leader;
                TestNode second = ReferenceEquals(first, victim) ? leader : victim;
                await first.RestartAsync();
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(200)), Ct);
                await second.RestartAsync();
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.WaitForStableAsync(ElectionBound, Ct);
                await cluster.Inspect.AssertNoSplitBrainAsync();

                byte[] marker = TestData.Bytes(0x0410 + (ulong)round, 500);
                string path = "/round" + round.ToString(CultureInfo.InvariantCulture);
                await volume.Files.WriteAllBytesAsync(path, marker, Ct);
                markers.Add((path, marker));
            }

            await RecoverAndConvergeAsync(cluster);
            await AssertContentAsync(volume, "/initial", initial, "committed data must survive every crash");
            foreach ((string path, byte[] bytes) in markers)
                await AssertContentAsync(volume, path, bytes, "every acknowledged round marker must survive");
        }, Ct);
    }

    /// <summary>
    /// Core-level complement of CON-004: production Raft cores hosted by <see cref="RaftSimulation"/> are crashed and
    /// restarted from persisted hard state and log only, at random points of elections; election safety must hold after
    /// every step.
    /// </summary>
    [Theory, Catalog("CON-004", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData(3, 11UL)]
    [InlineData(3, 12UL)]
    [InlineData(5, 13UL)]
    [InlineData(5, 14UL)]
    public void CON_004_PersistTermAndVoteBeforeReply_CoreCrashRestart(int voters, ulong seed)
    {
        var sim = new RaftSimulation(voters, seed);
        var random = new SeededRandom(SeededRandom.DeriveSeed(seed, "con-004"));
        var crashed = new SortedSet<int>();
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        Assert.NotNull(sim.LeaderIndex);
        for (int step = 0; step < 400; step++)
        {
            int action = random.Next(4);
            if (action == 0 && crashed.Count < (voters - 1) / 2)
            {
                // Crash a node at an arbitrary tick (often mid-election: elections are frequent with the leader isolated).
                int node = random.Next(voters);
                if (crashed.Add(node))
                    sim.Crash(node);
            }
            else if (action == 1 && crashed.Count > 0)
            {
                int node = crashed.Min;
                crashed.Remove(node);
                sim.Restart(node);
            }
            else if (action == 2 && sim.LeaderIndex is int leader)
            {
                sim.Partition([leader], Enumerable.Range(0, voters).Where(i => i != leader).ToArray());
            }
            else
            {
                sim.Heal();
            }

            sim.RunTicks(1 + random.Next(40));
            string? violation = sim.CheckSafety();
            Assert.True(violation is null, $"step {step}: {violation}{Environment.NewLine}{string.Join(Environment.NewLine, sim.Trace.TakeLast(30))}");
        }

        sim.Heal();
        foreach (int node in crashed)
            sim.Restart(node);
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        Assert.NotNull(sim.LeaderIndex);
        Assert.Null(sim.CheckSafety());
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-005 CommittedLogSurvivesLeaderChange
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-005", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("crash-leader")]
    [InlineData("stop-leader")]
    [InlineData("isolate-leader")]
    [InlineData("crash-leader-twice")]
    public async Task CON_005_CommittedLogSurvivesLeaderChange(string change)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);

            // Committed mutations of every kind, with their tokens and identities.
            byte[] big = TestData.Bytes(0x0501, 70_000);
            byte[] small = TestData.Bytes(0x0502, 200);
            byte[] prefix = TestData.Bytes(0x0503, 1000);
            byte[] suffix = TestData.Bytes(0x0504, 500);
            CommitToken tBig = await volume.Files.WriteAllBytesAsync("/big", big, Ct);
            await volume.Directories.CreateDirectoryAsync("/d/e", Ct);
            await volume.Files.WriteAllBytesAsync("/small", small, Ct);
            await volume.Files.WriteAllBytesAsync("/log", prefix, Ct);
            CommitToken tAppend = await volume.Files.AppendAllBytesAsync("/log", suffix, Ct);
            DfsFileInfo smallInfo = await volume.Files.GetInfoAsync("/small", Ct);
            CommitToken tMove = await volume.Files.MoveAsync("/small", "/d/e/small", false, Ct);
            await volume.Files.WriteAllBytesAsync("/gone", TestData.Bytes(0x0505, 100), Ct);
            CommitToken tDelete = await volume.Files.DeleteAsync("/gone", Ct);
            CommitToken tCopy = await volume.Files.CopyAsync("/big", "/bigcopy", false, Ct);
            Assert.True(tBig.CompareTo(tAppend) < 0 && tAppend.CompareTo(tMove) < 0 && tMove.CompareTo(tDelete) < 0 && tDelete.CompareTo(tCopy) < 0);
            DfsFileInfo bigInfo = await volume.Files.GetInfoAsync("/big", Ct);
            DfsFileInfo logInfo = await volume.Files.GetInfoAsync("/log", Ct);
            DfsFileInfo copyInfo = await volume.Files.GetInfoAsync("/bigcopy", Ct);

            // Outcome records of operations whose replies never reached any client (so no client acknowledged them).
            byte[] prefix2 = TestData.Bytes(0x0506, 800);
            byte[] suffix2 = TestData.Bytes(0x0507, 300);
            byte[] wBytes = TestData.Bytes(0x0508, 9000);
            await volume.Files.WriteAllBytesAsync("/log2", prefix2, Ct);
            DfsClient lost = await cluster.ConnectAsync(new TestClientOptions { Name = "lost", OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume lostVolume = lost.GetVolume(Vol);
            DfsOperationId kAppend = await lostVolume.NewOperationIdAsync(Ct);
            DfsOperationId kWrite = await lostVolume.NewOperationIdAsync(Ct);
            DfsOperationId kDir = await lostVolume.NewOperationIdAsync(Ct);
            FaultArm[] dropReplies =
            [
                cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.Append, Count: 1000), FaultAction.DropReply()),
                cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.WriteAll, Count: 1000), FaultAction.DropReply()),
                cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.CreateDirectory, Count: 1000), FaultAction.DropReply()),
            ];

            // The SDK's own outcome resolution must not deliver a decision either (premise: no reply reaches the client).
            FaultArm dropQueries = ArmDropOutcomeQueryReplies(cluster);
            Exception? e1 = await CaptureAsync(() => lostVolume.Files.AppendAllBytesAsync("/log2", suffix2, WithId(kAppend), Ct));
            Exception? e2 = await CaptureAsync(() => lostVolume.Directories.CreateDirectoryAsync("/dd", WithId(kDir), Ct));
            Exception? e3 = await CaptureAsync(() => lostVolume.Files.WriteAllBytesAsync("/w", wBytes, WithId(kWrite), Ct));
            foreach (FaultArm arm in dropReplies)
                cluster.Faults.AssertReached(arm);
            AssertUnknownOutcome(e1, kAppend, "Append whose every reply was lost");
            AssertUnknownOutcome(e2, kDir, "CreateDirectory whose every reply was lost");
            AssertUnknownOutcome(e3, kWrite, "WriteAll whose every reply was lost");

            // The client that issued them stays alive but idle (it never received a reply, so it never acknowledged these
            // identities). Killing it would let its session expire (10 s with Fast timings) during the slowest leader
            // change below, which would legitimately turn the retries into OperationExpired.
            foreach (FaultArm arm in dropReplies)
                arm.Disarm();
            dropQueries.Disarm();

            var before = new Dictionary<DfsOperationId, DfsOperationOutcome>();
            foreach (DfsOperationId id in new[] { kAppend, kWrite, kDir })
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                before[id] = outcome;
            }

            // A register workload runs across the leader change and is checked for linearizability afterwards.
            byte[] v0 = TestData.Bytes(0x0510, 300);
            await volume.Files.WriteAllBytesAsync("/reg", v0, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader", OperationTimeout = ElectionBound }, Ct);
            DfsVolume readerVolume = readerClient.GetVolume(Vol);
            async Task WriterLoopAsync()
            {
                for (int i = 1; i <= 8; i++)
                {
                    byte[] value = TestData.Bytes(0x0510 + (ulong)i, 300);
                    await CaptureAsync(() => cluster.History.RecordAsync("writer", "write", "/reg", value,
                        () => volume.Files.WriteAllBytesAsync("/reg", value, Ct)));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(120), Ct);
                }
            }

            async Task ReaderLoopAsync()
            {
                for (int i = 0; i < 10; i++)
                {
                    await CaptureAsync(() => cluster.History.RecordAsync<byte[]>("reader", "read", "/reg", null,
                        () => readerVolume.Files.ReadAllBytesAsync("/reg", Ct)));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(90), Ct);
                }
            }

            // Both loops run inside the simulation (no thread-pool work: HAR-002).
            Task writer = WriterLoopAsync();
            Task reader = ReaderLoopAsync();

            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] others = Others(cluster, oldLeader);
            switch (change)
            {
                case "crash-leader":
                    await oldLeader.CrashAsync(CrashMode.LoseUnflushed);
                    break;
                case "stop-leader":
                    await oldLeader.StopAsync();
                    break;
                case "isolate-leader":
                    cluster.Network.Isolate(oldLeader.Name);
                    break;
                case "crash-leader-twice":
                    await oldLeader.CrashAsync(CrashMode.LoseUnflushed);
                    break;
            }

            TestNode newLeader = await WaitForLeaderAmongAsync(cluster, others, "a surviving majority must elect a new leader");
            Assert.NotEqual(oldLeader.Name, newLeader.Name);

            if (change == "crash-leader-twice")
            {
                await oldLeader.RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await newLeader.CrashAsync(CrashMode.LoseUnflushed);
                TestNode third = await WaitForLeaderAmongAsync(cluster, Others(cluster, newLeader), "a second leader change must also elect a leader");
                Assert.NotEqual(newLeader.Name, third.Name);
                newLeader = third;
            }

            await Task.WhenAll(writer, reader);

            // Committed entries are preserved under the new leader (read before restoring the old one).
            await AssertContentAsync(volume, "/big", big, "WriteAll before the change");
            await AssertContentAsync(volume, "/bigcopy", big, "Copy before the change");
            await AssertContentAsync(volume, "/d/e/small", small, "moved file");
            await AssertContentAsync(volume, "/log", Concat(prefix, suffix), "append exactly once");
            Assert.False(await volume.Files.ExistsAsync("/small", Ct));
            Assert.False(await volume.Files.ExistsAsync("/gone", Ct));
            Assert.True(await volume.Directories.ExistsAsync("/d/e", Ct));
            DfsFileInfo bigAfter = await volume.Files.GetInfoAsync("/big", Ct);
            Assert.Equal(bigInfo.InodeId, bigAfter.InodeId);
            Assert.Equal(bigInfo.Revision, bigAfter.Revision);
            Assert.Equal(logInfo.Revision, (await volume.Files.GetInfoAsync("/log", Ct)).Revision);
            Assert.Equal(copyInfo.InodeId, (await volume.Files.GetInfoAsync("/bigcopy", Ct)).InodeId);
            Assert.Equal(smallInfo.InodeId, (await volume.Files.GetInfoAsync("/d/e/small", Ct)).InodeId);

            // Outcome records are preserved: same status and the same commit token as before the change.
            foreach ((DfsOperationId id, DfsOperationOutcome outcome) in before)
            {
                DfsOperationOutcome after = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, after.Status);
                Assert.Equal(outcome.Token, after.Token);
            }

            // Retrying with the same identity returns the original result and has no second effect.
            CommitToken retried = await volume.Files.AppendAllBytesAsync("/log2", suffix2, WithId(kAppend), Ct);
            Assert.Equal(before[kAppend].Token, retried);
            await AssertContentAsync(volume, "/log2", Concat(prefix2, suffix2), "retried append must not apply twice");
            CommitToken retriedWrite = await volume.Files.WriteAllBytesAsync("/w", wBytes, WithId(kWrite), Ct);
            Assert.Equal(before[kWrite].Token, retriedWrite);
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                () => volume.Files.AppendAllBytesAsync("/log2", TestData.Bytes(0x0509, 300), WithId(kAppend), Ct), DfsErrorCode.OperationIdMismatch);
            await AssertContentAsync(volume, "/log2", Concat(prefix2, suffix2), "rejected reuse must not apply");
            await AssertContentAsync(volume, "/w", wBytes, "WriteAll whose reply was lost, applied once");
            Assert.True(await volume.Directories.ExistsAsync("/dd", Ct), "CreateDirectory whose reply was lost");

            await RecoverAndConvergeAsync(cluster);
            await lost.DisposeAsync();

            CheckResult linearizable = LinearizabilityChecker.Check(
                cluster.History.Operations.Where(op => op.Key == "/reg").ToList(), new RegisterModel(v0));
            linearizable.AssertValid();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-006 UncommittedSuffixNotExposed
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-006", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("write-all", CrashMode.KeepUnflushed)]
    [InlineData("write-all", CrashMode.LoseUnflushed)]
    [InlineData("append", CrashMode.KeepUnflushed)]
    [InlineData("create-directory", CrashMode.KeepUnflushed)]
    [InlineData("move", CrashMode.TearLastWrite)]
    public async Task CON_006_UncommittedSuffixNotExposed(string operation, CrashMode crashMode)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsVolume setupVolume = setup.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(0x0601, 5000);
            byte[] logPrefix = TestData.Bytes(0x0602, 700);
            byte[] srcBytes = TestData.Bytes(0x0603, 400);
            await setupVolume.Files.WriteAllBytesAsync("/x", v1, Ct);
            await setupVolume.Files.WriteAllBytesAsync("/log", logPrefix, Ct);
            await setupVolume.Directories.CreateDirectoryAsync("/src", Ct);
            await setupVolume.Directories.CreateDirectoryAsync("/dst", Ct);
            await setupVolume.Files.WriteAllBytesAsync("/src/f", srcBytes, Ct);
            await setupVolume.Files.WriteAllBytesAsync("/marker", TestData.Bytes(0x0604, 10), Ct);
            ulong xRevision = (await setupVolume.Files.GetInfoAsync("/x", Ct)).Revision;
            ulong logRevision = (await setupVolume.Files.GetInfoAsync("/log", Ct)).Revision;

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);

            // The writer's session (and, should the SDK keep one per session, its staging session) is established while
            // replication still works: once replication is cut, nothing else can commit, so the faulted request must be
            // the only proposal it needs. All faulted payloads stay at or below the 4 KiB inline threshold, so no object
            // upload, receipt or staging registration has to commit before the entry is proposed.
            DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer", SeedNodes = [leader.Name], OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume writerVolume = writer.GetVolume(Vol);
            await writerVolume.Files.WriteAllBytesAsync("/writer-warm", TestData.Bytes(0x0608, 6000), Ct);
            await writerVolume.Files.AppendAllBytesAsync("/writer-warm", TestData.Bytes(0x0609, 100), Ct);
            DfsOperationId k = await writerVolume.NewOperationIdAsync(Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol, leader.Name)).InodeCount;

            // The leader can no longer send to its followers: it appends the entry to its own journal but can never
            // replicate or commit it. Once the entry is appended the writer is also confined to the leader, so no SDK
            // retry can reach a new leader.
            foreach (TestNode follower in followers)
                cluster.Network.Cut(leader.Name, follower.Name);

            DfsOperationKind kind;
            Func<Task> invoke;
            switch (operation)
            {
                case "write-all":
                    kind = DfsOperationKind.WriteAll;
                    invoke = () => writerVolume.Files.WriteAllBytesAsync("/x", TestData.Bytes(0x0605, 3500), WithId(k), Ct);
                    break;
                case "append":
                    kind = DfsOperationKind.Append;
                    invoke = () => writerVolume.Files.AppendAllBytesAsync("/log", TestData.Bytes(0x0606, 300), WithId(k), Ct);
                    break;
                case "create-directory":
                    kind = DfsOperationKind.CreateDirectory;
                    invoke = () => writerVolume.Directories.CreateDirectoryAsync("/newdir", WithId(k), Ct);
                    break;
                case "move":
                    kind = DfsOperationKind.Move;
                    invoke = () => writerVolume.Files.MoveAsync("/src/f", "/dst/f", false, WithId(k), Ct);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }

            FaultArm appended = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Volume: Vol, Operation: kind), FaultAction.Block());
            Task pending = invoke();
            await appended.WaitTriggeredAsync(ElectionBound);
            cluster.Faults.AssertReached(appended);
            foreach (TestNode follower in followers)
                cluster.Network.CutBoth(ClientNet("writer"), follower.Name);

            // Crash the leader while the suffix exists only in its local journal.
            await leader.CrashAsync(crashMode);
            Exception? error = await CaptureAsync(() => pending);
            AssertUnknownOutcome(error, k, $"[{operation}] a locally appended, unreplicated entry whose leader crashed");
            await writer.DisposeAsync();
            cluster.Network.Heal();

            TestNode newLeader = await WaitForLeaderAmongAsync(cluster, followers, "the followers must elect a leader without the suffix");
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer", SeedNodes = Names(followers) }, Ct);
            DfsVolume observerVolume = observer.GetVolume(Vol);
            await observerVolume.Files.WriteAllBytesAsync("/marker", TestData.Bytes(0x0607, 10), Ct);

            async Task AssertSuffixAbsentAsync(string where)
            {
                switch (operation)
                {
                    case "write-all":
                        await AssertContentAsync(observerVolume, "/x", v1, where);
                        Assert.Equal(xRevision, (await observerVolume.Files.GetInfoAsync("/x", Ct)).Revision);
                        break;
                    case "append":
                        await AssertContentAsync(observerVolume, "/log", logPrefix, where);
                        break;
                    case "create-directory":
                        Assert.False(await observerVolume.Directories.ExistsAsync("/newdir", Ct), where);
                        break;
                    case "move":
                        await AssertContentAsync(observerVolume, "/src/f", srcBytes, where);
                        Assert.False(await observerVolume.Files.ExistsAsync("/dst/f", Ct), where);
                        break;
                }
            }

            await AssertSuffixAbsentAsync("authoritative state before the old leader returns");

            // Restart the old leader from its durable image containing the uncommitted suffix and watch it recover: while
            // replaying it may legitimately show an older committed prefix (its applied index lags), but its applied state
            // must never present the suffix as committed. Every file below was written exactly once before the suffix, so
            // each is either absent (older prefix) or exactly its single committed revision.
            await leader.RestartAsync();
            for (int probe = 0; probe < 25; probe++)
            {
                if (leader.State == TestNodeState.Running)
                {
                    VolumeStateInspection state;
                    try
                    {
                        state = await cluster.Inspect.GetVolumeStateAsync(Vol, leader.Name);
                    }
                    catch (InvalidOperationException)
                    {
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(40), Ct);
                        continue;
                    }

                    Assert.True(state.InodeCount <= inodesBefore,
                        $"probe {probe}: the recovering replica shows {state.InodeCount} inodes, more than the {inodesBefore} that were ever committed");
                    FileInspection? x = state.GetFile("/x");
                    Assert.True(x is null || (x.Revision == xRevision && x.Length == v1.Length),
                        $"probe {probe}: /x on the recovering replica shows revision {x?.Revision} length {x?.Length}; the only committed revision is {xRevision} ({v1.Length} bytes)");
                    FileInspection? log = state.GetFile("/log");
                    Assert.True(log is null || (log.Revision == logRevision && log.Length == logPrefix.Length),
                        $"probe {probe}: /log on the recovering replica shows revision {log?.Revision} length {log?.Length}; the uncommitted append must not appear");
                    Assert.Null(state.GetFile("/dst/f"));
                }

                await AssertSuffixAbsentAsync($"probe {probe} through the authoritative leader");
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(40), Ct);
            }

            await RecoverAndConvergeAsync(cluster);
            DfsOperationOutcome outcome = await observerVolume.QueryOperationAsync(k, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
            await AssertSuffixAbsentAsync("after convergence");
            Assert.NotEqual(leader.Name, newLeader.Name);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-007 LeaderLocalAppendNotSuccess
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-007", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("3-voters-acks-held-then-released")]
    [InlineData("3-voters-acks-held-then-leader-crash")]
    [InlineData("5-voters-replicated-to-one-follower-only")]
    public async Task CON_007_LeaderLocalAppendNotSuccess(string variant)
    {
        bool five = variant.StartsWith("5-", StringComparison.Ordinal);
        bool crash = variant.EndsWith("crash", StringComparison.Ordinal);
        await ClusterTest.RunAsync(Det(five ? TestClusterOptions.FiveNodes : TestClusterOptions.ThreeZones), async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            byte[] v1 = TestData.Bytes(0x0701, 3000);
            await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/x", v1, Ct);
            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            DfsClient writer = await cluster.ConnectAsync(new TestClientOptions
            {
                Name = "writer",
                SeedNodes = [leader.Name],
                OperationTimeout = crash ? ShortOperationTimeout : ElectionBound,
            }, Ct);
            DfsVolume writerVolume = writer.GetVolume(Vol);
            DfsOperationId k = await writerVolume.NewOperationIdAsync(Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            // The WriteAll entry is appended after this index; once the holds are in place nothing appended later can
            // reach a quorum, so the leader's commit index stays at or below it. The new value stays within the 4 KiB
            // inline threshold: the WriteAll entry is then the only proposal the write needs (no object upload, receipt
            // validation or staging registration has to commit first while the quorum decision is withheld).
            ulong lastAtHold = (await GroupOnAsync(leader)).LastIndex;
            FaultArm appended = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.WriteAll),
                FaultAction.Delay(TimeSpan.Zero));
            var holds = new List<MessageHold>();
            if (five)
            {
                // Only one follower receives the appends: leader + one follower = two of five copies, below the quorum of three.
                foreach (TestNode follower in followers.Skip(1))
                    holds.Add(cluster.Network.Hold(new MessageFilter(From: leader.Name, To: follower.Name, Type: MessageType.RaftBatch)));
            }
            else
            {
                // Followers receive and persist the entry, but none of their acknowledgements reaches the leader.
                holds.Add(cluster.Network.Hold(new MessageFilter(To: leader.Name, Type: MessageType.RaftBatch)));
            }

            byte[] v2 = TestData.Bytes(0x0702, 3500);
            Task<CommitToken> pending = writerVolume.Files.WriteAllBytesAsync("/x", v2, WithId(k), Ct);
            await appended.WaitTriggeredAsync(ElectionBound);
            cluster.Faults.AssertReached(appended);
            if (crash)
            {
                // Confine the writer to the leader, so that after the crash no SDK retry can reach a new leader.
                foreach (TestNode follower in followers)
                    cluster.Network.CutBoth(ClientNet("writer"), follower.Name);
            }

            // Well below the minimum election timeout (30 ticks x 5 ms = 150 ms), counted from the moment the holds were
            // installed: with CheckQuorum the leader keeps its role for at least one election timeout without quorum
            // responses, so the observation is of a leader that has appended locally but has no quorum decision.
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
            Assert.False(pending.IsCompletedSuccessfully, "the client received success although only the leader's local append existed");
            if (!crash)
                Assert.False(pending.IsCompleted, $"the write must still be waiting for its quorum decision, but it completed: {pending.Exception?.InnerException}");
            GroupInspection held = await GroupOnAsync(leader);
            Assert.True(held.LastIndex > held.CommitIndex, $"the entry must be appended but uncommitted on the leader (last {held.LastIndex}, commit {held.CommitIndex})");
            Assert.True(held.CommitIndex <= lastAtHold, $"the leader committed beyond {lastAtHold} (commit {held.CommitIndex}) without a quorum decision");
            if (five)
            {
                int copies = 1;
                foreach (TestNode follower in followers)
                {
                    if ((await GroupOnAsync(follower)).LastIndex >= held.LastIndex)
                        copies++;
                }

                Assert.True(copies <= 2, $"the entry reached {copies} voters; the scenario requires fewer than the quorum");
            }

            if (!crash)
            {
                foreach (MessageHold hold in holds)
                    hold.ReleaseAll();
                CommitToken committed = await pending;
                await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, Ct);
                await AssertContentAsync(observer.GetVolume(Vol), "/x", v2, "released write");
                int holders = 0;
                foreach (TestNode node in cluster.Nodes)
                {
                    if ((await GroupOnAsync(node)).LastIndex >= committed.Sequence)
                        holders++;
                }

                Assert.True(holders >= (cluster.Nodes.Count / 2) + 1, $"success was reported with the entry on only {holders} voters");
                await writer.DisposeAsync();
            }
            else
            {
                // Both followers received and persisted the entry (only their acknowledgements are held).
                foreach (TestNode follower in followers)
                {
                    await cluster.WaitUntilAsync(async () => (await GroupOnAsync(follower)).LastIndex >= held.LastIndex, ElectionBound,
                        $"{follower.Name} must persist the replicated entry {held.LastIndex}", Ct);
                }

                await leader.CrashAsync(CrashMode.LoseUnflushed);
                foreach (MessageHold hold in holds)
                    hold.Dispose();
                Exception? error = await CaptureAsync(() => pending);

                // The leader never learned of a quorum, but the entry is on a majority: the commit may (and here will)
                // happen, so the client must be told "unknown", never success and never a definite failure.
                AssertUnknownOutcome(error, k, "the leader crashed before learning of any quorum");
                await writer.DisposeAsync();
                cluster.Network.Heal();
                await WaitForLeaderAmongAsync(cluster, followers, "the surviving majority must elect a leader");
                await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer", SeedNodes = Names(followers) }, Ct);
                DfsVolume observerVolume = observer.GetVolume(Vol);

                // Every voter that can win the election holds the entry, so the new leader commits it with its first
                // entry of the new term: the resolution is Committed and the content is the new revision.
                DfsOperationOutcome outcome = await observerVolume.QueryOperationAsync(k, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                await AssertContentAsync(observerVolume, "/x", v2, "the entry persisted by the majority is committed by the next leader");
            }

            foreach (MessageHold hold in holds)
                hold.Dispose();
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-008 StaleLeaderReadRejected
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-008", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("read-before-new-election")]
    [InlineData("read-after-majority-commit")]
    [InlineData("concurrent-history")]
    public async Task CON_008_StaleLeaderReadRejected(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            byte[] v1 = TestData.Bytes(0x0801, 2500);
            await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/x", v1, Ct);
            await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/dir", Ct);
            await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/dir/a", TestData.Bytes(0x0802, 10), Ct);
            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            await using DfsClient stale = await cluster.ConnectAsync(new TestClientOptions { Name = "stale", SeedNodes = [leader.Name], OperationTimeout = ShortOperationTimeout }, Ct);
            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "fresh", SeedNodes = Names(followers), OperationTimeout = ElectionBound }, Ct);
            DfsVolume staleVolume = stale.GetVolume(Vol);
            DfsVolume freshVolume = fresh.GetVolume(Vol);
            await staleVolume.Files.GetInfoAsync("/x", Ct);   // warm routing/connection to the (soon stale) leader

            // A live read handle opened while the leader was still authoritative: every later read call on it must select
            // a linearizable revision (FSD §6.2), so it may not be answered from the stale leader's state either.
            DfsStream liveHandle = await staleVolume.Files.OpenAsync("/x", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
            Assert.Equal(DfsReadView.Live, liveHandle.ReadView);
            ulong lastBefore = await MaxLastIndexAsync(cluster.Nodes);

            cluster.Network.Partition([leader.Name, ClientNet("stale")], [.. Names(followers), ClientNet("fresh")]);

            byte[]? latest = v1;
            if (variant == "concurrent-history")
            {
                byte[][] values = Enumerable.Range(1, 5).Select(i => TestData.Bytes(0x0810 + (ulong)i, 200)).ToArray();

                async Task FreshWriterAsync()
                {
                    await WaitForLeaderAmongAsync(cluster, followers, "majority side must elect a leader");
                    foreach (byte[] value in values)
                    {
                        await CaptureAsync(() => cluster.History.RecordAsync("fresh-writer", "write", "/x", value,
                            () => freshVolume.Files.WriteAllBytesAsync("/x", value, Ct)));
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
                    }
                }

                async Task ReaderAsync(string process, DfsVolume volume, int count)
                {
                    for (int i = 0; i < count; i++)
                    {
                        await CaptureAsync(() => cluster.History.RecordAsync<byte[]>(process, "read", "/x", null,
                            () => volume.Files.ReadAllBytesAsync("/x", Ct)));
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(150), Ct);
                    }
                }

                await Task.WhenAll(FreshWriterAsync(), ReaderAsync("stale-reader", staleVolume, 6), ReaderAsync("fresh-reader", freshVolume, 6));
                Assert.All(cluster.History.Operations.Where(op => op.Process == "stale-reader"),
                    op => Assert.True(op.Outcome != OpOutcome.Ok, $"the stale leader answered live read #{op.Id}"));

                // The final value is known only when the last fresh write was acknowledged; otherwise the linearizability
                // check below (which includes the read after healing) is the oracle.
                HistoryOp lastWrite = cluster.History.Operations.Last(op => op.Process == "fresh-writer");
                latest = lastWrite.Outcome == OpOutcome.Ok ? values[^1] : null;
            }
            else if (variant == "read-after-majority-commit")
            {
                await WaitForLeaderAmongAsync(cluster, followers, "majority side must elect a leader");
                latest = TestData.Bytes(0x0803, 2600);
                await freshVolume.Files.WriteAllBytesAsync("/x", latest, Ct);
            }

            // Every kind of live read through the leader that lost its quorum fails explicitly; none returns a value.
            await AssertStaleReadRejectedAsync(() => staleVolume.Files.ReadAllBytesAsync("/x", Ct), "ReadAllBytes");
            await AssertStaleReadRejectedAsync(() => staleVolume.Files.ExistsAsync("/x", Ct), "Exists");
            await AssertStaleReadRejectedAsync(() => staleVolume.Files.GetInfoAsync("/x", Ct), "GetInfo");
            await AssertStaleReadRejectedAsync(async () =>
            {
                var names = new List<string>();
                await foreach (DfsEntryInfo entry in staleVolume.Directories.EnumerateAsync("/dir", null, Ct))
                    names.Add(entry.Name);
                return names;
            }, "Enumerate");
            await AssertStaleReadRejectedAsync(async () =>
            {
                await using DfsStream stream = await staleVolume.Files.OpenAsync("/x", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
                var buffer = new byte[256];
                return await stream.ReadAsync(buffer, Ct);
            }, "open + stream Read (Live)");
            long positionBefore = liveHandle.Position;
            (int handleCount, Exception? handleError) = await CaptureValueAsync(async () => await liveHandle.ReadAsync(new byte[256], Ct));
            Assert.True(handleError is not null, $"a live handle opened before the partition read {handleCount} bytes through a leader cut off from its quorum");
            Assert.True(IsUnavailability(handleError!) || handleError is DfsLeaseLostException or DfsPinExpiredException,
                $"live handle read: expected an explicit unavailability (or, once its lease lapsed, lease/pin loss), got {handleError!.GetType().FullName}: {handleError.Message}");
            Assert.Equal(positionBefore, liveHandle.Position);
            Assert.True((await GroupOnAsync(leader)).CommitIndex <= lastBefore, "the isolated leader committed an entry proposed after the partition");
            await cluster.Inspect.AssertNoSplitBrainAsync();

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ElectionBound, Ct);

            // Once reconnected, the same client reads the latest committed value.
            byte[] afterHeal = await staleVolume.Files.ReadAllBytesAsync("/x", Ct);
            if (latest is not null)
                AssertEx.BytesEqual(latest, afterHeal, "after healing the client must observe the latest commit");
            if (variant == "concurrent-history")
            {
                await cluster.History.RecordAsync<byte[]>("stale-reader", "read", "/x", null, () => staleVolume.Files.ReadAllBytesAsync("/x", Ct));
                LinearizabilityChecker.Check(cluster.History.Operations.Where(op => op.Key == "/x").ToList(), new RegisterModel(v1)).AssertValid();
            }

            // The handle's lease may have lapsed during the partition; closing it is best effort.
            await CaptureAsync(async () => await liveHandle.DisposeAsync());
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    private static async Task AssertStaleReadRejectedAsync<T>(Func<Task<T>> read, string what)
    {
        (T? value, Exception? error) = await CaptureValueAsync(read);
        Assert.True(error is not null, $"{what}: a leader cut off from its quorum answered a live read (returned {value})");
        Assert.True(IsUnavailability(error!), $"{what}: expected an explicit unavailability error, got {error!.GetType().FullName}: {error.Message}");
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-009 ReadWaitsForAppliedIndex
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-009", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("apply-blocked-new-leader-by-crash")]
    [InlineData("apply-delayed-new-leader-by-crash")]
    [InlineData("apply-delayed-new-leader-by-transfer")]
    public async Task CON_009_ReadWaitsForAppliedIndex(string variant)
    {
        bool block = variant.Contains("blocked", StringComparison.Ordinal);
        bool transfer = variant.EndsWith("transfer", StringComparison.Ordinal);
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(0x0901, 4000);
            await volume.Files.WriteAllBytesAsync("/x", v1, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            TimeSpan applyDelay = TimeSpan.FromSeconds(2);
            FaultArm[] arms = followers.Select(f => cluster.Faults.Arm(
                new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: f.Name, Volume: Vol, Operation: DfsOperationKind.WriteAll),
                block ? FaultAction.Block() : FaultAction.Delay(applyDelay))).ToArray();

            // The write is acknowledged (committed by the quorum and applied on the leader) before any read below starts.
            byte[] v2 = TestData.Bytes(0x0902, 7000);
            CommitToken t2 = await volume.Files.WriteAllBytesAsync("/x", v2, Ct);
            foreach (FaultArm arm in arms)
            {
                await arm.WaitTriggeredAsync(ElectionBound);
                cluster.Faults.AssertReached(arm);
            }

            foreach (TestNode follower in followers)
            {
                GroupInspection group = await GroupOnAsync(follower);
                Assert.True(group.CommitIndex >= t2.Sequence, $"{follower.Name} must know the commit of {t2.Sequence}");
                Assert.True(group.AppliedIndex < t2.Sequence, $"{follower.Name} must not have applied {t2.Sequence} yet");
            }

            // Make a lagging-apply replica the serving leader.
            if (transfer)
                await cluster.TransferLeadershipAsync(Vol, followers[0].Name, waitUntilServing: false, ct: Ct);
            else
                await leader.CrashAsync(CrashMode.LoseUnflushed);

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader", SeedNodes = Names(followers), OperationTimeout = TimeSpan.FromSeconds(20) }, Ct);
            DfsVolume readerVolume = reader.GetVolume(Vol);
            Task<byte[]> read = readerVolume.Files.ReadAllBytesAsync("/x", Ct);
            Task<DfsFileInfo> info = readerVolume.Files.GetInfoAsync("/x", Ct);

            // Until the boundary is applied, the reads wait (or fail explicitly); they never return the pre-boundary state.
            for (int probe = 0; probe < 8; probe++)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(block ? 150 : 100), Ct);
                if (read.IsCompletedSuccessfully)
                    AssertEx.BytesEqual(v2, read.Result, $"probe {probe}: a read invoked after the acknowledged write returned an older revision");
                if (info.IsCompletedSuccessfully)
                {
                    Assert.Equal(v2.Length, info.Result.Length);
                    Assert.Equal(t2.Sequence, info.Result.Revision);
                }

                if (block)
                {
                    Assert.False(read.IsCompletedSuccessfully, "no live replica has applied the boundary, so no read can be answered yet");
                    Assert.False(info.IsCompletedSuccessfully, "no live replica has applied the boundary, so no metadata read can be answered yet");
                }
            }

            foreach (FaultArm arm in arms)
                arm.Release();

            (byte[]? bytes, Exception? readError) = await CaptureValueAsync(() => read);
            if (readError is not null)
            {
                Assert.True(IsUnavailability(readError), $"a read that could not wait must fail explicitly, got {readError}");
                bytes = await readerVolume.Files.ReadAllBytesAsync("/x", Ct);
            }

            AssertEx.BytesEqual(v2, bytes!, "the read must observe the acknowledged boundary");
            (DfsFileInfo? fileInfo, Exception? infoError) = await CaptureValueAsync(() => info);
            if (infoError is not null)
            {
                Assert.True(IsUnavailability(infoError), $"metadata read error must be explicit, got {infoError}");
                fileInfo = await readerVolume.Files.GetInfoAsync("/x", Ct);
            }

            Assert.Equal(v2.Length, fileInfo!.Length);
            Assert.Equal(t2.Sequence, fileInfo.Revision);
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-010 DuplicateLogDeliveryAppliedOnce
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-010", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("duplicate")]
    [InlineData("reorder")]
    [InlineData("duplicate-and-reorder")]
    public async Task CON_010_DuplicateLogDeliveryAppliedOnce(string variant)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] prefix = TestData.Bytes(0x1001, 100);
            await cluster.History.RecordAsync("w", "append", "/log", prefix, () => volume.Files.WriteAllBytesAsync("/log", prefix, Ct));
            await volume.Directories.CreateDirectoryAsync("/m", Ct);
            var moved = new List<byte[]>();
            for (int i = 0; i < 5; i++)
            {
                byte[] bytes = TestData.Bytes(0x1002 + (ulong)i, 64);
                moved.Add(bytes);
                await volume.Files.WriteAllBytesAsync("/m/src" + i, bytes, Ct);
            }

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            MessageRule? duplicates = variant.Contains("duplicate", StringComparison.Ordinal)
                ? cluster.Network.DuplicateNext(new MessageFilter(Type: MessageType.RaftBatch), count: 100_000)
                : null;
            int delayed = 0;
            bool reorder = variant.Contains("reorder", StringComparison.Ordinal);
            bool stop = false;

            async Task ReorderAsync()
            {
                var random = new SeededRandom(0x1010);
                while (!stop)
                {
                    // Delay single replication messages on one link so later messages overtake them.
                    TestNode follower = followers[random.Next(followers.Length)];
                    cluster.Network.DelayNext(new MessageFilter(From: leader.Name, To: follower.Name, Type: MessageType.RaftBatch),
                        TimeSpan.FromMilliseconds(15 + random.Next(40)), count: 1);
                    delayed++;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(10 + random.Next(20)), Ct);
                }
            }

            Task reordering = reorder ? ReorderAsync() : Task.CompletedTask;
            var appended = new List<byte[]> { prefix };
            var appendIds = new List<(DfsOperationId Id, CommitToken Token)>();
            for (int i = 0; i < 30; i++)
            {
                byte[] chunk = TestData.Bytes(0x1020 + (ulong)i, 100);
                DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                CommitToken token = await cluster.History.RecordAsync("w", "append", "/log", chunk,
                    () => volume.Files.AppendAllBytesAsync("/log", chunk, WithId(id), Ct));
                appended.Add(chunk);
                appendIds.Add((id, token));
            }

            for (int i = 0; i < 5; i++)
                await volume.Files.MoveAsync("/m/src" + i, "/m/dst" + i, false, Ct);
            for (int i = 0; i < 10; i++)
                await volume.Directories.CreateDirectoryAsync("/dirs/d" + i, Ct);

            stop = true;
            await reordering;
            duplicates?.Cancel();
            if (duplicates is not null)
                Assert.True(duplicates.Matched > 0, "replication messages must actually have been duplicated");
            if (reorder)
                Assert.True(delayed > 0, "replication messages must actually have been delayed");

            // Each logical mutation took effect exactly once, in order.
            byte[] expectedLog = Concat([.. appended]);
            await AssertContentAsync(volume, "/log", expectedLog, "each append applied exactly once");

            // Request outcomes were recorded once: every append has its own commit index (strictly increasing tokens), and
            // a retained outcome record still reports that same token (an acknowledged outcome may already be retired).
            for (int i = 1; i < appendIds.Count; i++)
                Assert.True(appendIds[i - 1].Token.CompareTo(appendIds[i].Token) < 0, $"append {i} must commit after append {i - 1}");
            foreach ((DfsOperationId id, CommitToken token) in appendIds)
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"{id}: unexpected outcome {outcome.Status}");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    Assert.Equal(token, outcome.Token);
            }
            for (int i = 0; i < 5; i++)
            {
                Assert.False(await volume.Files.ExistsAsync("/m/src" + i, Ct));
                await AssertContentAsync(volume, "/m/dst" + i, moved[i], "move applied once");
            }

            await cluster.History.RecordAsync<byte[]>("w", "read", "/log", null, () => volume.Files.ReadAllBytesAsync("/log", Ct));
            LinearizabilityChecker.Check(cluster.History.Operations.Where(op => op.Key == "/log").ToList(), new AppendLogModel()).AssertValid();

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            int? inodes = null;
            foreach (TestNode node in cluster.Nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, node.Name);
                FileInspection? log = state.GetFile("/log");
                Assert.True(log is not null && log.Length == expectedLog.Length, $"{node.Name}: /log length {log?.Length} differs from {expectedLog.Length}");
                inodes ??= state.InodeCount;
                Assert.Equal(inodes, state.InodeCount);
            }
        }, Ct);
    }

    /// <summary>Core-level complement of CON-010: duplicated and reordered Raft messages never apply an entry twice.</summary>
    [Theory, Catalog("CON-010", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData(21UL)]
    [InlineData(22UL)]
    [InlineData(23UL)]
    public void CON_010_DuplicateLogDeliveryAppliedOnce_Core(ulong seed)
    {
        var sim = new RaftSimulation(3, seed);
        sim.DuplicateProbability(0.3);
        sim.ReorderMessages(true);
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        Assert.NotNull(sim.LeaderIndex);
        var proposed = new List<byte[]>();
        var leaderships = new HashSet<(int Leader, ulong Term)>();
        for (int i = 0; i < 40; i++)
        {
            byte[] command = Encoding.ASCII.GetBytes(string.Create(CultureInfo.InvariantCulture, $"cmd-{seed}-{i}"));
            sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
            int leader = sim.LeaderIndex!.Value;
            leaderships.Add((leader, sim.Nodes[leader].Term));
            if (sim.Propose(leader, command))
                proposed.Add(command);
            sim.RunTicks(3);
            Assert.Null(sim.CheckSafety());
        }

        // Liveness after the duplication stops: a final command is applied everywhere.
        sim.DuplicateProbability(0);
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        int finalLeader = sim.LeaderIndex!.Value;
        leaderships.Add((finalLeader, sim.Nodes[finalLeader].Term));
        byte[] final = Encoding.ASCII.GetBytes(string.Create(CultureInfo.InvariantCulture, $"final-{seed}"));
        Assert.True(sim.Propose(finalLeader, final), "the leader must accept a proposal");
        sim.RunUntil(() => Enumerable.Range(0, 3).All(n => sim.AppliedLog(n).Any(a => a.AsSpan().SequenceEqual(final))), 10_000);

        for (int n = 0; n < 3; n++)
        {
            IReadOnlyList<byte[]> applied = sim.AppliedLog(n);
            Assert.Equal(1, applied.Count(a => a.AsSpan().SequenceEqual(final)));

            // Duplicated/reordered replication never applies an entry twice.
            Assert.Equal(applied.Count, applied.Select(Convert.ToHexString).Distinct(StringComparer.Ordinal).Count());
            foreach (byte[] command in proposed)
                Assert.True(applied.Count(a => a.AsSpan().SequenceEqual(command)) <= 1, $"n{n} applied {Encoding.ASCII.GetString(command)} twice");
            Assert.Equal(sim.AppliedLog(0).Select(Convert.ToHexString), applied.Select(Convert.ToHexString));

            // A proposal accepted by a leader that loses leadership before replicating it may legitimately be lost; with
            // one stable leadership throughout, every accepted proposal is committed exactly once.
            if (leaderships.Count == 1)
            {
                foreach (byte[] command in proposed)
                    Assert.Equal(1, applied.Count(a => a.AsSpan().SequenceEqual(command)));
            }
        }

        Assert.Null(sim.CheckSafety());
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-011 LogConflictReconcilesSafely
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-011", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData(1, 1)]
    [InlineData(3, 1)]
    [InlineData(5, 3)]
    public async Task CON_011_LogConflictReconcilesSafely(int divergent, int fresh)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones), async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            DfsVolume setupVolume = setup.GetVolume(Vol);
            var committed = new Dictionary<string, (byte[] Bytes, ulong Revision)>(StringComparer.Ordinal);
            await setupVolume.Directories.CreateDirectoryAsync("/pre", Ct);
            foreach (string name in new[] { "a", "b", "c" })
            {
                byte[] bytes = TestData.Bytes(0x1100 + (ulong)name[0], 900);
                CommitToken token = await setupVolume.Files.WriteAllBytesAsync("/pre/" + name, bytes, Ct);
                committed["/pre/" + name] = (bytes, token.Sequence);
            }

            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = Others(cluster, leader);
            await using DfsClient stale = await cluster.ConnectAsync(new TestClientOptions { Name = "stale", SeedNodes = [leader.Name], OperationTimeout = ShortOperationTimeout }, Ct);
            await using DfsClient freshClient = await cluster.ConnectAsync(new TestClientOptions { Name = "fresh", SeedNodes = Names(followers), OperationTimeout = ElectionBound }, Ct);
            DfsVolume staleVolume = stale.GetVolume(Vol);
            DfsVolume freshVolume = freshClient.GetVolume(Vol);
            var staleIds = new List<DfsOperationId>();
            for (int i = 0; i < divergent; i++)
                staleIds.Add(await staleVolume.NewOperationIdAsync(Ct));
            await cluster.WaitForReplicationAsync(Vol, Ct);
            GroupInspection leaderBefore = await GroupOnAsync(leader);

            // Entries proposed after the partition have indexes above this bound (in-flight messages may still spread
            // knowledge of earlier commits, see CON-002).
            ulong lastBefore = await MaxLastIndexAsync(cluster.Nodes);

            FaultArm localAppends = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.WriteAll, Count: divergent),
                FaultAction.Delay(TimeSpan.Zero));
            cluster.Network.Partition([leader.Name, ClientNet("stale")], [.. Names(followers), ClientNet("fresh")]);

            // The isolated leader appends divergent entries it can never commit.
            Task<Exception?>[] staleOps = Enumerable.Range(0, divergent)
                .Select(i => CaptureAsync(() => staleVolume.Files.WriteAllBytesAsync("/stale" + i, TestData.Bytes(0x1110 + (ulong)i, 500), WithId(staleIds[i]), Ct)))
                .ToArray();
            await cluster.WaitUntilAsync(() => Task.FromResult(localAppends.Hits >= divergent), ElectionBound,
                "the isolated leader must append every divergent entry to its local log", Ct);
            cluster.Faults.AssertReached(localAppends);

            // The majority elects a new leader and commits different entries at overlapping indexes.
            await WaitForLeaderAmongAsync(cluster, followers, "the majority must elect a new leader");
            var freshFiles = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            ulong lastFreshCommit = 0;
            for (int j = 0; j < fresh; j++)
            {
                byte[] bytes = TestData.Bytes(0x1120 + (ulong)j, 700);
                CommitToken token = await freshVolume.Files.WriteAllBytesAsync("/fresh" + j, bytes, Ct);
                freshFiles["/fresh" + j] = bytes;
                lastFreshCommit = Math.Max(lastFreshCommit, token.Sequence);
            }

            foreach (Exception? error in await Task.WhenAll(staleOps))
                AssertNoCommitClaimed(error, "an entry appended only by an isolated leader");
            GroupInspection staleGroup = await GroupOnAsync(leader);
            Assert.True(staleGroup.LastIndex >= leaderBefore.LastIndex + (ulong)divergent, "the old leader must hold the divergent suffix");
            Assert.True(staleGroup.CommitIndex <= lastBefore,
                $"the isolated leader committed beyond {lastBefore} (commit {staleGroup.CommitIndex}): its divergent entries have no quorum");

            cluster.Network.Heal();

            // While reconciling, the old leader never applies a divergent entry nor loses committed history.
            for (int probe = 0; probe < 20; probe++)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, leader.Name);
                for (int i = 0; i < divergent; i++)
                    Assert.Null(state.GetFile("/stale" + i));
                foreach ((string path, (byte[] bytes, ulong revision)) in committed)
                {
                    FileInspection? file = state.GetFile(path);
                    Assert.True(file is not null && file.Length == bytes.Length && file.Revision == revision, $"probe {probe}: committed {path} changed on the reconciling replica");
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            GroupInspection reconciled = await GroupOnAsync(leader);
            Assert.True(reconciled.AppliedIndex >= lastFreshCommit,
                $"the reconciled replica must apply the majority's commits (applied {reconciled.AppliedIndex}, last majority commit {lastFreshCommit})");

            foreach (TestNode node in cluster.Nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, node.Name);
                foreach ((string path, (byte[] _, ulong revision)) in committed)
                    Assert.Equal(revision, state.GetFile(path)?.Revision);
                foreach (string path in freshFiles.Keys)
                    Assert.NotNull(state.GetFile(path));
            }

            for (int i = 0; i < divergent; i++)
            {
                DfsOperationOutcome outcome = await freshVolume.QueryOperationAsync(staleIds[i], resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
                Assert.False(await freshVolume.Files.ExistsAsync("/stale" + i, Ct));
            }

            foreach ((string path, byte[] bytes) in freshFiles)
                await AssertContentAsync(freshVolume, path, bytes, "majority commits survive reconciliation");
            foreach ((string path, (byte[] bytes, ulong _)) in committed)
                await AssertContentAsync(freshVolume, path, bytes, "pre-partition commits survive reconciliation");
        }, Ct);
    }

    /// <summary>Core-level complement of CON-011 with production Raft cores.</summary>
    [Theory, Catalog("CON-011", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData(31UL, 5)]
    [InlineData(32UL, 1)]
    public void CON_011_LogConflictReconcilesSafely_Core(ulong seed, int divergent)
    {
        var sim = new RaftSimulation(3, seed);
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        int oldLeader = sim.LeaderIndex!.Value;
        byte[][] committed = Enumerable.Range(0, 3).Select(i => Encoding.ASCII.GetBytes("c" + i.ToString(CultureInfo.InvariantCulture))).ToArray();
        foreach (byte[] command in committed)
            Assert.True(sim.Propose(oldLeader, command));
        sim.RunUntil(() => Enumerable.Range(0, 3).All(n => sim.AppliedLog(n).Count >= committed.Length), 5_000);

        int[] others = Enumerable.Range(0, 3).Where(n => n != oldLeader).ToArray();
        sim.Partition([oldLeader], others);
        for (int i = 0; i < divergent; i++)
            Assert.True(sim.Propose(oldLeader, Encoding.ASCII.GetBytes("s" + i.ToString(CultureInfo.InvariantCulture))), "the isolated leader still accepts proposals locally");
        sim.RunUntil(() => sim.LeaderIndex is int l && l != oldLeader, 5_000);
        int newLeader = sim.LeaderIndex!.Value;
        byte[][] fresh = [Encoding.ASCII.GetBytes("f0"), Encoding.ASCII.GetBytes("f1")];
        foreach (byte[] command in fresh)
            Assert.True(sim.Propose(newLeader, command));
        sim.RunTicks(50);
        Assert.Null(sim.CheckSafety());

        sim.Heal();
        sim.RunUntil(() => Enumerable.Range(0, 3).All(n => sim.AppliedLog(n).Any(a => a.AsSpan().SequenceEqual(fresh[^1]))), 10_000);
        for (int n = 0; n < 3; n++)
        {
            string[] applied = sim.AppliedLog(n).Select(a => Encoding.ASCII.GetString(a)).ToArray();
            Assert.Equal(["c0", "c1", "c2", "f0", "f1"], applied.Where(a => a.StartsWith('c') || a.StartsWith('f')).ToArray());
            Assert.DoesNotContain(applied, a => a.StartsWith('s'));
        }

        Assert.Null(sim.CheckSafety());
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-012 ConsensusSnapshotIncludesSafetyState
    // ------------------------------------------------------------------------------------------------------------

    [Fact, Catalog("CON-012", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    public async Task CON_012_ConsensusSnapshotIncludesSafetyState()
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            DfsVolumeInfo infoBefore = await volume.GetInfoAsync(Ct);

            // The first identity of this session; it is retired by the per-session outcome window (256 with Fast timings).
            DfsOperationId retired = await volume.NewOperationIdAsync(Ct);
            byte[] retiredBytes = TestData.Bytes(0x1201, 50);
            CommitToken retiredToken = await volume.Files.WriteAllBytesAsync("/retired", retiredBytes, WithId(retired), Ct);

            byte[] pinnedOld = TestData.Bytes(0x1202, 8000);
            byte[] pinnedNew = TestData.Bytes(0x1203, 9000);
            byte[] resOld = TestData.Bytes(0x1204, 3000);
            byte[] dedupPrefix = TestData.Bytes(0x1205, 600);
            await volume.Files.WriteAllBytesAsync("/pinned", pinnedOld, Ct);
            await volume.Files.WriteAllBytesAsync("/res", resOld, Ct);
            await volume.Files.WriteAllBytesAsync("/dedup-log", dedupPrefix, Ct);
            await volume.Directories.CreateDirectoryAsync("/bulk", Ct);

            // Safety state: a writer reservation with its fencing generation, a version pin, and a retained snapshot.
            DfsStream writer = await volume.Files.OpenAsync("/res", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            ulong generation = writer.WriterGeneration;
            Assert.True(generation > 0, "a writable handle carries a fencing generation");
            DfsStream pinned = await volume.Files.OpenAsync("/pinned", new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                ReadView = DfsReadView.VersionPinned,
            }, Ct);
            await volume.Snapshots.CreateAsync("before-compaction", null, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode lagging = Others(cluster, leader)[0];
            cluster.Network.Isolate(lagging.Name);

            await volume.Files.WriteAllBytesAsync("/pinned", pinnedNew, Ct);
            // Far more entries than RaftSnapshotThreshold (200 with Fast timings): the lagging replica's history is compacted away.
            for (int i = 0; i < 300; i++)
                await volume.Files.WriteAllBytesAsync("/bulk/f" + (i % 25).ToString(CultureInfo.InvariantCulture), TestData.Bytes(0x1300 + (ulong)i, 64), Ct);

            // Safety state: deduplication record of an operation whose reply was lost (never acknowledged).
            DfsOperationId lostReply = await volume.NewOperationIdAsync(Ct);
            byte[] dedupSuffix = TestData.Bytes(0x1206, 250);
            FaultArm dropReply = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.Append, Count: 1000), FaultAction.DropReply());
            FaultArm dropQueries = ArmDropOutcomeQueryReplies(cluster);
            Exception? lostError = await CaptureAsync(() => volume.Files.AppendAllBytesAsync("/dedup-log", dedupSuffix, WithId(lostReply), Ct));
            cluster.Faults.AssertReached(dropReply);
            dropReply.Disarm();
            dropQueries.Disarm();
            AssertUnknownOutcome(lostError, lostReply, "an executed append whose every reply was lost");
            DfsOperationOutcome lostOutcomeBefore = await volume.QueryOperationAsync(lostReply, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, lostOutcomeBefore.Status);

            // Suffix operations after the compaction point.
            byte[] suffix = TestData.Bytes(0x1207, 1200);
            CommitToken suffixToken = await volume.Files.WriteAllBytesAsync("/suffix", suffix, Ct);

            FaultArm install = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: lagging.Name, Volume: Vol, Count: 1000), FaultAction.Delay(TimeSpan.Zero));
            cluster.Network.Heal();
            await install.WaitTriggeredAsync(ElectionBound);
            cluster.Faults.AssertReached(install);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            // The installed checkpoint + suffix carry every safety-relevant record, identical to the leader's view.
            (VolumeStateInspection leaderState, VolumeStateInspection laggingState) = await StableStatePairAsync(cluster, leader.Name, lagging.Name);
            AssertSameSafetyState(leaderState, laggingState);
            FileInspection? resOnLagging = laggingState.GetFile("/res");
            Assert.NotNull(resOnLagging);
            Assert.Equal(generation, resOnLagging!.WriterGeneration);
            Assert.Contains(laggingState.Handles, h => h.IsWriter && h.Generation == generation && !h.Expired);
            Assert.Contains(laggingState.Snapshots, s => s.Name == "before-compaction");
            Assert.NotEmpty(laggingState.Pins);

            // Make the snapshot-installed replica the authority and exercise every safety state through it.
            await cluster.TransferLeadershipAsync(Vol, lagging.Name, waitUntilServing: false, ct: Ct);
            Assert.Equal(lagging.Name, (await cluster.GetLeaderAsync(Vol, Ct)).Name);

            // Version pin: the pinned reader still reads the version it pinned.
            pinned.Seek(0, SeekOrigin.Begin);
            await AssertBytesAsync(pinnedOld, ReadToEndAsync(pinned), "version-pinned reader after snapshot install");

            // Reservation and fencing: a second writer is refused; the original generation still publishes.
            await using (DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "other" }, Ct))
            {
                DfsSharingViolationException conflict = await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(
                    () => other.GetVolume(Vol).Files.OpenWriteAsync("/res", Ct));
                Assert.True(conflict.ErrorCode is DfsErrorCode.WriterConflict or DfsErrorCode.SharingViolation);
            }

            byte[] resNew = TestData.Bytes(0x1208, 3500);
            writer.Seek(0, SeekOrigin.Begin);
            await writer.WriteAsync(resNew, Ct);
            await writer.SetLengthAsync(resNew.Length, Ct);
            CommitToken resToken = await writer.CommitAsync(Ct);
            await AssertContentAsync(volume, "/res", resNew, "the reservation holder's generation stays valid");

            // Deduplication: the lost-reply operation is answered from its outcome record, never re-executed.
            DfsOperationOutcome lostOutcomeAfter = await volume.QueryOperationAsync(lostReply, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, lostOutcomeAfter.Status);
            Assert.Equal(lostOutcomeBefore.Token, lostOutcomeAfter.Token);
            CommitToken retry = await volume.Files.AppendAllBytesAsync("/dedup-log", dedupSuffix, WithId(lostReply), Ct);
            Assert.Equal(lostOutcomeBefore.Token, retry);
            await AssertContentAsync(volume, "/dedup-log", Concat(dedupPrefix, dedupSuffix), "the retried append must not apply twice");

            // Retired window: the oldest identity stays rejected and is never replayed as a new operation.
            Assert.Equal(DfsOutcomeStatus.Expired, (await volume.QueryOperationAsync(retired, resolve: false, Ct)).Status);
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => volume.Files.WriteAllBytesAsync("/retired", retiredBytes, WithId(retired), Ct), DfsErrorCode.OperationExpired);
            Assert.Equal(retiredToken.Sequence, (await volume.Files.GetInfoAsync("/retired", Ct)).Revision);

            // Snapshot reference: still complete and readable through the new authority.
            await using (DfsSnapshotView view = await volume.Snapshots.OpenAsync("before-compaction", Ct))
            {
                await AssertBytesAsync(pinnedOld, view.Files.ReadAllBytesAsync("/pinned", Ct), "snapshot content");
                Assert.False(await view.Files.ExistsAsync("/suffix", Ct));
            }

            // Epoch: unchanged; tokens stay comparable and ordered.
            DfsVolumeInfo infoAfter = await volume.GetInfoAsync(Ct);
            Assert.Equal(infoBefore.Id, infoAfter.Id);
            Assert.Equal(infoBefore.Epoch, infoAfter.Epoch);
            Assert.True(resToken.IsComparableWith(retiredToken));
            Assert.True(retiredToken.CompareTo(suffixToken) < 0 && suffixToken.CompareTo(resToken) < 0);
            await AssertContentAsync(volume, "/suffix", suffix, "suffix replay");
            await AssertContentAsync(volume, "/pinned", pinnedNew, "current version");

            await writer.DisposeAsync();
            await pinned.DisposeAsync();
            await RecoverAndConvergeAsync(cluster);
        }, Ct);
    }

    private static async Task<(VolumeStateInspection A, VolumeStateInspection B)> StableStatePairAsync(TestCluster cluster, string a, string b)
    {
        // Background entries (keep-alives) may be applied between two inspections; take a pair at the same applied index.
        for (int attempt = 0; attempt < 50; attempt++)
        {
            VolumeStateInspection first = await cluster.Inspect.GetVolumeStateAsync(Vol, a);
            VolumeStateInspection second = await cluster.Inspect.GetVolumeStateAsync(Vol, b);
            if (first.AppliedIndex == second.AppliedIndex)
                return (first, second);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
        }

        Assert.Fail($"{a} and {b} never reported the same applied index");
        throw new InvalidOperationException("unreachable");
    }

    private static void AssertSameSafetyState(VolumeStateInspection expected, VolumeStateInspection actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Epoch, actual.Epoch);
        Assert.Equal(expected.AppliedIndex, actual.AppliedIndex);
        Assert.Equal(expected.InodeCount, actual.InodeCount);
        Assert.Equal(expected.ActiveTransactions, actual.ActiveTransactions);
        Assert.Equal(
            expected.Handles.OrderBy(h => h.Id.Value).Select(h => (h.Id, h.Inode, h.IsWriter, h.Generation, h.Access, h.Share, h.Expired)),
            actual.Handles.OrderBy(h => h.Id.Value).Select(h => (h.Id, h.Inode, h.IsWriter, h.Generation, h.Access, h.Share, h.Expired)));
        Assert.Equal(
            expected.Snapshots.OrderBy(s => s.Name, StringComparer.Ordinal).Select(s => (s.Id, s.Name, s.BoundaryIndex, s.Label, s.InodeCount)),
            actual.Snapshots.OrderBy(s => s.Name, StringComparer.Ordinal).Select(s => (s.Id, s.Name, s.BoundaryIndex, s.Label, s.InodeCount)));
        Assert.Equal(
            expected.Pins.OrderBy(p => p.Id).Select(p => (p.Id, p.Kind, p.CreatedIndex, p.Snapshot)),
            actual.Pins.OrderBy(p => p.Id).Select(p => (p.Id, p.Kind, p.CreatedIndex, p.Snapshot)));
        Assert.Equal(
            expected.Sessions.OrderBy(s => s.Id).Select(s => (s.Id, s.Principal, s.Expired, s.RetiredWatermark, s.RetainedOutcomes)),
            actual.Sessions.OrderBy(s => s.Id).Select(s => (s.Id, s.Principal, s.Expired, s.RetiredWatermark, s.RetainedOutcomes)));
        Assert.Equal(
            expected.Staging.OrderBy(s => s.Id.Value).Select(s => (s.Id, s.Open, s.CreatedIndex)),
            actual.Staging.OrderBy(s => s.Id.Value).Select(s => (s.Id, s.Open, s.CreatedIndex)));
        Assert.Equal(expected.ReachableObjects().OrderBy(o => o.Value), actual.ReachableObjects().OrderBy(o => o.Value));
    }

    private static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        while (true)
        {
            int read = await stream.ReadAsync(chunk, Ct);
            if (read == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, read);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-013 SnapshotInstallInterrupted
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, CrashMode> SnapshotInstallBoundaries => new()
    {
        { FaultSites.RaftSnapshotInstall, CrashMode.LoseUnflushed },
        { FaultSites.RaftSnapshotInstall, CrashMode.KeepUnflushed },
        { FaultSites.RaftSnapshotInstall, CrashMode.TearLastWrite },
        { FaultSites.RaftSnapshotInstall, CrashMode.RandomSubset },
        { FaultSites.RecoveryCheckpointInstall, CrashMode.LoseUnflushed },
        { FaultSites.RecoveryCheckpointInstall, CrashMode.TearLastWrite },
        { FaultSites.RecoveryJournalReplay, CrashMode.TearLastWrite },
    };

    [Theory, Catalog("CON-013", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [MemberData(nameof(SnapshotInstallBoundaries))]
    public async Task CON_013_SnapshotInstallInterrupted(string site, CrashMode crashMode)
    {
        // Reference run: count every hit of this boundary while a lagging replica installs a snapshot and restarts.
        int boundaryHits = await RunSnapshotInstallScenarioAsync(site, crashMode, crashAtOccurrence: null);
        Log($"CON-013 {site}: {boundaryHits} boundary hits in the reference run");
        Assert.True(boundaryHits >= 1, $"{site} was never reached on the installing replica");

        // Crash at every boundary occurrence of the bounded trace (first four).
        for (int occurrence = 1; occurrence <= Math.Min(boundaryHits, 4); occurrence++)
            await RunSnapshotInstallScenarioAsync(site, crashMode, occurrence);
    }

    private static async Task<int> RunSnapshotInstallScenarioAsync(string site, CrashMode crashMode, int? crashAtOccurrence)
    {
        TestClusterOptions options = Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) };
        return await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Directories.CreateDirectoryAsync("/bulk", Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode lagging = Others(cluster, leader)[0];
            VolumeStateInspection baseline = await cluster.Inspect.GetVolumeStateAsync(Vol, lagging.Name);
            cluster.Network.Isolate(lagging.Name);

            // New files only (one inode each), so every committed prefix has a unique, checkable file set.
            var created = new List<(string Path, ulong Sequence, byte[] Bytes)>();
            for (int i = 0; i < 260; i++)
            {
                string path = "/bulk/f" + i.ToString(CultureInfo.InvariantCulture);
                byte[] bytes = TestData.Bytes(0x1300 + (ulong)i, 40);
                CommitToken token = await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                created.Add((path, token.Sequence, bytes));
            }

            FaultArm arm = cluster.Faults.Arm(
                new FaultMatch(site, Node: lagging.Name, Volume: Vol, Occurrence: crashAtOccurrence ?? 1, Count: crashAtOccurrence is null ? 10_000 : 1),
                crashAtOccurrence is null ? FaultAction.Delay(TimeSpan.Zero) : FaultAction.CrashNode(crashMode));
            cluster.Network.Heal();

            bool recoverySite = site != FaultSites.RaftSnapshotInstall;
            if (recoverySite)
            {
                // Recovery boundaries run when the replica restarts from its local checkpoint/journal after the install.
                // The armed crash may stop the replica during the install or interrupt this very restart.
                await cluster.WaitUntilAsync(async () =>
                {
                    if (lagging.State != TestNodeState.Running)
                        return true;
                    (GroupInspection? group, _) = await CaptureValueAsync(() => lagging.Inspect.GetGroupAsync(Vol));
                    return group is not null && group.AppliedIndex >= created[^1].Sequence;
                }, ElectionBound * 3, "the lagging replica must catch up (or be stopped by the injected crash)", Ct);
                if (lagging.State == TestNodeState.Running)
                    await lagging.CrashAsync(CrashMode.LoseUnflushed);
                Exception? restartError = await CaptureAsync(() => lagging.RestartAsync());
                Assert.True(restartError is null || (crashAtOccurrence is not null && lagging.State != TestNodeState.Running),
                    $"the restart failed although no crash was injected into it: {restartError}");
            }

            if (crashAtOccurrence is not null)
            {
                await arm.WaitTriggeredAsync(ElectionBound * 2);
                cluster.Faults.AssertReached(arm);
                arm.Disarm();

                // The injected crash stopped the replica (possibly already restarted above when it hit during the
                // install); bring it back from its durable image.
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                if (lagging.State != TestNodeState.Running)
                    await lagging.RestartAsync();
            }

            // Whatever the replica recovers to is one complete committed prefix, never a mix of old and new files.
            for (int probe = 0; probe < 30; probe++)
            {
                if (lagging.State == TestNodeState.Running)
                {
                    VolumeStateInspection? state = null;
                    try
                    {
                        state = await cluster.Inspect.GetVolumeStateAsync(Vol, lagging.Name);
                    }
                    catch (InvalidOperationException)
                    {
                        // Not loaded yet.
                    }

                    if (state is not null)
                        AssertCommittedPrefix(state, baseline, created, $"probe {probe}");
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            VolumeStateInspection final = await cluster.Inspect.GetVolumeStateAsync(Vol, lagging.Name);
            AssertCommittedPrefix(final, baseline, created, "final");
            Assert.True(final.AppliedIndex >= created[^1].Sequence, "the replica must finish catching up");
            Assert.Equal(baseline.InodeCount + created.Count, final.InodeCount);

            // The recovered replica can serve as the authority with complete contents.
            await cluster.TransferLeadershipAsync(Vol, lagging.Name, waitUntilServing: false, ct: Ct);
            foreach (int index in new[] { 0, 1, created.Count / 2, created.Count - 2, created.Count - 1 })
                await AssertContentAsync(volume, created[index].Path, created[index].Bytes, $"file {index} through the recovered replica");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            return arm.Hits;
        }, Ct);
    }

    private static void AssertCommittedPrefix(VolumeStateInspection state, VolumeStateInspection baseline,
        IReadOnlyList<(string Path, ulong Sequence, byte[] Bytes)> created, string where)
    {
        if (state.AppliedIndex < baseline.AppliedIndex)
        {
            // Replaying from an older durable checkpoint: only pre-isolation state may be visible.
            Assert.True(state.InodeCount <= baseline.InodeCount, $"{where}: inode count {state.InodeCount} at applied index {state.AppliedIndex}");
            Assert.DoesNotContain(created, c => state.GetFile(c.Path) is not null);
            return;
        }

        int expected = created.Count(c => c.Sequence <= state.AppliedIndex);
        Assert.True(baseline.InodeCount + expected == state.InodeCount,
            $"{where}: at applied index {state.AppliedIndex} the replica has {state.InodeCount} inodes, expected {baseline.InodeCount + expected} (mixed state)");
        for (int i = 0; i < created.Count; i++)
        {
            (string path, ulong sequence, byte[] bytes) = created[i];
            FileInspection? file = state.GetFile(path);
            if (sequence <= state.AppliedIndex)
                Assert.True(file is not null && file.Length == bytes.Length && file.Revision == sequence, $"{where}: {path} (committed at {sequence}) missing or wrong at applied index {state.AppliedIndex}");
            else
                Assert.True(file is null, $"{where}: {path} (committed at {sequence}) visible at applied index {state.AppliedIndex}");
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-014 ConfigurationTransitionQuorums
    // ------------------------------------------------------------------------------------------------------------

    private static TestClusterOptions TransitionOptions => Det(new TestClusterOptions
    {
        NodeCount = 4,
        ControlVoters = 3,
        Volumes =
        [
            new TestVolumeSpec(Vol)
            {
                Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] },
            },
        ],
        ScenarioTimeout = TimeSpan.FromSeconds(300),
    });

    [Theory, Catalog("CON-014", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("old-and-joining-vs-rest")]
    [InlineData("removed-member-alone")]
    [InlineData("joining-member-alone")]
    [InlineData("even-split")]
    public async Task CON_014_ConfigurationTransitionQuorums(string shape)
    {
        // Reference run: the volume voter set moves from {n0,n1,n2} to {n1,n2,n3} by single-server changes.
        int transitions = await RunTransitionScenarioAsync(shape, blockAtStep: null);
        Log($"CON-014: {transitions} transition steps in the reference run");
        Assert.True(transitions >= 1, "the voter replacement must pass the reconfiguration boundary");
        for (int step = 1; step <= Math.Min(transitions, 4); step++)
            await RunTransitionScenarioAsync(shape, step);
    }

    private static async Task<int> RunTransitionScenarioAsync(string shape, int? blockAtStep)
    {
        return await ClusterTest.RunAsync(TransitionOptions, async cluster =>
        {
            DfsClient setup = await cluster.ConnectAsync(new TestClientOptions { Name = "setup" }, Ct);
            byte[] baseline = TestData.Bytes(0x1401, 3000);
            await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/baseline", baseline, Ct);
            await setup.DisposeAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);

            FaultArm arm = cluster.Faults.Arm(
                new FaultMatch(FaultSites.Reconfiguration, Volume: Vol, Occurrence: blockAtStep ?? 1, Count: blockAtStep is null ? 10_000 : 1),
                blockAtStep is null ? FaultAction.Delay(TimeSpan.Zero) : FaultAction.Block());
            Task drain = cluster.Admin.DrainNodeAsync("n0", Ct);

            if (blockAtStep is null)
            {
                await cluster.WaitUntilAsync(() => Task.FromResult(drain.IsCompleted), TimeSpan.FromSeconds(60), "the voter replacement must complete", Ct);
                await drain;
                GroupInspection done = await cluster.Inspect.GetVolumeGroupAsync(Vol);
                Assert.Equal(["n1", "n2", "n3"], done.Voters.Order(StringComparer.Ordinal).ToArray());
                return arm.Hits;
            }

            await arm.WaitTriggeredAsync(ElectionBound);
            cluster.Faults.AssertReached(arm);
            string[][] sides = shape switch
            {
                "old-and-joining-vs-rest" => [["n0", "n3"], ["n1", "n2"]],
                "removed-member-alone" => [["n0"], ["n1", "n2", "n3"]],
                "joining-member-alone" => [["n3"], ["n0", "n1", "n2"]],
                "even-split" => [["n0", "n1"], ["n2", "n3"]],
                _ => throw new ArgumentOutOfRangeException(nameof(shape)),
            };

            await using DfsClient a = await cluster.ConnectAsync(new TestClientOptions { Name = "a", SeedNodes = sides[0], OperationTimeout = ShortOperationTimeout }, Ct);
            await using DfsClient b = await cluster.ConnectAsync(new TestClientOptions { Name = "b", SeedNodes = sides[1], OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume aVolume = a.GetVolume(Vol);
            DfsVolume bVolume = b.GetVolume(Vol);
            DfsOperationId ka = await aVolume.NewOperationIdAsync(Ct);
            DfsOperationId kb = await bVolume.NewOperationIdAsync(Ct);
            byte[] aBytes = TestData.Bytes(0x1402, 500);
            byte[] bBytes = TestData.Bytes(0x1403, 500);

            cluster.Network.Partition([.. sides[0], ClientNet("a")], [.. sides[1], ClientNet("b")]);
            arm.Release();   // the transition continues while partitioned

            Task<Exception?> ta = CaptureAsync(() => aVolume.Files.WriteAllBytesAsync("/side-a", aBytes, WithId(ka), Ct));
            Task<Exception?> tb = CaptureAsync(() => bVolume.Files.WriteAllBytesAsync("/side-b", bBytes, WithId(kb), Ct));
            for (int probe = 0; probe < 10; probe++)
            {
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), Ct);
            }

            Exception?[] errors = await Task.WhenAll(ta, tb);
            int committedSides = errors.Count(e => e is null);
            Assert.True(committedSides <= 1, $"[{shape}, step {blockAtStep}] both sides committed during the transition: authority was split");
            foreach (Exception? error in errors)
            {
                if (error is not null)
                    AssertNoCommitClaimed(error, $"[{shape}, step {blockAtStep}] the side without a valid quorum");
            }

            cluster.Network.Heal();
            await cluster.WaitUntilAsync(() => Task.FromResult(drain.IsCompleted), TimeSpan.FromSeconds(60), "the transition must complete once healed", Ct);
            await drain;
            await RecoverAndConvergeAsync(cluster);
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            Assert.Equal(["n1", "n2", "n3"], group.Voters.Order(StringComparer.Ordinal).ToArray());
            Assert.DoesNotContain("n0", group.Learners);

            await using DfsClient check = await cluster.ConnectAsync(new TestClientOptions { Name = "check" }, Ct);
            DfsVolume checkVolume = check.GetVolume(Vol);
            int committedOutcomes = 0;
            foreach ((DfsOperationId id, string path, byte[] bytes, Exception? error) in new[] { (ka, "/side-a", aBytes, errors[0]), (kb, "/side-b", bBytes, errors[1]) })
            {
                // A side's write that was appended without a valid quorum may still be committed after healing (its leader
                // may win again); the resolution must be definite and agree with what the client was told and with the
                // namespace.
                DfsOperationOutcome outcome = await AssertResolvedConsistentlyAsync(checkVolume, id, error,
                    () => AssertContentAsync(checkVolume, path, bytes, $"[{shape}, step {blockAtStep}] committed side write {path}"),
                    () => AssertAbsentAsync(checkVolume, path, $"[{shape}, step {blockAtStep}] aborted side write"),
                    $"[{shape}, step {blockAtStep}] {path}");
                if (outcome.Status == DfsOutcomeStatus.Committed)
                    committedOutcomes++;
            }

            Assert.True(committedOutcomes <= 1, $"[{shape}] both partition sides' writes ended up committed");
            await AssertContentAsync(checkVolume, "/baseline", baseline, "pre-transition data");
            return arm.Hits;
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-015 NewReplicaCannotVotePrematurely
    // ------------------------------------------------------------------------------------------------------------

    [Fact, Catalog("CON-015", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    public async Task CON_015_NewReplicaCannotVotePrematurely()
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            var files = new List<(string Path, byte[] Bytes)>();
            await volume.Directories.CreateDirectoryAsync("/bulk", Ct);
            for (int i = 0; i < 250; i++)
            {
                byte[] bytes = TestData.Bytes(0x1500 + (ulong)i, 48);
                string path = "/bulk/f" + i.ToString(CultureInfo.InvariantCulture);
                await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                files.Add((path, bytes));
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);

            // An empty replacement joins; its catch-up of the volume group is held so it stays behind.
            TestNode joiner = await cluster.AddNodeAsync(new TestNodeSpec("n3", "z4"), join: true, Ct);
            FaultArm holdCatchUp = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: joiner.Name, Volume: Vol, Count: 1000), FaultAction.Block());
            Task drain = cluster.Admin.DrainNodeAsync("n0", Ct);
            await holdCatchUp.WaitTriggeredAsync(ElectionBound * 2);
            cluster.Faults.AssertReached(holdCatchUp);

            // While catching up, the replacement is a learner everywhere and the replaced voter is retained.
            foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            {
                GroupInspection? group = await node.Inspect.GetGroupAsync(Vol);
                if (group is null)
                    continue;
                Assert.DoesNotContain(joiner.Name, group.Voters);
                Assert.Contains("n0", group.Voters);
            }

            Assert.Contains(joiner.Name, (await cluster.Inspect.GetVolumeGroupAsync(Vol)).Learners);

            // Premature-vote probe: with two original voters down, the replacement's vote must not complete a quorum.
            TestNode n0 = cluster.Node("n0");
            TestNode n1 = cluster.Node("n1");
            TestNode n2 = cluster.Node("n2");
            await n1.CrashAsync(CrashMode.LoseUnflushed);
            await n2.CrashAsync(CrashMode.LoseUnflushed);
            await cluster.WaitUntilAsync(async () => await LeaderAmongAsync([n0, joiner]) is null, ElectionBound,
                "a leader without a quorum of the configured voters must step down", Ct);
            ulong n0Commit = (await GroupOnAsync(n0)).CommitIndex;
            Exception? error = await CaptureAsync(async () =>
            {
                await using DfsClient probeClient = await cluster.ConnectAsync(new TestClientOptions
                {
                    Name = "probe",
                    SeedNodes = ["n0", joiner.Name],
                    OperationTimeout = ShortOperationTimeout,
                    ConnectTimeout = ShortOperationTimeout,
                }, Ct);
                await probeClient.GetVolume(Vol).Files.WriteAllBytesAsync("/premature", TestData.Bytes(0x1501, 64), Ct);
            });
            AssertNoCommitClaimed(error, "one configured voter plus an unadmitted replacement");

            for (int probe = 0; probe < 10; probe++)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct);
                Assert.Null(await LeaderAmongAsync([n0, joiner]));
                Assert.Equal(n0Commit, (await GroupOnAsync(n0)).CommitIndex);
            }

            await n1.RestartAsync();
            await n2.RestartAsync();
            holdCatchUp.Release();
            holdCatchUp.Disarm();
            await cluster.WaitUntilAsync(() => Task.FromResult(drain.IsCompleted), TimeSpan.FromSeconds(90), "the drain completes once the replacement caught up", Ct);
            await drain;

            // Admitted only after catch-up: now a voter, holding the complete state.
            GroupInspection after = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            Assert.Contains(joiner.Name, after.Voters);
            Assert.DoesNotContain("n0", after.Voters);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            Assert.False(await volume.Files.ExistsAsync("/premature", Ct));
            await cluster.TransferLeadershipAsync(Vol, joiner.Name, waitUntilServing: false, ct: Ct);
            foreach (int index in new[] { 0, 1, 124, 248, 249 })
                await AssertContentAsync(volume, files[index].Path, files[index].Bytes, "served by the admitted replacement");
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-016 RemovedNodeCannotRejoinByOldDisk
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-016", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("crashed-then-force-removed", CrashMode.LoseUnflushed)]
    [InlineData("stopped-then-force-removed", CrashMode.LoseUnflushed)]
    [InlineData("removed-while-running", CrashMode.KeepUnflushed)]
    public async Task CON_016_RemovedNodeCannotRejoinByOldDisk(string variant, CrashMode crashMode)
    {
        await ClusterTest.RunAsync(TransitionOptions, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            byte[] before = TestData.Bytes(0x1601, 90_000);
            await volume.Files.WriteAllBytesAsync("/before-removal", before, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode victim = cluster.Node("n0");
            switch (variant)
            {
                case "crashed-then-force-removed":
                    await victim.CrashAsync(crashMode);
                    await cluster.Admin.RemoveNodeAsync("n0", force: true, Ct);
                    break;
                case "stopped-then-force-removed":
                    await victim.StopAsync();
                    await cluster.Admin.RemoveNodeAsync("n0", force: true, Ct);
                    break;
                default:
                    await cluster.Admin.RemoveNodeAsync("n0", force: false, Ct);
                    if (victim.State == TestNodeState.Running)
                        await victim.CrashAsync(crashMode);
                    break;
            }

            // The removal is committed: the member is Removed and every group is reconfigured without it.
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.ListNodesAsync(Ct)).Any(n => n.Name == "n0" && n.Status == DfsNodeStatus.Removed),
                ElectionBound * 3, "n0 must be marked removed", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Vol);
                return !group.Voters.Contains("n0") && group.Voters.Count == 3;
            }, TimeSpan.FromSeconds(60), "the volume group must replace the removed voter", Ct);
            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            Assert.DoesNotContain("n0", control.Voters);
            Assert.DoesNotContain("n0", control.Learners);

            byte[] after = TestData.Bytes(0x1602, 90_000);
            await volume.Files.WriteAllBytesAsync("/after-removal", after, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            GroupInspection volumeBefore = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            GroupInspection controlBefore = await cluster.Inspect.GetControlGroupAsync();

            // The removed member returns with its old disk and its old configuration.
            await victim.RestartAsync();
            var probeFiles = new List<string>();
            for (int probe = 0; probe < 20; probe++)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct);
                if (victim.State == TestNodeState.Running)
                {
                    GroupInspection? volumeOnVictim = await victim.Inspect.GetGroupAsync(Vol);
                    GroupInspection? controlOnVictim = await victim.Inspect.GetGroupAsync(Control);
                    Assert.True(volumeOnVictim is null || volumeOnVictim.Role != RaftRole.Leader, $"probe {probe}: removed n0 claims volume leadership");
                    Assert.True(controlOnVictim is null || controlOnVictim.Role != RaftRole.Leader, $"probe {probe}: removed n0 claims control leadership");
                }

                await cluster.Inspect.AssertNoSplitBrainAsync();
                DfsNodeInfo info = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == "n0");
                Assert.Equal(DfsNodeStatus.Removed, info.Status);
                Assert.False(info.IsControlVoter);
                string path = "/probe" + probe.ToString(CultureInfo.InvariantCulture);
                await volume.Files.WriteAllBytesAsync(path, TestData.Bytes(0x1610 + (ulong)probe, 70_000), Ct);
                probeFiles.Add(path);
            }

            // The healthy groups were not disrupted by stale votes or appends from the removed member.
            GroupInspection volumeAfter = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            GroupInspection controlAfter = await cluster.Inspect.GetControlGroupAsync();
            Assert.Equal(volumeBefore.Term, volumeAfter.Term);
            Assert.Equal(volumeBefore.Leader, volumeAfter.Leader);
            Assert.Equal(controlBefore.Term, controlAfter.Term);
            Assert.DoesNotContain("n0", volumeAfter.Voters);
            Assert.DoesNotContain("n0", volumeAfter.Learners);

            // Data placement never counts the removed member.
            foreach (string path in probeFiles.Prepend("/after-removal"))
            {
                FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
                Assert.NotNull(file);
                foreach (ExtentInspection extent in file!.Extents)
                    Assert.DoesNotContain("n0", await cluster.Inspect.GetObjectHoldersAsync(extent.Object));
            }

            await AssertContentAsync(volume, "/before-removal", before, "data written before the removal");
            await AssertContentAsync(volume, "/after-removal", after, "data written after the removal");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-017 ControlGroupUnavailable
    // ------------------------------------------------------------------------------------------------------------

    [Fact, Catalog("CON-017", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    public async Task CON_017_ControlGroupUnavailable()
    {
        TestClusterOptions options = Det(TestClusterOptions.FiveNodes) with
        {
            ControlVoters = 3,
            Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = 3 } }],
            ScenarioTimeout = TimeSpan.FromSeconds(300),
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            Assert.Equal(3, control.Voters.Count);
            string[] victims = control.Voters.Take(2).ToArray();
            string[] keep = cluster.Nodes.Select(n => n.Name).Except(victims, StringComparer.Ordinal).ToArray();
            Assert.Equal(3, keep.Length);

            // A volume whose voters all survive the loss of the control quorum.
            await cluster.Admin.CreateVolumeAsync(new DfsVolumeSpec
            {
                Name = "data",
                Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = keep },
            }, Ct);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", SeedNodes = keep, OperationTimeout = ElectionBound }, Ct);
            DfsVolume data = client.GetVolume("data");
            byte[] a = TestData.Bytes(0x1701, 6000);
            await data.Files.WriteAllBytesAsync("/a", a, Ct);
            await data.Directories.CreateDirectoryAsync("/dir", Ct);
            await cluster.WaitForReplicationAsync("data", Ct);
            await using DfsClient adminClient = await cluster.ConnectAsync(new TestClientOptions { Name = "admin2", SeedNodes = keep, OperationTimeout = ShortOperationTimeout }, Ct);
            DfsAdminClient admin = adminClient.Admin;

            foreach (string victim in victims)
                await cluster.Node(victim).CrashAsync(CrashMode.LoseUnflushed);
            await cluster.WaitUntilAsync(async () => await LeaderAmongAsync(cluster.Nodes, Control) is null, ElectionBound,
                "the control group has no quorum and must have no leader", Ct);

            // Established volume operations of the existing epoch continue.
            byte[] b = TestData.Bytes(0x1702, 70_000);
            byte[] suffix = TestData.Bytes(0x1703, 400);
            CommitToken tb = await data.Files.WriteAllBytesAsync("/b", b, Ct);
            CommitToken ta = await data.Files.AppendAllBytesAsync("/a", suffix, Ct);
            Assert.True(ta.CompareTo(tb) > 0);
            await data.Files.MoveAsync("/b", "/dir/b", false, Ct);
            await data.Directories.CreateDirectoryAsync("/dir/sub", Ct);
            await AssertContentAsync(data, "/a", Concat(a, suffix), "append during control outage");
            await AssertContentAsync(data, "/dir/b", b, "move during control outage");
            var names = new List<string>();
            await foreach (DfsEntryInfo entry in data.Directories.EnumerateAsync("/dir", null, Ct))
                names.Add(entry.Name);
            Assert.Equal(["b", "sub"], names.Order(StringComparer.Ordinal).ToArray());
            DfsSnapshotInfo snapshot = await data.Snapshots.CreateAsync("during-outage", null, Ct);
            Assert.True(snapshot.Boundary.CompareTo(ta) >= 0);
            await using (DfsTransaction tx = await data.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/tx1", TestData.Bytes(0x1704, 100), Ct);
                await tx.Files.WriteAllBytesAsync("/tx2", TestData.Bytes(0x1705, 100), Ct);
                await tx.CommitAsync(Ct);
            }

            await AssertContentAsync(data, "/tx2", TestData.Bytes(0x1705, 100), "transaction during control outage");

            // Configuration mutations fail explicitly and promptly rather than hang or pretend to succeed.
            var attempts = new (string Name, Func<Task> Action)[]
            {
                ("CreateVolume", () => admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "vol2" }, Ct)),
                ("DefinePolicy", () => admin.DefinePolicyAsync("data", "two", new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 }, Ct)),
                ("CreatePrincipal", () => admin.CreatePrincipalAsync("late-user", "late-secret-123", false, Ct)),
                ("SetFeature", () => admin.SetFeatureAsync("con-017-feature", true, Ct)),
                ("AddNode", () => admin.AddNodeAsync("n9", new DfsTopologyLabels("r1", "z9", "rk1", "h9", "d1"), ["mem://n9"], Ct)),
                ("Grant", () => admin.GrantAsync("data", "late-user", DfsPermissions.Read, Ct)),
            };
            var unknown = new HashSet<string>(StringComparer.Ordinal);
            foreach ((string name, Func<Task> action) in attempts)
            {
                long start = cluster.Time.GetTimestamp();
                Exception? error = await CaptureAsync(action);
                TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
                Assert.True(error is not null, $"{name} succeeded without a control quorum");
                Assert.True(IsUnavailability(error!) || IsUnknownOutcome(error!), $"{name}: expected an explicit availability error, got {error}");
                Assert.True(elapsed <= ShortOperationTimeout + TimeSpan.FromSeconds(5), $"{name} took {elapsed} to fail");
                if (IsUnknownOutcome(error!))
                    unknown.Add(name);
            }

            // Restore the control quorum: nothing that failed explicitly was applied later.
            foreach (string victim in victims)
                await cluster.Node(victim).RestartAsync();
            await cluster.WaitForStableAsync(ElectionBound * 3, Ct);
            IReadOnlyList<string> volumes = await cluster.Admin.ListVolumesAsync(Ct);
            if (!unknown.Contains("CreateVolume"))
                Assert.DoesNotContain("vol2", volumes);
            if (!unknown.Contains("AddNode"))
                Assert.DoesNotContain(await cluster.Admin.ListNodesAsync(Ct), n => n.Name == "n9");
            if (!unknown.Contains("DefinePolicy"))
                Assert.False((await data.GetInfoAsync(Ct)).NamedPolicies.ContainsKey("two"), "a policy definition that failed explicitly was applied later");
            if (!unknown.Contains("SetFeature"))
                Assert.DoesNotContain("con-017-feature", (await cluster.Admin.GetClusterStatusAsync(Ct)).ActiveFeatures);
            if (!unknown.Contains("CreatePrincipal"))
            {
                Exception? login = await CaptureAsync(async () =>
                {
                    await using DfsClient late = await cluster.ConnectAsync(new TestClientOptions
                    {
                        Name = "late",
                        Credentials = new DfsCredentials("late-user", "late-secret-123"),
                        SeedNodes = keep,
                    }, Ct);
                });
                Assert.True(login is DfsAuthenticationException, $"a principal whose creation failed explicitly must not exist later; login gave {login?.GetType().Name ?? "success"}");
            }

            await cluster.Admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "vol3" }, Ct);
            Assert.Contains("vol3", await cluster.Admin.ListVolumesAsync(Ct));
            await AssertContentAsync(data, "/a", Concat(a, suffix), "data after control recovery");
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-018 VolumeGroupsAreIndependent
    // ------------------------------------------------------------------------------------------------------------

    [Theory, Catalog("CON-018", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData("va-loses-quorum-5-nodes")]
    [InlineData("vb-loses-quorum-5-nodes")]
    [InlineData("va-loses-all-voters-6-nodes")]
    public async Task CON_018_VolumeGroupsAreIndependent(string variant)
    {
        bool six = variant.EndsWith("6-nodes", StringComparison.Ordinal);
        static TestVolumeSpec Explicit(string name, params string[] voters) =>
            new(name) { Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = voters } };
        TestClusterOptions options = Det(new TestClusterOptions
        {
            NodeCount = six ? 6 : 5,
            ControlVoters = 5,
            Volumes = six
                ? [Explicit("va", "n0", "n1", "n2"), Explicit("vb", "n3", "n4", "n5")]
                : [Explicit("va", "n0", "n1", "n2"), Explicit("vb", "n2", "n3", "n4")],
            ScenarioTimeout = TimeSpan.FromSeconds(300),
        });
        string[] victims = variant switch
        {
            "va-loses-quorum-5-nodes" => ["n0", "n1"],
            "vb-loses-quorum-5-nodes" => ["n3", "n4"],
            _ => ["n0", "n1", "n2"],
        };
        string lostName = variant.StartsWith("va", StringComparison.Ordinal) ? "va" : "vb";
        string healthyName = lostName == "va" ? "vb" : "va";

        await ClusterTest.RunAsync(options, async cluster =>
        {
            string[] survivors = cluster.Nodes.Select(n => n.Name).Except(victims, StringComparer.Ordinal).ToArray();
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume healthy = client.GetVolume(healthyName);
            byte[] healthyBefore = TestData.Bytes(0x1801, 5000);
            byte[] lostBefore = TestData.Bytes(0x1802, 5000);
            CommitToken healthyToken = await healthy.Files.WriteAllBytesAsync("/h", healthyBefore, Ct);
            await client.GetVolume(lostName).Files.WriteAllBytesAsync("/l", lostBefore, Ct);
            await cluster.WaitForReplicationAsync(healthyName, Ct);
            await cluster.WaitForReplicationAsync(lostName, Ct);

            await using DfsClient lostClient = await cluster.ConnectAsync(new TestClientOptions { Name = "lost", SeedNodes = survivors, OperationTimeout = ShortOperationTimeout }, Ct);
            DfsVolume lost = lostClient.GetVolume(lostName);
            DfsOperationId lostOp = await lost.NewOperationIdAsync(Ct);

            foreach (string victim in victims)
                await cluster.Node(victim).CrashAsync(CrashMode.LoseUnflushed);
            if (six)
                Assert.True(survivors.Length * 2 <= cluster.Nodes.Count, "this variant leaves no cluster-wide node majority");

            // The healthy volume keeps working on its own voters' majority.
            byte[] healthyAfter = TestData.Bytes(0x1803, 70_000);
            CommitToken t1 = await healthy.Files.WriteAllBytesAsync("/h2", healthyAfter, Ct);
            CommitToken t2 = await healthy.Files.AppendAllBytesAsync("/h", TestData.Bytes(0x1804, 100), Ct);
            await healthy.Directories.CreateDirectoryAsync("/hd", Ct);
            Assert.True(healthyToken.CompareTo(t1) < 0 && t1.CompareTo(t2) < 0);
            await AssertContentAsync(healthy, "/h2", healthyAfter, "healthy volume during the other's outage");
            await AssertContentAsync(healthy, "/h", Concat(healthyBefore, TestData.Bytes(0x1804, 100)), "healthy append");

            // The volume that lost its own voters' majority is explicitly unavailable: no commit, no false absence.
            Exception? writeError = await CaptureAsync(() => lost.Files.WriteAllBytesAsync("/l2", TestData.Bytes(0x1805, 100), WithId(lostOp), Ct));
            AssertNoCommitClaimed(writeError, $"{lostName} without its voters' majority");
            (bool exists, Exception? existsError) = await CaptureValueAsync(() => lost.Files.ExistsAsync("/l", Ct));
            Assert.True(existsError is not null && IsUnavailability(existsError), $"Exists must report unavailability, not {(existsError is null ? exists.ToString() : existsError.ToString())}");
            (byte[]? bytes, Exception? readError) = await CaptureValueAsync(() => lost.Files.ReadAllBytesAsync("/l", Ct));
            Assert.True(readError is not null && IsUnavailability(readError), $"a read must report unavailability, not {(readError is null ? bytes?.Length.ToString(CultureInfo.InvariantCulture) + " bytes" : readError.ToString())}");

            foreach (string victim in victims)
                await cluster.Node(victim).RestartAsync();
            await cluster.WaitForStableAsync(ElectionBound * 3, Ct);
            await cluster.WaitForReplicationAsync(lostName, Ct);
            await cluster.WaitForReplicationAsync(healthyName, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(lostName);
            await cluster.Inspect.AssertReplicasConvergedAsync(healthyName);

            // A surviving voter of the lost volume may have been its leader and appended the write locally; after the
            // victims return it may legitimately commit it. The resolution must be definite and match the client's report.
            await AssertResolvedConsistentlyAsync(client.GetVolume(lostName), lostOp, writeError,
                () => AssertContentAsync(client.GetVolume(lostName), "/l2", TestData.Bytes(0x1805, 100), "a write committed after the voters returned"),
                () => AssertAbsentAsync(client.GetVolume(lostName), "/l2", "an aborted write"),
                $"{lostName} write without its voters' majority");
            await AssertContentAsync(client.GetVolume(lostName), "/l", lostBefore, "the unavailable volume's data survives");
            await AssertContentAsync(healthy, "/h2", healthyAfter, "healthy volume after recovery");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-019 BootstrapWithoutFilesystemRecursion
    // ------------------------------------------------------------------------------------------------------------

    private static readonly MessageType[] NamespaceRequestTypes =
    [
        MessageType.OpenHandle, MessageType.CloseHandle, MessageType.ReadRange, MessageType.GetInfo, MessageType.ListDirectory,
        MessageType.CreateStaging, MessageType.CommitFile, MessageType.WriteAll, MessageType.Append, MessageType.Copy, MessageType.Move,
        MessageType.Delete, MessageType.CreateDirectory, MessageType.SetPolicy, MessageType.SetAccess, MessageType.TxBegin, MessageType.TxOp,
        MessageType.TxCommit, MessageType.TxAbort, MessageType.OpenSnapshot, MessageType.ListSnapshots, MessageType.OpenSession,
    ];

    private static readonly DfsOperationKind[] NamespaceOperations =
    [
        DfsOperationKind.Open, DfsOperationKind.CloseHandle, DfsOperationKind.Read, DfsOperationKind.GetInfo, DfsOperationKind.Exists,
        DfsOperationKind.Enumerate, DfsOperationKind.CreateStaging, DfsOperationKind.CommitFile, DfsOperationKind.WriteAll,
        DfsOperationKind.Append, DfsOperationKind.Copy, DfsOperationKind.Move, DfsOperationKind.Delete, DfsOperationKind.CreateDirectory,
        DfsOperationKind.TxBegin, DfsOperationKind.TxOp, DfsOperationKind.TxCommit, DfsOperationKind.TxAbort, DfsOperationKind.OpenSnapshot,
        DfsOperationKind.OpenSession,
    ];

    [Theory, Catalog("CON-019", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [InlineData(BootstrapMode.Static, "graceful-stop")]
    [InlineData(BootstrapMode.Static, "crash")]
    [InlineData(BootstrapMode.SeedJoin, "crash")]
    public async Task CON_019_BootstrapWithoutFilesystemRecursion(BootstrapMode bootstrap, string shutdown)
    {
        await ClusterTest.RunAsync(Det(TestClusterOptions.ThreeZones) with { Bootstrap = bootstrap }, async cluster =>
        {
            // User data whose names imitate internal metadata: recovery must not depend on (or be confused by) them.
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/data/report"] = TestData.Bytes(0x1901, 8000),
                ["/.tdfs/superblock"] = TestData.Bytes(0x1902, 512),
                ["/.tdfs/control/journal"] = TestData.Bytes(0x1903, 4096),
                ["/$raft/vol/0000000001.log"] = TestData.Bytes(0x1904, 777),
                ["/checkpoint"] = TestData.Bytes(0x1905, 2048),
                ["/trust/cluster.key"] = TestData.Bytes(0x1906, 32),
            };
            var inodes = new Dictionary<string, InodeId>(StringComparer.Ordinal);
            await using (DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct))
            {
                DfsVolume volume = client.GetVolume(Vol);
                foreach ((string path, byte[] bytes) in files)
                {
                    string parent = DfsPath.GetParent(path);
                    if (parent != DfsPath.Root)
                        await volume.Directories.CreateDirectoryAsync(parent, Ct);
                    await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                    inodes[path] = (await volume.Files.GetInfoAsync(path, Ct)).InodeId;
                }
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            foreach (TestNode node in cluster.Nodes)
            {
                if (shutdown == "crash")
                    await node.CrashAsync(CrashMode.LoseUnflushed);
                else
                    await node.StopAsync();
            }

            // Cold start from local superblocks, journals, checkpoints and trust data only (no client exists meanwhile).
            cluster.Network.Log.Reset();
            int hitsBefore = cluster.Faults.Coverage.Hits.Count;
            foreach (TestNode node in cluster.Nodes.Reverse())
                await node.RestartAsync();
            await cluster.WaitForStableAsync(ElectionBound * 3, Ct);

            foreach (MessageType type in NamespaceRequestTypes)
                Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(Type: type)));
            FaultContext[] recoveryHits = cluster.Faults.Coverage.Hits.Skip(hitsBefore).ToArray();
            Assert.DoesNotContain(recoveryHits, h => h.Site == FaultSites.RpcRequestAccepted && NamespaceOperations.Contains(h.Operation));
            Assert.DoesNotContain(recoveryHits, h => h.Site == FaultSites.ClientOperationStart && NamespaceOperations.Contains(h.Operation));
            Assert.Contains(recoveryHits, h => h.Site == FaultSites.RecoveryJournalReplay || h.Site == FaultSites.RecoveryCheckpointInstall);

            // The cluster identity was recovered from local trust data, and the data is intact.
            await using DfsClient after = await cluster.ConnectAsync(new TestClientOptions { Name = "after", ExpectedClusterId = cluster.ClusterId }, Ct);
            Assert.Equal(cluster.ClusterId, after.Cluster.ClusterId);
            DfsVolume afterVolume = after.GetVolume(Vol);
            foreach ((string path, byte[] bytes) in files)
            {
                await AssertContentAsync(afterVolume, path, bytes, "after a cold restart");
                Assert.Equal(inodes[path], (await afterVolume.Files.GetInfoAsync(path, Ct)).InodeId);
            }

            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // CON-020 AllNodesRestartFromDurableImages
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, CrashMode, string> RestartOrders => new()
    {
        { 3, CrashMode.LoseUnflushed, "0,1,2" },
        { 3, CrashMode.LoseUnflushed, "0,2,1" },
        { 3, CrashMode.LoseUnflushed, "1,0,2" },
        { 3, CrashMode.LoseUnflushed, "1,2,0" },
        { 3, CrashMode.LoseUnflushed, "2,0,1" },
        { 3, CrashMode.LoseUnflushed, "2,1,0" },
        { 3, CrashMode.TearLastWrite, "2,0,1" },
        { 3, CrashMode.RandomSubset, "1,2,0" },
        { 3, CrashMode.KeepUnflushed, "0,1,2" },
        { 5, CrashMode.LoseUnflushed, "4,2,0,3,1" },
        { 5, CrashMode.LoseUnflushed, "0,1,2,3,4" },
        { 5, CrashMode.TearLastWrite, "3,4,0,1,2" },
    };

    [Theory, Catalog("CON-020", "S", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [MemberData(nameof(RestartOrders))]
    public async Task CON_020_AllNodesRestartFromDurableImages(int nodeCount, CrashMode crashMode, string order)
    {
        TestClusterOptions options = Det(nodeCount == 5 ? TestClusterOptions.FiveNodes : TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = ElectionBound }, Ct);
            DfsVolume volume = client.GetVolume(Vol);

            // Acknowledged commits of every kind.
            var expected = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/big"] = TestData.Bytes(0x2001, 150_000),
                ["/small"] = TestData.Bytes(0x2002, 100),
                ["/tx/a"] = TestData.Bytes(0x2003, 3000),
                ["/tx/b"] = TestData.Bytes(0x2004, 70_000),
            };
            await volume.Files.WriteAllBytesAsync("/big", expected["/big"], Ct);
            await volume.Files.WriteAllBytesAsync("/small", expected["/small"], Ct);
            byte[] logPrefix = TestData.Bytes(0x2005, 900);
            byte[] logSuffix1 = TestData.Bytes(0x2006, 300);
            byte[] logSuffix2 = TestData.Bytes(0x2007, 70_000);
            await volume.Files.WriteAllBytesAsync("/log", logPrefix, Ct);
            await volume.Files.AppendAllBytesAsync("/log", logSuffix1, Ct);
            await volume.Files.AppendAllBytesAsync("/log", logSuffix2, Ct);
            expected["/log"] = Concat(logPrefix, logSuffix1, logSuffix2);
            await volume.Directories.CreateDirectoryAsync("/d/e", Ct);
            byte[] movedBytes = TestData.Bytes(0x2008, 2000);
            await volume.Files.WriteAllBytesAsync("/to-move", movedBytes, Ct);
            await volume.Files.MoveAsync("/to-move", "/d/e/moved", false, Ct);
            expected["/d/e/moved"] = movedBytes;
            await volume.Files.WriteAllBytesAsync("/deleted", TestData.Bytes(0x2009, 100), Ct);
            await volume.Files.DeleteAsync("/deleted", Ct);
            await volume.Directories.CreateDirectoryAsync("/tx", Ct);
            await using (DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/tx/a", expected["/tx/a"], Ct);
                await tx.Files.WriteAllBytesAsync("/tx/b", expected["/tx/b"], Ct);
                await tx.CommitAsync(Ct);
            }

            byte[] snapshotBig = expected["/big"];
            await volume.Snapshots.CreateAsync("s1", null, Ct);
            expected["/big"] = TestData.Bytes(0x200A, 160_000);
            await volume.Files.WriteAllBytesAsync("/big", expected["/big"], Ct);

            var identities = new Dictionary<string, (InodeId Inode, ulong Revision)>(StringComparer.Ordinal);
            foreach (string path in expected.Keys)
            {
                DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
                identities[path] = (info.InodeId, info.Revision);
            }

            // An operation whose reply was lost: its committed outcome record must also survive the cold restart.
            DfsOperationId lostReply = await volume.NewOperationIdAsync(Ct);
            byte[] lostBytes = TestData.Bytes(0x200B, 5000);
            FaultArm drop = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.WriteAll, Count: 1000), FaultAction.DropReply());
            FaultArm dropQueries = ArmDropOutcomeQueryReplies(cluster);
            Exception? lostError = await CaptureAsync(() => volume.Files.WriteAllBytesAsync("/lost-reply", lostBytes, WithId(lostReply), Ct));
            cluster.Faults.AssertReached(drop);
            drop.Disarm();
            dropQueries.Disarm();
            AssertUnknownOutcome(lostError, lostReply, "an executed WriteAll whose every reply was lost");
            DfsOperationOutcome lostOutcome = await volume.QueryOperationAsync(lostReply, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, lostOutcome.Status);
            expected["/lost-reply"] = lostBytes;
            await cluster.WaitForReplicationAsync(Vol, Ct);

            // A write in flight when every node dies: after recovery it is either wholly committed or wholly absent.
            DfsOperationId inflightOp = await volume.NewOperationIdAsync(Ct);
            byte[] inflightBytes = TestData.Bytes(0x200C, 90_000);
            Task<Exception?> inflight = CaptureAsync(() => volume.Files.WriteAllBytesAsync("/inflight", inflightBytes, WithId(inflightOp), Ct));
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), Ct);
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(crashMode);

            int[] restartOrder = order.Split(',').Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToArray();
            int majority = (nodeCount / 2) + 1;
            for (int i = 0; i < restartOrder.Length; i++)
            {
                await cluster[restartOrder[i]].RestartAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(150), Ct);
                if (i + 1 >= majority)
                    continue;

                // Fewer than a majority of voters are back: unavailability is reported, state is never fabricated.
                string[] up = restartOrder.Take(i + 1).Select(ix => cluster[ix].Name).ToArray();
                (DfsClient? partial, Exception? connectError) = await CaptureValueAsync(() => cluster.ConnectAsync(
                    new TestClientOptions { Name = "partial" + i.ToString(CultureInfo.InvariantCulture), SeedNodes = up, OperationTimeout = ShortOperationTimeout, ConnectTimeout = ShortOperationTimeout }, Ct));
                if (partial is null)
                {
                    Assert.True(connectError is not null && (IsUnavailability(connectError) || connectError is DfsException or IOException), $"connect failure must be explicit: {connectError}");
                    continue;
                }

                await using (partial)
                {
                    DfsVolume partialVolume = partial.GetVolume(Vol);
                    (bool exists, Exception? existsError) = await CaptureValueAsync(() => partialVolume.Files.ExistsAsync("/big", Ct));
                    Assert.True(existsError is not null && IsUnavailability(existsError), $"Exists with {i + 1} of {nodeCount} nodes must report unavailability, not {(existsError is null ? exists.ToString() : existsError.ToString())}");
                    (byte[]? read, Exception? readError) = await CaptureValueAsync(() => partialVolume.Files.ReadAllBytesAsync("/big", Ct));
                    Assert.True(readError is not null && IsUnavailability(readError), $"read with {i + 1} of {nodeCount} nodes must report unavailability (got {read?.Length} bytes / {readError})");
                }
            }

            await cluster.WaitForStableAsync(ElectionBound * 3, Ct);
            Exception? inflightError = await inflight;
            await client.DisposeAsync();

            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "fresh", OperationTimeout = ElectionBound }, Ct);
            DfsVolume freshVolume = fresh.GetVolume(Vol);
            foreach ((string path, byte[] bytes) in expected)
                await AssertContentAsync(freshVolume, path, bytes, $"acknowledged {path} after cold restart in order {order}");
            foreach ((string path, (InodeId inode, ulong revision)) in identities)
            {
                DfsFileInfo info = await freshVolume.Files.GetInfoAsync(path, Ct);
                Assert.Equal(inode, info.InodeId);
                Assert.Equal(revision, info.Revision);
            }

            Assert.False(await freshVolume.Files.ExistsAsync("/deleted", Ct));
            Assert.False(await freshVolume.Files.ExistsAsync("/to-move", Ct));
            DfsOperationOutcome lostAfter = await freshVolume.QueryOperationAsync(lostReply, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, lostAfter.Status);
            Assert.Equal(lostOutcome.Token, lostAfter.Token);
            await using (DfsSnapshotView view = await freshVolume.Snapshots.OpenAsync("s1", Ct))
                await AssertBytesAsync(snapshotBig, view.Files.ReadAllBytesAsync("/big", Ct), "snapshot after cold restart");

            // Wholly committed or wholly absent, consistent with what the writer was told (a definite failure may only be
            // reported for a write that can never commit).
            await AssertResolvedConsistentlyAsync(freshVolume, inflightOp, inflightError,
                () => AssertContentAsync(freshVolume, "/inflight", inflightBytes, "committed in-flight write"),
                () => AssertAbsentAsync(freshVolume, "/inflight", "an aborted in-flight write"),
                $"in-flight write across the cold restart in order {order}");

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }
}
