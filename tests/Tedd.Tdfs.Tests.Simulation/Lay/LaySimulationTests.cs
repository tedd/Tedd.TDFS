using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Lay.LayHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Lay;

/// <summary>
/// LAY — extent mapping, small files, and partial updates; deterministic-simulation rows (test plan §7.11; FSD §10, §15).
/// Invariants: INV-01, INV-02, INV-09, INV-16.
/// </summary>
public sealed class LaySimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // LAY-002 InlineHonorsStrongerPolicy
    // ------------------------------------------------------------------------------------------------------------

    private static readonly DfsStoragePolicy Strong = Policy(4, 4, 3, 3);

    /// <summary>Five zones; the volume has 3 or 5 explicit metadata voters and a named policy "strong" (ack 3 copies / 3 zones).</summary>
    private static TestClusterOptions FiveZones(int voters) => TestClusterOptions.FiveNodes with
    {
        Mode = ClusterMode.Deterministic,
        DefaultPolicy = Policy(3, 3, 2, 2),
        Volumes =
        [
            new TestVolumeSpec(Vol)
            {
                Metadata = new DfsMetadataPolicy
                {
                    Voters = voters,
                    Placement = DfsVoterPlacement.Explicit,
                    ExplicitVoterNodes = Enumerable.Range(0, voters).Select(i => $"n{i}").ToList(),
                },
                NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["strong"] = Strong },
            },
        ],
    };

    public static TheoryData<int, int> InlinePolicyCases()
    {
        var data = new TheoryData<int, int>();
        foreach (int length in new[] { 1, DefaultInline - 1, DefaultInline })
        {
            data.Add(length, 3);
            data.Add(length, 5);
        }

        return data;
    }

    /// <summary>
    /// A small file under a policy stronger than the metadata majority's guarantee (3 voters: any majority is 2 hosts in
    /// 2 zones; policy acknowledges 3/3) is not acknowledged on inline metadata alone: at acknowledgement its bytes are
    /// backed by extents whose objects are durable on ≥ 3 hosts in ≥ 3 zones. With 5 voters (majority 3 hosts / 3 zones)
    /// the metadata guarantee suffices and the file is inline; a default-policy file is inline in both configurations.
    /// </summary>
    [Theory]
    [MemberData(nameof(InlinePolicyCases))]
    [Catalog("LAY-002", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_002_InlineHonorsStrongerPolicy(int length, int voters)
    {
        await ClusterTest.RunAsync(FiveZones(voters), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes((ulong)(2_000 + length), length);

            await vol.Files.WriteAllBytesAsync("/default", data, Ct);
            Assert.True((await vol.Files.GetInfoAsync("/default", Ct)).IsInline, "default 2/2 policy is satisfied by any metadata majority");

            await vol.Files.WriteAllBytesAsync("/strong", data, new DfsMutationOptions { StoragePolicy = "strong" }, Ct);
            Same(data, await vol.Files.ReadAllBytesAsync("/strong", Ct), "strong small file contents");
            FileInspection file = await InspectAsync(cluster, "/strong");
            AssertWellFormed(file, "strong");
            if (voters == 5)
            {
                Assert.True(file.IsInline, "five voters: every majority spans 3 hosts in 3 zones, which satisfies ack 3/3");
                return;
            }

            Assert.False(file.IsInline && file.Extents.Count == 0, "inline-only storage would acknowledge 3/3 data on a 2-of-3 metadata majority");
            long covered = 0;
            foreach (ExtentInspection e in file.Extents.OrderBy(e => e.Offset))
            {
                Assert.True(e.Offset <= covered, $"gap before extent at {e.Offset}");
                covered = Math.Max(covered, e.Offset + e.Length);
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(e.Object);
                Assert.True(copies >= 3 && zones >= 3, $"object {e.Object} has {copies} copies in {zones} zones at acknowledgement; policy requires 3/3");
            }

            Assert.True(covered >= length, "the supplementing extents must cover the whole file");
        }, Ct);
    }

    /// <summary>
    /// Metadata majority available (all three voters up) but only two hosts can accept data: a small "strong" write must
    /// fail with placement unavailability (never be committed inline through the metadata group), the previous contents
    /// stay current, and a default-policy small write in the same state still succeeds inline.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(DefaultInline - 1)]
    [InlineData(DefaultInline)]
    [Catalog("LAY-002", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_002_InlineHonorsStrongerPolicy_UnsatisfiableDataPlacement(int length)
    {
        await ClusterTest.RunAsync(FiveZones(3), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] old = TestData.Bytes(2_100, 10);
            await vol.Files.WriteAllBytesAsync("/strong", old, new DfsMutationOptions { StoragePolicy = "strong" }, Ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/strong", Ct);

            foreach (string node in new[] { "n2", "n3", "n4" })
                cluster.Node(node).Disk.Fill(leaveFreeBytes: 4L << 20);

            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/strong", TestData.Bytes(2_101, length), Ct), DfsErrorCode.PlacementUnavailable);
            Assert.Equal(3, ex.RequiredCopies);
            Assert.True(ex.AchievedCopies <= 2, $"only two hosts can store data, reported {ex.AchievedCopies}");

            DfsFileInfo after = await vol.Files.GetInfoAsync("/strong", Ct);
            Assert.Equal(before.Revision, after.Revision);
            Same(old, await vol.Files.ReadAllBytesAsync("/strong", Ct), "previous revision stays authoritative");

            byte[] small = TestData.Bytes(2_102, length);
            await vol.Files.WriteAllBytesAsync("/default", small, Ct);
            Assert.True((await vol.Files.GetInfoAsync("/default", Ct)).IsInline);
            Same(small, await vol.Files.ReadAllBytesAsync("/default", Ct), "default policy inline write");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-010 SmallOverwriteDoesNotResendWholeFile
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, int> OverwriteCases() => new()
    {
        { "FirstByte", 1 },
        { "InsideChunk", 1 },
        { "InsideChunk", 100 },
        { "InsideChunk", 4096 },
        { "AcrossChunkBoundary", 100 },
        { "LastBytes", 50 },
    };

    /// <summary>
    /// Overwriting a small range of a 2 MiB file replicates only the changed update unit(s) and one bounded mapping update:
    /// stored bytes across all replicas, uploaded object bytes and the commit request are bounded by the change (plus an
    /// explicitly measured metadata overhead), every untouched extent keeps its object, and the contents are exact.
    /// </summary>
    [Theory]
    [MemberData(nameof(OverwriteCases))]
    [Catalog("LAY-010", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_010_SmallOverwriteDoesNotResendWholeFile(string where, int length)
    {
        const int size = 32 * 64 * 1024;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(10_000, size);
            await vol.Files.WriteAllBytesAsync("/large", data, Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            FileInspection before = await InspectAsync(cluster, "/large");
            int copies = (await vol.GetInfoAsync(Ct)).DefaultPolicy.TargetCopies;

            long offset = where switch
            {
                "FirstByte" => 0,
                "InsideChunk" => 13L * Chunk + 777,
                "AcrossChunkBoundary" => 20L * Chunk - length / 2,
                "LastBytes" => size - length,
                _ => throw new ArgumentOutOfRangeException(nameof(where)),
            };
            byte[] patch = TestData.Bytes(10_001, length);

            NodeCounters baseline = await cluster.Inspect.GetTotalCountersAsync();
            cluster.Network.Log.Reset();
            await using (DfsStream s = await vol.Files.OpenAsync("/large", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                await WriteAtAsync(s, offset, patch);
                await s.FlushAsync(Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            NodeCounters done = await cluster.Inspect.GetTotalCountersAsync();
            long stored = done.ObjectBytesReceived - baseline.ObjectBytesReceived;
            long uploaded = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.PutObject, Direction: LinkDirection.Request));
            long commit = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.CommitFile, Direction: LinkDirection.Request));
            long raft = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.RaftBatch));
            int unitsTouched = (int)((offset + length - 1) / Chunk - offset / Chunk + 1);
            long dataBound = copies * (long)unitsTouched * Chunk + 16 * 1024;
            Log($"{where}/{length}: stored {stored} B, uploaded {uploaded} B, commit {commit} B, raft {raft} B, puts {done.ObjectPuts - baseline.ObjectPuts}; " +
                $"data bound {dataBound} B; whole-file resend would be {(long)size * copies} B");

            Assert.True(stored <= dataBound, $"a {length}-byte overwrite stored {stored} bytes across replicas (bound {dataBound})");
            Assert.True(uploaded <= dataBound, $"a {length}-byte overwrite uploaded {uploaded} bytes (bound {dataBound})");
            Assert.True(commit > 0 && commit <= 4096, $"the mapping update for one changed range must be small, was {commit} bytes");
            Assert.True(raft <= 256 * 1024, $"metadata replication for one small overwrite used {raft} bytes");

            patch.CopyTo(data, offset);
            Same(data, await ReadCompatibleAsync(vol, "/large"), $"{where}/{length}: contents");

            FileInspection after = await InspectAsync(cluster, "/large");
            AssertWellFormed(after, "after overwrite");
            Assert.Equal(before.Inode, after.Inode);
            foreach (ExtentInspection e in before.Extents.Where(e => e.Offset + e.Length <= offset - Chunk || e.Offset >= offset + length + Chunk))
                Assert.Contains(e, after.Extents);
            int newObjects = Objects(after).Except(Objects(before)).Count();
            Assert.True(newObjects <= unitsTouched + 1, $"{newObjects} new objects for {unitsTouched} touched update units");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-016 MappingObjectsNeedDurability
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The mapping record of a publication (the metadata log entry carrying the new extent map) must be durable on a
    /// metadata majority before success is reported, even when every payload object is already durable:
    /// <list type="bullet">
    /// <item>FollowersCut: payload receipts are validated, then the leader is cut from both followers before it can
    /// replicate the entry — success is never reported while cut; after healing, the outcome is resolved and the file
    /// shows exactly the old or the complete new revision.</item>
    /// <item>CrashLeaderAfterLocalAppend: the leader crashes right after appending the entry locally — the outcome is a
    /// success (after a retried commit), or unknown and then resolved; never a half-published mapping.</item>
    /// <item>OneFollowerCut: the leader plus one follower form the majority — success is reported, and after crashing the
    /// leader the new leader serves the new mapping (it was durable on a majority at success).</item>
    /// </list>
    /// </summary>
    [Theory]
    [InlineData("FollowersCut")]
    [InlineData("CrashLeaderAfterLocalAppend")]
    [InlineData("OneFollowerCut")]
    [Catalog("LAY-016", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_016_MappingObjectsNeedDurability(string variant)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] oldData = TestData.Bytes(16_000, 4 * Chunk);
            await vol.Files.WriteAllBytesAsync("/map", oldData, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            FileInspection oldLayout = await InspectAsync(cluster, "/map");
            IReadOnlySet<ObjectId> storedBefore = await StoredAsync(cluster);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            string[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).Select(n => n.Name).ToArray();
            byte[] newData = TestData.Bytes(16_001, 4 * Chunk);
            DfsOperationId opId = await vol.NewOperationIdAsync(Ct);

            FaultArm arm = variant switch
            {
                "FollowersCut" => cluster.Faults.Arm(new FaultMatch(FaultSites.CommitReceiptValidated, Node: leader.Name, Volume: Vol), FaultAction.Block()),
                "CrashLeaderAfterLocalAppend" => cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.WriteAll), FaultAction.CrashNode()),
                "OneFollowerCut" => cluster.Faults.Arm(new FaultMatch(FaultSites.CommitReceiptValidated, Node: leader.Name, Volume: Vol), FaultAction.Block()),
                _ => throw new ArgumentOutOfRangeException(nameof(variant)),
            };

            Task<CommitToken> write = vol.Files.WriteAllBytesAsync("/map", newData, new DfsMutationOptions { OperationId = opId }, Ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(10));
            cluster.Faults.AssertReached(arm);

            IReadOnlySet<ObjectId> fresh = (await StoredAsync(cluster)).Except(storedBefore).ToHashSet();
            if (variant != "CrashLeaderAfterLocalAppend")
            {
                Assert.NotEmpty(fresh);
                foreach (ObjectId o in fresh)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    Assert.True(copies >= 2 && zones >= 2, $"payload {o} must already be durable ({copies}/{zones}) before the mapping commit");
                }
            }

            if (variant == "FollowersCut")
            {
                foreach (string f in followers)
                    cluster.Network.CutBoth(leader.Name, f);
                arm.Release();
                for (int i = 0; i < 20; i++)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
                    Assert.False(write.IsCompletedSuccessfully, "success reported while the mapping record existed only on the isolated leader");
                    foreach (string f in followers)
                    {
                        FileInspection? seen = (await cluster.Inspect.GetVolumeStateAsync(Vol, f)).GetFile("/map");
                        Assert.NotNull(seen);
                        Assert.Equal(oldLayout.Revision, seen.Revision);
                    }
                }

                cluster.Network.Heal();
            }
            else if (variant == "OneFollowerCut")
            {
                cluster.Network.CutBoth(leader.Name, followers[0]);
                arm.Release();
            }

            CommitToken? token = null;
            bool definiteFailure = false;
            try
            {
                token = await write;
            }
            catch (DfsCommitOutcomeUnknownException unknown)
            {
                Assert.Equal(opId, unknown.OperationId);
            }
            catch (Exception ex) when (variant != "OneFollowerCut" && ex is DfsUnavailableException or TimeoutException
                                       or DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NotLeader })
            {
                definiteFailure = true;
            }

            cluster.Network.Heal();
            if (leader.State == TestNodeState.Crashed)
                await leader.RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);

            await using DfsClient checker = await cluster.ConnectAsync(new TestClientOptions { Name = "checker" }, Ct);
            DfsVolume cv = checker.GetVolume(Vol);
            DfsOperationOutcome outcome = await cv.QueryOperationAsync(opId, resolve: true, Ct);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve must be definite, got {outcome.Status}");
            if (token is not null)
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            if (definiteFailure)
                Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
            if (variant == "OneFollowerCut")
                Assert.NotNull(token);

            byte[] expected = outcome.Status == DfsOutcomeStatus.Committed ? newData : oldData;
            Same(expected, await cv.Files.ReadAllBytesAsync("/map", Ct), $"{variant}: outcome {outcome.Status}");

            if (outcome.Status == DfsOutcomeStatus.Committed)
            {
                TestNode current = await cluster.GetLeaderAsync(Vol, Ct);
                await current.CrashAsync();
                await current.RestartAsync();
                await cluster.WaitForStableAsync(ct: Ct);
                await using DfsClient after = await cluster.ConnectAsync(new TestClientOptions { Name = "after-crash" }, Ct);
                Same(newData, await after.GetVolume(Vol).Files.ReadAllBytesAsync("/map", Ct), "an acknowledged mapping survives a leader crash");
                FileInspection layout = await InspectAsync(cluster, "/map");
                foreach (ObjectId o in Objects(layout))
                    Assert.True((await cluster.Inspect.GetObjectHoldersAsync(o)).Count >= 2, $"published object {o} lacks durable copies");
            }
            else
            {
                Assert.Equal(oldLayout.Extents, (await InspectAsync(cluster, "/map")).Extents);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-019 ExtentRetryIsIdempotent
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Interrupted and repeated object uploads (receipt reply dropped after the durable write, duplicated upload messages,
    /// a storage node crashing mid-object-write and being repaired later): the publication completes once, each object id
    /// is stored at most once per node, and every holder's copy has the same length and checksum as the published bytes.
    /// </summary>
    [Theory]
    [InlineData("ReceiptReplyDropped")]
    [InlineData("DuplicatedUpload")]
    [InlineData("WriterCrashMidObject")]
    [Catalog("LAY-019", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_019_ExtentRetryIsIdempotent(string variant)
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(19_000, 3 * Chunk + 5);

            FaultArm? arm = null;
            MessageRule? rule = null;
            switch (variant)
            {
                case "ReceiptReplyDropped":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectAfterFlushBeforeReceipt, Node: "n1", Count: 2), FaultAction.DropReply());
                    break;
                case "DuplicatedUpload":
                    rule = cluster.Network.DuplicateNext(new MessageFilter(Type: MessageType.PutObject, Direction: LinkDirection.Request), count: 4);
                    break;
                case "WriterCrashMidObject":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Node: "n1"), FaultAction.CrashNode(CrashMode.TearLastWrite));
                    break;
            }

            await vol.Files.WriteAllBytesAsync("/retry", data, Ct);
            if (arm is not null)
                cluster.Faults.AssertReached(arm);
            if (rule is not null)
                Assert.True(rule.Matched > 0, "the duplicated upload must have happened");

            if (cluster.Node("n1").State == TestNodeState.Crashed)
                await cluster.Node("n1").RestartAsync();
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            Same(data, await vol.Files.ReadAllBytesAsync("/retry", Ct), variant);

            FileInspection layout = await InspectAsync(cluster, "/retry");
            AssertWellFormed(layout, variant);
            foreach (IGrouping<ObjectId, ExtentInspection> group in layout.Extents.GroupBy(e => e.Object))
            {
                long objectLength = group.First().ObjectLength;
                byte[] content = new byte[objectLength];
                long referenced = 0;
                foreach (ExtentInspection e in group)
                {
                    data.AsSpan((int)e.Offset, (int)e.Length).CopyTo(content.AsSpan((int)e.ObjectOffset));
                    referenced += e.Length;
                }

                UInt128? expectedChecksum = referenced == objectLength ? ObjectChecksum.Compute(content) : null;
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(group.Key);
                Assert.True(holders.Count >= 3, $"object {group.Key}: {holders.Count} holders after repair");
                var checksums = new HashSet<UInt128>();
                foreach (string holder in holders)
                {
                    var records = (await cluster.Node(holder).Inspect.ListObjectsAsync(Vol)).Where(r => r.Id == group.Key).ToList();
                    Assert.Single(records);
                    Assert.Equal(objectLength, records[0].Length);
                    checksums.Add(records[0].Checksum);
                }

                Assert.Single(checksums);
                if (expectedChecksum is not null)
                    Assert.Equal(expectedChecksum.Value, checksums.Single());
            }
        }, Ct);
    }

    /// <summary>
    /// Raw retries of one object id: re-sending identical bytes returns an equivalent receipt; sending different bytes
    /// under the same id to a node that holds it is rejected as an integrity violation and leaves the original record;
    /// a conflicting copy accepted by another node can never be counted as proof for the published checksum, so the
    /// published object is held only with the original content.
    /// </summary>
    [Fact]
    [Catalog("LAY-019", "S", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_019_ExtentRetryIsIdempotent_ConflictingReupload()
    {
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            await using RawStagedWriter writer = await raw.OpenStagedWriterAsync(Vol, "/conflict", FileMode.CreateNew);

            ObjectId id = ObjectId.New(new SeededRandom(19_100));
            byte[] a = TestData.Bytes(19_101, 5000);
            byte[] b = TestData.Bytes(19_102, 5000);

            RawPutResult first = await writer.PutObjectAsync("n0", id, a);
            RawPutResult again = await writer.PutObjectAsync("n0", id, a);
            Assert.Equal(DfsErrorCode.None, first.Error);
            Assert.Equal(DfsErrorCode.None, again.Error);
            Assert.NotNull(first.Receipt);
            Assert.NotNull(again.Receipt);
            Assert.Equal(first.Receipt.Checksum, again.Receipt.Checksum);
            Assert.Equal(first.Receipt.Length, again.Receipt.Length);

            RawPutResult conflicting = await writer.PutObjectAsync("n0", id, b);
            Assert.Equal(DfsErrorCode.Integrity, conflicting.Error);
            Assert.Null(conflicting.Receipt);
            ObjectRecordInfo kept = (await cluster.Node("n0").Inspect.ListObjectsAsync(Vol)).Single(r => r.Id == id);
            Assert.Equal(ObjectChecksum.Compute(a), kept.Checksum);

            RawPutResult otherNodeB = await writer.PutObjectAsync("n1", id, b);
            RawPutResult thirdNodeA = await writer.PutObjectAsync("n2", id, a);
            Assert.Equal(DfsErrorCode.None, thirdNodeA.Error);
            Assert.NotNull(thirdNodeA.Receipt);
            var receipts = new List<RawReceipt> { first.Receipt, thirdNodeA.Receipt };
            if (otherNodeB.Receipt is not null)
            {
                Assert.Equal(ObjectChecksum.Compute(b), otherNodeB.Receipt.Checksum);
                receipts.Add(otherNodeB.Receipt);
            }

            DfsErrorCode commit = await writer.CommitExtentsAsync(a.Length, [RawExtentSpec.FromReceipt(first.Receipt, 0, a.Length)], receipts);
            Assert.Equal(DfsErrorCode.None, commit);
            await writer.DisposeAsync();

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            Same(a, await client.GetVolume(Vol).Files.ReadAllBytesAsync("/conflict", Ct), "published bytes are the original content");
            Assert.True((await cluster.Inspect.GetObjectHoldersAsync(id)).Count >= 2, "the two matching receipts' holders remain verified holders");
            foreach (string holder in await cluster.Inspect.GetObjectHoldersAsync(id))
            {
                ObjectRecordInfo record = (await cluster.Node(holder).Inspect.ListObjectsAsync(Vol)).Single(r => r.Id == id);
                Assert.Equal(ObjectChecksum.Compute(a), record.Checksum);
            }
        }, Ct);
    }
}
