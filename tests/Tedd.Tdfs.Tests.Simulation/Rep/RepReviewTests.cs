using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Rep.RepHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Rep;

/// <summary>Adversarial review scenarios of the repair coordinator (policy migration racing excess-replica deletions).</summary>
public sealed class RepReviewTests
{
    /// <summary>
    /// A rebalance (a node is added) queues excess-replica deletions, which are all held at their fault point. While they
    /// are held, the default policy is upgraded from two to three copies. A copy whose deletion is in flight must not count
    /// towards the new policy: whenever the migration is reported effective, every object keeps three copies in three
    /// zones once the held deletions execute. The held deletions must not stall the migration either: the coordinator gives
    /// up waiting for them (their holders stay out of every count) and makes the missing copies elsewhere.
    /// </summary>
    [Fact]
    [Catalog("REP-014", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_014_MigrationNeverCountsACopyBeingDeleted()
    {
        await ClusterTest.RunAsync(Deterministic(4, Policy(2, 2, 2, 2)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1", "n2"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 8; i++)
            {
                files[$"/m{i}"] = TestData.Bytes((ulong)(14_500 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/m{i}", files[$"/m{i}"], Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            var objects = new List<ObjectId>();
            foreach (string path in files.Keys)
                objects.AddRange(await ObjectsOfAsync(cluster, path));

            FaultArm held = cluster.Faults.Arm(new FaultMatch(FaultSites.ReplicaDelete, Count: int.MaxValue), FaultAction.Block());
            await cluster.AddNodeAsync(new TestNodeSpec("n4", "z5"), ct: Ct);
            await held.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);

            DfsStoragePolicy three = Policy(3, 3, 2, 2);
            await cluster.Admin.SetDefaultPolicyAsync(Vol, three, migrateExisting: true, Ct);
            bool effectiveWhileHeld = false;
            for (int step = 0; step < 240 && !effectiveWhileHeld; step++)
            {
                // Holders first, then the status ("effective" is monotonic): an effective status read afterwards proves the
                // holders below were observed no earlier than the transition.
                IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
                var deleting = held.Contexts.Where(c => c.Object is not null)
                    .GroupBy(c => c.Object!.Value)
                    .ToDictionary(g => g.Key, g => g.Select(c => c.Node).ToHashSet(StringComparer.Ordinal));
                var observed = new List<(ObjectId Object, List<string> Remaining)>();
                foreach (ObjectId o in objects)
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                    observed.Add((o, holders.Where(h => !deleting.TryGetValue(o, out HashSet<string>? d) || !d.Contains(h)).ToList()));
                }

                DfsPolicyMigrationStatus status = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, Ct);
                if (status.Effective && status.Target == three)
                {
                    foreach ((ObjectId o, List<string> remaining) in observed)
                    {
                        (int copies, int zones) = Domains(remaining, labels);
                        Assert.True(copies >= 3 && zones >= 3,
                            $"step {step}: the three-copy policy is effective but {o} keeps only [{string.Join(",", remaining)}] once its held deletions execute");
                    }

                    effectiveWhileHeld = true;
                    break;
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }

            Assert.True(held.Contexts.Count > 0, "the rebalance must have queued excess-replica deletions");
            Assert.True(effectiveWhileHeld, "held excess-replica deletions must not stall the migration");
            held.Disarm();
            held.Release();

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            foreach ((string path, byte[] data) in files)
            {
                Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), path);
                await AssertDurableAsync(cluster, path, 3, 3, "after the migration");
            }
        }, Ct);
    }
}
