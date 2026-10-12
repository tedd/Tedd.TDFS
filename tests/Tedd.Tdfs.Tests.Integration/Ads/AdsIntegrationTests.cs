using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Integration.Ads.AdsHelpers;

namespace Tedd.Tdfs.Tests.Integration.Ads;

/// <summary>Which node(s) ADS-026 crashes.</summary>
public enum CrashTarget { VolumeLeader, Follower, WholeCluster }

/// <summary>
/// ADS — alternate data streams over real boundaries (FSD Addendum A §A4; catalog §A5.3, ADS-026): a three-zone cluster
/// over QUIC with on-disk storage, crashed (losing unflushed state) and restarted.
/// </summary>
public sealed class AdsIntegrationTests
{
    /// <summary>
    /// Large named streams (written whole, and through a handle in odd pieces with intermediate flushes), a multibyte
    /// stream name and a directory stream round-trip byte-exactly over QUIC/disk; with one node crashed (leader or
    /// follower) every stream still reads back exactly and stream appends/creations continue; after restart — or after
    /// the whole cluster crashed and restarted cold — a fresh client reads every acknowledged stream byte-exactly, the
    /// applied stream set matches, every stream object is durably replicated per policy and replicas converge.
    /// </summary>
    [Theory]
    [InlineData(CrashTarget.VolumeLeader)]
    [InlineData(CrashTarget.Follower)]
    [InlineData(CrashTarget.WholeCluster)]
    [Catalog("ADS-026", "I", "INV-01", "INV-02", "INV-07", "INV-09")]
    public async Task ADS_026_StreamsOverRealTransport(CrashTarget target)
    {
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            int chunk = cluster.Options.Timings.ChunkSize;
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            DfsVolume v = writer.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/q", Ct);
            StreamModel model = new StreamModel(NonZero(2601, (1 << 20) + 3))
                .With("big", NonZero(2602, (3 << 20) + 17))
                .With("small", NonZero(2603, 100))
                .With("été", NonZero(2604, 5000));
            await WriteModelAsync(v.Files, "/q/f", model);

            byte[] pieced = NonZero(2605, (2 << 20) + 333);
            await using (DfsStream s = await v.Files.OpenAsync("/q/f:pieced", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read }, Ct))
            {
                int[] pieces = [7, chunk + 1, 300_000, 1, 4093, 512 * 1024];
                int offset = 0;
                for (int i = 0; offset < pieced.Length; i++)
                {
                    int n = Math.Min(pieces[i % pieces.Length], pieced.Length - offset);
                    await s.WriteAsync(pieced.AsMemory(offset, n), Ct);
                    offset += n;
                    if (i % 3 == 2)
                        await s.FlushAsync(Ct);
                }
            }

            model.With("pieced", pieced);
            byte[] directoryStream = NonZero(2606, (64 * 1024) + 9);
            await v.Files.WriteAllBytesAsync("/q:meta", directoryStream, Ct);

            await using (DfsClient reader = await ConnectAsync(cluster, "r"))
                await AssertStreamsAsync(reader.GetVolume(Vol).Files, "/q/f", model, "over QUIC/disk before any crash");

            switch (target)
            {
                case CrashTarget.WholeCluster:
                    foreach (TestNode node in cluster.Nodes)
                        await node.CrashAsync(CrashMode.LoseUnflushed);
                    foreach (TestNode node in cluster.Nodes)
                        await node.RestartAsync();
                    break;
                default:
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    TestNode victim = target == CrashTarget.VolumeLeader ? leader : cluster.Nodes.First(n => n.Name != leader.Name);
                    await victim.CrashAsync(CrashMode.LoseUnflushed);
                    string[] alive = [.. cluster.Nodes.Where(n => n.Name != victim.Name).Select(n => n.Name)];
                    await using (DfsClient during = await ConnectAsync(cluster, "during", alive))
                    {
                        DfsFileClient files = during.GetVolume(Vol).Files;
                        await AssertStreamsAsync(files, "/q/f", model, $"while {victim.Name} ({target}) is crashed");
                        byte[] more = NonZero(2607, (3 * chunk) + 1);
                        await files.AppendAllBytesAsync("/q/f:big", more, Ct);
                        model.NamedStreams["big"] = [.. model.NamedStreams["big"], .. more];
                        byte[] created = NonZero(2608, (1 << 20) + 5);
                        await files.WriteAllBytesAsync("/q/f:during", created, Ct);
                        model.With("during", created);
                        await AssertStreamsAsync(files, "/q/f", model, $"stream writes acknowledged while {victim.Name} is crashed");
                    }

                    await victim.RestartAsync();
                    break;
                }
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(90), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(180), Ct);

            await using DfsClient fresh = await ConnectAsync(cluster, "fresh");
            DfsVolume fv = fresh.GetVolume(Vol);
            await AssertStreamsAsync(fv.Files, "/q/f", model, $"after the {target} crash and restart");
            BytesEqual(directoryStream, await fv.Files.ReadAllBytesAsync("/q:meta", Ct), "the directory stream after restart");

            DfsStoragePolicy policy = (await fv.GetInfoAsync(Ct)).DefaultPolicy;
            FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, "/q/f");
            Assert.NotNull(inspected);
            Assert.Equal(model.NamedStreams.Keys, inspected.Streams.Select(s => s.Name));
            Assert.Equal(model.NamedStreams.Select(s => (long)s.Value.Length), inspected.Streams.Select(s => s.Length));
            foreach (ObjectId id in AllObjects(inspected))
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                Assert.True(copies >= policy.TargetCopies && zones >= policy.TargetDistinctZones,
                    $"stream object {id} is durable on {copies} hosts in {zones} zones after repair; policy targets {policy.TargetCopies}/{policy.TargetDistinctZones}");
            }

            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }
}
