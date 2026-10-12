using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Lay.LayHelpers;

namespace Tedd.Tdfs.Tests.Unit.Lay;

/// <summary>
/// LAY — extent mapping, small files, and partial updates (test plan §7.11; FSD §10, §15).
/// Invariants: INV-01, INV-02, INV-09, INV-16.
/// </summary>
public sealed class LayUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // LAY-001 InlineThresholdBoundaries
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, int> InlineBoundaryCases()
    {
        var data = new TheoryData<int, int>();
        foreach (int t in new[] { 64, DefaultInline })
        {
            foreach (int length in new[] { 0, 1, t - 1, t, t + 1, 2 * t, t + Chunk + 1 })
                data.Add(t, length);
        }

        return data;
    }

    /// <summary>
    /// A file of each boundary length around the configured inline threshold is stored inline exactly when its length is
    /// at most the threshold (ThreeZones: any metadata majority spans 2 hosts in 2 zones, which satisfies the default
    /// 2/2 acknowledgement policy, so inline storage is permitted), and contents/metadata are exact either way.
    /// </summary>
    [Theory]
    [MemberData(nameof(InlineBoundaryCases))]
    [Catalog("LAY-001", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_001_InlineThresholdBoundaries(int threshold, int length)
    {
        await ClusterTest.RunAsync(ThreeZones(v => v with { InlineThreshold = threshold }), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes((ulong)(threshold * 131 + length), length);

            CommitToken token = await vol.Files.WriteAllBytesAsync("/f", data, Ct);
            Assert.False(token.IsNone, "a standalone WriteAllBytes must return a real commit token");

            Same(data, await vol.Files.ReadAllBytesAsync("/f", Ct), "ReadAllBytes after write");
            Same(data, await ReadCompatibleAsync(vol, "/f", 7), "streamed 7-byte reads after write");

            bool expectInline = length <= threshold;
            DfsFileInfo info = await vol.Files.GetInfoAsync("/f", Ct);
            Assert.Equal((long)length, info.Length);
            Assert.Equal(expectInline, info.IsInline);
            Assert.False(info.IsSparse, "a fully written file is not sparse");
            if (expectInline)
                Assert.Equal(0, info.ExtentCount);
            else
                Assert.True(info.ExtentCount >= 1, $"a {length}-byte file above the {threshold}-byte threshold needs extents");

            FileInspection file = await InspectAsync(cluster, "/f");
            Assert.Equal(info.InodeId, file.Inode);
            Assert.Equal(info.Revision, file.Revision);
            Assert.Equal((long)length, file.Length);
            Assert.Equal(expectInline, file.IsInline);
            AssertWellFormed(file, $"length {length}");
            if (!expectInline)
            {
                AssertCovers(file, 0, length, $"length {length}");
                Assert.Equal(info.ExtentCount, file.Extents.Count);
            }
        }, Ct);
    }

    public static TheoryData<int, int, string> RepresentationChangeCases()
    {
        int t = DefaultInline;
        var pairs = new (int From, int To)[]
        {
            (t - 1, t + 1), (t + 1, t - 1), (t, t + 1), (t + 1, t), (0, t + 1), (t + 1, 0), (3 * Chunk, t), (t, 3 * Chunk),
        };
        var data = new TheoryData<int, int, string>();
        foreach ((int from, int to) in pairs)
        {
            foreach (string method in new[] { "WriteAll", "Stream", "AppendOrTruncate" })
                data.Add(from, to, method);
        }

        return data;
    }

    /// <summary>
    /// Representation changes (inline ↔ extents) through whole-file replacement, stream SetLength/overwrite and
    /// append/truncate keep the inode identity, advance the revision, and expose exactly the reference contents.
    /// </summary>
    [Theory]
    [MemberData(nameof(RepresentationChangeCases))]
    [Catalog("LAY-001", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_001_InlineThresholdBoundaries_RepresentationChange(int fromLength, int toLength, string method)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] initial = TestData.Bytes(11, fromLength);
            await vol.Files.WriteAllBytesAsync("/f", initial, Ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(fromLength <= DefaultInline, before.IsInline);

            var model = new ReferenceFileModel(initial);
            byte[] fresh = TestData.Bytes(12, toLength);
            switch (method)
            {
                case "WriteAll":
                    await vol.Files.WriteAllBytesAsync("/f", fresh, Ct);
                    model.SetLength(0);
                    model.Write(0, fresh);
                    break;
                case "Stream":
                {
                    await using DfsStream s = await vol.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                    await s.SetLengthAsync(toLength, Ct);
                    model.SetLength(toLength);
                    int overwrite = Math.Min(toLength, 17);
                    if (overwrite > 0)
                    {
                        await WriteAtAsync(s, toLength - overwrite, fresh[..overwrite]);
                        model.Write(toLength - overwrite, fresh.AsSpan(0, overwrite));
                    }

                    await s.FlushAsync(Ct);
                    break;
                }
                case "AppendOrTruncate":
                    if (toLength >= fromLength)
                    {
                        byte[] suffix = fresh[..(toLength - fromLength)];
                        await vol.Files.AppendAllBytesAsync("/f", suffix, Ct);
                        model.Write(fromLength, suffix);
                    }
                    else
                    {
                        await using DfsStream s = await vol.Files.OpenAsync("/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                        await s.SetLengthAsync(toLength, Ct);
                        await s.FlushAsync(Ct);
                        model.SetLength(toLength);
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(method), method, null);
            }

            model.Flush();

            DfsFileInfo after = await vol.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(before.InodeId, after.InodeId);
            Assert.True(after.Revision > before.Revision, $"revision must advance ({before.Revision} -> {after.Revision})");
            Assert.Equal((long)toLength, after.Length);
            Assert.Equal(toLength <= DefaultInline, after.IsInline);
            Same(model.Committed, await vol.Files.ReadAllBytesAsync("/f", Ct), $"{method} {fromLength}->{toLength}");
            Same(model.Committed, await ReadCompatibleAsync(vol, "/f", 1000), $"{method} {fromLength}->{toLength} streamed");

            FileInspection file = await InspectAsync(cluster, "/f");
            Assert.Equal(after.IsInline, file.IsInline);
            Assert.Equal(after.Revision, file.Revision);
            AssertWellFormed(file, $"{method} {fromLength}->{toLength}");
            if (!file.IsInline && method != "Stream")
                AssertCovers(file, 0, toLength, $"{method} {fromLength}->{toLength}");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-003 SmallFilesArePackedIndependently
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Many tiny objects packed into shared segments of one object store: deleting one, re-putting one (idempotent),
    /// rejecting a conflicting re-put, compacting and restarting from the durable image never alter any other object's
    /// bytes, length, checksum or identity.
    /// </summary>
    [Theory]
    [InlineData(1, 300)]
    [InlineData(64, 300)]
    [InlineData(4096, 200)]
    [Catalog("LAY-003", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_003_SmallFilesArePackedIndependently_ObjectStore(int maxObjectBytes, int count)
    {
        var random = new SeededRandom(0x1A7_003UL + (ulong)maxObjectBytes);
        var medium = new SimulatedMedium("lay003", 1L << 30, random.Fork("medium"));
        VolumeId volume = VolumeId.New(random);
        var options = new ObjectStoreOptions(SegmentSize: 1 << 20);

        var objects = new List<(ObjectId Id, byte[] Data)>(count);
        ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        try
        {
            for (int i = 0; i < count; i++)
            {
                ObjectId id = ObjectId.New(random);
                byte[] data = TestData.Bytes((ulong)(1000 + i), 1 + random.Next(maxObjectBytes));
                ObjectRecordInfo record = await store.PutAsync(id, volume, new StagingId((ulong)(1 + i % 3)), data, Ct);
                Assert.Equal(id, record.Id);
                Assert.Equal((long)data.Length, record.Length);
                Assert.Equal(ObjectChecksum.Compute(data), record.Checksum);
                objects.Add((id, data));
            }

            int files = medium.AllFiles().Count(f => f.Contains("objects", StringComparison.Ordinal));
            Assert.True(files * 4 < count, $"{count} tiny objects must be packed into shared segments, found {files} files");

            (ObjectId victimId, _) = objects[count / 2];
            await store.DeleteAsync(victimId, Ct);
            Assert.False(await store.ContainsAsync(victimId, Ct), "deleted object must be gone");

            (ObjectId sameId, byte[] sameData) = objects[count / 3];
            ObjectRecordInfo again = await store.PutAsync(sameId, volume, new StagingId(1), sameData, Ct);
            Assert.Equal(ObjectChecksum.Compute(sameData), again.Checksum);

            (ObjectId conflictId, byte[] conflictData) = objects[count / 4];
            byte[] different = (byte[])conflictData.Clone();
            different[0] ^= 0x5A;
            await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(
                () => store.PutAsync(conflictId, volume, new StagingId(1), different, Ct).AsTask(), DfsErrorCode.Integrity);

            await store.CompactAsync(Ct);
            await AssertOthersIntactAsync(store, objects, victimId, "after delete/re-put/conflict/compaction");
        }
        finally
        {
            await store.DisposeAsync();
        }

        medium.Crash(CrashMode.LoseUnflushed);
        ObjectStore reopened = await ObjectStore.OpenAsync(medium, "objects", options, NoFaults.Instance, Ct);
        try
        {
            await AssertOthersIntactAsync(reopened, objects, objects[count / 2].Id, "after crash and reopen");
        }
        finally
        {
            await reopened.DisposeAsync();
        }

        static async Task AssertOthersIntactAsync(ObjectStore store, List<(ObjectId Id, byte[] Data)> objects, ObjectId victim, string when)
        {
            var listed = store.List().ToDictionary(r => r.Id);
            foreach ((ObjectId id, byte[] data) in objects)
            {
                if (id == victim)
                    continue;
                Assert.True(await store.ContainsAsync(id, Ct), $"{when}: object {id} lost");
                Assert.True(listed.TryGetValue(id, out ObjectRecordInfo? record), $"{when}: object {id} missing from the index");
                Assert.Equal((long)data.Length, record!.Length);
                Assert.Equal(ObjectChecksum.Compute(data), record.Checksum);
                byte[] read = new byte[data.Length];
                int n = await store.ReadAsync(id, 0, read, Ct);
                Assert.Equal(data.Length, n);
                Same(data, read, $"{when}: object {id}");
            }
        }
    }

    /// <summary>
    /// Cluster-level packing: with inlining disabled every tiny file references its own object; overwriting, appending
    /// to and deleting individual files leaves every other file's bytes, inode, revision and object identity unchanged,
    /// and storage nodes pack the objects into far fewer files than objects.
    /// </summary>
    [Fact]
    [Catalog("LAY-003", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_003_SmallFilesArePackedIndependently_Cluster()
    {
        const int count = 128;
        await ClusterTest.RunAsync(ThreeZones(v => v with { InlineThreshold = 0 }), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var contents = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < count; i++)
            {
                string path = $"/small/{i:D3}";
                if (i == 0)
                    await vol.Directories.CreateDirectoryAsync("/small", Ct);
                byte[] data = TestData.Bytes((ulong)(3000 + i), 1 + (i * 7 % 100));
                await vol.Files.WriteAllBytesAsync(path, data, Ct);
                contents[path] = data;
            }

            var before = new Dictionary<string, FileInspection>(StringComparer.Ordinal);
            foreach (string path in contents.Keys)
            {
                FileInspection f = await InspectAsync(cluster, path);
                Assert.False(f.IsInline, $"{path}: inline threshold 0 must force an object");
                Assert.Single(Objects(f));
                before[path] = f;
            }

            Assert.Equal(count, before.Values.SelectMany(f => Objects(f)).Distinct().Count());

            await cluster.WaitForReplicationAsync(Vol, Ct);
            foreach (TestNode node in cluster.Nodes)
            {
                int files = node.Disk.Files().Count;
                Assert.True(files < count, $"{node.Name}: {count} tiny objects must be packed, but the disk has {files} files");
            }

            const string overwritten = "/small/010", appended = "/small/030", deleted = "/small/020";
            byte[] replacement = TestData.Bytes(9_010, 33);
            await vol.Files.WriteAllBytesAsync(overwritten, replacement, Ct);
            byte[] suffix = TestData.Bytes(9_030, 9);
            await vol.Files.AppendAllBytesAsync(appended, suffix, Ct);
            await vol.Files.DeleteAsync(deleted, Ct);

            Same(replacement, await vol.Files.ReadAllBytesAsync(overwritten, Ct), "overwritten file");
            Same(Concat(contents[appended], suffix), await vol.Files.ReadAllBytesAsync(appended, Ct), "appended file");
            Assert.False(await vol.Files.ExistsAsync(deleted, Ct), "deleted file must be absent");
            Assert.Equal(before[overwritten].Inode, (await InspectAsync(cluster, overwritten)).Inode);
            Assert.Equal(before[appended].Inode, (await InspectAsync(cluster, appended)).Inode);

            foreach ((string path, byte[] data) in contents)
            {
                if (path is overwritten or appended or deleted)
                    continue;
                Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), path);
                FileInspection now = await InspectAsync(cluster, path);
                Assert.Equal(before[path].Inode, now.Inode);
                Assert.Equal(before[path].Revision, now.Revision);
                Assert.Equal(before[path].Extents, now.Extents);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-004 ExtentBoundaryWrites
    // ------------------------------------------------------------------------------------------------------------

    public enum BoundaryWrite { ByteBefore, ByteAt, AcrossTwoBytes, AcrossWide, EndsAtBoundary, StartsAtBoundary }

    private static (long Offset, int Length) BoundaryRange(BoundaryWrite kind, long boundary) => kind switch
    {
        BoundaryWrite.ByteBefore => (boundary - 1, 1),
        BoundaryWrite.ByteAt => (boundary, 1),
        BoundaryWrite.AcrossTwoBytes => (boundary - 1, 2),
        BoundaryWrite.AcrossWide => (boundary - 1000, 2000),
        BoundaryWrite.EndsAtBoundary => (boundary - 100, 100),
        BoundaryWrite.StartsAtBoundary => (boundary, 100),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// Writes immediately before, at and across every interior extent boundary of a multi-extent file: the result equals
    /// the reference model, prefixes/suffixes are byte-exact, and extents that are not neighbours of the written range
    /// keep their object identity and object offset (no rewrite of unrelated data).
    /// </summary>
    [Theory]
    [InlineData(BoundaryWrite.ByteBefore)]
    [InlineData(BoundaryWrite.ByteAt)]
    [InlineData(BoundaryWrite.AcrossTwoBytes)]
    [InlineData(BoundaryWrite.AcrossWide)]
    [InlineData(BoundaryWrite.EndsAtBoundary)]
    [InlineData(BoundaryWrite.StartsAtBoundary)]
    [Catalog("LAY-004", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_004_ExtentBoundaryWrites(BoundaryWrite kind)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            int size = 5 * Chunk + 123;
            byte[] original = TestData.Bytes(4_000, size);
            await vol.Files.WriteAllBytesAsync("/big", original, Ct);

            FileInspection initial = await InspectAsync(cluster, "/big");
            AssertWellFormed(initial, "initial");
            Assert.True(initial.Extents.Count >= 2, $"a {size}-byte file written in {Chunk}-byte update units must span several extents");
            List<long> boundaries = initial.Extents.Skip(1).Select(e => e.Offset).Take(4).ToList();

            var model = new ReferenceFileModel(original);
            ulong seed = 4_100;
            await using DfsStream stream = await vol.Files.OpenAsync("/big", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            foreach (long boundary in boundaries)
            {
                FileInspection before = await InspectAsync(cluster, "/big");
                byte[] committedBefore = model.Committed;
                (long offset, int length) = BoundaryRange(kind, boundary);
                byte[] patch = TestData.Bytes(seed++, length);

                await WriteAtAsync(stream, offset, patch);
                Same(patch, await ReadRangeAsync(stream, offset, length), "own staged write must be visible to the writer");
                await stream.FlushAsync(Ct);
                model.Write(offset, patch);
                model.Flush();

                byte[] actual = await ReadCompatibleAsync(vol, "/big");
                string at = $"{kind} at boundary {boundary} -> [{offset},{offset + length})";
                Same(model.Committed, actual, at);
                Same(committedBefore.AsMemory(0, (int)offset), actual.AsMemory(0, (int)offset), $"{at}: prefix");
                Same(committedBefore.AsMemory((int)(offset + length)), actual.AsMemory((int)(offset + length)), $"{at}: suffix");

                FileInspection after = await InspectAsync(cluster, "/big");
                AssertWellFormed(after, at);
                Assert.Equal(before.Inode, after.Inode);
                Assert.True(after.Revision > before.Revision, $"{at}: revision must advance");
                long guardStart = offset - Chunk, guardEnd = offset + length + Chunk;
                foreach (ExtentInspection e in before.Extents.Where(e => e.Offset + e.Length <= guardStart || e.Offset >= guardEnd))
                {
                    foreach (long probe in new[] { e.Offset, e.Offset + e.Length / 2, e.Offset + e.Length - 1 })
                        Assert.Equal(PositionAt(before, probe), PositionAt(after, probe));
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-005 SparseMappingRoundTrip
    // ------------------------------------------------------------------------------------------------------------

    private static (List<(long Offset, int Length)> Segments, long Length, bool LargeHoles) SparseLayout(string pattern)
    {
        var segments = new List<(long, int)>();
        switch (pattern)
        {
            case "ByteGrain":
                for (int i = 0; i < 64; i++)
                    segments.Add((2L * i, 1));
                return (segments, 128, false);
            case "SmallDataLargeHoles":
                for (int i = 0; i < 20; i++)
                    segments.Add((i * 4196L + 4096, 100));
                return (segments, 20 * 4196L + 4096, true);
            case "ChunkStraddling":
                for (int i = 0; i < 4; i++)
                    segments.Add((i * (2L * Chunk), Chunk - 1));
                return (segments, 8L * Chunk, true);
            case "LeadingAndTrailingHole":
                segments.Add((3L * Chunk, 10));
                return (segments, 5L * Chunk, true);
            case "OneBytePerChunk":
                for (int i = 0; i < 8; i++)
                    segments.Add((i * (long)Chunk + 17, 1));
                return (segments, 8L * Chunk + 1, true);
            default:
                throw new ArgumentOutOfRangeException(nameof(pattern), pattern, null);
        }
    }

    /// <summary>
    /// Alternating holes and data written through one handle: reads return data and zero holes; no extent lies inside a
    /// hole; and the serialized mapping reproduces the identical logical stream on every replica, through a snapshot, a
    /// metadata copy, and after a crash/restart of every node (journal + checkpoint recovery).
    /// </summary>
    [Theory]
    [InlineData("ByteGrain")]
    [InlineData("SmallDataLargeHoles")]
    [InlineData("ChunkStraddling")]
    [InlineData("LeadingAndTrailingHole")]
    [InlineData("OneBytePerChunk")]
    [Catalog("LAY-005", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_005_SparseMappingRoundTrip(string pattern)
    {
        (List<(long Offset, int Length)> segments, long length, bool largeHoles) = SparseLayout(pattern);
        byte[] expected = new byte[length];
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            FileInspection written;
            await using (DfsClient client = await cluster.ConnectAsync(ct: Ct))
            {
                DfsVolume vol = client.GetVolume(Vol);
                await using (DfsStream s = await vol.Files.OpenAsync("/sparse", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, Ct))
                {
                    ulong seed = 5_000;
                    foreach ((long offset, int count) in segments)
                    {
                        byte[] data = TestData.Bytes(seed++, count);
                        data.CopyTo(expected, offset);
                        await WriteAtAsync(s, offset, data);
                    }

                    await s.SetLengthAsync(length, Ct);
                    await s.FlushAsync(Ct);
                }

                Same(expected, await vol.Files.ReadAllBytesAsync("/sparse", Ct), $"{pattern}: live read");
                DfsFileInfo info = await vol.Files.GetInfoAsync("/sparse", Ct);
                Assert.Equal(length, info.Length);
                if (largeHoles)
                    Assert.True(info.IsSparse, $"{pattern}: a file with unwritten ranges must report IsSparse");

                written = await InspectAsync(cluster, "/sparse");
                AssertWellFormed(written, pattern);
                foreach (ExtentInspection e in written.Extents)
                {
                    bool touchesData = segments.Any(s => s.Offset < e.Offset + e.Length && e.Offset < s.Offset + s.Length);
                    Assert.True(touchesData, $"{pattern}: extent [{e.Offset},{e.Offset + e.Length}) lies entirely inside a hole");
                }

                await cluster.WaitForReplicationAsync(Vol, Ct);
                foreach (TestNode node in cluster.Nodes)
                {
                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, node.Name);
                    FileInspection? replica = state.GetFile("/sparse");
                    Assert.NotNull(replica);
                    Assert.Equal(written.Extents, replica.Extents);
                    Assert.Equal(written.Length, replica.Length);
                }

                await vol.Snapshots.CreateAsync("lay005", ct: Ct);
                await vol.Files.CopyAsync("/sparse", "/sparse-copy", false, Ct);
                Same(expected, await vol.Files.ReadAllBytesAsync("/sparse-copy", Ct), $"{pattern}: metadata copy");
            }

            List<TestNode> nodes = cluster.Nodes.ToList();
            foreach (TestNode node in nodes)
                await node.CrashAsync(CrashMode.LoseUnflushed);
            foreach (TestNode node in nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);

            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v2 = fresh.GetVolume(Vol);
            Same(expected, await v2.Files.ReadAllBytesAsync("/sparse", Ct), $"{pattern}: after full-cluster crash/restart");
            Same(expected, await ReadCompatibleAsync(v2, "/sparse", 333), $"{pattern}: streamed after restart");
            Assert.Equal(written.Extents, (await InspectAsync(cluster, "/sparse")).Extents);
            await using DfsSnapshotView view = await v2.Snapshots.OpenAsync("lay005", Ct);
            Same(expected, await view.Files.ReadAllBytesAsync("/sparse", Ct), $"{pattern}: snapshot read after restart");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-006 OverlappingStagedWrites
    // ------------------------------------------------------------------------------------------------------------

    private static List<(long Offset, int Length)> OverlapScript(string scenario)
    {
        var ops = new List<(long, int)>();
        switch (scenario)
        {
            case "Handcrafted":
                ops.AddRange([(0, 100), (50, 100), (25, 50), (140, 160), (0, 1), (299, 2), (10, 280)]);
                break;
            case "AcrossUploadedChunks":
                // The first write exceeds the 64 KiB stream buffer, so complete chunks are uploaded while still staged;
                // later writes overlap the uploaded staged ranges partially.
                ops.AddRange([(0, 3 * Chunk), (Chunk - 10, 20), (2 * Chunk - 5000, Chunk), (100, 1), (3 * Chunk - 1, 5000)]);
                break;
            case "Random1":
            case "Random2":
            {
                var random = new SeededRandom(scenario == "Random1" ? 61UL : 62UL);
                for (int i = 0; i < 40; i++)
                    ops.Add((random.Next(3 * Chunk + 1000), 1 + random.Next(random.Next(4) == 0 ? Chunk + Chunk / 2 : 3000)));
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null);
        }

        return ops;
    }

    /// <summary>
    /// Overlapping writes staged on one handle: after every write the writer's own view equals the staged reference model
    /// (later writes replace exactly their overlapping bytes, including over chunks already uploaded under staging), a
    /// compatible reader still sees the old committed revision, and after flush everyone sees the merged result.
    /// </summary>
    [Theory]
    [InlineData("Handcrafted")]
    [InlineData("AcrossUploadedChunks")]
    [InlineData("Random1")]
    [InlineData("Random2")]
    [Catalog("LAY-006", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_006_OverlappingStagedWrites(string scenario)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(ct: Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            DfsVolume wv = writerClient.GetVolume(Vol), rv = readerClient.GetVolume(Vol);
            byte[] initial = TestData.Bytes(6_000, 3 * Chunk);
            await wv.Files.WriteAllBytesAsync("/o", initial, Ct);
            var model = new ReferenceFileModel(initial);

            await using DfsStream writer = await wv.Files.OpenAsync("/o", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            ulong seed = 6_100;
            int step = 0;
            foreach ((long offset, int length) in OverlapScript(scenario))
            {
                byte[] data = TestData.Bytes(seed++, length);
                await WriteAtAsync(writer, offset, data);
                model.Write(offset, data);
                step++;

                long windowStart = Math.Max(0, offset - 64);
                int windowLength = (int)Math.Min(model.StagedLength - windowStart, length + 128);
                Same(model.Read(windowStart, windowLength, staged: true), await ReadRangeAsync(writer, windowStart, windowLength),
                    $"{scenario} step {step}: writer view of [{windowStart},{windowStart + windowLength})");
                if (step % 5 == 0)
                    Same(model.Committed, await ReadCompatibleAsync(rv, "/o"), $"{scenario} step {step}: unpublished writes leaked to a reader");
            }

            writer.Seek(0, SeekOrigin.Begin);
            Same(model.Staged, await ReadToEndAsync(writer, 10_000), $"{scenario}: full staged view before flush");
            Same(model.Committed, await ReadCompatibleAsync(rv, "/o"), $"{scenario}: reader before flush");

            await writer.FlushAsync(Ct);
            model.Flush();
            Same(model.Committed, await ReadCompatibleAsync(rv, "/o"), $"{scenario}: reader after flush");
            Same(model.Committed, await rv.Files.ReadAllBytesAsync("/o", Ct), $"{scenario}: ReadAllBytes after flush");
            FileInspection file = await InspectAsync(cluster, "/o");
            AssertWellFormed(file, scenario);
            Assert.Equal(model.CommittedLength, file.Length);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-007 TruncateDropsMappingsSafely
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Shrinks through/at an extent (or to zero, or into inline range), flushes, then extends again by SetLength or by
    /// writing past the end: the discarded range reads as zeros, no mapping refers to discarded data, objects referenced
    /// only by the discarded range leave the reachable set and are eventually reclaimed, and nothing stale is exposed.
    /// </summary>
    [Theory]
    [InlineData("InsideExtent", "SetLength")]
    [InlineData("InsideExtent", "WriteBeyond")]
    [InlineData("AtExtentBoundary", "SetLength")]
    [InlineData("AtExtentBoundary", "WriteBeyond")]
    [InlineData("Zero", "SetLength")]
    [InlineData("Zero", "WriteBeyond")]
    [InlineData("InlineEligible", "SetLength")]
    [InlineData("InlineEligible", "WriteBeyond")]
    [Catalog("LAY-007", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_007_TruncateDropsMappingsSafely(string truncateAt, string extendBy)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            int size = 4 * Chunk + 77;
            byte[] data = TestData.Bytes(7_000, size);
            await vol.Files.WriteAllBytesAsync("/t", data, Ct);
            FileInspection original = await InspectAsync(cluster, "/t");
            Assert.True(original.Extents.Count >= 3, "test needs a multi-extent file");

            long cut = truncateAt switch
            {
                "InsideExtent" => original.Extents[1].Offset + original.Extents[1].Length / 2,
                "AtExtentBoundary" => original.Extents[2].Offset,
                "Zero" => 0,
                "InlineEligible" => 100,
                _ => throw new ArgumentOutOfRangeException(nameof(truncateAt)),
            };
            long extended = size + Chunk;

            await using (DfsStream s = await vol.Files.OpenAsync("/t", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct))
            {
                await s.SetLengthAsync(cut, Ct);
                await s.FlushAsync(Ct);
                Same(data.AsMemory(0, (int)cut), await ReadCompatibleAsync(vol, "/t"), "after truncate");

                FileInspection shrunk = await InspectAsync(cluster, "/t");
                AssertWellFormed(shrunk, "after truncate");
                Assert.Equal(cut, shrunk.Length);
                Assert.Equal(original.Inode, shrunk.Inode);
                Assert.All(shrunk.Extents, e => Assert.True(e.Offset + e.Length <= cut));
                if (truncateAt is "InlineEligible" or "Zero")
                    Assert.True(shrunk.IsInline, $"a {cut}-byte file is inline-eligible after truncation");

                if (extendBy == "SetLength")
                {
                    await s.SetLengthAsync(extended, Ct);
                }
                else
                {
                    s.Seek(extended - 1, SeekOrigin.Begin);
                    s.WriteByte(0x5A);
                }

                await s.FlushAsync(Ct);
            }

            byte[] actual = await ReadCompatibleAsync(vol, "/t");
            Assert.Equal(extended, actual.LongLength);
            Same(data.AsMemory(0, (int)cut), actual.AsMemory(0, (int)cut), "surviving prefix");
            AssertZeros(actual, cut, extended - 1, "re-extended range must not resurrect truncated bytes");
            Assert.Equal(extendBy == "WriteBeyond" ? (byte)0x5A : (byte)0, actual[^1]);

            FileInspection regrown = await InspectAsync(cluster, "/t");
            AssertWellFormed(regrown, "after re-extension");
            IReadOnlySet<ObjectId> originalObjects = Objects(original);
            foreach (ExtentInspection e in regrown.Extents.Where(e => e.Offset + e.Length > cut))
            {
                Assert.True(e.Offset >= cut, $"extent [{e.Offset},{e.Offset + e.Length}) straddles the truncation point {cut}");
                Assert.DoesNotContain(e.Object, originalObjects);
            }

            IReadOnlySet<ObjectId> stillMapped = Objects(regrown);
            var dropped = original.Extents.Where(e => e.Offset >= cut).Select(e => e.Object)
                .Where(o => !stillMapped.Contains(o)).ToHashSet();
            if (truncateAt is "Zero" or "InlineEligible")
                Assert.Equal(Objects(original).Count, dropped.Count);
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.All(dropped, o => Assert.DoesNotContain(o, state.ReachableObjects()));

            if (dropped.Count > 0)
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                    return !dropped.Any(stored.Contains);
                }, TimeSpan.FromSeconds(20), "objects referenced only by the truncated range must be reclaimed once no handle or snapshot pins them", Ct);
            }

            Same(actual, await vol.Files.ReadAllBytesAsync("/t", Ct), "contents unchanged by reclamation");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-008 InlineToExtentMigration
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Grows an inline file past the threshold (append, whole-file write, stream write, SetLength) while a snapshot holds
    /// the inline version: the live file migrates to extents with exact bytes and the same inode; the snapshot keeps
    /// reading the original inline bytes, also after the live file is deleted.
    /// </summary>
    [Theory]
    [InlineData("Append")]
    [InlineData("WriteAll")]
    [InlineData("StreamWrite")]
    [InlineData("SetLength")]
    [Catalog("LAY-008", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_008_InlineToExtentMigration(string growth)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] small = TestData.Bytes(8_000, DefaultInline - 10);
            await vol.Files.WriteAllBytesAsync("/m", small, Ct);
            DfsFileInfo inlineInfo = await vol.Files.GetInfoAsync("/m", Ct);
            Assert.True(inlineInfo.IsInline);

            await vol.Snapshots.CreateAsync("before-growth", ct: Ct);

            byte[] grown = TestData.Bytes(8_001, 3 * Chunk);
            byte[] expected;
            switch (growth)
            {
                case "Append":
                    await vol.Files.AppendAllBytesAsync("/m", grown, Ct);
                    expected = Concat(small, grown);
                    break;
                case "WriteAll":
                    await vol.Files.WriteAllBytesAsync("/m", grown, Ct);
                    expected = grown;
                    break;
                case "StreamWrite":
                {
                    await using DfsStream s = await vol.Files.OpenAsync("/m", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                    await WriteAtAsync(s, small.Length, grown);
                    await s.FlushAsync(Ct);
                    expected = Concat(small, grown);
                    break;
                }
                case "SetLength":
                {
                    await using DfsStream s = await vol.Files.OpenAsync("/m", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                    await s.SetLengthAsync(3L * Chunk, Ct);
                    await s.FlushAsync(Ct);
                    expected = Concat(small, new byte[3 * Chunk - small.Length]);
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(growth));
            }

            DfsFileInfo live = await vol.Files.GetInfoAsync("/m", Ct);
            Assert.Equal(inlineInfo.InodeId, live.InodeId);
            Assert.False(live.IsInline, $"{growth}: a {expected.Length}-byte file cannot stay inline");
            Assert.Equal(expected.LongLength, live.Length);
            Same(expected, await vol.Files.ReadAllBytesAsync("/m", Ct), $"{growth}: live contents");
            FileInspection liveLayout = await InspectAsync(cluster, "/m");
            AssertWellFormed(liveLayout, growth);
            if (growth != "WriteAll")
                AssertCovers(liveLayout, 0, small.Length, $"{growth}: migrated inline bytes must be backed by extents");

            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.Contains(state.Snapshots, s => s.Name == "before-growth");

            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("before-growth", Ct))
            {
                Same(small, await view.Files.ReadAllBytesAsync("/m", Ct), $"{growth}: snapshot keeps the inline bytes");
                DfsFileInfo old = await view.Files.GetInfoAsync("/m", Ct);
                Assert.Equal(inlineInfo.InodeId, old.InodeId);
                Assert.Equal(inlineInfo.Revision, old.Revision);
                Assert.True(old.IsInline, "the snapshot's version is still the inline representation");
            }

            await vol.Files.DeleteAsync("/m", Ct);
            await using DfsSnapshotView again = await vol.Snapshots.OpenAsync("before-growth", Ct);
            Same(small, await again.Files.ReadAllBytesAsync("/m", Ct), $"{growth}: snapshot after live deletion");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-009 ExtentToInlineMigration
    // ------------------------------------------------------------------------------------------------------------

    private static readonly DfsStoragePolicy AllThree = new() { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };

    /// <summary>
    /// Shrinks a multi-extent file into inline range while a snapshot holds the large version. Under the default policy
    /// the live file becomes inline; under a policy stronger than the metadata majority (3 copies/3 zones vs. 2-of-3
    /// voters) it must stay in extents that meet the policy. Either way the policy name is preserved, and the snapshot
    /// still reads its bytes with its objects retained.
    /// </summary>
    [Theory]
    [InlineData(null, "SetLength")]
    [InlineData(null, "WriteAll")]
    [InlineData("all3", "SetLength")]
    [InlineData("all3", "WriteAll")]
    [Catalog("LAY-009", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_009_ExtentToInlineMigration(string? policy, string shrink)
    {
        var options = ThreeZones(v => v with { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["all3"] = AllThree } });
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] big = TestData.Bytes(9_000, 3 * Chunk + 9);
            await vol.Files.WriteAllBytesAsync("/x", big, new DfsMutationOptions { StoragePolicy = policy }, Ct);
            DfsFileInfo bigInfo = await vol.Files.GetInfoAsync("/x", Ct);
            Assert.False(bigInfo.IsInline);
            FileInspection bigLayout = await InspectAsync(cluster, "/x");
            await vol.Snapshots.CreateAsync("big", ct: Ct);

            byte[] expected;
            if (shrink == "SetLength")
            {
                await using DfsStream s = await vol.Files.OpenAsync("/x", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                await s.SetLengthAsync(1000, Ct);
                await s.FlushAsync(Ct);
                expected = big[..1000];
            }
            else
            {
                expected = TestData.Bytes(9_001, 1000);
                await vol.Files.WriteAllBytesAsync("/x", expected, Ct);
            }

            DfsFileInfo small = await vol.Files.GetInfoAsync("/x", Ct);
            Assert.Equal(bigInfo.InodeId, small.InodeId);
            Assert.Equal(bigInfo.StoragePolicy, small.StoragePolicy);
            Assert.Equal(1000L, small.Length);
            Same(expected, await vol.Files.ReadAllBytesAsync("/x", Ct), "shrunk live contents");
            FileInspection smallLayout = await InspectAsync(cluster, "/x");
            AssertWellFormed(smallLayout, "after shrink");
            Assert.Equal(bigLayout.Policy, smallLayout.Policy);

            if (policy is null)
            {
                Assert.True(small.IsInline, "default policy: a 1000-byte file becomes inline");
                Assert.Equal(0, small.ExtentCount);
            }
            else
            {
                Assert.False(small.IsInline, "inline storage would weaken the 3-copy/3-zone policy below the metadata majority");
                foreach (ObjectId o in Objects(smallLayout))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    Assert.True(copies >= 3 && zones >= 3, $"object {o} has {copies} copies in {zones} zones; policy requires 3/3");
                }
            }

            await using DfsSnapshotView view = await vol.Snapshots.OpenAsync("big", Ct);
            Same(big, await view.Files.ReadAllBytesAsync("/x", Ct), "snapshot keeps the large version");
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            int requiredCopies = policy is null ? 2 : 3;
            foreach (ObjectId o in Objects(bigLayout))
            {
                Assert.Contains(o, state.ReachableObjects());
                Assert.True((await cluster.Inspect.GetObjectHoldersAsync(o)).Count >= requiredCopies, $"snapshot object {o} lost copies");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-011 LogicalUnitsIndependentFromPacking
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, int, int> UnitSizeCases() => new()
    {
        { 1000, 4096, 16 * 1024 },
        { 64 * 1024, 64 * 1024, 64 * 1024 },
        { 100_000, 8192, 64 * 1024 },
        { 7, 1 << 20, 256 * 1024 },
        { 300_000, 32 * 1024, 32 * 1024 },
    };

    /// <summary>
    /// Varies the application write size, the client stream buffer and the logical update unit (chunk size): the logical
    /// file is identical in every configuration, objects never exceed the maximum object size, and a later 100-byte
    /// overwrite replicates on the order of one update unit per copy — not a storage segment (64 MiB) or the file.
    /// </summary>
    [Theory]
    [MemberData(nameof(UnitSizeCases))]
    [Catalog("LAY-011", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_011_LogicalUnitsIndependentFromPacking(int writeSize, int bufferSize, int chunkSize)
    {
        const int fileSize = 600 * 1024;
        byte[] data = TestData.Bytes(11_000, fileSize);
        var options = ThreeZones() with { Timings = NodeTimings.Fast with { ChunkSize = chunkSize } };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { BufferSize = bufferSize }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            Assert.Equal(chunkSize, client.Capabilities.ChunkSize);
            await using (DfsStream s = await vol.Files.CreateAsync("/u", Ct))
            {
                for (int offset = 0; offset < fileSize; offset += writeSize)
                    await s.WriteAsync(data.AsMemory(offset, Math.Min(writeSize, fileSize - offset)), Ct);
                await s.FlushAsync(Ct);
            }

            Same(data, await vol.Files.ReadAllBytesAsync("/u", Ct), "logical contents independent of unit sizes");
            FileInspection layout = await InspectAsync(cluster, "/u");
            AssertWellFormed(layout, "after sequential write");
            AssertCovers(layout, 0, fileSize, "after sequential write");
            Assert.All(layout.Extents, e => Assert.True(e.ObjectLength <= client.Capabilities.MaxObjectSize));

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            long receivedBefore = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesReceived;
            cluster.Network.Log.Reset();

            byte[] patch = TestData.Bytes(11_001, 100);
            await using (DfsStream s = await vol.Files.OpenAsync("/u", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                await WriteAtAsync(s, 300_001, patch);
                await s.FlushAsync(Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            long received = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesReceived - receivedBefore;
            long putBytes = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.PutObject));
            int copies = (await vol.GetInfoAsync(Ct)).DefaultPolicy.TargetCopies;
            long bound = copies * 2L * chunkSize + 64 * 1024;
            Log($"write={writeSize} buffer={bufferSize} chunk={chunkSize}: overwrite stored {received} B, PutObject wire {putBytes} B, bound {bound} B");
            Assert.True(received <= bound, $"a 100-byte overwrite stored {received} bytes across replicas; bound {bound} (file {fileSize})");
            Assert.True(putBytes <= bound, $"a 100-byte overwrite uploaded {putBytes} bytes; bound {bound}");

            patch.CopyTo(data, 300_001);
            Same(data, await vol.Files.ReadAllBytesAsync("/u", Ct), "contents after overwrite");
        }, Ct);
    }

    /// <summary>
    /// The same logical objects stored with different local segment sizes yield identical object identities, lengths,
    /// checksums and bytes: physical packing is not part of the logical representation.
    /// </summary>
    [Fact]
    [Catalog("LAY-011", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_011_LogicalUnitsIndependentFromPacking_SegmentSizes()
    {
        var random = new SeededRandom(11_100);
        VolumeId volume = VolumeId.New(random);
        var objects = Enumerable.Range(0, 50)
            .Select(i => (Id: ObjectId.New(random), Data: TestData.Bytes((ulong)(11_200 + i), 1 + (i * 5_237 % (256 * 1024)))))
            .ToList();

        List<(ObjectId, long, UInt128)>? reference = null;
        foreach (long segmentSize in new[] { 1L << 20, 4L << 20, 64L << 20 })
        {
            var medium = new SimulatedMedium($"seg{segmentSize}", 1L << 30, new SeededRandom((ulong)segmentSize));
            ObjectStore store = await ObjectStore.OpenAsync(medium, "objects", new ObjectStoreOptions(SegmentSize: segmentSize), NoFaults.Instance, Ct);
            try
            {
                foreach ((ObjectId id, byte[] data) in objects)
                    await store.PutAsync(id, volume, new StagingId(1), data, Ct);
                var listing = store.List(volume).Select(r => (r.Id, r.Length, r.Checksum)).OrderBy(r => r.Id.Value).ToList();
                reference ??= listing;
                Assert.Equal(reference, listing);
                foreach ((ObjectId id, byte[] data) in objects)
                {
                    byte[] read = new byte[data.Length];
                    Assert.Equal(data.Length, await store.ReadAsync(id, 0, read, Ct));
                    Same(data, read, $"segment size {segmentSize}: object {id}");
                }
            }
            finally
            {
                await store.DisposeAsync();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-012 ExtentCoalescingPreservesBytes
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Builds a fragmented file (16 adjacent separately-flushed pieces, a hole, 8 more pieces, a trailing hole), coalesces
    /// the first run with one spanning write, then splits the coalesced extent with two small writes. Bytes and holes match
    /// the model at every step, the coalesced run uses fewer extents, the split keeps the remainder mapped to the same
    /// object, and snapshots taken before each step keep their versions.
    /// </summary>
    [Fact]
    [Catalog("LAY-012", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_012_ExtentCoalescingPreservesBytes()
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var model = new ReferenceFileModel();
            ulong seed = 12_000;
            byte[] fragmentedBytes = [], coalescedBytes = [];
            await using (DfsStream s = await vol.Files.OpenAsync("/c", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, Ct))
            {
                for (int i = 0; i < 16; i++)
                    await WriteAndFlushAsync(s, i * 1000L, 1000);
                for (int i = 0; i < 8; i++)
                    await WriteAndFlushAsync(s, 40_000 + i * 500L, 500);
                await s.SetLengthAsync(50_000, Ct);
                await s.FlushAsync(Ct);
                model.SetLength(50_000);
                model.Flush();

                FileInspection fragmented = await InspectAsync(cluster, "/c");
                AssertWellFormed(fragmented, "fragmented");
                await AssertBytesAsync("fragmented");
                AssertNoExtentInHoles(fragmented, "fragmented");
                await vol.Snapshots.CreateAsync("fragmented", ct: Ct);
                fragmentedBytes = model.Committed;
                int fragmentedInRun = fragmented.Extents.Count(e => e.Offset < 16_000);

                await WriteAndFlushAsync(s, 0, 16_000);
                FileInspection coalesced = await InspectAsync(cluster, "/c");
                AssertWellFormed(coalesced, "coalesced");
                await AssertBytesAsync("coalesced");
                AssertNoExtentInHoles(coalesced, "coalesced");
                int coalescedInRun = coalesced.Extents.Count(e => e.Offset < 16_000);
                Assert.True(coalescedInRun < fragmentedInRun, $"one spanning write must replace the {fragmentedInRun} adjacent mappings (now {coalescedInRun})");
                AssertCovers(coalesced, 0, 16_000, "coalesced run");
                await vol.Snapshots.CreateAsync("coalesced", ct: Ct);
                coalescedBytes = model.Committed;

                long[] probes = [100, 4_000, 4_999, 5_010, 9_000, 11_999, 12_010, 15_999];
                var positionsBefore = probes.ToDictionary(p => p, p => PositionAt(coalesced, p));
                await WriteAndFlushAsync(s, 5_000, 10);
                await WriteAndFlushAsync(s, 12_000, 10);
                FileInspection split = await InspectAsync(cluster, "/c");
                AssertWellFormed(split, "split");
                await AssertBytesAsync("split");
                AssertNoExtentInHoles(split, "split");
                Assert.True(split.Extents.Count(e => e.Offset < 16_000) > coalescedInRun, "small writes inside an extent split its mapping");
                foreach (long p in probes)
                    Assert.Equal(positionsBefore[p], PositionAt(split, p));
            }

            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("fragmented", Ct))
                Same(fragmentedBytes, await view.Files.ReadAllBytesAsync("/c", Ct), "snapshot of the fragmented version");
            await using (DfsSnapshotView coalescedView = await vol.Snapshots.OpenAsync("coalesced", Ct))
                Same(coalescedBytes, await coalescedView.Files.ReadAllBytesAsync("/c", Ct), "snapshot of the coalesced version");
            Same(model.Committed, await vol.Files.ReadAllBytesAsync("/c", Ct), "live version after split");

            async Task WriteAndFlushAsync(DfsStream s, long offset, int length)
            {
                byte[] bytes = TestData.Bytes(seed++, length);
                await WriteAtAsync(s, offset, bytes);
                await s.FlushAsync(Ct);
                model.Write(offset, bytes);
                model.Flush();
            }

            async Task AssertBytesAsync(string step)
            {
                byte[] actual = await ReadCompatibleAsync(vol, "/c");
                Same(model.Committed, actual, step);
                AssertZeros(actual, 16_000, 40_000, $"{step}: interior hole");
                AssertZeros(actual, 44_000, 50_000, $"{step}: trailing hole");
            }

            static void AssertNoExtentInHoles(FileInspection f, string step)
            {
                Assert.DoesNotContain(f.Extents, e => e.Offset < 40_000 && e.Offset + e.Length > 16_000);
                Assert.DoesNotContain(f.Extents, e => e.Offset + e.Length > 44_000);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-013 CompressionRangeAccess
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> CompressionCases()
    {
        var data = new TheoryData<string, string>();
        foreach (string kind in new[] { "Compressible", "Incompressible" })
        {
            foreach (string position in new[] { "UnitStart", "UnitEndMinus1", "AcrossUnit", "MidUnit", "LastByte" })
                data.Add(kind, position);
        }

        return data;
    }

    /// <summary>
    /// A small range read from compressible or incompressible data returns exact bytes at encoding-unit boundaries and
    /// makes storage nodes read/decode at most the units that overlap the range (plus the handle's buffer) — never the
    /// whole object or file.
    /// </summary>
    [Theory]
    [MemberData(nameof(CompressionCases))]
    [Catalog("LAY-013", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_013_CompressionRangeAccess(string kind, string position)
    {
        const int units = 16;
        const int handleBuffer = 4096;
        int size = units * EncodingUnit;
        byte[] data = kind == "Compressible"
            ? Enumerable.Range(0, size).Select(i => (byte)('A' + i / EncodingUnit)).ToArray()
            : TestData.Bytes(13_000, size);
        (long offset, int length) = position switch
        {
            "UnitStart" => (5L * EncodingUnit, 100),
            "UnitEndMinus1" => (6L * EncodingUnit - 1, 1),
            "AcrossUnit" => (7L * EncodingUnit - 50, 100),
            "MidUnit" => (9L * EncodingUnit + EncodingUnit / 2, 100),
            "LastByte" => (size - 1L, 1),
            _ => throw new ArgumentOutOfRangeException(nameof(position)),
        };

        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Files.WriteAllBytesAsync("/z", data, Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "cold-reader" }, Ct);
            await using DfsStream s = await reader.GetVolume(Vol).Files.OpenAsync("/z",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, BufferSize = handleBuffer, Options = FileOptions.RandomAccess }, Ct);

            long readBefore = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesRead;
            cluster.Network.Log.Reset();
            byte[] got = await ReadRangeAsync(s, offset, length);
            Same(data.AsMemory((int)offset, length), got, $"{kind} {position}");

            long decoded = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesRead - readBefore;
            long wire = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.GetObjectResult, Direction: LinkDirection.Reply));
            long bound = 2L * EncodingUnit + handleBuffer + 4096;
            Log($"{kind} {position}: storage read {decoded} B, object reply wire {wire} B, bound {bound} B (file {size} B)");
            Assert.True(decoded <= bound, $"reading {length} bytes decoded {decoded} bytes from storage; at most the overlapping units are allowed ({bound})");
            Assert.True(wire <= bound, $"reading {length} bytes transferred {wire} object bytes; bound {bound}");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-014 ObjectIdentityIndependentOfLocation
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Moves replicas between nodes (drain, empty-disk rebuild, node addition): file mappings keep exactly the same object
    /// ids, offsets and revision; every new holder stores the same object identity, length and checksum; reads are exact.
    /// </summary>
    [Theory]
    [InlineData("Drain")]
    [InlineData("EmptyDiskRebuild")]
    [InlineData("AddNode")]
    [Catalog("LAY-014", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_014_ObjectIdentityIndependentOfLocation(string move)
    {
        var options = TestClusterOptions.ThreeZones with { NodeCount = 4, Volumes = [new TestVolumeSpec(Vol)] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 3; i++)
            {
                byte[] data = TestData.Bytes((ulong)(14_000 + i), 3 * Chunk + i * 1000);
                await vol.Files.WriteAllBytesAsync($"/id{i}", data, Ct);
                files[$"/id{i}"] = data;
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            var layouts = new Dictionary<string, FileInspection>(StringComparer.Ordinal);
            foreach (string path in files.Keys)
                layouts[path] = await InspectAsync(cluster, path);
            var allObjects = layouts.Values.SelectMany(f => Objects(f)).ToHashSet();
            ObjectId first = layouts["/id0"].Extents[0].Object;
            string victim = (await cluster.Inspect.GetObjectHoldersAsync(first))[0];
            var records = new Dictionary<ObjectId, ObjectRecordInfo>();
            foreach (ObjectRecordInfo r in await cluster.Node(victim).Inspect.ListObjectsAsync(Vol))
                records[r.Id] = r;

            switch (move)
            {
                case "Drain":
                    await cluster.Admin.DrainNodeAsync(victim, Ct);
                    await cluster.WaitUntilAsync(async () =>
                    {
                        foreach (ObjectId o in allObjects)
                        {
                            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                            if (holders.Contains(victim) || holders.Count < 3)
                                return false;
                        }

                        return true;
                    }, TimeSpan.FromSeconds(30), $"drain must move every copy off {victim} to verified replacements", Ct);
                    break;
                case "EmptyDiskRebuild":
                    await cluster.Node(victim).CrashAsync();
                    await cluster.Node(victim).ReplaceDiskWithEmptyAsync();
                    await cluster.Node(victim).RestartAsync();
                    await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
                    break;
                case "AddNode":
                    await cluster.AddNodeAsync(new TestNodeSpec("n4", "z5"), ct: Ct);
                    await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
                    break;
            }

            foreach ((string path, byte[] data) in files)
            {
                FileInspection now = await InspectAsync(cluster, path);
                Assert.Equal(layouts[path].Revision, now.Revision);
                Assert.Equal(layouts[path].Extents, now.Extents);
                Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), $"{move}: {path}");
            }

            foreach (ObjectId o in allObjects)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                Assert.True(holders.Count >= 3, $"{move}: object {o} has {holders.Count} verified holders");
                foreach (string holder in holders)
                {
                    ObjectRecordInfo? record = (await cluster.Node(holder).Inspect.ListObjectsAsync(Vol)).SingleOrDefault(r => r.Id == o);
                    Assert.NotNull(record);
                    if (records.TryGetValue(o, out ObjectRecordInfo? original))
                    {
                        Assert.Equal(original.Length, record.Length);
                        Assert.Equal(original.Checksum, record.Checksum);
                    }
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-015 MetadataTreePathCopy
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Changing one leaf mapping of a file with 4 vs. 128 extents: the commit request is the same size (it carries only
    /// the changed mapping, not the whole tree), all other extents are unchanged, and the old root stays readable through
    /// a snapshot and a version-pinned handle.
    /// </summary>
    [Fact]
    [Catalog("LAY-015", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_015_MetadataTreePathCopy()
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var models = new Dictionary<string, ReferenceFileModel>(StringComparer.Ordinal);
            foreach ((string path, int pieces) in new[] { ("/few", 4), ("/many", 128) })
            {
                var model = new ReferenceFileModel();
                await using (DfsStream s = await vol.Files.OpenAsync(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
                {
                    for (int i = 0; i < pieces; i++)
                    {
                        byte[] piece = TestData.Bytes((ulong)(15_000 + i), 64);
                        await WriteAtAsync(s, i * 128L, piece);
                        model.Write(i * 128L, piece);
                    }

                    await s.SetLengthAsync(pieces * 128L + 4096, Ct);
                    model.SetLength(pieces * 128L + 4096);
                    await s.FlushAsync(Ct);
                    model.Flush();
                }

                models[path] = model;
                Assert.True((await InspectAsync(cluster, path)).Extents.Count >= pieces, $"{path} must map {pieces} separate data ranges");
            }

            await vol.Snapshots.CreateAsync("old-root", ct: Ct);
            await using DfsStream pinned = await vol.Files.OpenAsync("/many",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned }, Ct);
            byte[] oldMany = models["/many"].Committed;

            var commitBytes = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (string path in new[] { "/few", "/many" })
            {
                FileInspection before = await InspectAsync(cluster, path);
                byte[] patch = TestData.Bytes(15_500, 10);
                cluster.Network.Log.Reset();
                await using (DfsStream s = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
                {
                    await WriteAtAsync(s, 2 * 128 + 20, patch);
                    await s.FlushAsync(Ct);
                }

                commitBytes[path] = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.CommitFile, Direction: LinkDirection.Request));
                models[path].Write(2 * 128 + 20, patch);
                models[path].Flush();
                Same(models[path].Committed, await vol.Files.ReadAllBytesAsync(path, Ct), path);

                FileInspection after = await InspectAsync(cluster, path);
                var unchanged = before.Extents.Where(e => e.Offset + e.Length <= 2 * 128 || e.Offset >= 3 * 128).ToList();
                Assert.All(unchanged, e => Assert.Contains(e, after.Extents));
                int changed = after.Extents.Count(e => !before.Extents.Contains(e));
                Assert.True(changed <= 3, $"{path}: one leaf change produced {changed} new mapping records");
            }

            Log($"CommitFile bytes: 4 extents {commitBytes["/few"]} B, 128 extents {commitBytes["/many"]} B");
            Assert.True(commitBytes["/many"] > 0, "the commit request must be observed");
            Assert.True(commitBytes["/many"] <= commitBytes["/few"] + 512,
                $"commit size grew with the number of untouched extents ({commitBytes["/few"]} B for 4 vs {commitBytes["/many"]} B for 128)");

            Same(oldMany, await ReadToEndAsync(pinned, 4096), "version-pinned handle reads the old root");
            await using DfsSnapshotView view = await vol.Snapshots.OpenAsync("old-root", Ct);
            Same(oldMany, await view.Files.ReadAllBytesAsync("/many", Ct), "snapshot reads the old root");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-017 ManyExtentsStayBounded
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// An adversarially fragmented file (one byte every 16 bytes, 16 384 data ranges): staged accounting equals the data
    /// bytes (no mapping-proportional inflation), a 16-byte read transfers metadata proportional to the range rather than
    /// to the 16 384-entry mapping, streaming reads are exact, and materializing a file above
    /// <see cref="DfsClientOptions.MaxMaterializeBytes"/> fails with <see cref="DfsLimitExceededException"/> instead of allocating.
    /// </summary>
    [Fact]
    [Catalog("LAY-017", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_017_ManyExtentsStayBounded()
    {
        const int pieces = 16_384;
        const int stride = 16;
        const int bufferSize = 64 * 1024;
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { BufferSize = bufferSize, MaxMaterializeBytes = 1 << 20 }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] expected = new byte[pieces * stride];
            await using (DfsStream s = await vol.Files.OpenAsync("/frag", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
            {
                for (int i = 0; i < pieces; i++)
                {
                    byte b = (byte)(1 + i % 251);
                    expected[i * stride] = b;
                    s.Seek((long)i * stride, SeekOrigin.Begin);
                    s.WriteByte(b);
                }

                Assert.Equal((long)pieces, s.PendingBytes);
                await s.SetLengthAsync(expected.Length, Ct);
                await s.FlushAsync(Ct);
            }

            FileInspection layout = await InspectAsync(cluster, "/frag");
            AssertWellFormed(layout, "fragmented");
            Log($"fragmented file: {layout.Extents.Count} extents");

            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "frag-reader" }, Ct);
            await using DfsStream r = await reader.GetVolume(Vol).Files.OpenAsync("/frag",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite, BufferSize = 4096 }, Ct);
            cluster.Network.Log.Reset();
            byte[] small = await ReadRangeAsync(r, pieces / 2 * stride, 16);
            Same(expected.AsMemory(pieces / 2 * stride, 16), small, "16-byte read in the middle");
            long metadata = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.ReadRangeResult, Direction: LinkDirection.Reply));
            Log($"16-byte read: ReadRangeResult {metadata} B");
            Assert.True(metadata <= 16 * 1024, $"a 16-byte read returned {metadata} bytes of mapping metadata; must be bounded by the requested range, not the {layout.Extents.Count}-entry mapping");

            r.Seek(0, SeekOrigin.Begin);
            Same(expected, await ReadToEndAsync(r, 4096), "streamed read of the fragmented file");

            await using (DfsStream big = await vol.Files.OpenAsync("/sparse-big", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
            {
                await big.SetLengthAsync(8L << 20, Ct);
                await big.FlushAsync(Ct);
            }

            DfsLimitExceededException limit = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(
                () => vol.Files.ReadAllBytesAsync("/sparse-big", Ct), DfsErrorCode.LimitExceeded);
            Assert.Equal(1L << 20, limit.Limit);
            Assert.Equal(8L << 20, limit.Requested);
            Assert.False(string.IsNullOrEmpty(limit.LimitName));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-018 UntrustedEncodingLengthsRejected
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, byte[], string> MalformedWireCases() => new()
    {
        { "bytes length beyond remaining", [0x05, 0x01, 0x02], "Bytes" },
        { "bytes length 2^63", [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01], "Bytes" },
        { "bytes length above limit", [0x05, 1, 2, 3, 4, 5], "BytesLimit4" },
        { "string length beyond remaining", [0x7F, 0x41], "String" },
        { "string invalid UTF-8", [0x02, 0xC3, 0x28], "String" },
        { "varint longer than 10 bytes", [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00], "VarUInt" },
        { "varint overflowing 64 bits", [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02], "VarUInt" },
        { "truncated varint", [0x80], "VarUInt" },
        { "non-minimal varint", [0x80, 0x00], "VarUInt" },
        { "truncated fixed int64", [0x01, 0x02, 0x03], "Int64" },
        { "trailing bytes after body", [0x01, 0x00], "ByteThenEnd" },
    };

    /// <summary>
    /// Untrusted length/offset encodings (lengths beyond the buffer or limits, 2^63 lengths, overlong/overflowing/
    /// truncated varints, invalid UTF-8, trailing bytes) fail with <see cref="DfsProtocolException"/> before any
    /// allocation sized by the hostile value.
    /// </summary>
    [Theory]
    [MemberData(nameof(MalformedWireCases))]
    [Catalog("LAY-018", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public void LAY_018_UntrustedEncodingLengthsRejected_Wire(string description, byte[] input, string read)
    {
        WireLimits limits = read == "BytesLimit4" ? new WireLimits(MaxBytesLength: 4) : WireLimits.Default;
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() =>
        {
            var reader = new WireReader(input, limits);
            switch (read)
            {
                case "Bytes":
                case "BytesLimit4":
                    _ = reader.ReadBytes().Length;
                    break;
                case "String":
                    _ = reader.ReadString();
                    break;
                case "VarUInt":
                    _ = reader.ReadVarUInt();
                    break;
                case "Int64":
                    _ = reader.ReadInt64();
                    break;
                case "ByteThenEnd":
                    _ = reader.ReadByte();
                    reader.EnsureEnd();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(read));
            }
        });
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        Assert.True(ex.ErrorCode is DfsErrorCode.ProtocolError, $"{description}: expected ProtocolError, got {ex.ErrorCode}");
        Assert.True(allocated < 1 << 20, $"{description}: rejecting a hostile length allocated {allocated} bytes");
    }

    /// <summary>A negative offset survives decoding as a signed value; range validation is the consumer's job (covered by the server cases).</summary>
    [Fact]
    [Catalog("LAY-018", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public void LAY_018_UntrustedEncodingLengthsRejected_SignedDecoding()
    {
        var reader = new WireReader(new byte[] { 0x01, 0x03 }, WireLimits.Default);
        Assert.Equal(-1L, reader.ReadVarInt());
        Assert.Equal(-2L, reader.ReadVarInt());
        reader.EnsureEnd();
    }

    public static TheoryData<string, long, long, string> ReadRangeCases() => new()
    {
        { "negative offset", -1, 10, "InvalidArgument|ProtocolError" },
        { "negative length", 0, -1, "InvalidArgument|ProtocolError" },
        { "min offset", long.MinValue, 1, "InvalidArgument|ProtocolError" },
        { "offset+length overflows", long.MaxValue - 5, 100, "InvalidArgument|ProtocolError|None" },
        { "huge length", 0, long.MaxValue, "None|InvalidArgument|LimitExceeded" },
        { "offset beyond EOF", 1_000_000, 10, "None" },
        { "valid", 10, 100, "None" },
    };

    /// <summary>
    /// A raw client sends ReadRange requests with negative, overflowing and huge offsets/lengths: the server rejects or
    /// clamps them without crashing or allocating by the hostile length, and the node keeps serving correct reads.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReadRangeCases))]
    [Catalog("LAY-018", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_018_UntrustedEncodingLengthsRejected_ReadRange(string description, long offset, long length, string allowed)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(18_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/r", data, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            DfsErrorCode code = await raw.SendReadRangeAsync(Vol, "/r", offset, length);
            AssertCodeIn(code, allowed, description);

            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
            Same(data, await vol.Files.ReadAllBytesAsync("/r", Ct), $"{description}: node still serves exact reads");
        }, Ct);
    }

    public static TheoryData<string, string> RawCommitCases() => new()
    {
        { "valid", "None" },
        { "negative offset", "InvalidArgument|ProtocolError" },
        { "negative length", "InvalidArgument|ProtocolError" },
        { "zero length", "InvalidArgument" },
        { "object range beyond object", "InvalidArgument|Integrity" },
        { "negative object offset", "InvalidArgument|ProtocolError" },
        { "extent beyond new length", "InvalidArgument" },
        { "overlapping extents", "InvalidArgument" },
        { "offset overflow", "InvalidArgument|LimitExceeded" },
        { "negative new length", "InvalidArgument|ProtocolError" },
        { "new length above max", "LimitExceeded|InvalidArgument" },
        { "declared object length mismatch", "InvalidArgument|Integrity|PlacementUnavailable" },
        { "unsupported encoding", "Unsupported|InvalidArgument|PlacementUnavailable" },
    };

    /// <summary>
    /// A raw writer publishes extent maps with negative, zero, overflowing, overlapping, out-of-object, out-of-file,
    /// length-inconsistent or unsupported-encoding ranges: each is rejected with a precise error, nothing is published
    /// (length, revision and mapping unchanged), the node stays up, and a valid control commit succeeds.
    /// </summary>
    [Theory]
    [MemberData(nameof(RawCommitCases))]
    [Catalog("LAY-018", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_018_UntrustedEncodingLengthsRejected_CommitExtents(string description, string allowed)
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));

            await using RawStagedWriter writer = await raw.OpenStagedWriterAsync(Vol, "/raw", FileMode.CreateNew);
            byte[] payload = TestData.Bytes(18_100, 1000);
            ObjectId id = ObjectId.New(new SeededRandom(18_100));
            var receipts = new List<RawReceipt>();
            foreach (string node in new[] { "n0", "n1" })
            {
                RawPutResult put = await writer.PutObjectAsync(node, id, payload);
                Assert.Equal(DfsErrorCode.None, put.Error);
                Assert.NotNull(put.Receipt);
                receipts.Add(put.Receipt);
            }

            DfsFileInfo before = await vol.Files.GetInfoAsync("/raw", Ct);
            RawExtentSpec whole = RawExtentSpec.FromReceipt(receipts[0], 0, 1000);
            (long newLength, RawExtentSpec[] extents) = description switch
            {
                "valid" => (1000L, new[] { whole }),
                "negative offset" => (1000L, new[] { whole with { Offset = -1 } }),
                "negative length" => (1000L, new[] { whole with { Length = -5 } }),
                "zero length" => (1000L, new[] { whole with { Length = 0 } }),
                "object range beyond object" => (1000L, new[] { whole with { ObjectOffset = 900, Length = 200 } }),
                "negative object offset" => (1000L, new[] { whole with { ObjectOffset = -1 } }),
                "extent beyond new length" => (500L, new[] { whole }),
                "overlapping extents" => (1000L, new[] { whole with { Length = 600 }, whole with { Offset = 500, ObjectOffset = 500, Length = 500 } }),
                "offset overflow" => (long.MaxValue, new[] { whole with { Offset = long.MaxValue - 10, Length = 100 } }),
                "negative new length" => (-1L, new[] { whole }),
                "new length above max" => (client.Capabilities.MaxFileLength + 1, new[] { whole }),
                "declared object length mismatch" => (1000L, new[] { whole with { ObjectLength = 2000 } }),
                "unsupported encoding" => (1000L, new[] { whole with { Encoding = 0xEE } }),
                _ => throw new ArgumentOutOfRangeException(nameof(description)),
            };

            DfsErrorCode code = await writer.CommitExtentsAsync(newLength, extents, receipts);
            AssertCodeIn(code, allowed, description);
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));

            DfsFileInfo after = await vol.Files.GetInfoAsync("/raw", Ct);
            FileInspection layout = await InspectAsync(cluster, "/raw");
            AssertWellFormed(layout, description);
            if (code == DfsErrorCode.None)
            {
                Assert.Equal(1000L, after.Length);
                Same(payload, await ReadCompatibleAsync(vol, "/raw"), "valid raw commit");
            }
            else
            {
                Assert.Equal(before.Revision, after.Revision);
                Assert.Equal(0L, after.Length);
                Assert.Empty(layout.Extents);
            }
        }, Ct);
    }

    /// <summary>
    /// Inconsistent stored record headers (corrupted identity/length metadata) on one holder fail over to a verified
    /// replica; on every holder they surface <see cref="DfsIntegrityException"/> naming the object — never zero-filled or
    /// truncated data — and no node crashes.
    /// </summary>
    [Fact]
    [Catalog("LAY-018", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_018_UntrustedEncodingLengthsRejected_StoredRecordHeader()
    {
        await ClusterTest.RunAsync(ThreeZones(), async cluster =>
        {
            await using (DfsClient writer = await cluster.ConnectAsync(ct: Ct))
                await writer.GetVolume(Vol).Files.WriteAllBytesAsync("/h", TestData.Bytes(18_200, Chunk), Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            byte[] data = TestData.Bytes(18_200, Chunk);
            FileInspection layout = await InspectAsync(cluster, "/h");
            ObjectId target = layout.Extents[0].Object;
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(target);
            Assert.True(holders.Count >= 2);

            await cluster.Node(holders[0]).Disk.CorruptObjectHeaderAsync(target);
            await using (DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "h1" }, Ct))
                Same(data, await c1.GetVolume(Vol).Files.ReadAllBytesAsync("/h", Ct), "fail over past a corrupt header");

            foreach (string holder in holders.Skip(1))
                await cluster.Node(holder).Disk.CorruptObjectHeaderAsync(target);
            await using DfsClient c2 = await cluster.ConnectAsync(new TestClientOptions { Name = "h2" }, Ct);
            DfsIntegrityException ex = await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(
                () => c2.GetVolume(Vol).Files.ReadAllBytesAsync("/h", Ct), DfsErrorCode.Integrity);
            if (ex.ObjectId is not null)
                Assert.Equal(target, ex.ObjectId.Value);
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
        }, Ct);
    }

    private static void AssertCodeIn(DfsErrorCode code, string allowed, string context)
    {
        var set = allowed.Split('|').Select(s => Enum.Parse<DfsErrorCode>(s)).ToHashSet();
        Assert.True(set.Contains(code), $"{context}: server answered {code}; allowed {allowed}");
    }

    // ------------------------------------------------------------------------------------------------------------
    // LAY-020 DedupScopeCannotLeakData
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Identical contents written by two principals in two volumes (and twice by one principal) never share objects
    /// across principals/volumes; a second principal's identical write uploads the full payload (no "already present"
    /// shortcut that would reveal the other principal's content) and cannot fetch the other volume's object by id. If no
    /// deduplication feature is advertised, even same-scope identical writes must not share objects.
    /// </summary>
    [Fact]
    [Catalog("LAY-020", "U", "INV-01", "INV-02", "INV-09", "INV-16")]
    public async Task LAY_020_DedupScopeCannotLeakData()
    {
        var options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec("volA") { InlineThreshold = 0 }, new TestVolumeSpec("volB") { InlineThreshold = 0 }] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice", new Dictionary<string, DfsPermissions> { ["volA"] = DfsPermissions.Read | DfsPermissions.Write });
            TestPrincipal bob = await cluster.CreatePrincipalAsync("bob", new Dictionary<string, DfsPermissions> { ["volB"] = DfsPermissions.Read | DfsPermissions.Write });
            await using DfsClient a = await cluster.ConnectAsync(new TestClientOptions { Name = "alice", Credentials = alice.Credentials }, Ct);
            await using DfsClient b = await cluster.ConnectAsync(new TestClientOptions { Name = "bob", Credentials = bob.Credentials }, Ct);
            bool dedupAdvertised = a.Capabilities.Features.Any(f => f.Contains("dedup", StringComparison.OrdinalIgnoreCase));

            byte[] secret = TestData.Bytes(20_000, 2 * Chunk);
            await a.GetVolume("volA").Files.WriteAllBytesAsync("/secret", secret, Ct);
            await a.GetVolume("volA").Files.WriteAllBytesAsync("/secret-copy", secret, Ct);

            cluster.Network.Log.Reset();
            long storedBefore = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesReceived;
            await b.GetVolume("volB").Files.WriteAllBytesAsync("/guess", secret, Ct);
            long bobUpload = cluster.Network.Log.CountBytes(new MessageFilter(From: "client:bob", Type: MessageType.PutObject));
            long stored = (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesReceived - storedBefore;
            int ack = (await b.GetVolume("volB").GetInfoAsync(Ct)).DefaultPolicy.DurableCopiesBeforeAck;
            Assert.True(bobUpload >= secret.Length,
                $"bob transmitted {bobUpload} bytes for a {secret.Length}-byte file; a cross-principal dedup shortcut would reveal alice's content");
            Assert.True(stored >= (long)secret.Length * ack,
                $"bob's write stored {stored} new bytes; acknowledging {ack} copies requires {secret.Length * ack} without cross-scope sharing");

            FileInspection? fa = await cluster.Inspect.GetFileAsync("volA", "/secret");
            FileInspection? fa2 = await cluster.Inspect.GetFileAsync("volA", "/secret-copy");
            FileInspection? fb = await cluster.Inspect.GetFileAsync("volB", "/guess");
            Assert.NotNull(fa);
            Assert.NotNull(fa2);
            Assert.NotNull(fb);
            Assert.Empty(Objects(fa).Intersect(Objects(fb)));
            if (!dedupAdvertised)
                Assert.Empty(Objects(fa).Intersect(Objects(fa2)));

            await AssertEx.ThrowsDfsAsync<UnauthorizedAccessException>(() => b.GetVolume("volA").Files.ReadAllBytesAsync("/secret", Ct));
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(bob.Credentials));
            foreach (ObjectId o in Objects(fa))
            {
                DfsErrorCode code = await raw.GetObjectDirectAsync("volA", o);
                Assert.True(code is DfsErrorCode.AccessDenied or DfsErrorCode.AuthenticationFailed, $"bob fetched alice's object {o} by id: {code}");
            }

            Same(secret, await b.GetVolume("volB").Files.ReadAllBytesAsync("/guess", Ct), "bob reads his own copy");
        }, Ct);
    }
}
