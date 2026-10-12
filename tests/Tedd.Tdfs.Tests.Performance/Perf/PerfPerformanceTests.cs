using System.Diagnostics;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Performance.Perf;

/// <summary>
/// PERF suite, level P: small-scale but real measurements of latency, throughput, amplification and resource envelopes
/// (FSD §§10, 15; test plan §7.27). Every number is logged with the durability policy and topology it was measured under,
/// and every test asserts a bounded-resource or amplification property — never just "it ran".
/// </summary>
[Trait("Category", "Performance")]
public sealed class PerfPerformanceTests
{
    private const string Vol = PerfHelpers.Volume;

    private static CancellationToken Ct => PerfHelpers.Ct;

    // ============================================================================================================
    // PERF-001 SmallFileCreateOpenStatDelete
    // ============================================================================================================

    /// <summary>
    /// Size buckets around the inline threshold (4 KiB) and the chunk size (64 KiB), created among 500 existing entries:
    /// create/open/stat/delete latency percentiles and network bytes per file are reported per bucket; inline-sized files
    /// store no objects and report <c>IsInline</c>, larger files store exactly ceil(size/chunk) objects; metadata cost per
    /// file stays bounded; every byte read back is exact and every deleted file is gone.
    /// </summary>
    [Fact, Catalog("PERF-001", "P", "INV-16", "INV-17")]
    public async Task PERF_001_SmallFileCreateOpenStatDelete()
    {
        int[] sizes = [0, 1, 4095, 4096, 4097, 65535, 65536, 65537];
        const int perBucket = 30;
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolumeInfo info = await vol.GetInfoAsync(Ct);
            int chunk = cluster.Options.Timings.ChunkSize;
            string context = $"{PerfHelpers.Describe(info.DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; inline {info.InlineThreshold} B; chunk {chunk} B";
            await vol.Directories.CreateDirectoryAsync("/perf001/pop", Ct);
            for (int i = 0; i < 500; i++)
                await vol.Files.WriteAllBytesAsync($"/perf001/pop/p{i}", TestData.Bytes((ulong)i, 64), Ct);

            foreach (int size in sizes)
            {
                var create = new LatencyRecorder();
                var open = new LatencyRecorder();
                var stat = new LatencyRecorder();
                var delete = new LatencyRecorder();
                IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                cluster.Network.Log.Reset();
                var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                for (int j = 0; j < perBucket; j++)
                {
                    string path = $"/perf001/pop/s{size}-{j}";
                    byte[] content = TestData.Bytes((ulong)(size * 1000 + j), size);
                    files[path] = content;
                    await create.TimeAsync(() => vol.Files.WriteAllBytesAsync(path, content, Ct));
                }

                long createBytes = cluster.Network.Log.CountBytes(MessageFilter.Any);
                foreach ((string path, byte[] content) in files)
                {
                    byte[] read = await open.TimeAsync(async () =>
                    {
                        await using DfsStream stream = await vol.Files.OpenReadAsync(path, Ct);
                        var buffer = new byte[content.Length];
                        await stream.ReadExactlyAsync(buffer, Ct);
                        Assert.Equal(0, await stream.ReadAsync(new byte[1], Ct));
                        return buffer;
                    });
                    PerfHelpers.BytesEqual(content, read, path);
                }

                int expectedExtents = size <= info.InlineThreshold ? 0 : (size + chunk - 1) / chunk;
                foreach (string path in files.Keys)
                {
                    DfsFileInfo fileInfo = await stat.TimeAsync(() => vol.Files.GetInfoAsync(path, Ct));
                    Assert.Equal(size, fileInfo.Length);
                    Assert.Equal(size <= info.InlineThreshold, fileInfo.IsInline);
                    Assert.Equal(expectedExtents, fileInfo.ExtentCount);
                }

                IReadOnlySet<ObjectId> storedAfter = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                int newObjects = storedAfter.Count(id => !storedBefore.Contains(id));
                Assert.Equal(perBucket * expectedExtents, newObjects);

                foreach (string path in files.Keys)
                {
                    await delete.TimeAsync(() => vol.Files.DeleteAsync(path, Ct));
                    Assert.False(await vol.Files.ExistsAsync(path, Ct));
                }

                long networkPerFile = createBytes / perBucket;
                long payloadCopies = size <= info.InlineThreshold ? 3L * size : (long)info.DefaultPolicy.TargetCopies * size;
                long metadataPerFile = Math.Max(0, networkPerFile - payloadCopies);
                PerfHelpers.Log($"size {size,6}: {create.Summary("create")} | {open.Summary("open+read")} | {stat.Summary("stat")} | {delete.Summary("delete")}");
                PerfHelpers.Report($"perf001.size{size}.create.p99", create.Percentile(99).TotalMilliseconds, "ms", context);
                PerfHelpers.Report($"perf001.size{size}.network-bytes-per-file", networkPerFile, "B", context);
                PerfHelpers.Report($"perf001.size{size}.metadata-bytes-per-file", metadataPerFile, "B", context);
                PerfHelpers.Report($"perf001.size{size}.objects-per-file", (double)newObjects / perBucket, "objects", context);
                Assert.True(metadataPerFile <= 32 * 1024, $"size {size}: {metadataPerFile} metadata bytes per file is unbounded overhead");
                Assert.True(create.Percentile(99) < TimeSpan.FromSeconds(2), $"size {size}: create p99 {create.Percentile(99)}");
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-002 RandomReadWriteBoundaryMatrix
    // ============================================================================================================

    /// <summary>
    /// Aligned and chunk-boundary-crossing writes and reads of 1 B..64 KiB in 256 KiB and 1 MiB files (WriteThrough, so
    /// each write is one durable publication). Reports p50/p95/p99, useful vs stored and network bytes; every small write
    /// stores at most copies × (size + 2 chunks) — never a whole-file rewrite — every read moves at most size + 2 chunks of
    /// object data, and the final file equals the independent reference model byte for byte.
    /// </summary>
    [Fact, Catalog("PERF-002", "P", "INV-16", "INV-17")]
    public async Task PERF_002_RandomReadWriteBoundaryMatrix()
    {
        int[] fileSizes = [256 * 1024, 1024 * 1024];
        int[] ioSizes = [1, 512, 4096, 65536];
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            int chunk = cluster.Options.Timings.ChunkSize;
            string context = $"{PerfHelpers.Describe(policy)}; {PerfHelpers.Describe(cluster.Options)}; chunk {chunk} B; WriteThrough";
            await vol.Directories.CreateDirectoryAsync("/perf002", Ct);
            var random = new SeededRandom(2002);

            foreach (int fileSize in fileSizes)
            {
                string path = $"/perf002/f{fileSize}";
                byte[] initial = TestData.Bytes((ulong)fileSize, fileSize);
                await vol.Files.WriteAllBytesAsync(path, initial, Ct);
                var model = new ReferenceFileModel(initial);
                await using DfsStream stream = await vol.Files.OpenAsync(path,
                    new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough }, Ct);

                foreach (int ioSize in ioSizes)
                {
                    foreach (bool aligned in new[] { true, false })
                    {
                        var writes = new LatencyRecorder();
                        var reads = new LatencyRecorder();
                        long useful = 0, stored = 0, readUseful = 0, readMoved = 0;
                        for (int n = 0; n < 10; n++)
                        {
                            long offset = aligned
                                ? (long)random.Next(fileSize / ioSize) * ioSize
                                : (long)(1 + random.Next(fileSize / chunk - 1)) * chunk - Math.Max(1, ioSize / 2);
                            byte[] data = TestData.Bytes((ulong)(fileSize + ioSize * 31 + n), ioSize);
                            long receivedBefore = await PerfHelpers.TotalReceivedObjectBytesAsync(cluster);
                            await writes.TimeAsync(async () =>
                            {
                                stream.Position = offset;
                                await stream.WriteAsync(data, Ct);
                            });
                            long storedDelta = await PerfHelpers.TotalReceivedObjectBytesAsync(cluster) - receivedBefore;
                            model.Write(offset, data);
                            model.Flush();
                            useful += ioSize;
                            stored += storedDelta;
                            Assert.True(storedDelta <= policy.TargetCopies * (ioSize + 2L * chunk),
                                $"a {ioSize}-byte write at {offset} stored {storedDelta} bytes (whole file is {fileSize})");

                            long readOffset = aligned ? (long)random.Next(fileSize / ioSize) * ioSize : offset;
                            var buffer = new byte[ioSize];
                            cluster.Network.Log.Reset();
                            await reads.TimeAsync(async () =>
                            {
                                stream.Position = readOffset;
                                await stream.ReadExactlyAsync(buffer, Ct);
                            });
                            long moved = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.GetObjectResult));
                            PerfHelpers.BytesEqual(model.Read(readOffset, ioSize, staged: false), buffer, $"read {ioSize}@{readOffset}");
                            Assert.True(moved <= ioSize + 2L * chunk + 4096, $"a {ioSize}-byte read moved {moved} bytes of object data");
                            readUseful += ioSize;
                            readMoved += moved;
                        }

                        string cell = $"file {fileSize} io {ioSize} {(aligned ? "aligned" : "boundary")}";
                        PerfHelpers.Log($"{cell}: {writes.Summary("write")} | {reads.Summary("read")}");
                        PerfHelpers.Report($"perf002.{fileSize}.{ioSize}.{(aligned ? "aligned" : "boundary")}.write-amplification", (double)stored / useful, "x", context);
                        PerfHelpers.Report($"perf002.{fileSize}.{ioSize}.{(aligned ? "aligned" : "boundary")}.read-amplification", (double)readMoved / readUseful, "x", context);
                    }
                }

                PerfHelpers.BytesEqual(model.Committed, await vol.Files.ReadAllBytesAsync(path, Ct), $"final {path}");
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-003 SequentialStreamingBoundedMemory
    // ============================================================================================================

    /// <summary>
    /// 16 MiB and 64 MiB files are streamed in and out (data on the real filesystem, never held whole by the test). Live
    /// managed memory, measured after full collections every 4 MiB, follows the configured buffer/in-flight limits: the
    /// writer's pending bytes never exceed buffer + 2 chunks, peak live memory stays under half the larger file, and
    /// quadrupling the file size adds at most 8 MiB of peak live memory. Every streamed byte is verified.
    /// </summary>
    [Fact, Catalog("PERF-003", "P", "INV-16", "INV-17")]
    public async Task PERF_003_SequentialStreamingBoundedMemory()
    {
        await ClusterTest.RunAsync(PerfHelpers.RealtimeOnDisk(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/perf003", Ct);
            int chunk = cluster.Options.Timings.ChunkSize;
            int bufferSize = cluster.CreateClientOptions(new TestClientOptions { Name = "c0" }).DefaultBufferSize;
            string context = $"{PerfHelpers.Describe((await vol.GetInfoAsync(Ct)).DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; buffer {bufferSize} B";
            const int block = 64 * 1024;
            var peaks = new Dictionary<long, long>();

            foreach (long size in new[] { 16L << 20, 64L << 20 })
            {
                string path = $"/perf003/f{size}";
                ulong seed = (ulong)size;
                long baseline = PerfHelpers.LiveBytes();
                long peak = 0;
                long maxPending = 0;
                var timer = Stopwatch.StartNew();

                await using (DfsStream writer = await vol.Files.CreateAsync(path, Ct))
                {
                    for (long b = 0; b < size / block; b++)
                    {
                        await writer.WriteAsync(PerfHelpers.Block(seed, b, block), Ct);
                        maxPending = Math.Max(maxPending, writer.PendingBytes);
                        if ((b + 1) * block % (4L << 20) == 0)
                            peak = Math.Max(peak, PerfHelpers.LiveBytes() - baseline);
                    }

                    await writer.FlushAsync(Ct);
                }

                TimeSpan writeTime = timer.Elapsed;
                timer.Restart();
                var sink = new VerifyingSink(seed, block);
                await using (DfsStream reader = await vol.Files.OpenReadAsync(path, Ct))
                {
                    var buffer = new byte[block];
                    long nextCheck = 4L << 20;
                    int n;
                    while ((n = await reader.ReadAsync(buffer, Ct)) > 0)
                    {
                        await sink.WriteAsync(buffer.AsMemory(0, n), Ct);
                        if (sink.Received >= nextCheck)
                        {
                            peak = Math.Max(peak, PerfHelpers.LiveBytes() - baseline);
                            nextCheck += 4L << 20;
                        }
                    }
                }

                TimeSpan readTime = timer.Elapsed;
                Assert.Equal(size, sink.Received);
                peaks[size] = peak;
                PerfHelpers.Report($"perf003.{size >> 20}MiB.write-throughput", size / 1048576.0 / writeTime.TotalSeconds, "MiB/s", context);
                PerfHelpers.Report($"perf003.{size >> 20}MiB.read-throughput", size / 1048576.0 / readTime.TotalSeconds, "MiB/s", context);
                PerfHelpers.Report($"perf003.{size >> 20}MiB.peak-live-delta", peak / 1048576.0, "MiB", context);
                PerfHelpers.Report($"perf003.{size >> 20}MiB.max-pending", maxPending, "B", context);
                Assert.True(maxPending <= bufferSize + 2L * chunk, $"pending bytes reached {maxPending} (> buffer {bufferSize} + 2 chunks)");
            }

            Assert.True(peaks[64L << 20] < 32L << 20, $"peak live memory {peaks[64L << 20]} for a 64 MiB stream is not bounded by buffers");
            Assert.True(peaks[64L << 20] <= peaks[16L << 20] + (8L << 20),
                $"peak live memory grew from {peaks[16L << 20]} to {peaks[64L << 20]} when the file grew 4x");
        }, Ct);
    }

    // ============================================================================================================
    // PERF-004 DurableLatencyNotBufferLatency
    // ============================================================================================================

    /// <summary>
    /// With a 10 ms storage I/O delay on every node, OnFlush write acceptance (private buffer, invisible to others) is
    /// measured separately from the FlushAsync durability barrier, WriteThrough writes and the backup barrier. Acceptance is
    /// faster than one storage I/O and publishes nothing; a flush takes at least one storage I/O and leaves the data at the
    /// ack policy's durable copies; every WriteThrough write is durable and visible on return; and a flush never waits for
    /// the (paused) backup host, whose barrier times out separately. Buffer throughput is reported separately from durable
    /// throughput and is never presented as replication throughput.
    /// </summary>
    [Fact, Catalog("PERF-004", "P", "INV-16", "INV-17")]
    public async Task PERF_004_DurableLatencyNotBufferLatency()
    {
        TimeSpan ioDelay = TimeSpan.FromMilliseconds(10);
        await ClusterTest.RunAsync(PerfHelpers.Realtime(backupNodes: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsFileClient observed = observer.GetVolume(Vol).Files;
            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            string context = $"{PerfHelpers.Describe(policy)}; {PerfHelpers.Describe(cluster.Options)}; storage I/O delay {ioDelay.TotalMilliseconds} ms";
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);
            await vol.Directories.CreateDirectoryAsync("/perf004", Ct);
            const string path = "/perf004/file";
            byte[] old = TestData.Bytes(4000, 40 * 1024);
            await vol.Files.WriteAllBytesAsync(path, old, Ct);
            foreach (TestNode node in cluster.Nodes)
                node.Disk.Faults.IoDelay = ioDelay;

            // OnFlush: acceptance into the private buffer.
            var acceptance = new LatencyRecorder();
            byte[] next = TestData.Bytes(4001, 40 * 1024);
            TimeSpan flushLatency;
            await using (DfsStream stream = await vol.Files.OpenAsync(path,
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.OnFlush }, Ct))
            {
                for (int i = 0; i < 10; i++)
                    await acceptance.TimeAsync(async () => await stream.WriteAsync(next.AsMemory(i * 4096, 4096), Ct));
                Assert.True(stream.PendingBytes > 0);
                PerfHelpers.BytesEqual(old, await observed.ReadAllBytesAsync(path, Ct), "accepted-but-unflushed bytes must stay invisible");
                var flushTimer = Stopwatch.StartNew();
                await stream.FlushAsync(Ct);
                flushLatency = flushTimer.Elapsed;
                Assert.Equal(0, stream.PendingBytes);
            }

            PerfHelpers.BytesEqual(next, await observed.ReadAllBytesAsync(path, Ct), "flushed bytes");
            foreach (ObjectId id in await PerfHelpers.ObjectsOfAsync(cluster, path))
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck, $"after FlushAsync {id} has {copies}/{zones}");
            }

            Assert.True(acceptance.Percentile(50) < ioDelay, $"OnFlush acceptance p50 {acceptance.Percentile(50)} waited for storage");
            Assert.True(flushLatency >= ioDelay, $"FlushAsync returned in {flushLatency}, faster than one storage I/O ({ioDelay})");

            // WriteThrough: every write is a durable publication.
            var writeThrough = new LatencyRecorder();
            byte[] third = TestData.Bytes(4002, 40 * 1024);
            await using (DfsStream stream = await vol.Files.OpenAsync(path,
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough }, Ct))
            {
                for (int i = 0; i < 10; i++)
                {
                    await writeThrough.TimeAsync(async () => await stream.WriteAsync(third.AsMemory(i * 4096, 4096), Ct));
                    Assert.Equal(0, stream.PendingBytes);
                }
            }

            PerfHelpers.BytesEqual(third, await observed.ReadAllBytesAsync(path, Ct), "write-through bytes");
            Assert.True(writeThrough.Percentile(50) >= ioDelay, $"WriteThrough p50 {writeThrough.Percentile(50)} is faster than one storage I/O");

            // Backup barrier, measured separately.
            await vol.Snapshots.CreateAsync("perf004-s1", null, Ct);
            var backupTimer = Stopwatch.StartNew();
            await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "perf004-s1", TimeSpan.FromSeconds(60), Ct);
            TimeSpan backupLatency = backupTimer.Elapsed;

            // A flush never waits for the backup host.
            await b0.PauseAsync();
            CommitToken token;
            var independentTimer = Stopwatch.StartNew();
            await using (DfsStream stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                await stream.WriteAsync(TestData.Bytes(4003, 4096), Ct);
                token = await stream.CommitAsync(Ct);
            }

            TimeSpan commitWithBackupPaused = independentTimer.Elapsed;
            Assert.True(commitWithBackupPaused < TimeSpan.FromSeconds(2), $"a live commit took {commitWithBackupPaused} while the backup host was paused");
            await AssertEx.ThrowsDfsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupAsync(b0.TargetName, token, TimeSpan.FromMilliseconds(500), Ct));
            await b0.ResumeAsync();

            double bufferThroughput = 40 * 1024 / acceptance.Total.TotalSeconds / 1048576.0;
            double durableThroughput = 40 * 1024 / (acceptance.Total + flushLatency).TotalSeconds / 1048576.0;
            PerfHelpers.Log(acceptance.Summary("OnFlush acceptance (buffer only, not durable)"));
            PerfHelpers.Log(writeThrough.Summary("WriteThrough (durable per call)"));
            PerfHelpers.Report("perf004.onflush.acceptance-throughput (NOT durable)", bufferThroughput, "MiB/s", context);
            PerfHelpers.Report("perf004.flush.latency (durable)", flushLatency.TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf004.durable-throughput (accept+flush)", durableThroughput, "MiB/s", context);
            PerfHelpers.Report("perf004.backup-barrier.latency", backupLatency.TotalMilliseconds, "ms", context + "; late-sync backup");
            Assert.True(durableThroughput < bufferThroughput, "durable throughput must include the flush and be reported separately from buffer throughput");
        }, Ct);
    }

    // ============================================================================================================
    // PERF-005 ReplicationFactorAndZoneCost
    // ============================================================================================================

    /// <summary>
    /// Four named policies (1/1 ack 1/1, 2/2 ack 1/1, 3/3 ack 2/2, 3/3 ack 3/3) under 5 ms inter-node latency: each policy's
    /// latency and storage cost are reported together with the policy. At acknowledgement each object has at least the
    /// policy's ack copies/zones; after repair exactly its target copies/zones; and stored bytes per useful byte equal the
    /// target copy count (within encoding overhead).
    /// </summary>
    [Fact, Catalog("PERF-005", "P", "INV-16", "INV-17")]
    public async Task PERF_005_ReplicationFactorAndZoneCost()
    {
        var policies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
        {
            ["p1"] = PerfHelpers.Policy(1, 1, 1, 1),
            ["p2"] = PerfHelpers.Policy(2, 2, 1, 1),
            ["p3a2"] = PerfHelpers.Policy(3, 3, 2, 2),
            ["p3a3"] = PerfHelpers.Policy(3, 3, 3, 3),
        };
        TestClusterOptions options = PerfHelpers.Realtime() with { Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = policies }] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TimeSpan interNode = TimeSpan.FromMilliseconds(5);
            foreach (TestNode a in cluster.Nodes)
            {
                foreach (TestNode b in cluster.Nodes.Where(n => n.Name != a.Name))
                    cluster.Network.SetLatency(a.Name, b.Name, interNode);
                cluster.Network.SetLatency("client:c0", a.Name, TimeSpan.FromMilliseconds(1));
                cluster.Network.SetLatency(a.Name, "client:c0", TimeSpan.FromMilliseconds(1));
            }

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/perf005", Ct);
            const int files = 15;
            const int size = 128 * 1024;
            foreach ((string name, DfsStoragePolicy policy) in policies)
            {
                string context = $"{PerfHelpers.Describe(policy)}; {PerfHelpers.Describe(cluster.Options)}; inter-node latency {interNode.TotalMilliseconds} ms";
                var latency = new LatencyRecorder();
                var paths = new List<string>();
                long receivedBefore = await PerfHelpers.TotalReceivedObjectBytesAsync(cluster);
                for (int i = 0; i < files; i++)
                {
                    string path = $"/perf005/{name}-{i}";
                    paths.Add(path);
                    await latency.TimeAsync(() => vol.Files.WriteAllBytesAsync(path, TestData.Bytes((ulong)(5000 + i), size), new DfsMutationOptions { StoragePolicy = name }, Ct));
                    foreach (ObjectId id in await PerfHelpers.ObjectsOfAsync(cluster, path))
                    {
                        (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                        Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck,
                            $"{name}: object {id} acknowledged with {copies}/{zones}");
                    }
                }

                await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
                foreach (string path in paths)
                {
                    foreach (ObjectId id in await PerfHelpers.ObjectsOfAsync(cluster, path))
                        Assert.Equal((policy.TargetCopies, policy.TargetDistinctZones), await cluster.Inspect.CountDurableDomainsAsync(id));
                }

                double amplification = (await PerfHelpers.TotalReceivedObjectBytesAsync(cluster) - receivedBefore) / (double)(files * size);
                PerfHelpers.Log($"{name}: {latency.Summary("write")}");
                PerfHelpers.Report($"perf005.{name}.write.p50", latency.Percentile(50).TotalMilliseconds, "ms", context);
                PerfHelpers.Report($"perf005.{name}.write.p99", latency.Percentile(99).TotalMilliseconds, "ms", context);
                PerfHelpers.Report($"perf005.{name}.stored-per-useful", amplification, "x", context);
                Assert.InRange(amplification, policy.TargetCopies * 0.95, policy.TargetCopies * 1.3);
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-006 HotInodeSingleWriterLimit
    // ============================================================================================================

    /// <summary>
    /// One hot inode written through a single WriteThrough stream is measured separately from eight independent inodes
    /// written concurrently. The single-writer limit is explicit — a second writer on the hot inode is refused with a
    /// sharing violation and the inode never has more than one writer handle or generation — and both throughputs are
    /// reported separately with exact resulting contents.
    /// </summary>
    [Fact, Catalog("PERF-006", "P", "INV-16", "INV-17")]
    public async Task PERF_006_HotInodeSingleWriterLimit()
    {
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient contender = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            string context = $"{PerfHelpers.Describe((await vol.GetInfoAsync(Ct)).DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; WriteThrough 1 KiB";
            await vol.Directories.CreateDirectoryAsync("/perf006", Ct);
            var writeThrough = new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough };

            const int hotWrites = 200;
            var hot = new LatencyRecorder();
            var hotTimer = Stopwatch.StartNew();
            await using (DfsStream stream = await vol.Files.OpenAsync("/perf006/hot", writeThrough, Ct))
            {
                ulong generation = stream.WriterGeneration;
                await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(
                    () => contender.GetVolume(Vol).Files.OpenAsync("/perf006/hot", FileMode.Open, FileAccess.Write, FileShare.Read, Ct));
                for (int i = 0; i < hotWrites; i++)
                {
                    await hot.TimeAsync(async () => await stream.WriteAsync(TestData.Bytes((ulong)i, 1024), Ct));
                    if (i % 50 == 0)
                    {
                        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                        Assert.Single(state.Handles, h => h.Inode == stream.InodeId && h.IsWriter && !h.Expired);
                    }
                }

                Assert.Equal(generation, stream.WriterGeneration);
            }

            double hotOps = hotWrites / hotTimer.Elapsed.TotalSeconds;
            Assert.Equal(hotWrites * 1024L, (await vol.Files.GetInfoAsync("/perf006/hot", Ct)).Length);

            const int writers = 8;
            const int perWriter = 50;
            var clients = new List<DfsClient>();
            for (int w = 0; w < writers; w++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"m{w}" }, Ct));
            try
            {
                var manyTimer = Stopwatch.StartNew();
                await Task.WhenAll(clients.Select(async (c, w) =>
                {
                    await using DfsStream stream = await c.GetVolume(Vol).Files.OpenAsync($"/perf006/m{w}", writeThrough, Ct);
                    for (int i = 0; i < perWriter; i++)
                        await stream.WriteAsync(TestData.Bytes((ulong)(w * 1000 + i), 1024), Ct);
                }));
                double manyOps = writers * perWriter / manyTimer.Elapsed.TotalSeconds;
                for (int w = 0; w < writers; w++)
                    Assert.Equal(perWriter * 1024L, (await vol.Files.GetInfoAsync($"/perf006/m{w}", Ct)).Length);

                PerfHelpers.Log(hot.Summary("hot inode write"));
                PerfHelpers.Report("perf006.single-hot-inode.ops", hotOps, "ops/s", context + "; 1 writer, serialized by the inode writer lease");
                PerfHelpers.Report("perf006.independent-inodes.aggregate-ops", manyOps, "ops/s", context + $"; {writers} writers on {writers} inodes");
                PerfHelpers.Report("perf006.independent-inodes.per-inode-ops", manyOps / writers, "ops/s", context);
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-007 HotVolumeMetadataLimit
    // ============================================================================================================

    /// <summary>
    /// The same metadata workload (directory creation and tiny inline writes from eight clients) runs against one volume and
    /// then spread over four volumes. Each volume has its own consensus group, leadership is spread over at least two
    /// nodes, per-volume throughput is reported explicitly (the initial per-volume bottleneck), and spreading the load does
    /// not reduce aggregate throughput below half of the single-volume figure.
    /// </summary>
    [Fact, Catalog("PERF-007", "P", "INV-16", "INV-17")]
    public async Task PERF_007_HotVolumeMetadataLimit()
    {
        string[] volumes = ["v0", "v1", "v2", "v3"];
        TestClusterOptions options = PerfHelpers.Realtime() with { Volumes = volumes.Select(v => new TestVolumeSpec(v)).ToArray() };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            string context = $"{PerfHelpers.Describe(cluster.Options)}; metadata ops (CreateDirectory + 32 B inline WriteAll)";
            const int clientsCount = 8;
            const int opsPerClient = 40;
            var clients = new List<DfsClient>();
            for (int i = 0; i < clientsCount; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"c{i}" }, Ct));
            try
            {
                var leaders = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (string v in volumes)
                    leaders[v] = (await cluster.GetLeaderAsync(v, Ct)).Name;
                PerfHelpers.Log($"leaders: {string.Join(", ", leaders.Select(kv => $"{kv.Key}@{kv.Value}"))}");
                Assert.True(leaders.Values.Distinct(StringComparer.Ordinal).Count() >= 2, "volume leadership must be spread over the cluster");
                foreach (string v in volumes)
                    Assert.Contains(v, (await cluster.Admin.GetVolumeStatusAsync(v, Ct)).Group.Group, StringComparison.Ordinal);

                double single = await RunAsync("single", i => "v0");
                double spread = await RunAsync("spread", i => volumes[i % volumes.Length]);
                PerfHelpers.Report("perf007.one-volume.aggregate", single, "ops/s", context + "; all clients on v0 (one consensus group)");
                PerfHelpers.Report("perf007.four-volumes.aggregate", spread, "ops/s", context + "; clients spread over v0..v3");
                PerfHelpers.Report("perf007.four-volumes.per-volume", spread / volumes.Length, "ops/s", context);
                Assert.True(spread >= single * 0.5, $"spreading over independent volumes collapsed throughput ({single:0} -> {spread:0} ops/s)");
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }

            async Task<double> RunAsync(string phase, Func<int, string> volumeOf)
            {
                var timer = Stopwatch.StartNew();
                await Task.WhenAll(clients.Select(async (c, i) =>
                {
                    DfsVolume vol = c.GetVolume(volumeOf(i));
                    for (int n = 0; n < opsPerClient; n++)
                    {
                        if (n % 2 == 0)
                            await vol.Directories.CreateDirectoryAsync($"/{phase}/c{i}/d{n}", Ct);
                        else
                            await vol.Files.WriteAllBytesAsync($"/{phase}/c{i}/d{n - 1}/f", TestData.Bytes((ulong)(i * 100 + n), 32), Ct);
                    }
                }));
                return clientsCount * opsPerClient / timer.Elapsed.TotalSeconds;
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-008 LargeDirectoryEnumeration
    // ============================================================================================================

    /// <summary>
    /// A 2000-entry directory is enumerated through pinned pagination with page sizes 100 and 1000: exactly every entry is
    /// returned once, the number of list RPCs is ceil(N / page) (+1), pages never exceed the page size and share one pinned
    /// version, the enumeration pin exists while enumerating and is released afterwards, and live memory stays bounded.
    /// </summary>
    [Fact, Catalog("PERF-008", "P", "INV-16", "INV-17")]
    public async Task PERF_008_LargeDirectoryEnumeration()
    {
        const int entries = 2000;
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            const string dir = "/perf008/big";
            var writers = new List<DfsClient>();
            for (int w = 0; w < 8; w++)
                writers.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"w{w}" }, Ct));
            await writers[0].GetVolume(Vol).Directories.CreateDirectoryAsync(dir, Ct);
            await Task.WhenAll(writers.Select(async (c, w) =>
            {
                for (int i = w; i < entries; i += writers.Count)
                    await c.GetVolume(Vol).Files.WriteAllBytesAsync($"{dir}/e{i:D5}", Array.Empty<byte>(), Ct);
            }));
            foreach (DfsClient c in writers)
                await c.DisposeAsync();

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var expected = Enumerable.Range(0, entries).Select(i => $"e{i:D5}").ToHashSet(StringComparer.Ordinal);
            string context = $"{PerfHelpers.Describe(cluster.Options)}; {entries} entries";

            foreach (int pageSize in new[] { 100, 1000 })
            {
                cluster.Network.Log.Reset();
                long baseline = PerfHelpers.LiveBytes();
                long peak = 0;
                var names = new List<string>(entries);
                bool pinSeen = false;
                var timer = Stopwatch.StartNew();
                await foreach (DfsEntryInfo entry in vol.Directories.EnumerateAsync(dir, new DfsEnumerationOptions { PageSize = pageSize }, Ct))
                {
                    names.Add(entry.Name);
                    if (names.Count == 1)
                        pinSeen = (await cluster.Inspect.GetVolumeStateAsync(Vol)).Pins.Any(p => p.Kind == "Enumeration");
                    if (names.Count % 500 == 0)
                        peak = Math.Max(peak, PerfHelpers.LiveBytes() - baseline);
                }

                TimeSpan elapsed = timer.Elapsed;
                long rpcs = cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c0", Type: MessageType.ListDirectory, Direction: LinkDirection.Request));
                long bytes = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.ListDirectoryResult));
                int expectedRpcs = (entries + pageSize - 1) / pageSize;
                Assert.Equal(entries, names.Count);
                Assert.True(expected.SetEquals(names), "enumeration must return exactly the directory's entries");
                Assert.True(pinSeen, "the enumeration must hold a pinned namespace version");
                Assert.InRange(rpcs, expectedRpcs, expectedRpcs + 1);
                Assert.True(peak <= 8L << 20, $"enumeration retained {peak} bytes of live memory");
                PerfHelpers.Report($"perf008.page{pageSize}.entries-per-second", entries / elapsed.TotalSeconds, "entries/s", context);
                PerfHelpers.Report($"perf008.page{pageSize}.rpcs", rpcs, "requests", context);
                PerfHelpers.Report($"perf008.page{pageSize}.bytes-per-entry", (double)bytes / entries, "B", context);

                TimeSpan pinBound = cluster.Options.Timings.EnumerationPinDuration + TimeSpan.FromSeconds(5);
                await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetVolumeStateAsync(Vol)).Pins.Any(p => p.Kind == "Enumeration"),
                    pinBound, "the enumeration pin must be released after the enumeration completes", Ct);
            }

            string? token = null;
            CommitToken? pinnedAt = null;
            int total = 0;
            do
            {
                DfsDirectoryPage page = await vol.Directories.ListPageAsync(dir, new DfsEnumerationOptions { PageSize = 300 }, token, Ct);
                Assert.True(page.Entries.Count <= 300);
                pinnedAt ??= page.PinnedAt;
                Assert.Equal(pinnedAt, page.PinnedAt);
                total += page.Entries.Count;
                token = page.ContinuationToken;
            }
            while (token is not null);
            Assert.Equal(entries, total);
        }, Ct);
    }

    // ============================================================================================================
    // PERF-009 FragmentedFileReadAmplification
    // ============================================================================================================

    /// <summary>
    /// 128 one-byte WriteThrough writes fragment a 1 MiB file. The extent map stays valid (sorted, non-overlapping, covering
    /// the file) and bounded by the number of writes; reading the fragmented file moves at most 2x its size in object data
    /// with a bounded number of metadata lookups; rewriting it coalesces the map back to ceil(size/chunk) extents with
    /// near-1x read amplification. Contents always equal the reference model.
    /// </summary>
    [Fact, Catalog("PERF-009", "P", "INV-16", "INV-17")]
    public async Task PERF_009_FragmentedFileReadAmplification()
    {
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            int chunk = cluster.Options.Timings.ChunkSize;
            string context = $"{PerfHelpers.Describe((await vol.GetInfoAsync(Ct)).DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; chunk {chunk} B";
            const int size = 1 << 20;
            const int writes = 128;
            const string path = "/perf009/fragmented";
            await vol.Directories.CreateDirectoryAsync("/perf009", Ct);
            byte[] initial = TestData.Bytes(9000, size);
            await vol.Files.WriteAllBytesAsync(path, initial, Ct);
            var model = new ReferenceFileModel(initial);
            var random = new SeededRandom(9001);
            var offsets = new HashSet<long>();
            await using (DfsStream stream = await vol.Files.OpenAsync(path,
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough }, Ct))
            {
                while (offsets.Count < writes)
                {
                    long offset = random.Next(size);
                    if (!offsets.Add(offset))
                        continue;
                    byte[] one = [(byte)(offsets.Count & 0xFF)];
                    stream.Position = offset;
                    await stream.WriteAsync(one, Ct);
                    model.Write(offset, one);
                    model.Flush();
                }
            }

            int baseExtents = (size + chunk - 1) / chunk;
            FileInspection fragmented = (await cluster.Inspect.GetFileAsync(Vol, path))!;
            AssertValidMap(fragmented, size);
            Assert.InRange(fragmented.Extents.Count, baseExtents + 1, baseExtents + 2 * writes + 1);

            (double fragmentedAmp, long fragmentedLookups, TimeSpan fragmentedTime) = await MeasureReadAsync();
            Assert.True(fragmentedAmp <= 2.0, $"reading the fragmented file moved {fragmentedAmp:0.00}x its size");
            Assert.True(fragmentedLookups <= 16, $"reading the fragmented file needed {fragmentedLookups} metadata lookups");

            await vol.Files.WriteAllBytesAsync(path, model.Committed, Ct);
            FileInspection coalesced = (await cluster.Inspect.GetFileAsync(Vol, path))!;
            AssertValidMap(coalesced, size);
            Assert.Equal(baseExtents, coalesced.Extents.Count);
            (double coalescedAmp, long coalescedLookups, TimeSpan coalescedTime) = await MeasureReadAsync();
            Assert.True(coalescedAmp <= 1.2, $"reading the coalesced file moved {coalescedAmp:0.00}x its size");

            PerfHelpers.Report("perf009.fragmented.extents", fragmented.Extents.Count, "extents", context);
            PerfHelpers.Report("perf009.fragmented.read-amplification", fragmentedAmp, "x", context);
            PerfHelpers.Report("perf009.fragmented.metadata-lookups", fragmentedLookups, "requests", context);
            PerfHelpers.Report("perf009.fragmented.read-latency", fragmentedTime.TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf009.coalesced.extents", coalesced.Extents.Count, "extents", context);
            PerfHelpers.Report("perf009.coalesced.read-amplification", coalescedAmp, "x", context);
            PerfHelpers.Report("perf009.coalesced.metadata-lookups", coalescedLookups, "requests", context);
            PerfHelpers.Report("perf009.coalesced.read-latency", coalescedTime.TotalMilliseconds, "ms", context);

            async Task<(double Amplification, long Lookups, TimeSpan Time)> MeasureReadAsync()
            {
                cluster.Network.Log.Reset();
                var timer = Stopwatch.StartNew();
                byte[] read = await vol.Files.ReadAllBytesAsync(path, Ct);
                TimeSpan time = timer.Elapsed;
                PerfHelpers.BytesEqual(model.Committed, read, "file contents");
                long moved = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.GetObjectResult));
                long lookups = cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c0", Type: MessageType.ReadRange, Direction: LinkDirection.Request));
                return ((double)moved / size, lookups, time);
            }
        }, Ct);

        static void AssertValidMap(FileInspection file, long length)
        {
            Assert.Equal(length, file.Length);
            long cursor = 0;
            foreach (ExtentInspection extent in file.Extents.OrderBy(e => e.Offset))
            {
                Assert.Equal(cursor, extent.Offset);
                Assert.True(extent.Length > 0);
                Assert.True(extent.ObjectOffset >= 0 && extent.ObjectLength >= extent.Length);
                cursor = extent.Offset + extent.Length;
            }

            Assert.Equal(length, cursor);
        }
    }

    // ============================================================================================================
    // PERF-010 RepairUnderForegroundLoad
    // ============================================================================================================

    /// <summary>
    /// A node of a four-zone cluster (target 3 copies in 3 zones) is lost under foreground load. Recovery time, repair
    /// bytes, foreground tail latency and capacity usage are measured together: repair copies about what the lost node held
    /// (at least 90 %, at most 1.5x + 1 MiB), finishes within 2 × bytes / budget + 10 s, foreground p99 stays within
    /// max(1 s, 5 × baseline p99), and surviving nodes stay outside their capacity reserve.
    /// </summary>
    [Fact, Catalog("PERF-010", "P", "INV-16", "INV-17")]
    public async Task PERF_010_RepairUnderForegroundLoad()
    {
        TestClusterOptions options = PerfHelpers.Realtime() with
        {
            NodeCount = 4,
            ControlVoters = 3,
            DefaultPolicy = PerfHelpers.Policy(3, 3, 2, 2),
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume vol = c0.GetVolume(Vol);
            string context = $"{PerfHelpers.Describe(cluster.Options.GetEffectiveDefaultPolicy())}; {PerfHelpers.Describe(cluster.Options)}; repair budget {cluster.Options.Timings.RepairBytesPerSecond} B/s";
            await vol.Directories.CreateDirectoryAsync("/perf010/seed", Ct);
            await vol.Directories.CreateDirectoryAsync("/perf010/fg", Ct);
            var seeded = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 64; i++)
            {
                seeded[$"/perf010/seed/f{i}"] = TestData.Bytes(10_000 + (ulong)i, 128 * 1024);
                await vol.Files.WriteAllBytesAsync($"/perf010/seed/f{i}", seeded[$"/perf010/seed/f{i}"], Ct);
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);

            var baseline = new LatencyRecorder();
            await RunForegroundAsync(baseline, TimeSpan.FromSeconds(2));

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode victim = cluster.Nodes.First(n => n.Name != leader.Name);
            long held = (await victim.Inspect.ListObjectsAsync(Vol)).Sum(o => o.Length);
            long repairBefore = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes;
            var during = new LatencyRecorder();
            using var stop = new CancellationTokenSource();
            Task foreground = RunForegroundAsync(during, TimeSpan.FromMinutes(1), stop.Token);
            var recovery = Stopwatch.StartNew();
            await victim.CrashAsync();
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            TimeSpan recoveryTime = recovery.Elapsed;
            await stop.CancelAsync();
            await foreground;
            long repaired = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes - repairBefore;

            PerfHelpers.Log(baseline.Summary("foreground before loss"));
            PerfHelpers.Log(during.Summary("foreground during repair"));
            PerfHelpers.Report("perf010.recovery-time", recoveryTime.TotalSeconds, "s", context);
            PerfHelpers.Report("perf010.repair-bytes", repaired, "B", context + $"; lost node held {held} B");
            PerfHelpers.Report("perf010.repair-bandwidth", repaired / Math.Max(recoveryTime.TotalSeconds, 0.001), "B/s", context);
            PerfHelpers.Report("perf010.foreground.p99.during", during.Percentile(99).TotalMilliseconds, "ms", context);
            Assert.InRange(repaired, (long)(held * 0.9), (long)(held * 1.5) + (1 << 20));
            Assert.True(recoveryTime <= TimeSpan.FromSeconds(2.0 * held / cluster.Options.Timings.RepairBytesPerSecond + 10), $"repair took {recoveryTime}");
            TimeSpan bound = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, baseline.Percentile(99).Ticks * 5));
            Assert.True(during.Percentile(99) <= bound, $"foreground p99 {during.Percentile(99)} exceeded {bound} during repair");
            foreach (TestNode node in cluster.Nodes.Where(n => n.Name != victim.Name))
                Assert.True(node.Disk.UsedBytes <= cluster.Options.NodeCapacityBytes - cluster.Options.ReservedBytes, $"{node.Name} entered its reserve");
            foreach ((string path, byte[] content) in seeded)
                PerfHelpers.BytesEqual(content, await vol.Files.ReadAllBytesAsync(path, Ct), path);

            async Task RunForegroundAsync(LatencyRecorder recorder, TimeSpan duration, CancellationToken stopToken = default)
            {
                var timer = Stopwatch.StartNew();
                int n = 0;
                while (timer.Elapsed < duration && !stopToken.IsCancellationRequested)
                {
                    DfsClient c = n % 2 == 0 ? c0 : c1;
                    string key = $"/perf010/fg/k{n % 4}";
                    await recorder.TimeAsync(() => c.GetVolume(Vol).Files.WriteAllBytesAsync(key, TestData.Bytes((ulong)n, 16 * 1024), Ct));
                    await recorder.TimeAsync(() => c.GetVolume(Vol).Files.ReadAllBytesAsync(key, Ct));
                    n++;
                }
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-011 BackupCatchupAndReseedCost
    // ============================================================================================================

    /// <summary>
    /// The initial full sync, an incremental catch-up after 256 KiB of changes, and a full reseed after the target outlived
    /// its pin budget are measured independently. The incremental sync sends about the changed bytes (not the data set),
    /// the reseed sends at least the live data set, the source's transfer pins never exceed their byte budget and are
    /// released when the lineage is abandoned, and live writes keep a bounded tail latency throughout.
    /// </summary>
    [Fact, Catalog("PERF-011", "P", "INV-16", "INV-17")]
    public async Task PERF_011_BackupCatchupAndReseedCost()
    {
        const long pinBudget = 64L << 20;
        await ClusterTest.RunAsync(PerfHelpers.Realtime(backupNodes: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            TestBackupNode b0 = cluster.BackupNodes[0];
            string target = b0.TargetName;
            string context = $"{PerfHelpers.Describe((await vol.GetInfoAsync(Ct)).DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; 1 late-sync target";
            await b0.RegisterAsync(Vol, budgets: new DfsBackupBudgets(TimeSpan.FromSeconds(3), pinBudget));
            await vol.Directories.CreateDirectoryAsync("/perf011", Ct);
            const int files = 32;
            const int size = 128 * 1024;
            long liveBytes = (long)files * size;
            for (int i = 0; i < files; i++)
                await vol.Files.WriteAllBytesAsync($"/perf011/f{i}", TestData.Bytes(11_000 + (ulong)i, size), Ct);

            var foreground = new LatencyRecorder();
            async Task<(long Bytes, TimeSpan Time)> SyncAsync(string snapshot)
            {
                long sentBefore = (await cluster.Inspect.GetTotalCountersAsync()).BackupBytesSent;
                var timer = Stopwatch.StartNew();
                await vol.Snapshots.CreateAsync(snapshot, null, Ct);
                await vol.Backups.RequestSyncAsync(target, snapshot, Ct);
                Task<DfsBackupCheckpoint> wait = vol.Backups.AwaitBackupSnapshotAsync(target, snapshot, TimeSpan.FromSeconds(60), Ct);
                int n = 0;
                while (!wait.IsCompleted)
                    await foreground.TimeAsync(() => vol.Files.WriteAllBytesAsync($"/perf011/live{n++ % 4}", TestData.Bytes((ulong)n, 4096), Ct));
                await wait;
                return ((await cluster.Inspect.GetTotalCountersAsync()).BackupBytesSent - sentBefore, timer.Elapsed);
            }

            (long fullBytes, TimeSpan fullTime) = await SyncAsync("perf011-s0");
            Assert.True(fullBytes >= liveBytes * 0.9, $"the initial sync sent {fullBytes} bytes for {liveBytes} bytes of data");

            for (int i = 0; i < 2; i++)
                await vol.Files.WriteAllBytesAsync($"/perf011/f{i}", TestData.Bytes(11_100 + (ulong)i, size), Ct);
            long changed = 2L * size;
            (long incrementalBytes, TimeSpan incrementalTime) = await SyncAsync("perf011-s1");
            Assert.InRange(incrementalBytes, (long)(changed * 0.9), (long)(changed * 1.5) + (256 << 10));

            Guid lineageBefore = (await vol.Backups.GetStatusAsync(target, Ct)).LineageId;
            await b0.StopAsync();
            await vol.Files.WriteAllBytesAsync("/perf011/f2", TestData.Bytes(11_200, size), Ct);
            await vol.Snapshots.CreateAsync("perf011-s2", null, Ct);
            await vol.Backups.RequestSyncAsync(target, "perf011-s2", Ct);
            var watch = Stopwatch.StartNew();
            bool reseedRequired = false;
            while (watch.Elapsed < TimeSpan.FromSeconds(15) && !reseedRequired)
            {
                DfsBackupStatus status = await vol.Backups.GetStatusAsync(target, Ct);
                Assert.True(status.PinnedSourceBytes <= pinBudget, $"transfer pins hold {status.PinnedSourceBytes} bytes, over the {pinBudget} budget");
                reseedRequired = status.ReseedRequired;
                await foreground.TimeAsync(() => vol.Files.WriteAllBytesAsync("/perf011/while-offline", TestData.Bytes(11_300, 4096), Ct));
                await Task.Delay(200, Ct);
            }

            Assert.True(reseedRequired, "the target must be marked for reseed once it outlives its 3 s pin budget");
            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(target, Ct)).PinnedSourceBytes == 0,
                TimeSpan.FromSeconds(10), "abandoning the lineage must release the source's transfer pins", Ct);

            await b0.RestartAsync();
            (long reseedBytes, TimeSpan reseedTime) = await SyncAsync("perf011-s3");
            Assert.True(reseedBytes >= liveBytes * 0.9, $"a reseed sent only {reseedBytes} bytes for {liveBytes} bytes of live data");
            Assert.NotEqual(lineageBefore, (await vol.Backups.GetStatusAsync(target, Ct)).LineageId);

            PerfHelpers.Report("perf011.full.bytes", fullBytes, "B", context);
            PerfHelpers.Report("perf011.full.seconds", fullTime.TotalSeconds, "s", context);
            PerfHelpers.Report("perf011.incremental.bytes", incrementalBytes, "B", context + $"; {changed} B changed");
            PerfHelpers.Report("perf011.incremental.seconds", incrementalTime.TotalSeconds, "s", context);
            PerfHelpers.Report("perf011.reseed.bytes", reseedBytes, "B", context);
            PerfHelpers.Report("perf011.reseed.seconds", reseedTime.TotalSeconds, "s", context);
            PerfHelpers.Log(foreground.Summary("live writes during backup activity"));
            Assert.True(foreground.Percentile(99) < TimeSpan.FromSeconds(1), $"live write p99 {foreground.Percentile(99)} during backup work");
        }, Ct);
    }

    // ============================================================================================================
    // PERF-012 SnapshotCreationAndRetentionCost
    // ============================================================================================================

    /// <summary>
    /// Snapshot capture latency is measured with 10 and with 1010 files and is independent of namespace size (O(1) capture,
    /// no payload copied: object bytes received during capture are zero); backup transfer is measured separately. A retained
    /// snapshot keeps an overwritten 1 MiB file's old objects (space growth ≈ size × copies, reported), and deleting the
    /// snapshot lets GC reclaim exactly those objects.
    /// </summary>
    [Fact, Catalog("PERF-012", "P", "INV-16", "INV-17")]
    public async Task PERF_012_SnapshotCreationAndRetentionCost()
    {
        await ClusterTest.RunAsync(PerfHelpers.Realtime(backupNodes: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            string context = $"{PerfHelpers.Describe(policy)}; {PerfHelpers.Describe(cluster.Options)}";
            await vol.Directories.CreateDirectoryAsync("/perf012/small", Ct);
            for (int i = 0; i < 10; i++)
                await vol.Files.WriteAllBytesAsync($"/perf012/small/f{i}", TestData.Bytes((ulong)i, 64), Ct);
            LatencyRecorder small = await CaptureAsync("small");

            await vol.Directories.CreateDirectoryAsync("/perf012/large", Ct);
            for (int i = 0; i < 1000; i++)
                await vol.Files.WriteAllBytesAsync($"/perf012/large/f{i}", TestData.Bytes((ulong)(100 + i), 64), Ct);
            LatencyRecorder large = await CaptureAsync("large");
            Assert.True(large.Percentile(50) <= small.Percentile(50) * 10 + TimeSpan.FromMilliseconds(50),
                $"capture with 1010 files ({large.Percentile(50)}) is not independent of namespace size (10 files: {small.Percentile(50)})");

            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);
            var transfer = Stopwatch.StartNew();
            await vol.Backups.RequestSyncAsync(b0.TargetName, "perf012-large-4", Ct);
            await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "perf012-large-4", TimeSpan.FromSeconds(60), Ct);
            TimeSpan transferTime = transfer.Elapsed;

            const string big = "/perf012/big";
            await vol.Files.WriteAllBytesAsync(big, TestData.Bytes(12_000, 1 << 20), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            IReadOnlyList<ObjectId> oldObjects = await PerfHelpers.ObjectsOfAsync(cluster, big);
            await vol.Snapshots.CreateAsync("perf012-keep", null, Ct);
            await vol.Files.WriteAllBytesAsync(big, TestData.Bytes(12_001, 1 << 20), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            await Task.Delay(cluster.Options.Timings.GcInterval * 8, Ct);

            long retained = 0;
            foreach (ObjectId id in oldObjects)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.True(holders.Count >= policy.TargetCopies, $"snapshot-retained object {id} has only {holders.Count} copies");
                FileInspection? current = await cluster.Inspect.GetFileAsync(Vol, big);
                Assert.DoesNotContain(current!.Extents, e => e.Object == id);
                retained += (await cluster.Node(holders[0]).Inspect.ListObjectsAsync(Vol)).Single(o => o.Id == id).Length * holders.Count;
            }

            Assert.InRange(retained, (long)((1 << 20) * policy.TargetCopies * 0.9), (long)((1 << 20) * policy.TargetCopies * 1.5));

            await vol.Snapshots.DeleteAsync("perf012-keep", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                return !oldObjects.Any(stored.Contains);
            }, TimeSpan.FromSeconds(30), "deleting the only snapshot referencing the old revision must let GC reclaim it", Ct);

            PerfHelpers.Log(small.Summary("capture, 10 files"));
            PerfHelpers.Log(large.Summary("capture, 1010 files"));
            PerfHelpers.Report("perf012.capture.p50.10-files", small.Percentile(50).TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf012.capture.p50.1010-files", large.Percentile(50).TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf012.backup-transfer", transferTime.TotalMilliseconds, "ms", context + "; measured separately from capture");
            PerfHelpers.Report("perf012.retained-overwritten-bytes", retained, "B", context + "; 1 MiB overwritten while a snapshot is retained");

            async Task<LatencyRecorder> CaptureAsync(string label)
            {
                var recorder = new LatencyRecorder();
                for (int i = 0; i < 5; i++)
                {
                    long before = await PerfHelpers.TotalReceivedObjectBytesAsync(cluster);
                    await recorder.TimeAsync(() => vol.Snapshots.CreateAsync($"perf012-{label}-{i}", null, Ct));
                    Assert.Equal(before, await PerfHelpers.TotalReceivedObjectBytesAsync(cluster));
                }

                return recorder;
            }
        }, Ct);
    }

    // ============================================================================================================
    // PERF-013 CancellationAndFailureLeakSoak
    // ============================================================================================================

    /// <summary>
    /// Repeats cancelled reads and writes, unknown-outcome writes, connection resets and failing stream disposals. After
    /// safe cleanup (lease expiry), no handles, open staging sessions or pins remain, and live memory and thread count at
    /// the end of the soak stay within 16 MiB / 8 threads of the early sample; cancellations surface only as cancellation
    /// or unknown outcome, and the last committed content is intact.
    /// </summary>
    [Fact, Catalog("PERF-013", "P", "INV-16", "INV-17")]
    public async Task PERF_013_CancellationAndFailureLeakSoak()
    {
        const int iterations = 32;
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            await using DfsClient unknownClient = await cluster.ConnectAsync(new TestClientOptions { Name = "u0", OperationTimeout = TimeSpan.FromSeconds(1) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            string context = PerfHelpers.Describe(cluster.Options);
            await vol.Directories.CreateDirectoryAsync("/perf013", Ct);
            byte[] last = TestData.Bytes(13_000, 32 * 1024);
            await vol.Files.WriteAllBytesAsync("/perf013/file", last, Ct);
            var samples = new List<(int Iteration, long Live, int Threads)>();

            for (int i = 1; i <= iterations; i++)
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1)))
                {
                    Exception? read = await Record.ExceptionAsync(() => vol.Files.ReadAllBytesAsync("/perf013/file", cts.Token));
                    Assert.True(read is null or OperationCanceledException, $"a cancelled read surfaced as {read?.GetType().Name}");
                }

                byte[] candidate = TestData.Bytes(13_000 + (ulong)i, 32 * 1024);
                using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1)))
                {
                    Exception? write = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync("/perf013/file", candidate, cts.Token));
                    Assert.True(write is null or OperationCanceledException or DfsCommitOutcomeUnknownException,
                        $"a cancelled write surfaced as {write?.GetType().Name}");
                    if (write is null)
                        last = candidate;
                }

                if (i % 4 == 0)
                {
                    MessageRule drop = cluster.Network.DropReplies(new MessageFilter(From: "client:u0"), count: 100_000);
                    Exception? unknown = await Record.ExceptionAsync(() => unknownClient.GetVolume(Vol).Files.WriteAllBytesAsync($"/perf013/u{i}", TestData.Bytes((ulong)i, 2000), Ct));
                    drop.Cancel();
                    Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(unknown);
                }

                cluster.Network.ResetConnections("client:c0");

                if (i % 4 == 2)
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    DfsStream stream = await vol.Files.OpenAsync($"/perf013/d{i}", FileMode.Create, FileAccess.Write, FileShare.Read, Ct);
                    await stream.WriteAsync(TestData.Bytes((ulong)i, 1024), Ct);
                    cluster.Network.CutBoth("client:c0", leader.Name);
                    Exception? dispose = await Record.ExceptionAsync(async () => await stream.DisposeAsync());
                    cluster.Network.Restore("client:c0", leader.Name);
                    cluster.Network.Restore(leader.Name, "client:c0");
                    Assert.NotNull(dispose);
                }

                if (i == 8 || i == iterations)
                {
                    await Task.Delay(cluster.Options.Timings.LeaseDuration * 2 + TimeSpan.FromSeconds(1), Ct);
                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                    Assert.DoesNotContain(state.Handles, h => !h.Expired);
                    Assert.DoesNotContain(state.Staging, s => s.Open);
                    Assert.Empty(state.Pins);
                    samples.Add((i, PerfHelpers.LiveBytes(), Process.GetCurrentProcess().Threads.Count));
                }
            }

            (int _, long earlyLive, int earlyThreads) = samples[0];
            (int _, long lateLive, int lateThreads) = samples[^1];
            PerfHelpers.Report("perf013.live-memory.early", earlyLive / 1048576.0, "MiB", context);
            PerfHelpers.Report("perf013.live-memory.late", lateLive / 1048576.0, "MiB", context);
            PerfHelpers.Report("perf013.threads.early", earlyThreads, "threads", context);
            PerfHelpers.Report("perf013.threads.late", lateThreads, "threads", context);
            Assert.True(lateLive - earlyLive <= 16L << 20, $"live memory grew by {lateLive - earlyLive} bytes over the soak");
            Assert.True(lateThreads - earlyThreads <= 8, $"thread count grew from {earlyThreads} to {lateThreads}");

            byte[] final = await vol.Files.ReadAllBytesAsync("/perf013/file", Ct);
            Assert.True(final.Length == 32 * 1024, "the file must hold one complete committed revision");
        }, Ct);
    }

    // ============================================================================================================
    // PERF-014 BackpressureAtBandwidthLimit
    // ============================================================================================================

    /// <summary>
    /// With every client link throttled to 2 MiB/s, a 6 MiB streamed write and a 4 MiB read into a slow consumer keep client
    /// memory bounded (pending bytes ≤ buffer + 2 chunks, live memory delta ≤ 16 MiB) and never report throughput above the
    /// physical link capacity — backpressure instead of unbounded buffering or premature acknowledgement.
    /// </summary>
    [Fact, Catalog("PERF-014", "P", "INV-16", "INV-17")]
    public async Task PERF_014_BackpressureAtBandwidthLimit()
    {
        const long linkBytesPerSecond = 2L << 20;
        await ClusterTest.RunAsync(PerfHelpers.Realtime(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(30) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/perf014", Ct);
            int chunk = cluster.Options.Timings.ChunkSize;
            int bufferSize = cluster.CreateClientOptions(new TestClientOptions { Name = "c0" }).DefaultBufferSize;
            foreach (TestNode node in cluster.Nodes)
            {
                cluster.Network.SetBandwidth("client:c0", node.Name, linkBytesPerSecond);
                cluster.Network.SetBandwidth(node.Name, "client:c0", linkBytesPerSecond);
            }

            string context = $"{PerfHelpers.Describe((await vol.GetInfoAsync(Ct)).DefaultPolicy)}; {PerfHelpers.Describe(cluster.Options)}; client links {linkBytesPerSecond} B/s";
            const long writeSize = 6L << 20;
            const int block = 64 * 1024;
            long baseline = PerfHelpers.LiveBytes();
            long peak = 0;
            long maxPending = 0;
            var timer = Stopwatch.StartNew();
            await using (DfsStream writer = await vol.Files.CreateAsync("/perf014/slow-write", Ct))
            {
                for (long b = 0; b < writeSize / block; b++)
                {
                    await writer.WriteAsync(PerfHelpers.Block(14, b, block), Ct);
                    maxPending = Math.Max(maxPending, writer.PendingBytes);
                    if ((b + 1) % 16 == 0)
                        peak = Math.Max(peak, PerfHelpers.LiveBytes() - baseline);
                }

                await writer.FlushAsync(Ct);
            }

            TimeSpan writeTime = timer.Elapsed;
            double writeThroughput = writeSize / writeTime.TotalSeconds;
            Assert.True(maxPending <= bufferSize + 2L * chunk, $"pending bytes reached {maxPending}");
            Assert.True(peak <= 16L << 20, $"live memory grew by {peak} bytes behind a throttled link");
            Assert.True(writeThroughput <= cluster.Nodes.Count * linkBytesPerSecond * 1.2,
                $"write throughput {writeThroughput:0} B/s exceeds the physical link capacity — data acknowledged before it moved");

            timer.Restart();
            long readBaseline = PerfHelpers.LiveBytes();
            long readPeak = 0;
            var sink = new VerifyingSink(14, block, TimeSpan.FromMilliseconds(1));
            await using (DfsStream reader = await vol.Files.OpenReadAsync("/perf014/slow-write", Ct))
            {
                var buffer = new byte[block];
                long nextCheck = 1L << 20;
                int n;
                while (sink.Received < 4L << 20 && (n = await reader.ReadAsync(buffer, Ct)) > 0)
                {
                    await sink.WriteAsync(buffer.AsMemory(0, n), Ct);
                    if (sink.Received >= nextCheck)
                    {
                        readPeak = Math.Max(readPeak, PerfHelpers.LiveBytes() - readBaseline);
                        nextCheck += 1L << 20;
                    }
                }
            }

            TimeSpan readTime = timer.Elapsed;
            Assert.True(readPeak <= 16L << 20, $"read-ahead grew live memory by {readPeak} bytes behind a slow consumer");
            PerfHelpers.Report("perf014.write.throughput", writeThroughput / 1048576.0, "MiB/s", context);
            PerfHelpers.Report("perf014.write.max-pending", maxPending, "B", context);
            PerfHelpers.Report("perf014.write.peak-live-delta", peak / 1048576.0, "MiB", context);
            PerfHelpers.Report("perf014.read.throughput", sink.Received / readTime.TotalSeconds / 1048576.0, "MiB/s", context + "; slow consumer");
            PerfHelpers.Report("perf014.read.peak-live-delta", readPeak / 1048576.0, "MiB", context);
        }, Ct);
    }

    // ============================================================================================================
    // PERF-015 CapacityNearReserveBoundary
    // ============================================================================================================

    /// <summary>
    /// 512 KiB writes run up to the admission threshold of 64 MiB nodes with an 8 MiB reserve. Latency near the boundary
    /// stays controlled (p99 of the last 10 admitted writes ≤ 5 × early p50 + 250 ms), rejections are fast explicit
    /// capacity errors, rejected files never appear, and every admitted file is byte-exact and durably stored at the ack
    /// policy — no false durability near full.
    /// </summary>
    [Fact, Catalog("PERF-015", "P", "INV-16", "INV-17")]
    public async Task PERF_015_CapacityNearReserveBoundary()
    {
        TestClusterOptions options = PerfHelpers.Realtime() with { NodeCapacityBytes = 64L << 20, ReservedBytes = 8L << 20 };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            string context = $"{PerfHelpers.Describe(policy)}; {PerfHelpers.Describe(cluster.Options)}; capacity 64 MiB, reserve 8 MiB per node";
            await vol.Directories.CreateDirectoryAsync("/perf015", Ct);
            var admitted = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            var latencies = new List<TimeSpan>();
            int i = 0;
            DfsUnavailableException? first = null;
            for (; i < 200 && first is null; i++)
            {
                string path = $"/perf015/f{i}";
                byte[] content = TestData.Bytes(15_000 + (ulong)i, 512 * 1024);
                var timer = Stopwatch.StartNew();
                try
                {
                    await vol.Files.WriteAllBytesAsync(path, content, Ct);
                    latencies.Add(timer.Elapsed);
                    admitted[path] = content;
                }
                catch (DfsUnavailableException ex) when (ex is DfsCapacityException or DfsPlacementUnavailableException)
                {
                    first = ex;
                    Assert.True(ex.IsRetryable);
                    Assert.False(await vol.Files.ExistsAsync(path, Ct), $"{path} was rejected but appeared");
                }
            }

            Assert.NotNull(first);
            Assert.True(latencies.Count >= 30, $"only {latencies.Count} writes were admitted before the boundary");
            TimeSpan earlyP50 = latencies.Take(20).OrderBy(t => t).ElementAt(10);
            TimeSpan[] nearBoundary = latencies.TakeLast(10).OrderBy(t => t).ToArray();
            TimeSpan nearP99 = nearBoundary[^1];
            Assert.True(nearP99 <= earlyP50 * 5 + TimeSpan.FromMilliseconds(250), $"latency near the reserve boundary rose to {nearP99} (early p50 {earlyP50})");

            var rejections = new List<TimeSpan>();
            for (int r = 0; r < 10; r++)
            {
                string path = $"/perf015/rejected{r}";
                var timer = Stopwatch.StartNew();
                DfsUnavailableException rejected = await AssertEx.ThrowsDfsAsync<DfsUnavailableException>(() => vol.Files.WriteAllBytesAsync(path, TestData.Bytes((ulong)r, 512 * 1024), Ct));
                Assert.True(rejected is DfsCapacityException or DfsPlacementUnavailableException, $"a write at the capacity boundary failed with {rejected.GetType().Name}");
                rejections.Add(timer.Elapsed);
                Assert.False(await vol.Files.ExistsAsync(path, Ct));
            }

            Assert.All(rejections, t => Assert.True(t < TimeSpan.FromSeconds(1), $"a capacity rejection took {t}"));
            foreach ((string path, byte[] content) in admitted)
            {
                PerfHelpers.BytesEqual(content, await vol.Files.ReadAllBytesAsync(path, Ct), path);
                foreach (ObjectId id in await PerfHelpers.ObjectsOfAsync(cluster, path))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                    Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck, $"{path}: {id} has {copies}/{zones}");
                }
            }

            PerfHelpers.Report("perf015.admitted-files", admitted.Count, "files", context);
            PerfHelpers.Report("perf015.early.p50", earlyP50.TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf015.near-boundary.p99", nearP99.TotalMilliseconds, "ms", context);
            PerfHelpers.Report("perf015.rejection.max", rejections.Max().TotalMilliseconds, "ms", context);
        }, Ct);
    }

    // ============================================================================================================
    // PERF-016 RegressionBaselinesReproducible
    // ============================================================================================================

    /// <summary>
    /// A deterministic workload run twice with the same seed, build, platform, topology, storage settings, policy and cache
    /// state produces identical run records (compatibility key) and identical workload measurements (message and byte
    /// counts, stored objects, Raft entries, replicated-state digest, virtual elapsed time). A run under a different
    /// durability policy produces an incompatible record, and the comparison is refused with the differing field named.
    /// </summary>
    [Fact, Catalog("PERF-016", "P", "INV-16", "INV-17")]
    public async Task PERF_016_RegressionBaselinesReproducible()
    {
        (PerfRunRecord a, string resultA) = await RunWorkloadAsync(null);
        (PerfRunRecord b, string resultB) = await RunWorkloadAsync(null);
        (PerfRunRecord c, string resultC) = await RunWorkloadAsync(PerfHelpers.Policy(3, 3, 3, 3));
        PerfHelpers.Log($"run A: {a.ToJson()} => {resultA}");
        PerfHelpers.Log($"run B: {b.ToJson()} => {resultB}");
        PerfHelpers.Log($"run C: {c.ToJson()} => {resultC}");

        Assert.Equal(a.CompatibilityKey, b.CompatibilityKey);
        Assert.True(PerfRunRecord.CanCompare(a, b, out string? sameReason), sameReason);
        Assert.Equal(resultA, resultB);

        Assert.NotEqual(a.CompatibilityKey, c.CompatibilityKey);
        Assert.False(PerfRunRecord.CanCompare(a, c, out string? reason));
        Assert.Contains(nameof(PerfRunRecord.Policy), reason, StringComparison.Ordinal);

        static Task<(PerfRunRecord, string)> RunWorkloadAsync(DfsStoragePolicy? policy)
        {
            TestClusterOptions options = TestClusterOptions.ThreeZones with
            {
                Mode = ClusterMode.Deterministic,
                Seed = 0x16_16_16,
                DefaultPolicy = policy,
                ScenarioTimeout = TimeSpan.FromMinutes(5),
            };
            return ClusterTest.RunAsync(options, async cluster =>
            {
                PerfRunRecord record = PerfRunRecord.Capture(cluster, "perf016: 50 creates, 10 overwrites, 5 deletes, 1 snapshot", "cold");
                long start = cluster.Time.GetTimestamp();
                await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
                DfsVolume vol = client.GetVolume(Vol);
                await vol.Directories.CreateDirectoryAsync("/perf016", Ct);
                for (int i = 0; i < 50; i++)
                    await vol.Files.WriteAllBytesAsync($"/perf016/f{i}", TestData.Bytes((ulong)i, 1000 + i * 3000), Ct);
                for (int i = 0; i < 10; i++)
                    await vol.Files.WriteAllBytesAsync($"/perf016/f{i}", TestData.Bytes((ulong)(100 + i), 70_000), Ct);
                for (int i = 40; i < 45; i++)
                    await vol.Files.DeleteAsync($"/perf016/f{i}", Ct);
                await vol.Snapshots.CreateAsync("perf016", null, Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);

                NodeCounters counters = await cluster.Inspect.GetTotalCountersAsync();
                string result = string.Join(";",
                    $"messages={cluster.Network.Log.CountMessages(MessageFilter.Any)}",
                    $"bytes={cluster.Network.Log.CountBytes(MessageFilter.Any)}",
                    $"objects={(await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Count}",
                    $"puts={counters.ObjectPuts}",
                    $"raftApplied={counters.RaftEntriesApplied}",
                    $"digest={await cluster.ComputeDigestAsync(Ct)}",
                    $"virtualElapsedTicks={cluster.Time.GetElapsedTime(start).Ticks}");
                return (record, result);
            }, Ct);
        }
    }
}
