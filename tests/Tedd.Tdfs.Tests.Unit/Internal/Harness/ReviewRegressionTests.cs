using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Host;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>
/// Regression tests for defects found in review of the harness: directional link faults, partial frames, tampering below
/// integrity protection, stop/crash races, crash-loss accounting, fenced views and the host executable's process control.
/// </summary>
public sealed class ReviewRegressionTests
{
    // ------------------------------------------------------------------------------------------------ directional faults

    [Fact]
    public Task OneWayCut_ReverseDirectionCanStillConnectAndDeliver() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xD1C7), NodeHostOnly, async cluster =>
        {
            const string raw1 = "client:raw1";
            cluster.Network.Cut("n0", raw1);
            Assert.False(cluster.Fabric.Policy.CanConnect("n0", raw1));
            Assert.True(cluster.Fabric.Policy.CanConnect(raw1, "n0"));

            // The client opens a new connection in the uncut direction; its request reaches n0, only the reply (the cut
            // direction) is lost, so the handshake times out instead of the connection being refused.
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            MessageLog log = cluster.Network.Log;
            log.Reset();
            Assert.Equal(DfsErrorCode.Timeout, await raw.AuthenticateAsync(cluster.AdminCredentials));
            Assert.Equal(1L, log.CountMessages(new MessageFilter(From: raw1, To: "n0", Type: MessageType.Hello)));
            Assert.Equal(0L, log.CountMessages(new MessageFilter(From: "n0", To: raw1)));
            Assert.Single(cluster.Network.GetMessageAttempts(new MessageFilter(From: "n0", To: raw1, Type: MessageType.HelloChallenge)),
                a => a.Decision == LinkDecisionKind.Drop);
        }, Ct);

    [Fact]
    public Task RejectedPartition_LeavesTheCurrentPartitionInForce() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x9A27), NodeHostOnly, cluster =>
        {
            HarnessLinkPolicy policy = cluster.Fabric.Policy;
            cluster.Network.Partition(["n0"], ["n1", "n2"]);
            Assert.Throws<ArgumentException>(() => cluster.Network.Partition(["n0", "n1"], ["n1", "n2"])); // n1 listed twice
            Assert.False(policy.IsLinkUp("n0", "n1"));
            Assert.False(policy.IsLinkUp("n0", "n2"));
            Assert.True(policy.IsLinkUp("n1", "n2"));
            cluster.Network.Heal();
            Assert.True(policy.IsLinkUp("n0", "n1"));
            return Task.CompletedTask;
        }, Ct);

    /// <summary>
    /// A follower cut off from the leader's direction only re-establishes its own connection: its connection attempts and
    /// handshake requests travel the uncut direction and reach the leader (the cut direction then loses the replies).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task LeaderToFollowerCut_FollowerCanStillOpenConnectionsToTheLeader(bool blackHole) =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x0D1E + (blackHole ? 1UL : 0UL)), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            string follower = control.Voters.First(v => v != leader.Name);
            if (blackHole)
                cluster.Network.BlackHole(leader.Name, follower);
            else
                cluster.Network.Cut(leader.Name, follower);
            MessageLog log = cluster.Network.Log;
            log.Reset();

            // The follower's own connections are closed, so reaching the leader needs a new one in the uncut direction.
            cluster.Network.ResetConnections(follower);
            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);

            Assert.Equal(0L, log.CountMessages(new MessageFilter(From: leader.Name, To: follower)));
            Assert.True(log.CountMessages(new MessageFilter(From: follower, To: leader.Name, Type: MessageType.Hello)) > 0,
                $"{follower} must still reach {leader.Name} over a new connection while only {leader.Name}->{follower} is {(blackHole ? "black-holed" : "cut")}");

            cluster.Network.Restore(leader.Name, follower);
            await cluster.WaitForStableAsync(null, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    // ------------------------------------------------------------------------------------------------ partial frames

    [Fact]
    public Task TruncatedRequest_NeverReachesTheReceivingApplication() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7A0C), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            using MessageHold received = cluster.Network.Hold(new MessageFilter(From: "client:raw1"));
            MessageRule truncate = cluster.Network.TruncateNext(new MessageFilter(From: "client:raw1", Type: MessageType.Hello), keepBytes: 3);

            DfsErrorCode code = await raw.AuthenticateAsync(cluster.AdminCredentials);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct); // anything still travelling towards n0 has arrived

            Assert.Equal(1, truncate.Matched);
            Assert.Empty(received.Captured); // a frame cut by the end of the stream is discarded by the receiving transport
            Assert.Equal(DfsErrorCode.Unavailable, code); // the caller saw the connection close, never an answer to a short frame
            Assert.False(raw.IsConnected);
        }, Ct);

    [Fact]
    public Task TruncatedReply_IsALostConnectionNotAShortReply() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7A0D), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            MessageRule truncate = cluster.Network.TruncateNext(new MessageFilter(To: "client:raw1", Type: MessageType.HelloChallenge), keepBytes: 5);

            DfsErrorCode code = await raw.AuthenticateAsync(cluster.AdminCredentials);

            Assert.Equal(1, truncate.Matched);
            Assert.Equal(DfsErrorCode.Unavailable, code); // a 5-byte "reply" would have been a ProtocolError
            Assert.False(raw.IsConnected);

            // keepBytes at or above the frame length delivers the frame, then closes.
            await using RawProtocolClient whole = await cluster.ConnectRawAsync("n0", Ct);
            MessageRule all = cluster.Network.TruncateNext(new MessageFilter(To: "client:raw2", Type: MessageType.HelloChallenge), keepBytes: 1 << 20);
            DfsErrorCode afterWhole = await whole.AuthenticateAsync(cluster.AdminCredentials);
            Assert.Equal(1, all.Matched);
            Assert.Equal(DfsErrorCode.Unavailable, afterWhole); // the challenge arrived, the proof found the connection closed
        }, Ct);

    // ------------------------------------------------------------------------------------------------ tampering

    [Theory]
    [InlineData(TransportTamper.FlipBit)]
    [InlineData(TransportTamper.Truncate)]
    [InlineData(TransportTamper.AppendGarbage)]
    [InlineData(TransportTamper.Scramble)]
    public Task TamperedRequest_IsRejectedBelowTheApplication(TransportTamper mode) =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7A3F), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            using MessageHold received = cluster.Network.Hold(new MessageFilter(From: "client:raw1"));
            MessageRule tamper = cluster.Network.TamperNext(new MessageFilter(From: "client:raw1", Type: MessageType.Hello), mode);

            DfsErrorCode code = await raw.AuthenticateAsync(cluster.AdminCredentials);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct); // anything still travelling towards n0 has arrived

            Assert.Equal(1, tamper.Matched);
            Assert.Empty(received.Captured); // no altered frame is ever handed to the receiving node
            Assert.Equal(DfsErrorCode.Unavailable, code);
            Assert.Contains(cluster.Trace, l => l.Contains("integrity", StringComparison.Ordinal));
        }, Ct);

    [Fact]
    public Task ReplayedTransmission_IsDeliveredExactlyOnce() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7A40), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            MessageLog log = cluster.Network.Log;
            log.Reset();
            MessageRule replay = cluster.Network.TamperNext(new MessageFilter(From: "client:raw1", Type: MessageType.Hello), TransportTamper.Replay);

            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            Assert.Equal(1, replay.Matched);
            Assert.Equal(1L, log.CountMessages(new MessageFilter(From: "client:raw1", Type: MessageType.Hello)));
        }, Ct);

    [Fact]
    public Task TamperedReply_IsRejectedNotDecodedAsGarbage() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7A41), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            MessageRule tamper = cluster.Network.TamperNext(new MessageFilter(To: "client:raw1", Type: MessageType.HelloChallenge), TransportTamper.Scramble);

            DfsErrorCode code = await raw.AuthenticateAsync(cluster.AdminCredentials);

            Assert.Equal(1, tamper.Matched);
            Assert.Equal(DfsErrorCode.Unavailable, code); // a scrambled challenge would have surfaced as a ProtocolError
        }, Ct);

    // ------------------------------------------------------------------------------------------------ replay

    [Fact]
    public Task HeldRequest_IsReplayedFromItsSendersIdentity() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x8E91), NodeHostOnly, async cluster =>
        {
            const string raw1 = "client:raw1";
            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            var request = new MessageFilter(From: raw1, To: "n0", Type: MessageType.GetVolumeRoute);
            MessageHold hold = cluster.Network.Hold(request);
            Task<IWireMessage> call = raw.CallAsync(new GetVolumeRouteMessage("vol"));
            await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
            CapturedMessage captured = hold.Captured[0];
            hold.ReleaseAll();
            Assert.IsType<VolumeRouteResultMessage>(await call);
            MessageLog log = cluster.Network.Log;
            log.Reset();

            // Over the sender's own, still open connection (the receiver authenticated it as "admin").
            Assert.Equal(DfsErrorCode.None, await cluster.Network.ReplayAsync(captured));
            Assert.Equal(1L, log.CountMessages(request));

            // The sender's connection is gone: a fresh connection authenticated as the same principal carries the replay.
            await raw.DisposeAsync();
            Assert.Equal(DfsErrorCode.None, await cluster.Network.ReplayAsync(captured));
            Assert.Equal(2L, log.CountMessages(request));
        }, Ct);

    // ------------------------------------------------------------------------------------------------ node lifecycle

    [Fact]
    public Task CrashDuringGracefulStop_LeavesTheNodeCrashed() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x5C2A), NodeHostOnly, async cluster =>
        {
            TestNode node = cluster[1];
            Task stopping = node.StopAsync();
            Assert.False(stopping.IsCompleted);
            await node.CrashAsync();
            await stopping;

            Assert.Equal(TestNodeState.Crashed, node.State); // the crash decided the disk image; the stop must not relabel it
            Assert.Null(node.Host);
            await node.RestartAsync();
            Assert.Equal(TestNodeState.Running, node.State);
            await cluster.WaitForStableAsync(null, Ct);
        }, Ct);

    // ------------------------------------------------------------------------------------------------ storage

    [Fact]
    public async Task CrashUndoingAnUnflushedCreation_ReportsItsAcknowledgedWrites_OnBothMedia()
    {
        byte[] data = TestData.Bytes(0xAC, 1000);
        var clock = new TestClock();

        var simulated = new SimulatedMedium("sim", 1 << 20, new SeededRandom(1), clock);
        await AcknowledgeWriteInUnflushedFileAsync(simulated, data);
        simulated.Crash(CrashMode.LoseUnflushed);

        string root = NewScratchDirectory("ffsm-loss");
        try
        {
            var real = new FaultingFileSystemMedium(Path.Combine(root, "d"), Path.Combine(root, "t"), 1 << 20, new SeededRandom(1), clock);
            await AcknowledgeWriteInUnflushedFileAsync(real, data);
            real.Crash(CrashMode.LoseUnflushed);

            Assert.Equal(1, simulated.LastCrashLoss.Writes);
            Assert.Equal(simulated.LastCrashLoss.Writes, real.LastCrashLoss.Writes);
            Assert.Equal(simulated.LastCrashLoss.Bytes, real.LastCrashLoss.Bytes);
            Assert.Equal(simulated.LastCrashLoss.Paths, real.LastCrashLoss.Paths);
            Assert.False(await real.ExistsAsync("dir/f", Ct));
        }
        finally
        {
            DeleteScratchDirectory(root);
        }

        static async Task AcknowledgeWriteInUnflushedFileAsync(IStorageMedium medium, byte[] data)
        {
            await medium.CreateDirectoryAsync("dir", Ct);
            await medium.FlushDirectoryAsync(string.Empty, Ct);
            Faults(medium).DishonestFlush = true;
            await using IStorageFile file = await medium.OpenAsync("dir/f", StorageOpenMode.CreateNew, Ct);
            await file.WriteAsync(0, data, Ct);
            await file.FlushAsync(Ct); // reported durable, kept volatile; the directory entry was never flushed at all
        }

        static SimulatedMediumFaults Faults(IStorageMedium medium) => medium switch
        {
            SimulatedMedium s => s.Faults,
            FaultingFileSystemMedium f => f.Faults,
            _ => throw new InvalidOperationException(),
        };
    }

    [Fact]
    public Task VerifiedHolders_DoNotTrustAMutatedReadPath() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xC0C0) with
        {
            EncryptAtRest = false,
            Mutations = new SafetyMutations { SkipChecksumVerification = true },
        }, NodeHostOnly, async cluster =>
        {
            TestNode node = cluster[0];
            var id = new ObjectId(0xC0C0_0001UL);
            byte[] data = TestData.Bytes(0xC0C0, 3000);
            await node.Host!.InvokeAsync(s => s.ObjectStore.PutAsync(id, new VolumeId(Guid.Empty), default, data).AsTask());
            Assert.Equal([node.Name], await cluster.Inspect.GetObjectHoldersAsync(id));

            // The mutation makes the node's own reads skip verification; the harness's ground truth must still see the damage.
            await node.Disk.CorruptObjectAsync(id, offset: 100);
            Assert.Empty(await cluster.Inspect.GetObjectHoldersAsync(id));
            Assert.Equal((0, 0), await cluster.Inspect.CountDurableDomainsAsync(id));
        }, Ct);

    [Fact]
    public async Task FencedView_DelayedOpenNeverCreatesAFileAfterTheFence()
    {
        string root = NewScratchDirectory("fence");
        try
        {
            var clock = new TestClock();
            clock.Freeze();
            var medium = new FaultingFileSystemMedium(Path.Combine(root, "d"), Path.Combine(root, "t"), 1 << 20, new SeededRandom(2), clock);
            await medium.CreateDirectoryAsync("dir", Ct);
            medium.Faults.IoDelay = TimeSpan.FromSeconds(1);
            var view = new FencedMedium(medium, "n0 (start 1)");

            ValueTask<IStorageFile> opening = view.OpenAsync("dir/zombie", StorageOpenMode.CreateNew, Ct);
            Assert.False(opening.IsCompleted); // waiting for the device
            view.Fence(); // the process crashes meanwhile
            clock.Advance(TimeSpan.FromSeconds(2));

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await opening);
            Assert.False(await medium.ExistsAsync("dir/zombie", Ct), "a dead incarnation must not create files on the next incarnation's image");
        }
        finally
        {
            DeleteScratchDirectory(root);
        }
    }

    // ------------------------------------------------------------------------------------------------ host executable

    [Fact]
    public async Task HostExecutable_EndOfInputStopsOnlyALauncherControlledHost()
    {
        string root = NewScratchDirectory("host-stdin");
        try
        {
            string dll = OutOfProcessLauncher.LocateHost();
            string config = Path.Combine(root, "h0.json");
            int port = PortAllocator.Allocate();
            int bootstrap = await RunToExitAsync(dll, ["bootstrap", "--name", "h0", "--listen", $"quic://127.0.0.1:{port}", "--storage",
                Path.Combine(root, "storage"), "--out", config, "--timings", "Fast"]);
            Assert.Equal(0, bootstrap);

            var trace = new List<string>();
            void Record(string line)
            {
                lock (trace)
                    trace.Add(line);
            }

            // A host run as a service (no launcher on standard input) keeps running when its input ends.
            (HostProcess service, HostReadyMessage ready) = await HostProcess.StartAsync(dll, "node", config, "h0", TimeSpan.FromSeconds(90), Record, Ct,
                stopAtEndOfInput: false);
            await using (service)
            {
                Assert.Equal("h0", ready.Name);
                service.CloseInput();
                Task exited = service.Exited;
                Assert.NotSame(exited, await Task.WhenAny(exited, Task.Delay(TimeSpan.FromSeconds(3), Ct)));
                Assert.True(service.IsRunning);
                service.Kill();
                await service.Exited.WaitAsync(TimeSpan.FromSeconds(30), Ct);
                if (ready.KeyContainer is { } key)
                    Assert.False(DevelopmentCertificates.DeletePersistedKey(key), "the killed host's key container must already be gone");
            }

            // A launcher-controlled host stops gracefully when its launcher's end of the pipe closes.
            (HostProcess controlled, _) = await HostProcess.StartAsync(dll, "node", config, "h0", TimeSpan.FromSeconds(90), Record, Ct);
            await using (controlled)
            {
                controlled.CloseInput();
                await controlled.Exited.WaitAsync(TimeSpan.FromSeconds(60), Ct);
                Assert.Equal(0, controlled.ExitCode);
            }
        }
        finally
        {
            DeleteScratchDirectory(root);
        }
    }

    [Fact]
    public void PlannedQuicPorts_AvoidTheEphemeralRange()
    {
        // Ports a node binds later (and again after a restart) must never be ports the OS hands to connect-only transports.
        int[] ports = Enumerable.Range(0, 32).Select(_ => PortAllocator.Allocate()).ToArray();
        Assert.Equal(ports.Length, ports.Distinct().Count());
        Assert.All(ports, p => Assert.InRange(p, 1024, 32767));
    }

    [Fact]
    public void PlannedQuicPorts_AreReservedMachineWide()
    {
        // A stopped node's port is free until its restart binds it again: another test process must not allocate it
        // meanwhile. Each issued port holds a named system reservation, which another allocator opens instead of creating.
        int port = PortAllocator.Allocate();
        using var other = new System.Threading.Mutex(false, PortAllocator.ReservationPrefix + port.ToString(System.Globalization.CultureInfo.InvariantCulture), out bool createdNew);
        Assert.False(createdNew, $"port {port} was handed out without a machine-wide reservation");
    }

    [Fact]
    public void BoundUdpPort_OwnerIsNamedInTheTrace()
    {
        // A start whose port stayed bound names the process holding it, so the trace tells a lingering listener of this
        // process from another program.
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the owner lookup uses the Windows IP helper tables");
        using var socket = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Dgram, System.Net.Sockets.ProtocolType.Udp);
        socket.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        int port = ((System.Net.IPEndPoint)socket.LocalEndPoint!).Port;
        string owners = UdpPortOwners.Describe(port);
        Assert.Contains($"pid {Environment.ProcessId} (", owners, StringComparison.Ordinal);
        Assert.Contains("this process", owners, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private static string NewScratchDirectory(string purpose)
    {
        string path = Path.Combine(Path.GetFullPath(TestClusterOptions.ThreeZones.ResolveDataRoot()), "harness-review",
            $"{purpose}-{Environment.ProcessId}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteScratchDirectory(string path)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
                return;
            }
            catch (Exception ex) when ((ex is IOException or UnauthorizedAccessException) && attempt < 20)
            {
                Thread.Sleep(100); // handles of a process that just exited may still be closing (Windows)
            }
        }
    }

    private static async Task<int> RunToExitAsync(string dll, IReadOnlyList<string> arguments)
    {
        var info = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        info.ArgumentList.Add("exec");
        info.ArgumentList.Add(dll);
        foreach (string argument in arguments)
            info.ArgumentList.Add(argument);
        using var process = System.Diagnostics.Process.Start(info) ?? throw new InvalidOperationException("dotnet could not be started.");
        Task<string> output = process.StandardOutput.ReadToEndAsync(Ct);
        Task<string> error = process.StandardError.ReadToEndAsync(Ct);
        await process.WaitForExitAsync(Ct).WaitAsync(TimeSpan.FromSeconds(60), Ct);
        Assert.True(process.ExitCode == 0, $"exit {process.ExitCode}: {await output}{await error}");
        return process.ExitCode;
    }
}
