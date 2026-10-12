using System.Globalization;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>
/// The runtime core under the deterministic simulation (<see cref="SimScheduler"/>): node loops, clocks, network and disks
/// are simulated; any thread-pool escape is reported as a violation (HAR-002) and fails the run.
/// </summary>
public sealed class RuntimeSimulationTests
{
    private static readonly TimeSpan VirtualTimeout = TimeSpan.FromMinutes(2);

    [Fact]
    public void ThreeNodeCluster_BootstrapsCreatesAVolumeAndReplicates_WithoutThreadPoolEscapes()
    {
        SimRunRecord first = RunScenario(seed: 7);
        Assert.Contains("vol=", first.Digest, StringComparison.Ordinal);
        SimRunRecord second = RunScenario(seed: 7);
        Assert.Equal(first.Digest, second.Digest);
        Assert.Null(first.FirstDifference(second));
    }

    [Fact]
    public void ThreeNodeCluster_SurvivesALeaderCrashUnderSimulation()
    {
        var scheduler = new SimScheduler(11, new SimSchedulerOptions { ThrowOnViolation = false });
        var network = new SimNetwork(scheduler);
        SimCluster cluster = SimCluster.Create(scheduler, network, 3, seed: 11);
        scheduler.Run(async () =>
        {
            await cluster.StartAllAsync();
            int leader = await cluster.WaitForLeaderAsync("control", scheduler);
            GroupInspection before = (await cluster.Hosts[leader]!.Inspect.GetGroupAsync("control"))!;
            cluster.Hosts[leader]!.Kill();
            cluster.Media[leader].Crash(CrashMode.LoseUnflushed);
            cluster.Hosts[leader] = null;
            int next = await cluster.WaitForLeaderAsync("control", scheduler);
            Assert.NotEqual(leader, next);
            GroupInspection after = (await cluster.Hosts[next]!.Inspect.GetGroupAsync("control"))!;
            Assert.True(after.Term > before.Term);

            // The crashed node restarts from its disk with a new incarnation and rejoins.
            await cluster.StartAsync(leader);
            Assert.Equal(2UL, cluster.Hosts[leader]!.Incarnation);
            await WaitAsync(scheduler, async () =>
            {
                GroupInspection? g = await cluster.Hosts[leader]!.Inspect.GetGroupAsync("control");
                return g is not null && g.Leader is not null && g.AppliedIndex >= after.CommitIndex;
            });
            await cluster.StopAllAsync();
        }, VirtualTimeout);
        AssertNoViolations(scheduler);
    }

    private static SimRunRecord RunScenario(ulong seed)
    {
        var scheduler = new SimScheduler(seed, new SimSchedulerOptions { ThrowOnViolation = false });
        var network = new SimNetwork(scheduler);
        SimCluster cluster = SimCluster.Create(scheduler, network, 3, seed);
        string digest = scheduler.Run(async () =>
        {
            await cluster.StartAllAsync();
            await cluster.WaitForLeaderAsync("control", scheduler);
            await WaitAsync(scheduler, async () =>
            {
                foreach (NodeHost? host in cluster.Hosts)
                {
                    if (!await host!.InvokeAsync(s => s.Control.IsSelfRegistered))
                        return false;
                }

                return true;
            });

            // An administrator creates a volume through the control leader (client process, SDK-style awaits).
            int controlLeader = await cluster.WaitForLeaderAsync("control", scheduler);
            IRpcTransport transport = network.Create(scheduler.ClientProcess.Name, string.Empty);
            IRpcConnection connection = await transport.ConnectAsync(cluster.Hosts[controlLeader]!.Endpoint).ConfigureAwait(false);
            await HandshakeClient.AuthenticatePrincipalAsync(connection, cluster.ClusterId, "sim-admin", RuntimeCluster.AdminCredentials,
                scheduler.ForkRandom("client"), TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            var spec = new AdminVolumeSpecBody("vol", new DfsMetadataPolicy { Voters = 3 },
                new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 1, DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1 },
                new Dictionary<string, DfsStoragePolicy>(), 4096, new DfsVolumeLimits());
            ReadOnlyMemory<byte> replyFrame = await connection.CallAsync(
                MessageCodec.Encode(AdminRequestMessage.Create(AdminOp.CreateVolume, new UInt128(1, 2), spec)), TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            var reply = (AdminResultMessage)MessageCodec.Decode(replyFrame.Span);
            VolumeEntry volume = reply.ReadBody<AdminVolumeInfoBody>().Volume;
            await connection.DisposeAsync().ConfigureAwait(false);
            await transport.DisposeAsync().ConfigureAwait(false);

            int volumeLeader = await cluster.WaitForLeaderAsync("vol", scheduler);
            string group = ProtocolInfo.VolumeGroup(volume.Id);
            for (int i = 0; i < 10; i++)
            {
                ApplyOutcome outcome = await cluster.Hosts[volumeLeader]!.InvokeAsync(s =>
                    s.Raft.ProposeAsync(group, Encoding.UTF8.GetBytes("sim-" + i.ToString(CultureInfo.InvariantCulture)), ProposalContext.None, CancellationToken.None));
                Assert.True(outcome.IsSuccess);
            }

            string? expected = null;
            await WaitAsync(scheduler, async () =>
            {
                expected = await cluster.Hosts[volumeLeader]!.Inspect.ComputeGroupDigestAsync("vol");
                foreach (NodeHost? host in cluster.Hosts)
                {
                    if (await host!.Inspect.ComputeGroupDigestAsync("vol") != expected)
                        return false;
                }

                return true;
            });

            string control = (await cluster.Hosts[0]!.Inspect.ComputeGroupDigestAsync("control"))!;
            await cluster.StopAllAsync();
            return "vol=" + expected + " control-state=" + control[control.IndexOf(':', StringComparison.Ordinal)..];
        }, VirtualTimeout);
        AssertNoViolations(scheduler);
        return SimRunRecord.Capture(scheduler, digest);
    }

    private static void AssertNoViolations(SimScheduler scheduler)
    {
        if (scheduler.Violations.Count == 0)
            return;
        SimViolation first = scheduler.Violations[0];
        Assert.Fail($"{scheduler.Violations.Count} simulation violation(s); first: {first.Kind} {first.Operation}: {first.Description}{System.Environment.NewLine}{first.StackTrace}");
    }

    private static async Task WaitAsync(SimScheduler scheduler, Func<Task<bool>> condition)
    {
        for (int i = 0; i < 6000; i++)
        {
            if (await condition())
                return;
            await scheduler.DelayAsync(TimeSpan.FromMilliseconds(10)).ConfigureAwait(false);
        }

        throw new TimeoutException("The simulated condition was not reached.");
    }

    /// <summary>A static-bootstrap cluster of <see cref="NodeHost"/>s on simulated loops, clocks, network and disks.</summary>
    private sealed class SimCluster
    {
        private readonly SimScheduler _scheduler;
        private readonly SimNetwork _network;
        private readonly NodeOptions[] _options;
        private readonly SimTimeProvider[] _clocks;
        private readonly int[] _starts;

        private SimCluster(SimScheduler scheduler, SimNetwork network, NodeOptions[] options, SimTimeProvider[] clocks, SimulatedMedium[] media, Guid clusterId)
        {
            _scheduler = scheduler;
            _network = network;
            _options = options;
            _clocks = clocks;
            Media = media;
            ClusterId = clusterId;
            Hosts = new NodeHost?[options.Length];
            _starts = new int[options.Length];
        }

        public Guid ClusterId { get; }

        public NodeHost?[] Hosts { get; }

        public SimulatedMedium[] Media { get; }

        public static SimCluster Create(SimScheduler scheduler, SimNetwork network, int count, ulong seed)
        {
            IRandomSource setup = scheduler.ForkRandom("setup");
            Guid clusterId = setup.NextGuid();
            byte[] secret = setup.NextBytes(32);
            var members = new List<BootstrapMember>();
            var clocks = new SimTimeProvider[count];
            var media = new SimulatedMedium[count];
            for (int i = 0; i < count; i++)
            {
                string name = "n" + i.ToString(CultureInfo.InvariantCulture);
                var labels = new DfsTopologyLabels("r1", "z" + i.ToString(CultureInfo.InvariantCulture), "rk1", "h" + i.ToString(CultureInfo.InvariantCulture), "d1");
                members.Add(new BootstrapMember(NodeId.New(setup), name, labels, ["mem://" + name]));
                clocks[i] = scheduler.CreateClock(name);
                media[i] = new SimulatedMedium(name, 1L << 30, scheduler.ForkRandom("disk/" + name), clocks[i]);
            }

            var bootstrap = new ClusterBootstrap(clusterId, "sim", members, members.Select(m => m.Name).ToArray(),
                [new BootstrapPrincipal(RuntimeCluster.AdminPrincipal, RuntimeCluster.AdminSecret, true)]);
            NodeOptions[] options = members.Select(m => new NodeOptions
            {
                Name = m.Name,
                Labels = m.Labels,
                ListenEndpoint = m.Endpoints[0],
                DataDirectory = "node",
                ClusterSecret = secret,
                Bootstrap = bootstrap,
                Timings = NodeTimings.Fast,
                ReservedBytes = 1 << 20,
            }).ToArray();
            _ = seed;
            return new SimCluster(scheduler, network, options, clocks, media, clusterId);
        }

        public async Task StartAllAsync()
        {
            for (int i = 0; i < Hosts.Length; i++)
                await StartAsync(i);
        }

        public async Task StartAsync(int index)
        {
            _starts[index]++;
            var environment = new NodeEnvironment
            {
                Medium = Media[index],
                TransportFactory = _network,
                Time = _clocks[index],
                Random = _scheduler.ForkRandom("node/" + _options[index].Name + "/" + _starts[index].ToString(CultureInfo.InvariantCulture)),
                LoopFactory = _scheduler.LoopFactory,
                VolumeStateMachines = TestVolumeStateMachineFactory.Instance,
            };
            Hosts[index] = await NodeHost.StartAsync(_options[index], environment).ConfigureAwait(false);
        }

        public async Task StopAllAsync()
        {
            foreach (NodeHost? host in Hosts)
            {
                if (host is not null)
                    await host.StopAsync().ConfigureAwait(false);
            }
        }

        public async Task<int> WaitForLeaderAsync(string group, SimScheduler scheduler)
        {
            int found = -1;
            await WaitAsync(scheduler, async () =>
            {
                for (int i = 0; i < Hosts.Length; i++)
                {
                    if (Hosts[i] is { } host && (await host.Inspect.GetGroupAsync(group))?.Role == RaftRole.Leader)
                    {
                        found = i;
                        return true;
                    }
                }

                return false;
            });
            return found;
        }
    }
}
