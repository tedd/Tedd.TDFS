using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Sec;

/// <summary>
/// SEC — authentication, authorization and confidentiality (FSD §§4, 7, 12, 14). Integration level: real QUIC transport,
/// real disk storage, server-side enforcement exercised both through the SDK and through <see cref="RawProtocolClient"/>.
/// </summary>

/// <summary>The collection of SEC-014: it runs alone (allocation is measured process-wide).</summary>
[CollectionDefinition(nameof(SecResourceLimitCollection), DisableParallelization = true)]
public sealed class SecResourceLimitCollection;

/// <summary>
/// SEC-014 — unauthenticated resource limits. Its allocation bound is measured process-wide
/// (<see cref="GC.GetTotalAllocatedBytes(bool)"/>), so it runs in a collection that is never parallelized with other tests:
/// concurrently running clusters of other tests would otherwise be charged to the abuse (several GiB in a full run).
/// </summary>
[Collection(nameof(SecResourceLimitCollection))]
public sealed class SecResourceLimitIntegrationTests
{
    private const string Vol = SecHelpers.Vol;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-014
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Unauthenticated abuse in SEC-014.</summary>
    public enum UnauthenticatedAbuse
    {
        /// <summary>Frames whose length prefixes claim gigabytes to terabytes.</summary>
        OversizedLengthPrefixes,

        /// <summary>Hundreds of idle unauthenticated connections.</summary>
        ConnectionFlood,

        /// <summary>Thousands of seeded malformed frames on one connection.</summary>
        MalformedFrameFlood,
    }

    /// <summary>
    /// Unauthenticated oversized lengths, connection floods and malformed-frame floods are refused without unbounded
    /// allocation, without crashing any node and without stalling unrelated authenticated work (including admitting a new
    /// authenticated connection during the flood); how idle unauthenticated connections are bounded is recorded.
    /// </summary>
    [Theory]
    [InlineData(UnauthenticatedAbuse.OversizedLengthPrefixes)]
    [InlineData(UnauthenticatedAbuse.ConnectionFlood)]
    [InlineData(UnauthenticatedAbuse.MalformedFrameFlood)]
    [Catalog("SEC-014", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_014_UnauthenticatedResourceLimits(UnauthenticatedAbuse abuse) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            TestPrincipal user = await SecHelpers.CreatePrincipalAsync(cluster, PrincipalKind.Writer, "legit");
            await using DfsClient legit = await SecHelpers.ConnectAsAsync(cluster, user.Credentials, "legit", Ct);
            DfsVolume volume = legit.GetVolume(Vol);
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            var raws = new List<RawProtocolClient>();
            int refusedConnections = 0, reconnects = 0, frames = 0;
            try
            {
                switch (abuse)
                {
                    case UnauthenticatedAbuse.OversizedLengthPrefixes:
                        foreach (TestNode node in cluster.Nodes)
                        {
                            RawProtocolClient raw = await cluster.ConnectRawAsync(node.Name, Ct);
                            raws.Add(raw);
                            foreach (MessageType type in new[] { MessageType.Hello, MessageType.HelloProof, MessageType.OpenSession, MessageType.OpenHandle,
                                         MessageType.PutObject, MessageType.WriteAll, MessageType.AdminRequest, MessageType.BackupPutManifest, MessageType.RaftBatch })
                            {
                                // 256 MiB and 1 GiB are allocatable (a server pre-allocating them would allocate 27 × 1.25 GiB
                                // across these frames); the larger claims exceed the array limit and probe overflow handling.
                                foreach (ulong claimed in new[] { 256UL << 20, 1UL << 30, 1UL << 31, 1UL << 40, ulong.MaxValue >> 1 })
                                {
                                    if (!raw.IsConnected)
                                    {
                                        raw = await cluster.ConnectRawAsync(node.Name, Ct);
                                        raws.Add(raw);
                                        reconnects++;
                                    }

                                    DfsErrorCode code = await raw.SendFrameAsync(SecHelpers.OversizedLengthFrame(type, claimed));
                                    frames++;
                                    Assert.True(code != DfsErrorCode.None, $"{type} claiming {claimed} bytes was accepted on {node.Name}");
                                }
                            }
                        }

                        break;

                    case UnauthenticatedAbuse.ConnectionFlood:
                        for (int i = 0; i < 300; i++)
                        {
                            try
                            {
                                raws.Add(await cluster.ConnectRawAsync(cluster[i % cluster.Nodes.Count].Name, Ct));
                            }
                            catch (Exception ex) when (ex is IOException or DfsException or TimeoutException or UnauthorizedAccessException)
                            {
                                refusedConnections++;
                            }
                        }

                        break;

                    default:
                    {
                        RawProtocolClient raw = await cluster.ConnectRawAsync(cluster[0].Name, Ct);
                        raws.Add(raw);
                        foreach (byte[] frame in SecHelpers.FuzzFrames(0x5EC014, 20))
                        {
                            if (!raw.IsConnected)
                            {
                                raw = await cluster.ConnectRawAsync(cluster[0].Name, Ct);
                                raws.Add(raw);
                                reconnects++;
                            }

                            DfsErrorCode code = await raw.SendFrameAsync(frame);
                            frames++;
                            Assert.True(code != DfsErrorCode.None || SecHelpers.DecodesLocally(frame),
                                $"a malformed unauthenticated frame (type bytes {Convert.ToHexString(frame.AsSpan(0, Math.Min(2, frame.Length)))}) was accepted");
                        }

                        break;
                    }
                }

                // Allocation during the abuse phase only (process-wide, so other concurrently running tests add noise).
                // Pre-allocating the allocatable claims (256 MiB / 1 GiB on every node and message type) crosses this bound
                // many times over; normal bounded handling stays far below it.
                long abuseAllocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
                Assert.True(abuseAllocated < 1L << 31, $"{abuse} caused {abuseAllocated} bytes of allocation; claimed lengths must not be pre-allocated");

                // Unrelated authenticated work keeps running while the abusive connections are still open, including a new
                // authenticated connection (admission is not starved by unauthenticated peers).
                await using DfsClient fresh = await SecHelpers.ConnectAsAsync(cluster, user.Credentials, "legit-fresh", Ct);
                for (int i = 0; i < 5; i++)
                {
                    DfsVolume target = i % 2 == 0 ? volume : fresh.GetVolume(Vol);
                    byte[] payload = TestData.Bytes(1400 + (ulong)i, 20_000);
                    var watch = Stopwatch.StartNew();
                    await target.Files.WriteAllBytesAsync("/legit-" + SecHelpers.Inv(i) + ".bin", payload, Ct);
                    await RecoveryScenario.AssertFileBytesAsync(target.Files, "/legit-" + SecHelpers.Inv(i) + ".bin", payload, "unrelated authenticated work under abuse", Ct);
                    Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"unrelated work stalled for {watch.Elapsed} under {abuse}");
                }

                Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
                TestContext.Current.TestOutputHelper?.WriteLine($"{abuse}: frames={frames} connections={raws.Count} refused={refusedConnections} reconnects={reconnects} allocated={abuseAllocated / (1 << 20)} MiB");

                if (abuse == UnauthenticatedAbuse.ConnectionFlood)
                {
                    // How the server bounds idle unauthenticated connections (handshake deadline, cap, refusal) is not fixed
                    // by the FSD; record it for the report instead of asserting one mechanism.
                    await cluster.DelayAsync(TimeSpan.FromSeconds(30), Ct);
                    TestContext.Current.TestOutputHelper?.WriteLine(
                        $"after 30 s idle: {raws.Count(r => r.IsConnected)} of {raws.Count} unauthenticated connections still open, {refusedConnections} refused");
                    Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
                }
            }
            finally
            {
                foreach (RawProtocolClient raw in raws)
                    await RecoveryScenario.CaptureAsync(async () => await raw.DisposeAsync());
            }
        }, Ct);
}
