using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Ops;

/// <summary>
/// OPS suite, level U: operation tracing across retries (FSD §§13, 15; test plan §7.26). Runs against
/// <see cref="TestClusterOptions.ThreeZones"/> with a recording <see cref="IDfsDiagnostics"/> sink.
/// </summary>
public sealed class OpsUnitTests
{
    private const string Vol = OpsHelpers.Volume;

    private static CancellationToken Ct => OpsHelpers.Ct;

    /// <summary>
    /// One logical append is retried through a lost request, a lost reply (each followed by a connection reset, so the loss
    /// is observable regardless of the SDK's per-attempt timeout), a duplicated delivery and a leader crash with reroute. Every transmission carries the same stable operation id while each SDK transmission has its own attempt
    /// (correlation) id; the file receives the suffix exactly once (prefix + suffix, one new revision equal to the returned
    /// token); the outcome record reports that single success; an explicit application retry with the same id returns the
    /// same token without a second effect; and the SDK's diagnostics attribute at most one commit token to the operation.
    /// </summary>
    [Theory, Catalog("OPS-007", "U", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData("request-dropped")]
    [InlineData("reply-dropped")]
    [InlineData("duplicated")]
    [InlineData("leader-crash-reroute")]
    public async Task OPS_007_OperationTraceCorrelatesRetries(string fault)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            var diagnostics = new DiagnosticsRecorder();
            await using DfsClient client = await cluster.ConnectAsync(
                new TestClientOptions { Name = "c0", Diagnostics = diagnostics, OperationTimeout = TimeSpan.FromSeconds(15) }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsFileClient observed = observer.GetVolume(Vol).Files;

            const string path = "/ops007/log";
            byte[] prefix = TestData.Bytes(70, 3000);
            byte[] suffix = TestData.Bytes(71, 5000);
            await vol.Directories.CreateDirectoryAsync("/ops007", Ct);
            await vol.Files.WriteAllBytesAsync(path, prefix, Ct);
            DfsFileInfo before = await observed.GetInfoAsync(path, Ct);
            DfsOperationId id = await vol.NewOperationIdAsync(Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            int mark = diagnostics.Count;
            var appendRequests = new MessageFilter(From: "client:c0", Type: MessageType.Append, Direction: LinkDirection.Request);
            MessageRule? rule = null;
            MessageHold? hold = null;
            switch (fault)
            {
                case "request-dropped":
                    rule = cluster.Network.DropNext(appendRequests, count: 1);
                    break;
                case "reply-dropped":
                    rule = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.Append), count: 1);
                    break;
                case "duplicated":
                    rule = cluster.Network.DuplicateNext(appendRequests, count: 1);
                    break;
                case "leader-crash-reroute":
                    hold = cluster.Network.Hold(appendRequests with { To = leader.Name });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(fault));
            }

            Task<CommitToken> append = vol.Files.AppendAllBytesAsync(path, suffix, new DfsMutationOptions { OperationId = id }, Ct);
            if (hold is not null)
            {
                await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                await leader.CrashAsync();
                hold.DropAll();
                hold.Dispose();
            }
            else if (fault is "request-dropped" or "reply-dropped")
            {
                // Make the loss observable to the SDK independently of its per-attempt timeout policy: once the request
                // (or its reply) has been lost, the client's connections are reset, so the SDK must retransmit the same
                // operation (or query its outcome) on a new connection.
                await cluster.WaitUntilAsync(() => Task.FromResult(rule!.Matched >= 1), TimeSpan.FromSeconds(10),
                    $"the {fault} rule must match the Append request", Ct);
                if (fault == "reply-dropped")
                    await Task.Delay(TimeSpan.FromMilliseconds(200), Ct); // let the executed request's reply be produced and dropped
                cluster.Network.ResetConnections("client:c0");
            }

            CommitToken token = await append;
            if (rule is not null)
                Assert.True(rule.Matched >= 1, $"the {fault} rule must have matched the Append request");
            Assert.False(token.IsNone);

            // Exactly one logical effect on the same inode, published as one revision equal to the returned token.
            byte[] expected = OpsHelpers.Concat(prefix, suffix);
            OpsHelpers.BytesEqual(expected, await observed.ReadAllBytesAsync(path, Ct), "file after one retried append");
            DfsFileInfo after = await observed.GetInfoAsync(path, Ct);
            Assert.Equal(before.InodeId, after.InodeId);
            Assert.Equal(token.Sequence, after.Revision);
            Assert.True(after.Revision > before.Revision);

            // The outcome record reports the single success under the stable identity.
            DfsOperationOutcome outcome = await observer.GetVolume(Vol).QueryOperationAsync(id, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(token, outcome.Token);

            // An explicit application retry with the same identity returns the original result and applies nothing.
            CommitToken again = await vol.Files.AppendAllBytesAsync(path, suffix, new DfsMutationOptions { OperationId = id }, Ct);
            Assert.Equal(token, again);
            OpsHelpers.BytesEqual(expected, await observed.ReadAllBytesAsync(path, Ct), "file after an explicit retry with the same id");
            Assert.Equal(after.Revision, (await observed.GetInfoAsync(path, Ct)).Revision);

            // Wire: every Append transmission of this client carries the stable id; attempts are individually identified.
            IReadOnlyList<MessageAttempt> attempts = cluster.Network.GetMessageAttempts(appendRequests);
            Assert.NotEmpty(attempts);
            Assert.All(attempts, a => Assert.Equal(id, a.OperationId));
            int transmissionsOfFirstCall = attempts.Count - 1; // the last transmission is the explicit application retry
            var correlations = attempts.Select(a => a.CorrelationId).ToList();
            OpsHelpers.Log($"{fault}: {attempts.Count} Append transmissions, correlation ids [{string.Join(", ", correlations)}]");
            if (fault == "duplicated")
            {
                Assert.True(transmissionsOfFirstCall >= 2, "the duplicated request must appear twice on the wire");
                Assert.Equal(attempts[0].CorrelationId, attempts[1].CorrelationId);
            }
            else if (fault == "reply-dropped")
            {
                // The request executed; the SDK may learn the outcome by retransmitting the same id or by querying it.
                long queries = cluster.Network.GetMessageAttempts(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation, Direction: LinkDirection.Request))
                    .Count(a => a.OperationId == id);
                Assert.True(transmissionsOfFirstCall >= 2 || queries >= 1, "after a lost reply the SDK must retry or query the same operation id");
                Assert.Equal(attempts.Count, correlations.Distinct().Count());
            }
            else
            {
                Assert.True(transmissionsOfFirstCall >= 2, $"{fault}: the SDK must have retransmitted the append at least once");
                Assert.Equal(attempts.Count, correlations.Distinct().Count());
            }

            // Diagnostics: the mutation is traced under its stable id, and at most one commit token is attributed to it.
            IReadOnlyList<DfsDiagnosticEvent> events = diagnostics.Since(mark);
            List<DfsDiagnosticEvent> traced = events.Where(e => e.OperationId == id).ToList();
            Assert.NotEmpty(traced);
            List<CommitToken> tokens = traced.Where(e => e.Token is { IsNone: false }).Select(e => e.Token!.Value).Distinct().ToList();
            Assert.True(tokens.Count <= 1, $"the operation was accounted as succeeding with {tokens.Count} different commit tokens");
            if (tokens.Count == 1)
                Assert.Equal(token, tokens[0]);
            Assert.DoesNotContain(events, e => e.OperationId is { } other && other != id && e.Token is { IsNone: false } t && t == token);

            if (fault == "leader-crash-reroute")
            {
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }
        }, Ct);
    }
}
