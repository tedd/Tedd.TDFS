using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Out;

/// <summary>
/// OUT — idempotency, cancellation and uncertain outcomes: SDK-level contracts (FSD §13, §6.4; INV-01, INV-02, INV-06,
/// INV-08). Cluster scenarios use the default three-zone in-memory cluster.
/// </summary>
public sealed class OutUnitTests
{
    public static TheoryData<MutatingApi> AllMutatingApis => new(OutHelpers.AllApis);

    public static TheoryData<MutatingApi> CallerIdApis =>
        new(OutHelpers.AllApis.Where(api => OutHelpers.CreateProbe(api).SupportsCallerId));

    public static TheoryData<MutatingApi, UnknownCause> UnknownOutcomeCases
    {
        get
        {
            var data = new TheoryData<MutatingApi, UnknownCause>();
            foreach (MutatingApi api in OutHelpers.UnknownOutcomeApis)
            {
                foreach (UnknownCause cause in Enum.GetValues<UnknownCause>())
                    data.Add(api, cause);
            }

            return data;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // OUT-001
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// OUT-001: the first transmission of every mutating API already carries a non-empty operation id; every
    /// retransmission and every server-side acceptance carries the same id; an uncertain result surfaces that id; and the
    /// id resolves to the durable outcome. The first reply is lost so the SDK must retry or report uncertainty.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllMutatingApis))]
    [Catalog("OUT-001", "U", "INV-06", "INV-02")]
    public Task OUT_001_OperationIdAssignedBeforeSend(MutatingApi api)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(OutHelpers.ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = OutHelpers.ShortOperationTimeout }, ct);
            DfsVolume volume = client.GetVolume(OutHelpers.Volume);
            MutationProbe probe = OutHelpers.CreateProbe(api);
            try
            {
                await probe.Setup(volume, ct);

                var sends = new List<FaultContext>();
                var accepts = new List<FaultContext>();
                FaultArm sendArm = OutHelpers.Capture(cluster, new FaultMatch(FaultSites.ClientRequestSend, Operation: probe.Operation), sends);
                FaultArm acceptArm = OutHelpers.Capture(cluster, new FaultMatch(FaultSites.RpcRequestAccepted, Operation: probe.Operation), accepts);
                FaultArm dropFirstReply = cluster.Faults.Arm(
                    new FaultMatch(FaultSites.RpcReplySend, Operation: probe.Operation, Occurrence: 1, Count: 1), FaultAction.DropReply());

                CommitToken? returned = null;
                DfsCommitOutcomeUnknownException? unknown = null;
                try
                {
                    returned = await probe.Invoke(volume, null, ct);
                }
                catch (DfsCommitOutcomeUnknownException ex)
                {
                    unknown = ex;
                }

                cluster.Faults.AssertReached(sendArm);
                cluster.Faults.AssertReached(acceptArm);
                cluster.Faults.AssertReached(dropFirstReply);
                cluster.Faults.DisarmAll();

                FaultContext[] clientHits = OutHelpers.Snapshot(sends);
                FaultContext first = clientHits[0];
                Assert.True(first.OperationId is { IsEmpty: false }, $"{api}: the first transmission must already carry an operation id.");
                DfsOperationId id = first.OperationId!.Value;
                Assert.All(clientHits, hit => Assert.Equal(id, hit.OperationId));
                FaultContext[] serverHits = OutHelpers.Snapshot(accepts).Where(h => h.OperationId is not null).ToArray();
                Assert.NotEmpty(serverHits);
                Assert.All(serverHits, hit => Assert.Equal(id, hit.OperationId));

                if (unknown is not null)
                {
                    Assert.Equal(DfsErrorCode.OutcomeUnknown, unknown.ErrorCode);
                    Assert.Equal(id, unknown.OperationId);
                    Assert.Equal(id, ((DfsException)unknown).OperationId);
                }

                // The id is caller-recoverable: its persisted text form resolves to the durable outcome.
                DfsOperationId recovered = DfsOperationId.Parse(id.ToString());
                Assert.Equal(id, recovered);
                DfsOperationOutcome outcome = await probe.ResolveAsync(volume, recovered, ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.False(outcome.Token.IsNone);
                if (returned is { IsNone: false } token)
                    Assert.Equal(token, outcome.Token);

                await probe.AssertApplied(volume, ct);
            }
            finally
            {
                await probe.Cleanup();
            }
        }, ct);
    }

    /// <summary>
    /// OUT-001 (caller-supplied identity): an id reserved through <see cref="DfsVolume.NewOperationIdAsync"/> and passed
    /// in the options is exactly the id of the first transmission, of the server acceptance and of the durable outcome.
    /// </summary>
    [Theory]
    [MemberData(nameof(CallerIdApis))]
    [Catalog("OUT-001", "U", "INV-06", "INV-02")]
    public Task OUT_001_OperationIdAssignedBeforeSend_CallerSuppliedId(MutatingApi api)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(OutHelpers.ThreeZones(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume volume = client.GetVolume(OutHelpers.Volume);
            MutationProbe probe = OutHelpers.CreateProbe(api);
            Assert.True(probe.SupportsCallerId);
            try
            {
                await probe.Setup(volume, ct);
                DfsOperationId reserved = await volume.NewOperationIdAsync(ct);
                Assert.False(reserved.IsEmpty);

                var sends = new List<FaultContext>();
                var accepts = new List<FaultContext>();
                FaultArm sendArm = OutHelpers.Capture(cluster, new FaultMatch(FaultSites.ClientRequestSend, Operation: probe.Operation), sends);
                FaultArm acceptArm = OutHelpers.Capture(cluster, new FaultMatch(FaultSites.RpcRequestAccepted, Operation: probe.Operation), accepts);

                CommitToken? returned = await probe.Invoke(volume, reserved, ct);

                cluster.Faults.AssertReached(sendArm);
                cluster.Faults.AssertReached(acceptArm);
                Assert.All(OutHelpers.Snapshot(sends), hit => Assert.Equal(reserved, hit.OperationId));
                FaultContext[] serverHits = OutHelpers.Snapshot(accepts).Where(h => h.OperationId is not null).ToArray();
                Assert.NotEmpty(serverHits);
                Assert.All(serverHits, hit => Assert.Equal(reserved, hit.OperationId));

                DfsOperationOutcome outcome = await volume.QueryOperationAsync(reserved, resolve: false, ct);
                Assert.Equal(reserved, outcome.Id);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                if (returned is { IsNone: false } token)
                    Assert.Equal(token, outcome.Token);
                await probe.AssertApplied(volume, ct);
            }
            finally
            {
                await probe.Cleanup();
            }
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OUT-015
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// OUT-015: for every way an outcome can become unknown after the server applied the mutation, the surfaced
    /// <see cref="DfsCommitOutcomeUnknownException"/> carries the operation id (and the transaction id for transaction
    /// commits) and preserves the underlying cause; the carried identity resolves to the committed outcome and the effect
    /// exists exactly once. Cancellation after apply must never surface as a definite <see cref="OperationCanceledException"/>.
    /// </summary>
    [Theory]
    [MemberData(nameof(UnknownOutcomeCases))]
    [Catalog("OUT-015", "U", "INV-06", "INV-08")]
    public Task OUT_015_UnknownExceptionCarriesIdentity(MutatingApi api, UnknownCause cause)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(OutHelpers.ThreeZones(), async cluster =>
        {
            const string clientName = "c0";
            await using DfsClient client = await cluster.ConnectAsync(
                new TestClientOptions { Name = clientName, OperationTimeout = OutHelpers.ShortOperationTimeout }, ct);
            DfsVolume volume = client.GetVolume(OutHelpers.Volume);
            MutationProbe probe = OutHelpers.CreateProbe(api);
            try
            {
                await probe.Setup(volume, ct);
                DfsOperationId? callerId = probe.SupportsCallerId ? await volume.NewOperationIdAsync(ct) : null;
                string marker = $"injected-cause-{api}-{cause}";
                using var callerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

                FaultArm arm = cause switch
                {
                    UnknownCause.ServerReplyDropped => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.RpcReplySend, Operation: probe.Operation, Count: int.MaxValue), FaultAction.DropReply()),
                    UnknownCause.ConnectionResetDuringReply => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.RpcReplySend, Operation: probe.Operation, Count: int.MaxValue),
                        FaultAction.Custom(_ =>
                        {
                            cluster.Network.ResetConnections("client:" + clientName);
                            throw new IOException("reply transmission interrupted by connection reset");
                        })),
                    UnknownCause.ClientReceivePathFailed => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.ClientReplyReceived, Operation: probe.Operation, Count: int.MaxValue),
                        FaultAction.Throw(() => new IOException(marker))),
                    UnknownCause.CallerCancelledAfterApply => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.ApplyAfterBeforeReply, Operation: probe.Operation, Count: 1),
                        FaultAction.Custom(_ =>
                        {
                            callerCts.Cancel();
                            return Task.CompletedTask;
                        })),
                    _ => throw new ArgumentOutOfRangeException(nameof(cause), cause, null),
                };

                CommitToken? returned = null;
                Exception? failure = null;
                try
                {
                    returned = await probe.Invoke(volume, callerId, callerCts.Token);
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
                {
                    failure = ex;
                }

                cluster.Faults.AssertReached(arm);
                cluster.Faults.DisarmAll();

                DfsOperationId? resolveId;
                if (failure is null)
                {
                    // Only legal for a caller cancelled after apply: completion may win the race, but then it must be a
                    // real, resolvable commit (checked below through the caller id, or through the effect itself).
                    Assert.Equal(UnknownCause.CallerCancelledAfterApply, cause);
                    resolveId = callerId;
                }
                else
                {
                    Assert.False(failure is OperationCanceledException,
                        $"cancellation after the server applied the mutation must not be reported as a definite cancel: {failure}");
                    DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(failure);
                    Assert.Equal(DfsErrorCode.OutcomeUnknown, unknown.ErrorCode);
                    Assert.False(unknown.OperationId.IsEmpty, "an unknown outcome must carry its operation id");
                    Assert.Equal(unknown.OperationId, ((DfsException)unknown).OperationId);
                    if (callerId is not null)
                        Assert.Equal(callerId.Value, unknown.OperationId);
                    Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(unknown));
                    Assert.False(string.IsNullOrWhiteSpace(unknown.Message));

                    if (probe.IsTransactionCommit)
                    {
                        Assert.NotNull(unknown.TransactionId);
                        Assert.Equal(probe.TransactionId(), unknown.TransactionId);
                    }
                    else
                    {
                        Assert.Null(unknown.TransactionId);
                    }

                    // The original cause is preserved.
                    Assert.NotNull(unknown.InnerException);
                    switch (cause)
                    {
                        case UnknownCause.ServerReplyDropped:
                            Assert.True(OutHelpers.ChainContains(unknown, e => e is TimeoutException or DfsException { ErrorCode: DfsErrorCode.Timeout }),
                                $"lost replies must be preserved as a timeout cause: {unknown}");
                            break;
                        case UnknownCause.ConnectionResetDuringReply:
                            Assert.True(OutHelpers.ChainContains(unknown, e => e is RpcConnectionException || (e is IOException && e is not DfsException)),
                                $"a reset connection must be preserved as the transport cause: {unknown}");
                            break;
                        case UnknownCause.ClientReceivePathFailed:
                            Assert.True(OutHelpers.ChainContains(unknown, e => e is IOException && e.Message == marker),
                                $"the injected receive-path exception must be preserved: {unknown}");
                            break;
                        case UnknownCause.CallerCancelledAfterApply:
                            Assert.True(OutHelpers.ChainContains(unknown, e => e is OperationCanceledException),
                                $"the cancellation must be preserved as the cause, not reported as a definite cancel: {unknown}");
                            break;
                    }

                    resolveId = unknown.OperationId;
                }

                // The carried identity is sufficient for later resolution by a fresh client (no in-memory SDK state).
                await using DfsClient resolver = await cluster.ConnectAsync(new TestClientOptions { Name = "resolver" }, ct);
                DfsVolume resolverVolume = resolver.GetVolume(OutHelpers.Volume);
                DfsOperationOutcome? outcome = probe.IsTransactionCommit
                    ? await resolverVolume.Transactions.QueryOutcomeAsync(probe.TransactionId()!.Value, resolve: false, ct)
                    : resolveId is { } id ? await resolverVolume.QueryOperationAsync(id, resolve: false, ct) : null;
                if (outcome is not null)
                {
                    Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                    Assert.False(outcome.Token.IsNone);
                    if (returned is { IsNone: false } token)
                        Assert.Equal(token, outcome.Token);
                }

                await probe.AssertApplied(resolverVolume, ct);
            }
            finally
            {
                await probe.Cleanup();
            }
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // OUT-018
    // ------------------------------------------------------------------------------------------------------------

    public enum TokenDifference
    {
        Cluster,
        Volume,
        Epoch,
        ClusterAndVolume,
        VolumeAndEpoch,
        LeftNone,
        RightNone,
        BothNone,
    }

    public static TheoryData<TokenDifference, long> TokenDifferences
    {
        get
        {
            var data = new TheoryData<TokenDifference, long>();
            foreach (TokenDifference difference in Enum.GetValues<TokenDifference>())
            {
                // The sequence of the second token is below, equal to and above the first: none of them may be ordered.
                foreach (long delta in new long[] { -1, 0, 1, 1_000_000 })
                    data.Add(difference, delta);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-018 (value contract): tokens from different clusters, volumes or epochs (and the None token) are never
    /// comparable, whatever their sequence numbers; comparisons throw instead of comparing sequences, in both directions.
    /// </summary>
    [Theory]
    [MemberData(nameof(TokenDifferences))]
    [Catalog("OUT-018", "U", "INV-06", "INV-08")]
    public void OUT_018_CommitTokensNotGloballyOrdered(TokenDifference difference, long sequenceDelta)
    {
        var clusterA = new Guid("6f0b4d2a-1a11-4c3e-9d2c-000000000001");
        var clusterB = new Guid("6f0b4d2a-1a11-4c3e-9d2c-000000000002");
        var volumeA = new VolumeId(new Guid("9a3e0c71-77aa-4f5b-8e10-00000000000a"));
        var volumeB = new VolumeId(new Guid("9a3e0c71-77aa-4f5b-8e10-00000000000b"));
        const ulong baseSequence = 5_000;
        ulong otherSequence = (ulong)((long)baseSequence + sequenceDelta);

        var left = new CommitToken(clusterA, volumeA, 3, baseSequence);
        CommitToken right = difference switch
        {
            TokenDifference.Cluster => new CommitToken(clusterB, volumeA, 3, otherSequence),
            TokenDifference.Volume => new CommitToken(clusterA, volumeB, 3, otherSequence),
            TokenDifference.Epoch => new CommitToken(clusterA, volumeA, 4, otherSequence),
            TokenDifference.ClusterAndVolume => new CommitToken(clusterB, volumeB, 3, otherSequence),
            TokenDifference.VolumeAndEpoch => new CommitToken(clusterA, volumeB, 2, otherSequence),
            TokenDifference.LeftNone or TokenDifference.BothNone => CommitToken.None,
            TokenDifference.RightNone => CommitToken.None,
            _ => throw new ArgumentOutOfRangeException(nameof(difference)),
        };
        if (difference == TokenDifference.LeftNone)
            (left, right) = (CommitToken.None, new CommitToken(clusterA, volumeA, 3, otherSequence));
        if (difference == TokenDifference.BothNone)
            left = CommitToken.None;

        Assert.False(left.IsComparableWith(right));
        Assert.False(right.IsComparableWith(left));
        Assert.Throws<ArgumentException>(() => left.CompareTo(right));
        Assert.Throws<ArgumentException>(() => right.CompareTo(left));
        Assert.Throws<ArgumentException>(() => left.IsAtOrAfter(right));
        Assert.Throws<ArgumentException>(() => right.IsAtOrAfter(left));
        if (difference != TokenDifference.BothNone)
            Assert.NotEqual(left, right);

        // Every identity component survives the persisted text form, so a stored token cannot silently lose its history.
        foreach (CommitToken token in new[] { left, right })
        {
            CommitToken parsed = CommitToken.Parse(token.ToString());
            Assert.Equal(token, parsed);
            Assert.Equal(token.ClusterId, parsed.ClusterId);
            Assert.Equal(token.VolumeId, parsed.VolumeId);
            Assert.Equal(token.VolumeEpoch, parsed.VolumeEpoch);
        }
    }

    /// <summary>OUT-018 (control): tokens of one history are ordered by commit sequence, consistently in both directions.</summary>
    [Theory]
    [InlineData(1UL, 2UL)]
    [InlineData(7UL, 7UL)]
    [InlineData(100UL, 99UL)]
    [InlineData(0UL, ulong.MaxValue)]
    [Catalog("OUT-018", "U", "INV-06")]
    public void OUT_018_CommitTokensNotGloballyOrdered_SameHistoryIsOrdered(ulong a, ulong b)
    {
        var cluster = new Guid("1f6c3a90-2b3e-4f9a-8c71-0000000000c1");
        var volume = new VolumeId(new Guid("1f6c3a90-2b3e-4f9a-8c71-0000000000f1"));
        var ta = new CommitToken(cluster, volume, 1, a);
        var tb = new CommitToken(cluster, volume, 1, b);
        // Sequence 0 with real identities is not None; only the all-zero token is.
        Assert.False(ta.IsNone);
        Assert.False(tb.IsNone);

        Assert.True(ta.IsComparableWith(tb));
        Assert.Equal(Math.Sign(a.CompareTo(b)), Math.Sign(ta.CompareTo(tb)));
        Assert.Equal(-Math.Sign(ta.CompareTo(tb)), Math.Sign(tb.CompareTo(ta)));
        Assert.Equal(a >= b, ta.IsAtOrAfter(tb));
        Assert.Equal(b >= a, tb.IsAtOrAfter(ta));
    }

    /// <summary>
    /// OUT-018 (live tokens): tokens returned by two volumes of one cluster, and by a volume restored from a snapshot, are
    /// not comparable; tokens of one volume are ordered by commit; and APIs taking a token boundary reject a token from
    /// another volume or epoch instead of comparing sequence integers (no snapshot is created).
    /// </summary>
    [Fact]
    [Catalog("OUT-018", "U", "INV-06", "INV-08")]
    public Task OUT_018_CommitTokensNotGloballyOrdered_LiveVolumesAndEpochs()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec("vol"), new TestVolumeSpec("other")] };
        return ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume("vol");
            DfsVolume other = client.GetVolume("other");

            CommitToken a1 = await vol.Files.WriteAllBytesAsync("/t", TestData.Bytes(18, 64), ct);
            // Advance "other" far beyond "vol" so a sequence-only comparison would claim b > a.
            CommitToken b1 = CommitToken.None;
            for (int i = 0; i < 5; i++)
                b1 = await other.Files.WriteAllBytesAsync("/t" + i, TestData.Bytes(1800 + (ulong)i, 64), ct);
            CommitToken a2 = await vol.Files.WriteAllBytesAsync("/t", TestData.Bytes(19, 64), ct);

            Assert.Equal(cluster.ClusterId, a1.ClusterId);
            Assert.Equal(cluster.ClusterId, b1.ClusterId);
            Assert.NotEqual(a1.VolumeId, b1.VolumeId);
            Assert.False(a1.IsComparableWith(b1));
            Assert.Throws<ArgumentException>(() => a1.CompareTo(b1));
            Assert.Throws<ArgumentException>(() => b1.IsAtOrAfter(a1));
            Assert.True(a2.IsComparableWith(a1));
            Assert.True(a2.CompareTo(a1) > 0, "later commit on the same volume must order after the earlier one");

            // A restored volume is a new history (new volume identity / epoch).
            await vol.Snapshots.CreateAsync("s1", null, ct);
            DfsVolumeInfo restoredInfo = await vol.Snapshots.RestoreAsync("s1", "restored", ct);
            DfsVolume restored = client.GetVolume("restored");
            CommitToken r1 = await restored.Files.WriteAllBytesAsync("/t", TestData.Bytes(20, 64), ct);
            Assert.Equal(restoredInfo.Id, r1.VolumeId);
            Assert.NotEqual(a2.VolumeId, r1.VolumeId);
            Assert.False(r1.IsComparableWith(a2));
            Assert.Throws<ArgumentException>(() => r1.CompareTo(a2));

            // Token-boundary APIs must reject foreign histories rather than compare sequences.
            CommitToken forgedEpoch = a2 with { VolumeEpoch = a2.VolumeEpoch + 1 };
            CommitToken forgedCluster = a2 with { ClusterId = new Guid("00000000-0000-0000-0000-00000000dead") };
            (string Name, CommitToken Token)[] foreign =
            [
                ("min-other-volume", b1),
                ("min-restored-volume", r1),
                ("min-forged-epoch", forgedEpoch),
                ("min-forged-cluster", forgedCluster),
            ];
            foreach ((string name, CommitToken token) in foreign)
            {
                using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
                bounded.CancelAfter(OutHelpers.Bounded);
                Exception? rejected = null;
                try
                {
                    await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { MinimumCommit = token }, bounded.Token);
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
                {
                    rejected = ex;
                }

                Assert.NotNull(rejected);
                bool legal = rejected is ArgumentException
                    || rejected is DfsException { ErrorCode: DfsErrorCode.InvalidArgument or DfsErrorCode.CrossVolume or DfsErrorCode.EpochMismatch or DfsErrorCode.ClusterMismatch };
                Assert.True(legal, $"{name}: a foreign MinimumCommit must be rejected as invalid/cross-volume/epoch mismatch, got {rejected}");
                Assert.DoesNotContain(await vol.Snapshots.ListAsync(ct), s => s.Name == name);
            }
        }, ct);
    }
}
