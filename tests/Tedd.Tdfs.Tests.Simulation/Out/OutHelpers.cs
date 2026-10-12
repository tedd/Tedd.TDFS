using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Out;

/// <summary>Mutations whose retry/replay behaviour is exercised by the OUT simulation suite.</summary>
public enum RetryOp
{
    Append,
    WriteAll,
    Copy,
    Move,
    Delete,
    CreateDirectory,
}

/// <summary>The observable result of one mutation: its commit token and, where the API returns one, the inode it produced.</summary>
internal readonly record struct Fingerprint(CommitToken Token, InodeId? Inode);

/// <summary>Result of an attempted call: a value, or the exception it failed with.</summary>
internal readonly record struct Attempt<T>(T? Value, Exception? Error)
{
    public bool Succeeded => Error is null;

    /// <summary>History classification of the attempt (Ok / Failed / Unknown).</summary>
    public OpOutcome Classification => Error is null ? OpOutcome.Ok : HistoryRecorder.Classify(Error);

    public override string ToString() => Error is null ? $"Ok({Value})" : $"{Error.GetType().Name}: {Error.Message}";
}

/// <summary>
/// One mutation under a caller-supplied operation id: setup, invocation (variant 0 = the original payload), and checks
/// that the effect is present exactly once or not at all. Paths live under <see cref="Root"/>.
/// </summary>
internal sealed class RetryScenario
{
    public required RetryOp Op { get; init; }
    public required string Root { get; init; }
    public required DfsOperationKind Operation { get; init; }
    public required MessageType RequestType { get; init; }

    /// <summary>The path whose file/directory revision must equal the commit token after the single effect (null: not applicable).</summary>
    public string? RevisionPath { get; init; }

    public required Func<DfsVolume, CancellationToken, Task> Setup { get; init; }
    public required Func<DfsVolume, DfsOperationId, CancellationToken, Task<Fingerprint>> Invoke { get; init; }
    public required Func<DfsVolume, CancellationToken, Task> AssertAppliedOnce { get; init; }
    public required Func<DfsVolume, CancellationToken, Task> AssertNotApplied { get; init; }

    public override string ToString() => Op.ToString();
}

/// <summary>Shared helpers for the OUT simulation tests.</summary>
internal static class OutHelpers
{
    public const string Volume = "vol";
    public const string AlternatePolicy = "alt";

    /// <summary>Bounded liveness wait (virtual time in deterministic mode).</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    /// <summary>Fast timings with a small per-session outcome window so retirement is reachable in a few operations.</summary>
    public static NodeTimings SmallOutcomeWindow(int maxOutcomes = 4) => NodeTimings.Fast with { MaxOutcomesPerSession = maxOutcomes };

    /// <summary>Deterministic cluster: three zones (or <paramref name="nodes"/> one-per-zone nodes with up to five voters).</summary>
    public static TestClusterOptions Sim(int nodes = 3, NodeTimings? timings = null, int backups = 0, params string[] extraVolumes)
    {
        int voters = Math.Min(nodes, nodes >= 5 ? 5 : 3);
        var volumes = new List<TestVolumeSpec>
        {
            new(Volume)
            {
                Metadata = new DfsMetadataPolicy { Voters = voters },
                NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
                {
                    [AlternatePolicy] = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 },
                },
            },
        };
        foreach (string extra in extraVolumes)
            volumes.Add(new TestVolumeSpec(extra) { Metadata = new DfsMetadataPolicy { Voters = voters } });

        return TestClusterOptions.ThreeZones with
        {
            NodeCount = nodes,
            ControlVoters = voters,
            Volumes = volumes,
            Mode = ClusterMode.Deterministic,
            Timings = timings ?? NodeTimings.Fast,
            BackupNodeCount = backups,
        };
    }

    /// <summary>Connects a named client (same "admin" principal unless <paramref name="credentials"/> is given).</summary>
    public static async Task<(DfsClient Client, DfsVolume Volume)> ConnectAsync(TestCluster cluster, string name, CancellationToken ct,
        DfsCredentials? credentials = null, IReadOnlyList<string>? seedNodes = null, string volume = Volume, TimeSpan? operationTimeout = null)
    {
        DfsClient client = await cluster.ConnectAsync(new TestClientOptions
        {
            Name = name,
            Credentials = credentials,
            SeedNodes = seedNodes,
            OperationTimeout = operationTimeout,
        }, ct);
        return (client, client.GetVolume(volume));
    }

    public static string ClientName(string name) => "client:" + name;

    /// <summary>Runs <paramref name="action"/> and captures its value or exception (assertion failures propagate).</summary>
    public static async Task<Attempt<T>> TryAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return new Attempt<T>(await action(), null);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
        {
            return new Attempt<T>(default, ex);
        }
    }

    public static async Task<Attempt<T>> TryAsync<T>(Task<T> task)
    {
        try
        {
            return new Attempt<T>(await task, null);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
        {
            return new Attempt<T>(default, ex);
        }
    }

    public static async Task<Attempt<bool>> TryAsync(Func<Task> action)
    {
        try
        {
            await action();
            return new Attempt<bool>(true, null);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
        {
            return new Attempt<bool>(false, ex);
        }
    }

    /// <summary>Awaits a whole-file read and asserts its exact bytes.</summary>
    public static async Task ExpectContentAsync(byte[] expected, Task<byte[]> read, string because)
    {
        byte[] actual = await read;
        AssertEx.BytesEqual(expected, actual, because);
    }

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }

        return result;
    }

    /// <summary>Captures every matching fault-site hit (no other action).</summary>
    public static FaultArm Capture(TestCluster cluster, FaultMatch match, List<FaultContext> sink)
    {
        return cluster.Faults.Arm(match with { Count = int.MaxValue }, FaultAction.Custom(ctx =>
        {
            lock (sink)
                sink.Add(ctx);
            return Task.CompletedTask;
        }));
    }

    public static FaultContext[] Hits(List<FaultContext> sink)
    {
        lock (sink)
            return [.. sink];
    }

    /// <summary>Asserts that an outcome is definite (Committed or Aborted), as required after <c>resolve: true</c>.</summary>
    public static DfsOperationOutcome AssertDefinite(DfsOperationOutcome outcome, DfsOperationId id)
    {
        Assert.Equal(id, outcome.Id);
        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
            $"resolve:true must return a definite outcome for {id}, got {outcome.Status} ({outcome.Detail})");
        if (outcome.Status == DfsOutcomeStatus.Committed)
            Assert.False(outcome.Token.IsNone);
        return outcome;
    }

    /// <summary>Asserts the exception is a legal "the request did not succeed" answer after a definite abort.</summary>
    public static void AssertNotSuccess<T>(Attempt<T> attempt, string because)
    {
        Assert.False(attempt.Succeeded, $"{because}: the call succeeded with {attempt.Value}");
    }

    /// <summary>Waits until every running voter of the volume group has taken a Raft snapshot at or beyond <paramref name="index"/>.</summary>
    public static Task WaitForCompactionAsync(TestCluster cluster, ulong index, TimeSpan timeout, CancellationToken ct) =>
        cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            {
                GroupInspection? group = await node.Inspect.GetGroupAsync(Volume);
                if (group is null || group.SnapshotIndex < index)
                    return false;
            }

            return true;
        }, timeout, $"every running replica of '{Volume}' compacts its log beyond index {index}", ct);

    /// <summary>Commits <paramref name="count"/> small unrelated entries from <paramref name="volume"/> (a separate session).</summary>
    public static async Task GenerateLoadAsync(DfsVolume volume, int count, CancellationToken ct)
    {
        await volume.Directories.CreateDirectoryAsync("/load", ct);
        for (int i = 0; i < count; i++)
            await volume.Files.WriteAllBytesAsync($"/load/{i % 16}", TestData.Bytes(0x10AD_0000UL + (ulong)i, 48), ct);
    }

    /// <summary>Crashes every running live node, then restarts all of them from their durable images and waits for stability.</summary>
    public static async Task CrashAndRestartAllAsync(TestCluster cluster, CancellationToken ct)
    {
        foreach (TestNode node in cluster.Nodes.Where(n => n.State is TestNodeState.Running or TestNodeState.Paused))
            await node.CrashAsync();
        foreach (TestNode node in cluster.Nodes)
            await node.RestartAsync();
        await cluster.WaitForStableAsync(Bounded, ct);
    }

    /// <summary>A follower (not the current leader) of the volume group.</summary>
    public static async Task<TestNode> GetFollowerAsync(TestCluster cluster, CancellationToken ct, int skip = 0)
    {
        TestNode leader = await cluster.GetLeaderAsync(Volume, ct);
        GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Volume);
        return cluster.Nodes.Where(n => n.Name != leader.Name && group.Voters.Contains(n.Name)).Skip(skip).First();
    }

    // ------------------------------------------------------------------------------------------------------------
    // Retry scenarios
    // ------------------------------------------------------------------------------------------------------------

    public static readonly byte[] Prefix = TestData.Bytes(0x0B_0001, 777);
    public static readonly byte[] Suffix = TestData.Bytes(0x0B_0002, 555);
    public static readonly byte[] OtherSuffix = TestData.Bytes(0x0B_0003, 555);
    public static readonly byte[] ContentA = TestData.Bytes(0x0B_0004, 900);
    public static readonly byte[] ContentB = TestData.Bytes(0x0B_0005, 1300);

    public static RetryScenario Scenario(RetryOp op, string root = "/r")
    {
        switch (op)
        {
            case RetryOp.Append:
            {
                string log = root + "/log";
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.Append,
                    RequestType = MessageType.Append,
                    RevisionPath = log,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(log, Prefix, ct);
                    },
                    Invoke = async (v, id, ct) =>
                        new Fingerprint(await v.Files.AppendAllBytesAsync(log, Suffix, new DfsMutationOptions { OperationId = id }, ct), null),
                    AssertAppliedOnce = async (v, ct) => await ExpectContentAsync(Concat(Prefix, Suffix), v.Files.ReadAllBytesAsync(log, ct), "prefix + suffix exactly once"),
                    AssertNotApplied = async (v, ct) => await ExpectContentAsync(Prefix, v.Files.ReadAllBytesAsync(log, ct), "prefix only"),
                };
            }

            case RetryOp.WriteAll:
            {
                string path = root + "/f";
                InodeId inode = default;
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.WriteAll,
                    RequestType = MessageType.WriteAll,
                    RevisionPath = path,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(path, ContentA, ct);
                        inode = (await v.Files.GetInfoAsync(path, ct)).InodeId;
                    },
                    Invoke = async (v, id, ct) =>
                        new Fingerprint(await v.Files.WriteAllBytesAsync(path, ContentB, new DfsMutationOptions { OperationId = id }, ct), null),
                    AssertAppliedOnce = async (v, ct) =>
                    {
                        await ExpectContentAsync(ContentB, v.Files.ReadAllBytesAsync(path, ct), "replacement");
                        Assert.Equal(inode, (await v.Files.GetInfoAsync(path, ct)).InodeId);
                    },
                    AssertNotApplied = async (v, ct) => await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(path, ct), "original"),
                };
            }

            case RetryOp.Copy:
            {
                string src = root + "/src";
                string dst = root + "/dst";
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.Copy,
                    RequestType = MessageType.Copy,
                    RevisionPath = dst,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(src, ContentA, ct);
                    },
                    Invoke = async (v, id, ct) =>
                        new Fingerprint(await v.Files.CopyAsync(src, dst, false, new DfsMutationOptions { OperationId = id }, ct), null),
                    AssertAppliedOnce = async (v, ct) =>
                    {
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(dst, ct), "copy destination");
                        Assert.NotEqual((await v.Files.GetInfoAsync(src, ct)).InodeId, (await v.Files.GetInfoAsync(dst, ct)).InodeId);
                    },
                    AssertNotApplied = async (v, ct) => Assert.False(await v.Files.ExistsAsync(dst, ct)),
                };
            }

            case RetryOp.Move:
            {
                string a = root + "/a";
                string b = root + "/b";
                InodeId inode = default;
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.Move,
                    RequestType = MessageType.Move,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(a, ContentA, ct);
                        inode = (await v.Files.GetInfoAsync(a, ct)).InodeId;
                    },
                    Invoke = async (v, id, ct) =>
                        new Fingerprint(await v.Files.MoveAsync(a, b, false, new DfsMutationOptions { OperationId = id }, ct), null),
                    AssertAppliedOnce = async (v, ct) =>
                    {
                        Assert.False(await v.Files.ExistsAsync(a, ct));
                        Assert.Equal(inode, (await v.Files.GetInfoAsync(b, ct)).InodeId);
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(b, ct), "moved file");
                    },
                    AssertNotApplied = async (v, ct) =>
                    {
                        Assert.Equal(inode, (await v.Files.GetInfoAsync(a, ct)).InodeId);
                        Assert.False(await v.Files.ExistsAsync(b, ct));
                    },
                };
            }

            case RetryOp.Delete:
            {
                string victim = root + "/victim";
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.Delete,
                    RequestType = MessageType.Delete,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(victim, ContentA, ct);
                    },
                    Invoke = async (v, id, ct) =>
                        new Fingerprint(await v.Files.DeleteAsync(victim, new DfsMutationOptions { OperationId = id }, ct), null),
                    AssertAppliedOnce = async (v, ct) => Assert.False(await v.Files.ExistsAsync(victim, ct)),
                    AssertNotApplied = async (v, ct) => await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(victim, ct), "undeleted"),
                };
            }

            case RetryOp.CreateDirectory:
            {
                string dir = root + "/made";
                return new RetryScenario
                {
                    Op = op,
                    Root = root,
                    Operation = DfsOperationKind.CreateDirectory,
                    RequestType = MessageType.CreateDirectory,
                    Setup = async (v, ct) => await v.Directories.CreateDirectoryAsync(root, ct),
                    Invoke = async (v, id, ct) =>
                    {
                        DfsDirectoryInfo info = await v.Directories.CreateDirectoryAsync(dir, new DfsMutationOptions { OperationId = id }, ct);
                        return new Fingerprint(info.ObservedAt, info.InodeId);
                    },
                    AssertAppliedOnce = async (v, ct) =>
                    {
                        Assert.True(await v.Directories.ExistsAsync(dir, ct));
                        Assert.Single(await v.Directories.EnumerateAsync(root, null, ct).Where(e => e.Name == "made").ToListAsync(ct));
                    },
                    AssertNotApplied = async (v, ct) => Assert.False(await v.Directories.ExistsAsync(dir, ct)),
                };
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(op), op, null);
        }
    }

    /// <summary>
    /// Asserts the single committed effect of <paramref name="scenario"/>: its effect is present once, the operation id
    /// resolves to <paramref name="token"/>, and (for file results) the result inode was not modified after that commit.
    /// </summary>
    public static async Task AssertSingleCommittedEffectAsync(RetryScenario scenario, DfsVolume volume, DfsOperationId id, CommitToken token, CancellationToken ct)
    {
        await scenario.AssertAppliedOnce(volume, ct);
        DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: false, ct);
        Assert.Equal(id, outcome.Id);
        Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
        Assert.Equal(token, outcome.Token);
        if (scenario.RevisionPath is not null)
            Assert.Equal(token.Sequence, (await volume.Files.GetInfoAsync(scenario.RevisionPath, ct)).Revision);
    }

    /// <summary>True for a legal answer to a replay whose identity belongs to someone else (no disclosure, no replay).</summary>
    public static bool IsCrossPrincipalRejection(Exception ex) =>
        ex is UnauthorizedAccessException
        || ex is DfsOperationIdMismatchException
        || ex is DfsException { ErrorCode: DfsErrorCode.AccessDenied or DfsErrorCode.OperationIdMismatch };
}
