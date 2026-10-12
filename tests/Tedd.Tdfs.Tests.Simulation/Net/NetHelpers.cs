using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Con.ConHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Net;

/// <summary>
/// One mutating SDK operation with an explicit operation identity, its wire request type and independent oracles for
/// "applied exactly once" and "not applied" (used by NET-001/002/008/017 and the NET-020 campaign).
/// </summary>
internal sealed class MutationCase
{
    public required string Name { get; init; }

    /// <summary>The request message that carries the mutation (the one that faults target).</summary>
    public required MessageType RequestType { get; init; }

    public required DfsOperationKind Operation { get; init; }

    /// <summary>True when <see cref="InvokeAsync"/> returns the operation's commit token (comparable with its outcome token).</summary>
    public bool ReturnsCommitToken { get; init; } = true;

    /// <summary>True when an application may retry the same identity with the same payload through the public API.</summary>
    public bool SupportsRetry { get; init; } = true;

    public required Func<DfsVolume, Task> SetupAsync { get; init; }

    /// <summary>Performs the mutation with the identity; returns its commit token (or <see cref="CommitToken.None"/>).</summary>
    public required Func<DfsVolume, DfsOperationId, CancellationToken, Task<CommitToken>> InvokeAsync { get; init; }

    /// <summary>The same identity with a different payload (must be rejected), or null when not expressible.</summary>
    public Func<DfsVolume, DfsOperationId, Task>? InvokeChangedAsync { get; init; }

    public required Func<DfsVolume, Task> AssertAppliedOnceAsync { get; init; }

    public required Func<DfsVolume, Task> AssertNotAppliedAsync { get; init; }

    /// <summary>Queries the authoritative outcome of the identity.</summary>
    public Func<DfsVolume, DfsOperationId, bool, Task<DfsOperationOutcome>> QueryAsync { get; init; } =
        (volume, id, resolve) => volume.QueryOperationAsync(id, resolve, Ct);

    public override string ToString() => Name;
}

/// <summary>Shared helpers for the NET simulation suite.</summary>
internal static class NetHelpers
{
    public static readonly string[] MutationNames =
        ["write-all", "append", "create-directory", "move", "delete", "copy", "stream-commit", "tx-commit", "create-snapshot"];

    public static TheoryData<string> MutationTheory()
    {
        var data = new TheoryData<string>();
        foreach (string name in MutationNames)
            data.Add(name);
        return data;
    }

    /// <summary>Builds a fresh mutation case (each case owns its state; create one per scenario).</summary>
    public static MutationCase CreateMutation(string name)
    {
        switch (name)
        {
            case "write-all":
            {
                byte[] old = TestData.Bytes(0x5001, 20_000);
                byte[] updated = TestData.Bytes(0x5002, 30_000);
                InodeId inode = default;
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.WriteAll,
                    Operation = DfsOperationKind.WriteAll,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/wa", old, Ct);
                        inode = (await v.Files.GetInfoAsync("/wa", Ct)).InodeId;
                    },
                    InvokeAsync = (v, id, ct) => v.Files.WriteAllBytesAsync("/wa", updated, WithId(id), ct),
                    InvokeChangedAsync = (v, id) => v.Files.WriteAllBytesAsync("/wa", TestData.Bytes(0x5003, 30_000), WithId(id), Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        await AssertContentAsync(v, "/wa", updated, "WriteAll applied");
                        Assert.Equal(inode, (await v.Files.GetInfoAsync("/wa", Ct)).InodeId);
                    },
                    AssertNotAppliedAsync = async v =>
                    {
                        await AssertContentAsync(v, "/wa", old, "WriteAll not applied");
                        Assert.Equal(inode, (await v.Files.GetInfoAsync("/wa", Ct)).InodeId);
                    },
                };
            }

            case "append":
            {
                byte[] prefix = TestData.Bytes(0x5011, 1000);
                byte[] suffix = TestData.Bytes(0x5012, 2000);
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.Append,
                    Operation = DfsOperationKind.Append,
                    SetupAsync = v => v.Files.WriteAllBytesAsync("/ap", prefix, Ct),
                    InvokeAsync = (v, id, ct) => v.Files.AppendAllBytesAsync("/ap", suffix, WithId(id), ct),
                    InvokeChangedAsync = (v, id) => v.Files.AppendAllBytesAsync("/ap", TestData.Bytes(0x5013, 2000), WithId(id), Ct),
                    AssertAppliedOnceAsync = v => AssertContentAsync(v, "/ap", Concat(prefix, suffix), "append applied exactly once"),
                    AssertNotAppliedAsync = v => AssertContentAsync(v, "/ap", prefix, "append not applied"),
                };
            }

            case "create-directory":
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.CreateDirectory,
                    Operation = DfsOperationKind.CreateDirectory,
                    ReturnsCommitToken = false,
                    SetupAsync = _ => Task.CompletedTask,
                    InvokeAsync = async (v, id, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync("/cd/sub", WithId(id), ct);
                        return CommitToken.None;
                    },
                    InvokeChangedAsync = (v, id) => v.Directories.CreateDirectoryAsync("/cd/other", WithId(id), Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        Assert.True(await v.Directories.ExistsAsync("/cd/sub", Ct));
                        var names = new List<string>();
                        await foreach (DfsEntryInfo entry in v.Directories.EnumerateAsync("/cd", null, Ct))
                            names.Add(entry.Name);
                        Assert.Equal(["sub"], names);
                    },
                    AssertNotAppliedAsync = async v => Assert.False(await v.Directories.ExistsAsync("/cd", Ct)),
                };

            case "move":
            {
                byte[] bytes = TestData.Bytes(0x5021, 7000);
                InodeId inode = default;
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.Move,
                    Operation = DfsOperationKind.Move,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/mv-src", bytes, Ct);
                        inode = (await v.Files.GetInfoAsync("/mv-src", Ct)).InodeId;
                    },
                    InvokeAsync = (v, id, ct) => v.Files.MoveAsync("/mv-src", "/mv-dst", false, WithId(id), ct),
                    InvokeChangedAsync = (v, id) => v.Files.MoveAsync("/mv-src", "/mv-other", false, WithId(id), Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        Assert.False(await v.Files.ExistsAsync("/mv-src", Ct));
                        Assert.False(await v.Files.ExistsAsync("/mv-other", Ct));
                        await AssertContentAsync(v, "/mv-dst", bytes, "moved file");
                        Assert.Equal(inode, (await v.Files.GetInfoAsync("/mv-dst", Ct)).InodeId);
                    },
                    AssertNotAppliedAsync = async v =>
                    {
                        await AssertContentAsync(v, "/mv-src", bytes, "move not applied");
                        Assert.False(await v.Files.ExistsAsync("/mv-dst", Ct));
                    },
                };
            }

            case "delete":
            {
                byte[] bytes = TestData.Bytes(0x5031, 3000);
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.Delete,
                    Operation = DfsOperationKind.Delete,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/del", bytes, Ct);
                        await v.Files.WriteAllBytesAsync("/del-other", bytes, Ct);
                    },
                    InvokeAsync = (v, id, ct) => v.Files.DeleteAsync("/del", WithId(id), ct),
                    InvokeChangedAsync = (v, id) => v.Files.DeleteAsync("/del-other", WithId(id), Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        Assert.False(await v.Files.ExistsAsync("/del", Ct));
                        await AssertContentAsync(v, "/del-other", bytes, "unrelated file untouched");
                    },
                    AssertNotAppliedAsync = v => AssertContentAsync(v, "/del", bytes, "delete not applied"),
                };
            }

            case "copy":
            {
                byte[] bytes = TestData.Bytes(0x5041, 40_000);
                InodeId source = default;
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.Copy,
                    Operation = DfsOperationKind.Copy,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/cp-src", bytes, Ct);
                        source = (await v.Files.GetInfoAsync("/cp-src", Ct)).InodeId;
                    },
                    InvokeAsync = (v, id, ct) => v.Files.CopyAsync("/cp-src", "/cp-dst", false, WithId(id), ct),
                    InvokeChangedAsync = (v, id) => v.Files.CopyAsync("/cp-src", "/cp-dst2", false, WithId(id), Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        await AssertContentAsync(v, "/cp-dst", bytes, "copy applied");
                        await AssertContentAsync(v, "/cp-src", bytes, "copy source unchanged");
                        Assert.NotEqual(source, (await v.Files.GetInfoAsync("/cp-dst", Ct)).InodeId);
                        Assert.False(await v.Files.ExistsAsync("/cp-dst2", Ct));
                    },
                    AssertNotAppliedAsync = async v =>
                    {
                        Assert.False(await v.Files.ExistsAsync("/cp-dst", Ct));
                        await AssertContentAsync(v, "/cp-src", bytes, "copy source unchanged");
                    },
                };
            }

            case "stream-commit":
            {
                byte[] old = TestData.Bytes(0x5051, 10_000);
                byte[] updated = TestData.Bytes(0x5052, 140_000);
                InodeId inode = default;
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.CommitFile,
                    Operation = DfsOperationKind.CommitFile,
                    SupportsRetry = false,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/sc", old, Ct);
                        inode = (await v.Files.GetInfoAsync("/sc", Ct)).InodeId;
                    },
                    InvokeAsync = async (v, id, ct) =>
                    {
                        DfsStream stream = await v.Files.OpenAsync("/sc", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, ct);
                        try
                        {
                            await stream.WriteAsync(updated, ct);
                            await stream.SetLengthAsync(updated.Length, ct);
                            return await stream.CommitAsync(WithId(id), ct);
                        }
                        catch
                        {
                            await CaptureAsync(() => stream.AbortAsync(Ct));
                            throw;
                        }
                        finally
                        {
                            await CaptureAsync(async () => await stream.DisposeAsync());
                        }
                    },
                    AssertAppliedOnceAsync = async v =>
                    {
                        await AssertContentAsync(v, "/sc", updated, "stream commit applied");
                        Assert.Equal(inode, (await v.Files.GetInfoAsync("/sc", Ct)).InodeId);
                    },
                    AssertNotAppliedAsync = v => AssertContentAsync(v, "/sc", old, "stream commit not applied"),
                };
            }

            case "tx-commit":
            {
                byte[] a0 = TestData.Bytes(0x5061, 500);
                byte[] b0 = TestData.Bytes(0x5062, 500);
                byte[] a1 = TestData.Bytes(0x5063, 600);
                byte[] b1 = TestData.Bytes(0x5064, 70_000);
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.TxCommit,
                    Operation = DfsOperationKind.TxCommit,
                    SupportsRetry = false,
                    SetupAsync = async v =>
                    {
                        await v.Files.WriteAllBytesAsync("/t1", a0, Ct);
                        await v.Files.WriteAllBytesAsync("/t2", b0, Ct);
                    },
                    InvokeAsync = async (v, id, ct) =>
                    {
                        DfsTransaction tx = await v.Transactions.BeginAsync(new DfsTransactionOptions { OperationId = id }, ct);
                        try
                        {
                            await tx.Files.WriteAllBytesAsync("/t1", a1, ct);
                            await tx.Files.WriteAllBytesAsync("/t2", b1, ct);
                            return await tx.CommitAsync(ct);
                        }
                        finally
                        {
                            // Disposal after an uncertain commit only requests an abort; a failure there must not mask the
                            // commit's own outcome report, which is what the caller is held to.
                            await CaptureAsync(async () => await tx.DisposeAsync());
                        }
                    },
                    QueryAsync = (v, id, resolve) => v.Transactions.QueryOutcomeAsync(new DfsTransactionId(id), resolve, Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        await AssertContentAsync(v, "/t1", a1, "transaction applied (t1)");
                        await AssertContentAsync(v, "/t2", b1, "transaction applied (t2)");
                    },
                    AssertNotAppliedAsync = async v =>
                    {
                        await AssertContentAsync(v, "/t1", a0, "transaction not applied (t1)");
                        await AssertContentAsync(v, "/t2", b0, "transaction not applied (t2)");
                    },
                };
            }

            case "create-snapshot":
            {
                byte[] bytes = TestData.Bytes(0x5071, 9000);
                return new MutationCase
                {
                    Name = name,
                    RequestType = MessageType.CreateSnapshot,
                    Operation = DfsOperationKind.CreateSnapshot,
                    ReturnsCommitToken = false,
                    SetupAsync = v => v.Files.WriteAllBytesAsync("/snapfile", bytes, Ct),
                    InvokeAsync = async (v, id, ct) =>
                    {
                        await v.Snapshots.CreateAsync("net-snap", new DfsSnapshotOptions { OperationId = id }, ct);
                        return CommitToken.None;
                    },
                    InvokeChangedAsync = (v, id) => v.Snapshots.CreateAsync("net-snap-2", new DfsSnapshotOptions { OperationId = id }, Ct),
                    AssertAppliedOnceAsync = async v =>
                    {
                        IReadOnlyList<DfsSnapshotInfo> list = await v.Snapshots.ListAsync(Ct);
                        Assert.Single(list, s => s.Name == "net-snap");
                        Assert.DoesNotContain(list, s => s.Name == "net-snap-2");
                        await using DfsSnapshotView view = await v.Snapshots.OpenAsync("net-snap", Ct);
                        await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync("/snapfile", Ct), "snapshot content");
                    },
                    AssertNotAppliedAsync = async v =>
                        await AssertEx.ThrowsDfsAsync<DfsException>(() => v.Snapshots.GetAsync("net-snap", Ct), DfsErrorCode.SnapshotNotFound),
                };
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown mutation case.");
        }
    }

    /// <summary>
    /// Checks one attempt of <paramref name="mutation"/> with identity <paramref name="id"/>: a reported success must be
    /// a committed outcome with the same token and exactly one effect; a failure is resolved authoritatively and the state
    /// must match the resolution (committed → exactly once, aborted → untouched). A definite failure (anything but an
    /// explicit unknown outcome, including a cancellation) is a claim that the operation did not take effect, so it must
    /// resolve as aborted (FSD §13: once publication is potentially committed, unknown takes precedence). Returns the final
    /// outcome.
    /// </summary>
    public static async Task<DfsOperationOutcome> AssertExactlyOnceAsync(MutationCase mutation, DfsVolume volume, DfsOperationId id,
        CommitToken? returned, Exception? error)
    {
        if (error is null)
        {
            // An acknowledged outcome may already be retired (the client's acknowledgement watermark or the retention
            // period, ARCHITECTURE §5.2): Expired is then the truthful answer; it must never be Aborted or NotFound.
            DfsOperationOutcome outcome = await mutation.QueryAsync(volume, id, false);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired,
                $"{mutation}: success was reported but the outcome is {outcome.Status}");
            if (outcome.Status == DfsOutcomeStatus.Committed && mutation.ReturnsCommitToken && returned is { } token)
                Assert.Equal(outcome.Token, token);
            await mutation.AssertAppliedOnceAsync(volume);
            return outcome;
        }

        DfsOperationOutcome resolved = await mutation.QueryAsync(volume, id, true);
        Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
            $"{mutation}: resolution must be definite, got {resolved.Status}");
        if (!IsUnknownOutcome(error))
            Assert.True(resolved.Status == DfsOutcomeStatus.Aborted,
                $"{mutation}: a definite failure ({error.GetType().Name}: {error.Message}) was reported for an operation that committed; it had to be reported as unknown");
        if (resolved.Status == DfsOutcomeStatus.Committed)
            await mutation.AssertAppliedOnceAsync(volume);
        else
            await mutation.AssertNotAppliedAsync(volume);
        return resolved;
    }

    /// <summary>
    /// Asserts the FSD §13 surface of a mutation whose request was transmitted without a decision reaching the caller: an
    /// explicit unknown outcome carrying the identity (the transaction identity for a transaction commit).
    /// </summary>
    public static void AssertUnknownWithIdentity(MutationCase mutation, Exception? error, DfsOperationId id, string because)
    {
        Assert.True(error is not null, $"{mutation}: {because}: success was reported although no decision reached the client");
        Assert.True(IsUnknownOutcome(error!), $"{mutation}: {because}: expected DfsCommitOutcomeUnknownException, got {error!.GetType().FullName}: {error.Message}");
        if (error is DfsCommitOutcomeUnknownException unknown)
        {
            if (mutation.Operation == DfsOperationKind.TxCommit)
                Assert.Equal(new DfsTransactionId(id), unknown.TransactionId);
            else
                Assert.Equal(id, unknown.OperationId);
        }
    }

    /// <summary>Asserts an error is a typed, explicit failure (never a raw/untyped exception).</summary>
    public static void AssertTypedFailure(Exception error, string because) =>
        Assert.True(error is DfsException or IOException or TimeoutException or OperationCanceledException or UnauthorizedAccessException,
            $"{because}: unexpected exception type {error.GetType().FullName}: {error.Message}");

    /// <summary>Reads a stream to its end with repeated calls; returns the bytes and the per-call segments.</summary>
    public static async Task<(byte[] All, List<(long Offset, byte[] Bytes)> Calls)> ReadAllCallsAsync(Stream stream, int bufferSize, CancellationToken ct)
    {
        var all = new MemoryStream();
        var calls = new List<(long Offset, byte[] Bytes)>();
        var buffer = new byte[bufferSize];
        while (true)
        {
            long offset = stream.Position;
            int read = await stream.ReadAsync(buffer, ct);
            if (read == 0)
                return (all.ToArray(), calls);
            calls.Add((offset, buffer.AsSpan(0, read).ToArray()));
            all.Write(buffer, 0, read);
        }
    }

    /// <summary>The objects referenced by a file's extents on the leader.</summary>
    public static async Task<IReadOnlyList<ExtentInfo>> ExtentsAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.NotNull(file);
        return file!.Extents.Select(e => new ExtentInfo(e.Object, e.ObjectLength)).ToArray();
    }

    /// <summary>
    /// Asserts that every object referenced by a committed file is durably held by at least the policy's acknowledgement
    /// copies in its acknowledgement zones (the "no false acknowledgement" oracle, INV-08/INV-11).
    /// </summary>
    public static async Task AssertDurablyPlacedAsync(TestCluster cluster, string path, string because)
    {
        DfsStoragePolicy policy = cluster.Options.GetEffectiveDefaultPolicy();
        foreach (ExtentInfo extent in await ExtentsAsync(cluster, path))
        {
            (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
            Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck,
                $"{because}: object {extent.Object} of {path} has {copies} copies in {zones} zones; policy requires {policy.DurableCopiesBeforeAck}/{policy.DurableZonesBeforeAck}");
        }
    }
}

/// <summary>An extent's object id and the object's length.</summary>
internal readonly record struct ExtentInfo(ObjectId Object, long ObjectLength);
