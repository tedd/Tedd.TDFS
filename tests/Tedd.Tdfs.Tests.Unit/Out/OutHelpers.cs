using System.Text;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Out;

/// <summary>Every public mutating SDK entry point exercised by the OUT unit contract tests (OUT-001, OUT-015).</summary>
public enum MutatingApi
{
    WriteAllBytes,
    WriteAllBytesSync,
    WriteAllText,
    WriteAllLines,
    AppendAllBytes,
    AppendAllBytesSync,
    AppendAllText,
    AppendAllLines,
    CopyFile,
    CopyFileSync,
    MoveFile,
    MoveFileSync,
    DeleteFile,
    DeleteFileSync,
    SetStoragePolicy,
    SetAccess,
    CreateDirectory,
    CreateDirectorySync,
    MoveDirectory,
    DeleteDirectoryRecursive,
    OpenCreateNew,
    OpenCreate,
    OpenOrCreateAbsent,
    OpenTruncate,
    OpenAppendAbsent,
    OpenWriteAbsent,
    CreateHelper,
    StreamFlush,
    StreamCommit,
    StreamWriteThrough,
    StreamSetLengthWriteThrough,
    StreamAbort,
    StreamDispose,
    TransactionBegin,
    TransactionCommit,
    TransactionCommitWithSnapshot,
    TransactionAbort,
    SnapshotCreate,
    SnapshotDelete,
    SnapshotSetRetention,
    SnapshotRestore,
}

/// <summary>How an unknown commit outcome is produced (OUT-015).</summary>
public enum UnknownCause
{
    /// <summary>The server executes every attempt but discards each reply (F13 drop).</summary>
    ServerReplyDropped,

    /// <summary>The server executes every attempt, then the client connection is reset while the reply is transmitted.</summary>
    ConnectionResetDuringReply,

    /// <summary>Every reply reaches the client but the client receive path fails with a marked <see cref="IOException"/> (F14).</summary>
    ClientReceivePathFailed,

    /// <summary>The caller's token is cancelled after the server applied the mutation, before the reply is sent (F12).</summary>
    CallerCancelledAfterApply,
}

/// <summary>
/// One mutating API call with its own setup, invocation, resolution and effect check. Instances carry per-run state
/// (for example an open stream) and must not be reused across scenarios.
/// </summary>
internal sealed class MutationProbe
{
    public required MutatingApi Api { get; init; }

    /// <summary>Logical operation kind reported at client/server fault sites and on the wire for the mutating request.</summary>
    public required DfsOperationKind Operation { get; init; }

    /// <summary>True when the API accepts a caller-supplied <see cref="DfsOperationId"/>.</summary>
    public bool SupportsCallerId { get; init; }

    /// <summary>True for transaction commits (the unknown-outcome exception must also carry the transaction id).</summary>
    public bool IsTransactionCommit { get; init; }

    public required Func<DfsVolume, CancellationToken, Task> Setup { get; init; }

    /// <summary>Invokes the API (with the caller id when not null); returns the commit token when the API returns one.</summary>
    public required Func<DfsVolume, DfsOperationId?, CancellationToken, Task<CommitToken?>> Invoke { get; init; }

    /// <summary>Verifies the mutation's effect is present exactly once.</summary>
    public required Func<DfsVolume, CancellationToken, Task> AssertApplied { get; init; }

    /// <summary>Transaction id of the transaction under test, when <see cref="IsTransactionCommit"/>.</summary>
    public Func<DfsTransactionId?> TransactionId { get; init; } = static () => null;

    /// <summary>Releases per-run resources (streams, transactions) without asserting on their disposal result.</summary>
    public Func<Task> Cleanup { get; init; } = static () => Task.CompletedTask;

    /// <summary>Resolves the durable outcome of <paramref name="id"/> (transaction commits resolve by transaction id).</summary>
    public async Task<DfsOperationOutcome> ResolveAsync(DfsVolume volume, DfsOperationId id, CancellationToken ct)
    {
        DfsTransactionId? tx = TransactionId();
        if (IsTransactionCommit && tx is not null)
            return await volume.Transactions.QueryOutcomeAsync(tx.Value, resolve: false, ct);
        return await volume.QueryOperationAsync(id, resolve: false, ct);
    }

    public override string ToString() => Api.ToString();
}

/// <summary>Shared helpers for the OUT unit tests.</summary>
internal static class OutHelpers
{
    public const string Volume = "vol";
    public const string AlternatePolicy = "alt";

    /// <summary>Bounded wait used for liveness conditions in realtime unit scenarios.</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(15);

    /// <summary>Short per-operation timeout so all-replies-dropped scenarios surface their unknown outcome quickly.</summary>
    public static readonly TimeSpan ShortOperationTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Default three-zone cluster with a named alternative policy for <see cref="MutatingApi.SetStoragePolicy"/>.</summary>
    public static TestClusterOptions ThreeZones() => TestClusterOptions.ThreeZones with
    {
        Volumes =
        [
            new TestVolumeSpec(Volume)
            {
                NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
                {
                    [AlternatePolicy] = new DfsStoragePolicy
                    {
                        TargetCopies = 3,
                        TargetDistinctZones = 3,
                        DurableCopiesBeforeAck = 2,
                        DurableZonesBeforeAck = 2,
                    },
                },
            },
        ],
    };

    public static IEnumerable<MutatingApi> AllApis => Enum.GetValues<MutatingApi>();

    /// <summary>APIs used for OUT-015 (one per distinct publication path, including a transaction commit).</summary>
    public static readonly MutatingApi[] UnknownOutcomeApis =
    [
        MutatingApi.WriteAllBytes, MutatingApi.AppendAllBytes, MutatingApi.CopyFile, MutatingApi.MoveFile,
        MutatingApi.DeleteFile, MutatingApi.CreateDirectory, MutatingApi.MoveDirectory, MutatingApi.OpenCreateNew,
        MutatingApi.StreamCommit, MutatingApi.TransactionCommit, MutatingApi.TransactionCommitWithSnapshot, MutatingApi.SnapshotCreate,
    ];

    private static readonly byte[] Small = TestData.Bytes(0x0F_0001, 333);
    private static readonly byte[] Small2 = TestData.Bytes(0x0F_0002, 211);

    /// <summary>Creates a fresh probe for <paramref name="api"/>.</summary>
    public static MutationProbe CreateProbe(MutatingApi api)
    {
        string root = "/out/" + api;
        switch (api)
        {
            case MutatingApi.WriteAllBytes:
            case MutatingApi.WriteAllBytesSync:
            {
                string path = root + "/f";
                bool sync = api == MutatingApi.WriteAllBytesSync;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.WriteAll,
                    SupportsCallerId = true,
                    Setup = async (v, ct) => await v.Directories.CreateDirectoryAsync(root, ct),
                    Invoke = async (v, id, ct) => sync
                        ? v.Files.WriteAllBytes(path, Small, new DfsMutationOptions { OperationId = id })
                        : await v.Files.WriteAllBytesAsync(path, Small, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) => await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(path, ct), path),
                };
            }

            case MutatingApi.WriteAllText:
            {
                string path = root + "/f.txt";
                const string text = "operation-id-before-send ÆØÅ ✓";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.WriteAll,
                    Setup = async (v, ct) => await v.Directories.CreateDirectoryAsync(root, ct),
                    Invoke = async (v, _, ct) => await v.Files.WriteAllTextAsync(path, text, null, ct),
                    AssertApplied = async (v, ct) => await ExpectContentAsync(Encoding.UTF8.GetBytes(text), v.Files.ReadAllBytesAsync(path, ct), path),
                };
            }

            case MutatingApi.WriteAllLines:
            {
                string path = root + "/f.txt";
                string[] lines = ["alpha", "beta", "gamma"];
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.WriteAll,
                    Setup = async (v, ct) => await v.Directories.CreateDirectoryAsync(root, ct),
                    Invoke = async (v, _, ct) => await v.Files.WriteAllLinesAsync(path, lines, null, ct),
                    AssertApplied = async (v, ct) => Assert.Equal(lines, await v.Files.ReadAllLinesAsync(path, null, ct)),
                };
            }

            case MutatingApi.AppendAllBytes:
            case MutatingApi.AppendAllBytesSync:
            {
                string path = root + "/log";
                bool sync = api == MutatingApi.AppendAllBytesSync;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Append,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(path, Small2, ct);
                    },
                    Invoke = async (v, id, ct) => sync
                        ? v.Files.AppendAllBytes(path, Small, new DfsMutationOptions { OperationId = id })
                        : await v.Files.AppendAllBytesAsync(path, Small, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) =>
                    {
                        byte[] actual = await v.Files.ReadAllBytesAsync(path, ct);
                        byte[] expected = [.. Small2, .. Small];
                        AssertEx.BytesEqual(expected, actual, "prefix + suffix exactly once");
                    },
                };
            }

            case MutatingApi.AppendAllText:
            {
                string path = root + "/log.txt";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Append,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllTextAsync(path, "head;", null, ct);
                    },
                    Invoke = async (v, _, ct) => await v.Files.AppendAllTextAsync(path, "tail;", null, ct),
                    AssertApplied = async (v, ct) => Assert.Equal("head;tail;", await v.Files.ReadAllTextAsync(path, null, ct)),
                };
            }

            case MutatingApi.AppendAllLines:
            {
                string path = root + "/log.txt";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Append,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllLinesAsync(path, ["one"], null, ct);
                    },
                    Invoke = async (v, _, ct) => await v.Files.AppendAllLinesAsync(path, ["two", "three"], null, ct),
                    AssertApplied = async (v, ct) => Assert.Equal(["one", "two", "three"], await v.Files.ReadAllLinesAsync(path, null, ct)),
                };
            }

            case MutatingApi.CopyFile:
            case MutatingApi.CopyFileSync:
            {
                string src = root + "/src";
                string dst = root + "/dst";
                bool sync = api == MutatingApi.CopyFileSync;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Copy,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(src, Small, ct);
                    },
                    Invoke = async (v, id, ct) => sync
                        ? v.Files.Copy(src, dst, false, new DfsMutationOptions { OperationId = id })
                        : await v.Files.CopyAsync(src, dst, false, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) =>
                    {
                        await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(dst, ct), dst);
                        await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(src, ct), src);
                        Assert.NotEqual((await v.Files.GetInfoAsync(src, ct)).InodeId, (await v.Files.GetInfoAsync(dst, ct)).InodeId);
                    },
                };
            }

            case MutatingApi.MoveFile:
            case MutatingApi.MoveFileSync:
            {
                string src = root + "/a";
                string dst = root + "/b";
                bool sync = api == MutatingApi.MoveFileSync;
                InodeId inode = default;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Move,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(src, Small, ct);
                        inode = (await v.Files.GetInfoAsync(src, ct)).InodeId;
                    },
                    Invoke = async (v, id, ct) => sync
                        ? v.Files.Move(src, dst, false, new DfsMutationOptions { OperationId = id })
                        : await v.Files.MoveAsync(src, dst, false, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) =>
                    {
                        Assert.False(await v.Files.ExistsAsync(src, ct));
                        Assert.Equal(inode, (await v.Files.GetInfoAsync(dst, ct)).InodeId);
                        await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(dst, ct), dst);
                    },
                };
            }

            case MutatingApi.DeleteFile:
            case MutatingApi.DeleteFileSync:
            {
                string path = root + "/victim";
                bool sync = api == MutatingApi.DeleteFileSync;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Delete,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (v, id, ct) => sync
                        ? v.Files.Delete(path, new DfsMutationOptions { OperationId = id })
                        : await v.Files.DeleteAsync(path, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) => Assert.False(await v.Files.ExistsAsync(path, ct)),
                };
            }

            case MutatingApi.SetStoragePolicy:
            {
                string path = root + "/p";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.PolicyChange,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (v, _, ct) => await v.Files.SetStoragePolicyAsync(path, AlternatePolicy, ct),
                    AssertApplied = async (v, ct) => Assert.Equal(AlternatePolicy, (await v.Files.GetInfoAsync(path, ct)).StoragePolicy),
                };
            }

            case MutatingApi.SetAccess:
            {
                string path = root + "/acl";

                // API.md §H.2: when both flags are set OwnerOnly wins, so only a single flag round-trips through GetInfo.
                var access = new DfsInodeAccess(OwnerOnly: true);
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Admin,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        await v.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (v, _, ct) => await v.Files.SetAccessAsync(path, access, ct),
                    AssertApplied = async (v, ct) => Assert.Equal(access, (await v.Files.GetInfoAsync(path, ct)).Access),
                };
            }

            case MutatingApi.CreateDirectory:
            case MutatingApi.CreateDirectorySync:
            {
                string path = root + "/x/y/z";
                bool sync = api == MutatingApi.CreateDirectorySync;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.CreateDirectory,
                    SupportsCallerId = true,
                    Setup = static (_, _) => Task.CompletedTask,
                    Invoke = async (v, id, ct) =>
                    {
                        DfsDirectoryInfo info = sync
                            ? v.Directories.CreateDirectory(path, new DfsMutationOptions { OperationId = id })
                            : await v.Directories.CreateDirectoryAsync(path, new DfsMutationOptions { OperationId = id }, ct);
                        // ObservedAt is the token at which the returned metadata was read (at or after the creating commit),
                        // not the operation's commit token, so it is not compared with the resolved outcome.
                        Assert.False(info.ObservedAt.IsNone);
                        return null;
                    },
                    AssertApplied = async (v, ct) =>
                    {
                        Assert.True(await v.Directories.ExistsAsync(path, ct));
                        Assert.Equal(0, (await v.Directories.GetInfoAsync(path, ct)).EntryCount);
                    },
                };
            }

            case MutatingApi.MoveDirectory:
            {
                string src = root + "/d1";
                string dst = root + "/d2";
                InodeId inode = default;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Move,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        inode = (await v.Directories.CreateDirectoryAsync(src, ct)).InodeId;
                        await v.Files.WriteAllBytesAsync(src + "/child", Small, ct);
                    },
                    Invoke = async (v, id, ct) => await v.Directories.MoveAsync(src, dst, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) =>
                    {
                        Assert.False(await v.Directories.ExistsAsync(src, ct));
                        Assert.Equal(inode, (await v.Directories.GetInfoAsync(dst, ct)).InodeId);
                        await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(dst + "/child", ct), "moved child");
                    },
                };
            }

            case MutatingApi.DeleteDirectoryRecursive:
            {
                string dir = root + "/tree";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Delete,
                    SupportsCallerId = true,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(dir + "/sub", ct);
                        await v.Files.WriteAllBytesAsync(dir + "/sub/leaf", Small, ct);
                    },
                    Invoke = async (v, id, ct) => await v.Directories.DeleteAsync(dir, true, new DfsMutationOptions { OperationId = id }, ct),
                    AssertApplied = async (v, ct) =>
                    {
                        Assert.False(await v.Directories.ExistsAsync(dir, ct));
                        Assert.False(await v.Files.ExistsAsync(dir + "/sub/leaf", ct));
                    },
                };
            }

            case MutatingApi.OpenCreateNew:
            case MutatingApi.OpenCreate:
            case MutatingApi.OpenOrCreateAbsent:
            case MutatingApi.OpenTruncate:
            case MutatingApi.OpenAppendAbsent:
            case MutatingApi.OpenWriteAbsent:
            case MutatingApi.CreateHelper:
                return OpenProbe(api, root);

            case MutatingApi.StreamFlush:
            case MutatingApi.StreamCommit:
            case MutatingApi.StreamWriteThrough:
            case MutatingApi.StreamSetLengthWriteThrough:
                return StreamProbe(api, root);

            case MutatingApi.StreamAbort:
            case MutatingApi.StreamDispose:
                return ReleaseProbe(api, root);

            case MutatingApi.TransactionBegin:
            {
                DfsTransaction? tx = null;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.TxBegin,
                    SupportsCallerId = true,
                    Setup = static (_, _) => Task.CompletedTask,
                    Invoke = async (v, id, ct) =>
                    {
                        tx = await v.Transactions.BeginAsync(new DfsTransactionOptions { OperationId = id }, ct);
                        return null;
                    },
                    AssertApplied = (_, _) =>
                    {
                        // A begin has no namespace effect; when it returned, the transaction must be active and identified.
                        if (tx is not null)
                        {
                            Assert.Equal(DfsTransactionState.Active, tx.State);
                            Assert.False(tx.Id.Operation.IsEmpty);
                        }

                        return Task.CompletedTask;
                    },
                    Cleanup = async () =>
                    {
                        if (tx is not null)
                            await DisposeQuietlyAsync(tx);
                    },
                };
            }

            case MutatingApi.TransactionCommit:
            {
                string path = root + "/tx";
                DfsTransaction? tx = null;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.TxCommit,
                    IsTransactionCommit = true,
                    TransactionId = () => tx?.Id,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        tx = await v.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (_, _, ct) => await tx!.CommitAsync(ct),
                    AssertApplied = async (v, ct) => await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(path, ct), path),
                    Cleanup = async () =>
                    {
                        if (tx is not null)
                            await DisposeQuietlyAsync(tx);
                    },
                };
            }

            case MutatingApi.TransactionCommitWithSnapshot:
            {
                string path = root + "/tx-snap";
                string snapshot = "tx-snap-" + api;
                DfsTransaction? tx = null;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.TxCommit,
                    IsTransactionCommit = true,
                    TransactionId = () => tx?.Id,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        tx = await v.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (_, _, ct) => (await tx!.CommitWithSnapshotAsync(snapshot, ct)).Boundary,
                    AssertApplied = async (v, ct) =>
                    {
                        await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(path, ct), path);
                        // Commit and snapshot are one decision: exactly one snapshot, holding the committed file.
                        Assert.Single(await v.Snapshots.ListAsync(ct), s => s.Name == snapshot);
                        await using DfsSnapshotView view = await v.Snapshots.OpenAsync(snapshot, ct);
                        await ExpectContentAsync(Small, view.Files.ReadAllBytesAsync(path, ct), "snapshot taken with the commit");
                    },
                    Cleanup = async () =>
                    {
                        if (tx is not null)
                            await DisposeQuietlyAsync(tx);
                    },
                };
            }

            case MutatingApi.TransactionAbort:
            {
                string path = root + "/tx-aborted";
                DfsTransaction? tx = null;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.TxAbort,
                    TransactionId = () => tx?.Id,
                    Setup = async (v, ct) =>
                    {
                        await v.Directories.CreateDirectoryAsync(root, ct);
                        tx = await v.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync(path, Small, ct);
                    },
                    Invoke = async (_, _, ct) =>
                    {
                        await tx!.AbortAsync(ct);
                        return null;
                    },
                    AssertApplied = async (v, ct) => Assert.False(await v.Files.ExistsAsync(path, ct)),
                    Cleanup = async () =>
                    {
                        if (tx is not null)
                            await DisposeQuietlyAsync(tx);
                    },
                };
            }

            case MutatingApi.SnapshotCreate:
            {
                string name = "snap-" + api;
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.CreateSnapshot,
                    SupportsCallerId = true,
                    Setup = async (v, ct) => await v.Files.WriteAllBytesAsync("/snap-src", Small, ct),
                    Invoke = async (v, id, ct) =>
                    {
                        // The snapshot boundary is not by contract the creating operation's commit token: not compared.
                        await v.Snapshots.CreateAsync(name, new DfsSnapshotOptions { OperationId = id }, ct);
                        return null;
                    },
                    AssertApplied = async (v, ct) =>
                    {
                        IReadOnlyList<DfsSnapshotInfo> all = await v.Snapshots.ListAsync(ct);
                        Assert.Single(all, s => s.Name == name);
                    },
                };
            }

            case MutatingApi.SnapshotDelete:
            {
                string name = "snap-del";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.DeleteSnapshot,
                    Setup = async (v, ct) => await v.Snapshots.CreateAsync(name, null, ct),
                    Invoke = async (v, _, ct) =>
                    {
                        await v.Snapshots.DeleteAsync(name, ct);
                        return null;
                    },
                    AssertApplied = async (v, ct) => Assert.DoesNotContain(await v.Snapshots.ListAsync(ct), s => s.Name == name),
                };
            }

            case MutatingApi.SnapshotSetRetention:
            {
                string name = "snap-ret";
                var retention = new DfsRetention(null, Hold: true);
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.Admin,
                    Setup = async (v, ct) => await v.Snapshots.CreateAsync(name, null, ct),
                    Invoke = async (v, _, ct) =>
                    {
                        await v.Snapshots.SetRetentionAsync(name, retention, ct);
                        return null;
                    },
                    AssertApplied = async (v, ct) => Assert.True((await v.Snapshots.GetAsync(name, ct)).Retention.Hold),
                };
            }

            case MutatingApi.SnapshotRestore:
            {
                const string name = "snap-restore";
                const string newVolume = "vol-restored";
                return new MutationProbe
                {
                    Api = api,
                    Operation = DfsOperationKind.RestoreSnapshot,
                    Setup = async (v, ct) =>
                    {
                        await v.Files.WriteAllBytesAsync("/restore-me", Small, ct);
                        await v.Snapshots.CreateAsync(name, null, ct);
                    },
                    Invoke = async (v, _, ct) =>
                    {
                        // The restored volume's LastCommit belongs to a different volume history than the restore
                        // operation's outcome token on the source volume, so it is never compared with it.
                        DfsVolumeInfo info = await v.Snapshots.RestoreAsync(name, newVolume, ct);
                        Assert.Equal(newVolume, info.Name);
                        return null;
                    },
                    AssertApplied = async (v, ct) =>
                    {
                        DfsVolume restored = v.Client.GetVolume(newVolume);
                        await ExpectContentAsync(Small, restored.Files.ReadAllBytesAsync("/restore-me", ct), "restored file");
                    },
                };
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(api), api, null);
        }
    }

    private static MutationProbe OpenProbe(MutatingApi api, string root)
    {
        string path = root + "/opened";
        DfsStream? stream = null;
        bool existing = api == MutatingApi.OpenTruncate;
        (FileMode mode, FileAccess access) = api switch
        {
            MutatingApi.OpenCreateNew => (FileMode.CreateNew, FileAccess.ReadWrite),
            MutatingApi.OpenCreate => (FileMode.Create, FileAccess.ReadWrite),
            MutatingApi.OpenOrCreateAbsent => (FileMode.OpenOrCreate, FileAccess.ReadWrite),
            MutatingApi.OpenTruncate => (FileMode.Truncate, FileAccess.Write),
            MutatingApi.OpenAppendAbsent => (FileMode.Append, FileAccess.Write),
            MutatingApi.OpenWriteAbsent => (FileMode.OpenOrCreate, FileAccess.Write),
            MutatingApi.CreateHelper => (FileMode.Create, FileAccess.ReadWrite),
            _ => throw new ArgumentOutOfRangeException(nameof(api), api, null),
        };

        return new MutationProbe
        {
            Api = api,
            Operation = DfsOperationKind.Open,
            SupportsCallerId = api is not (MutatingApi.OpenWriteAbsent or MutatingApi.CreateHelper),
            Setup = async (v, ct) =>
            {
                await v.Directories.CreateDirectoryAsync(root, ct);
                if (existing)
                    await v.Files.WriteAllBytesAsync(path, Small, ct);
            },
            Invoke = async (v, id, ct) =>
            {
                stream = api switch
                {
                    MutatingApi.OpenWriteAbsent => await v.Files.OpenWriteAsync(path, ct),
                    MutatingApi.CreateHelper => await v.Files.CreateAsync(path, ct),
                    _ => await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = access, Share = FileShare.Read, OperationId = id }, ct),
                };
                return null;
            },
            AssertApplied = async (v, ct) =>
            {
                // Creating/truncating opens commit the namespace change at open: the file exists with length zero.
                if (stream is not null)
                    await DisposeQuietlyAsync(stream);
                stream = null;
                DfsFileInfo info = await v.Files.GetInfoAsync(path, ct);
                Assert.Equal(0, info.Length);
            },
            Cleanup = async () =>
            {
                if (stream is not null)
                    await DisposeQuietlyAsync(stream);
            },
        };
    }

    private static MutationProbe StreamProbe(MutatingApi api, string root)
    {
        string path = root + "/stream";
        DfsStream? stream = null;
        byte[] expected = api == MutatingApi.StreamSetLengthWriteThrough ? Small[..100] : Small;
        return new MutationProbe
        {
            Api = api,
            Operation = DfsOperationKind.CommitFile,
            SupportsCallerId = api == MutatingApi.StreamCommit,
            Setup = async (v, ct) =>
            {
                await v.Directories.CreateDirectoryAsync(root, ct);
                bool writeThrough = api is MutatingApi.StreamWriteThrough or MutatingApi.StreamSetLengthWriteThrough;
                if (api == MutatingApi.StreamSetLengthWriteThrough)
                    await v.Files.WriteAllBytesAsync(path, Small, ct);
                stream = await v.Files.OpenAsync(path, new DfsOpenOptions
                {
                    Mode = FileMode.OpenOrCreate,
                    Access = FileAccess.ReadWrite,
                    Share = FileShare.Read,
                    WriteMode = writeThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush,
                }, ct);
                if (!writeThrough)
                    await stream.WriteAsync(Small, ct);
            },
            Invoke = async (_, id, ct) =>
            {
                DfsStream s = stream!;
                switch (api)
                {
                    case MutatingApi.StreamFlush:
                        await s.FlushAsync(ct);
                        return null;
                    case MutatingApi.StreamCommit:
                        return await s.CommitAsync(new DfsMutationOptions { OperationId = id }, ct);
                    case MutatingApi.StreamWriteThrough:
                        await s.WriteAsync(Small, ct);
                        return null;
                    default:
                        await s.SetLengthAsync(100, ct);
                        return null;
                }
            },
            AssertApplied = async (v, ct) =>
            {
                if (stream is not null)
                    await DisposeQuietlyAsync(stream);
                stream = null;
                // A faulted (outcome-unknown) writer may keep its handle until it expires: read with a compatible share mode.
                await ExpectContentAsync(expected, ReadSharedAsync(v, path, ct), path);
            },
            Cleanup = async () =>
            {
                if (stream is not null)
                    await DisposeQuietlyAsync(stream);
            },
        };
    }

    /// <summary>
    /// Reservation release (<see cref="DfsStream.AbortAsync"/> discarding staged bytes, or disposal of a clean writer):
    /// the release is a mutation of its own (CloseHandle) and must carry its operation id from the first transmission.
    /// </summary>
    private static MutationProbe ReleaseProbe(MutatingApi api, string root)
    {
        string path = root + "/released";
        DfsStream? stream = null;
        bool abort = api == MutatingApi.StreamAbort;
        return new MutationProbe
        {
            Api = api,
            Operation = DfsOperationKind.CloseHandle,
            Setup = async (v, ct) =>
            {
                await v.Directories.CreateDirectoryAsync(root, ct);
                await v.Files.WriteAllBytesAsync(path, Small, ct);
                stream = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, ct);
                if (abort)
                    await stream.WriteAsync(Small2, ct);   // staged only; discarded by the abort
            },
            Invoke = async (_, _, ct) =>
            {
                if (abort)
                    await stream!.AbortAsync(ct);
                else
                    await stream!.DisposeAsync();
                return null;
            },
            AssertApplied = async (v, ct) =>
            {
                // The writer reservation is released (an exclusive writer can open) and nothing staged was published.
                await using (DfsStream exclusive = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.None }, ct))
                    Assert.Equal(Small.Length, await exclusive.GetLengthAsync(ct));
                await ExpectContentAsync(Small, v.Files.ReadAllBytesAsync(path, ct), path);
            },
            Cleanup = async () =>
            {
                if (stream is not null)
                    await DisposeQuietlyAsync(stream);
            },
        };
    }

    /// <summary>
    /// Whole-file read that coexists with a writer handle that may still be open: a version-pinned handle with
    /// <c>FileShare.ReadWrite | FileShare.Delete</c> (test plan §5: concurrent readers use compatible sharing).
    /// </summary>
    public static async Task<byte[]> ReadSharedAsync(DfsVolume volume, string path, CancellationToken ct)
    {
        await using DfsStream s = await volume.Files.OpenAsync(path, new DfsOpenOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            ReadView = DfsReadView.VersionPinned,
        }, ct);
        using var ms = new MemoryStream();
        await s.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    /// <summary>Awaits a whole-file read and asserts its exact bytes.</summary>
    public static async Task ExpectContentAsync(byte[] expected, Task<byte[]> read, string because)
    {
        byte[] actual = await read;
        AssertEx.BytesEqual(expected, actual, because);
    }

    /// <summary>Disposes a stream whose state may be faulted; the disposal outcome is not part of the assertion.</summary>
    public static async Task DisposeQuietlyAsync(IAsyncDisposable disposable)
    {
        try
        {
            await disposable.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException or UnauthorizedAccessException)
        {
            // Faulted streams/transactions surface their fault on disposal; the scenario asserts on durable state instead.
        }
    }

    /// <summary>True when <paramref name="exception"/> or any inner exception satisfies <paramref name="predicate"/>.</summary>
    public static bool ChainContains(Exception exception, Func<Exception, bool> predicate)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (predicate(current))
                return true;
            if (current is AggregateException aggregate && aggregate.InnerExceptions.Any(inner => ChainContains(inner, predicate)))
                return true;
        }

        return false;
    }

    /// <summary>Captures every matching fault-site hit (the action itself does nothing).</summary>
    public static FaultArm Capture(TestCluster cluster, FaultMatch match, List<FaultContext> sink)
    {
        return cluster.Faults.Arm(match with { Count = int.MaxValue }, FaultAction.Custom(ctx =>
        {
            lock (sink)
                sink.Add(ctx);
            return Task.CompletedTask;
        }));
    }

    public static FaultContext[] Snapshot(List<FaultContext> sink)
    {
        lock (sink)
            return [.. sink];
    }
}
