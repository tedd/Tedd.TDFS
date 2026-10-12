using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Acl.AclHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Acl;

/// <summary>
/// ACL — NTFS-like permissions (FSD Addendum A, ACL-03..ACL-08), deterministic simulation: atomic subtree changes under
/// concurrent readers, decisions at the authoritative point, idempotence and durability, transactions, snapshots,
/// backups and replicated-apply determinism.
/// </summary>
public sealed class AclSimulationTests
{
    // =============================================================================================================
    // ACL-020
    // =============================================================================================================

    private sealed record Observation(int Reader, string Kind, string Path, long Invoke, long Complete, bool Allowed, CommitToken? Token);

    /// <summary>
    /// Replacing an inheritable entry on /t/d1 is one decision for the whole six-level subtree: concurrent readers at
    /// every depth succeed only at revisions before the change's commit and are denied at or after it; reads completed
    /// before the change are allowed and reads started after it are denied.
    /// </summary>
    [Fact]
    [Catalog("ACL-020", "S", "INV-08", "INV-12")]
    public Task ACL_020_InheritanceChangeIsAtomicForSubtree() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            TestPrincipal alice = await cluster.CreateUserAsync("alice");
            await admin.Directories.CreateDirectoryAsync("/t", Ct);
            await SetAclAsync(admin, "/t", true, true, AdminsFull);
            const int depth = 6;
            var dirs = new List<string>();
            var files = new List<string>();
            var contents = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            string dir = "/t";
            for (int k = 1; k <= depth; k++)
            {
                dir += "/d" + k;
                dirs.Add(dir);
                await admin.Directories.CreateDirectoryAsync(dir, Ct);
                string f = dir + "/f" + k;
                files.Add(f);
                contents[f] = TestData.Bytes((ulong)(2000 + k), 100 * k);
                await admin.Files.WriteAllBytesAsync(f, contents[f], Ct);
            }

            await SetAclAsync(admin, "/t/d1", true, false, Allow(U("alice"), Read, CIOI));
            foreach (string p in files)
                Assert.Equal(Read, await admin.Files.GetEffectiveRightsAsync(p, U("alice"), Ct) & Read);

            var readers = new List<DfsClient>();
            for (int r = 0; r < 3; r++)
                readers.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "r" + r, Credentials = alice.Credentials }, Ct));
            var observations = new List<Observation>();
            bool changed = false;

            async Task ReaderAsync(int r)
            {
                DfsVolume v = readers[r].GetVolume(Vol);
                int afterChange = 0;
                for (int i = 0; afterChange < 8 && i < 500; i++)
                {
                    int k = (r * 5 + i * 3) % depth;
                    string kind = ((r + i) % 3) switch { 0 => "info", 1 => "list", _ => "read" };
                    string path = kind == "list" ? dirs[k] : files[k];
                    bool startedAfterChange = changed;
                    long invoke = cluster.Time.GetTimestamp();
                    CommitToken? token = null;
                    bool allowed = await AllowedAsync(async () =>
                    {
                        switch (kind)
                        {
                            case "info":
                                token = (await v.Files.GetInfoAsync(path, Ct)).ObservedAt;
                                break;
                            case "list":
                                token = (await v.Directories.ListPageAsync(path, null, null, Ct)).PinnedAt;
                                break;
                            default:
                                await AssertBytesAsync(contents[path], v.Files.ReadAllBytesAsync(path, Ct), path);
                                break;
                        }
                    }, $"{kind} {path}");
                    observations.Add(new Observation(r, kind, path, invoke, cluster.Time.GetTimestamp(), allowed, token));
                    if (startedAfterChange)
                        afterChange++;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(2 + r), Ct);
                }
            }

            Task[] running = [ReaderAsync(0), ReaderAsync(1), ReaderAsync(2)];
            await cluster.WaitUntilAsync(() => Task.FromResult(observations.Count >= 18), Wait, "readers progressed before the change", Ct);
            long changeInvoke = cluster.Time.GetTimestamp();
            CommitToken change = await SetAclAsync(admin, "/t/d1", true, false, Deny(U("alice"), Read, CIOI));
            long changeComplete = cluster.Time.GetTimestamp();
            changed = true;
            await Task.WhenAll(running);

            Assert.Contains(observations, o => o.Allowed);
            Assert.Contains(observations, o => !o.Allowed);
            foreach (Observation o in observations)
            {
                string what = $"reader {o.Reader} {o.Kind} {o.Path} [{o.Invoke}, {o.Complete}] allowed={o.Allowed} at {o.Token}";
                if (o.Allowed && o.Token is { } seen)
                {
                    Assert.True(seen.IsComparableWith(change), $"{what}: token not comparable with the change {change}");
                    Assert.True(seen.CompareTo(change) < 0, $"{what}: allowed at or after the revision of the change {change}");
                }

                if (o.Complete < changeInvoke)
                    Assert.True(o.Allowed, $"{what}: denied although it completed before the change was invoked");
                if (o.Invoke > changeComplete)
                    Assert.False(o.Allowed, $"{what}: allowed although it started after the change committed");
            }

            foreach (string p in files)
                Assert.Equal((DfsFileSystemRights)0, await admin.Files.GetEffectiveRightsAsync(p, U("alice"), Ct) & Read);
            foreach (string p in dirs)
                Assert.Equal((DfsFileSystemRights)0, await admin.Directories.GetEffectiveRightsAsync(p, U("alice"), Ct) & Read);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            foreach (DfsClient c in readers)
                await c.DisposeAsync();
        });

    // =============================================================================================================
    // ACL-026
    // =============================================================================================================

    /// <summary>The operation whose authoritative decision races a revocation in ACL-026.</summary>
    public enum CheckedOp
    {
        /// <summary>A reader's open while a writer holds staged data on the file.</summary>
        ReaderOpen,

        /// <summary>WriteAllBytes over an existing file (data uploaded, then one commit).</summary>
        WriteAllCommit,

        /// <summary>AppendAllBytes (suffix uploaded, then one commit).</summary>
        AppendCommit,

        /// <summary>A transaction's staged whole-file write, then commit.</summary>
        TransactionCommit,
    }

    /// <summary>Where the operation is held while the right is revoked.</summary>
    public enum DecisionPoint
    {
        /// <summary>F04: the leader accepted the request.</summary>
        RequestAccepted,

        /// <summary>F10: the leader validated the request and is about to propose it.</summary>
        BeforeProposal,
    }

    public static TheoryData<CheckedOp, DecisionPoint> DecisionCases
    {
        get
        {
            var data = new TheoryData<CheckedOp, DecisionPoint>();
            foreach (CheckedOp op in Enum.GetValues<CheckedOp>())
            {
                foreach (DecisionPoint p in Enum.GetValues<DecisionPoint>())
                    data.Add(op, p);
            }

            return data;
        }
    }

    /// <summary>
    /// A right revoked after the request reached the leader (F04) or after the leader's pre-proposal check (F10) is
    /// enforced at the decision: the reader's open is denied while the writer's staging is unaffected, and staged
    /// commits (WriteAll, Append, transaction) are rejected with data and revision unchanged; re-granting restores them.
    /// </summary>
    [Theory]
    [MemberData(nameof(DecisionCases))]
    [Catalog("ACL-026", "S", "INV-08", "INV-12")]
    public Task ACL_026_AclIsCheckedAtDecision(CheckedOp op, DecisionPoint point) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            SimUser alice = await AddUserAsync(cluster, "alice");
            SimUser bob = await AddUserAsync(cluster, "bob");
            await admin.Directories.CreateDirectoryAsync("/t", Ct);
            await SetAclAsync(admin, "/t", true, true, AdminsFull);
            byte[] original = TestData.Bytes(2601, 3 * 65536 + 17);
            await admin.Files.WriteAllBytesAsync("/t/f", original, Ct);
            InodeId inode = (await admin.Files.GetInfoAsync("/t/f", Ct)).InodeId;
            string site = point == DecisionPoint.RequestAccepted ? FaultSites.RpcRequestAccepted : FaultSites.RaftBeforePropose;
            DfsAccessRule bobModify = Allow(bob.Identity, Modify);

            if (op == CheckedOp.ReaderOpen)
            {
                await SetAclAsync(admin, "/t/f", false, false, Allow(alice.Identity, Read), bobModify);
                Assert.Equal(original.Length, (await alice.Volume.Files.GetInfoAsync("/t/f", Ct)).Length);
                byte[] staged = TestData.Bytes(2602, 70000);
                DfsStream writer = await bob.Volume.Files.OpenAsync("/t/f", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                try
                {
                    await writer.WriteAsync(staged, Ct);
                    Assert.True(writer.PendingBytes >= staged.Length, "the writer's bytes are staged, not published");

                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Volume: Vol, Operation: DfsOperationKind.Open, PathPrefix: "/t/f"), FaultAction.Block());
                    Task<DfsStream> open = alice.Volume.Files.OpenAsync("/t/f", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                    await arm.WaitTriggeredAsync(Wait);
                    await SetAclAsync(admin, "/t/f", false, false, bobModify);
                    arm.Release();
                    await AssertDeniedAsync(async () =>
                    {
                        await using DfsStream s = await open;
                    }, $"reader open held at {site} across the revocation");
                    cluster.Faults.AssertReached(arm);
                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                    var aliceSessions = state.Sessions.Where(s => s.Principal == "alice").Select(s => s.Id).ToHashSet();
                    Assert.DoesNotContain(state.Handles, h => h.Inode == inode && aliceSessions.Contains(h.Session) && !h.Expired);

                    await writer.FlushAsync(Ct);
                }
                finally
                {
                    await writer.DisposeAsync();
                }

                byte[] expected = [.. TestData.Bytes(2602, 70000), .. original.AsSpan(70000)];
                await AssertBytesAsync(expected, admin.Files.ReadAllBytesAsync("/t/f", Ct), "the writer's staged data published after the revocation of another principal");
                await AssertDeniedAsync(() => alice.Volume.Files.OpenReadAsync("/t/f", Ct), "later open after revocation");
                return;
            }

            DfsAccessRule aliceWrite = Allow(alice.Identity, Modify);
            await SetAclAsync(admin, "/t/f", false, false, aliceWrite, bobModify);
            byte[] data = TestData.Bytes(2603, 2 * 65536 + 5);
            DfsOperationKind kind = op switch
            {
                CheckedOp.WriteAllCommit => DfsOperationKind.WriteAll,
                CheckedOp.AppendCommit => DfsOperationKind.Append,
                _ => DfsOperationKind.TxCommit,
            };
            DfsTransaction? tx = null;
            if (op == CheckedOp.TransactionCommit)
            {
                tx = await alice.Volume.Transactions.BeginAsync(null, Ct);
                await tx.Files.WriteAllBytesAsync("/t/f", data, Ct);
            }

            try
            {
                FaultArm arm = cluster.Faults.Arm(
                    new FaultMatch(site, Volume: Vol, Operation: kind, PathPrefix: op == CheckedOp.TransactionCommit ? null : "/t/f"),
                    FaultAction.Block());
                Task commit = op switch
                {
                    CheckedOp.WriteAllCommit => alice.Volume.Files.WriteAllBytesAsync("/t/f", data, Ct),
                    CheckedOp.AppendCommit => alice.Volume.Files.AppendAllBytesAsync("/t/f", data, Ct),
                    _ => tx!.CommitAsync(Ct),
                };
                await arm.WaitTriggeredAsync(Wait);
                await SetAclAsync(admin, "/t/f", false, false, Allow(alice.Identity, Read), bobModify);
                FileInspection afterRevoke = (await cluster.Inspect.GetFileAsync(Vol, "/t/f"))!;
                arm.Release();
                Exception? failure = await Record.ExceptionAsync(() => commit);
                cluster.Faults.AssertReached(arm);
                if (op == CheckedOp.TransactionCommit && failure is DfsTransactionConflictException)
                {
                    // The ACL change is a revision of the inode the transaction wrote; a conflict is also a rejection.
                }
                else
                {
                    await AssertDeniedAsync(() => failure is null ? Task.CompletedTask : Task.FromException(failure), $"{op} held at {site} across the revocation of WriteData");
                }

                FileInspection now = (await cluster.Inspect.GetFileAsync(Vol, "/t/f"))!;
                Assert.Equal(afterRevoke.Revision, now.Revision);
                Assert.Equal(original.Length, now.Length);
                await AssertBytesAsync(original, admin.Files.ReadAllBytesAsync("/t/f", Ct), "rejected commit left the data");
            }
            finally
            {
                if (tx is not null)
                    await tx.DisposeAsync();
            }

            Func<Task> retry = op switch
            {
                CheckedOp.WriteAllCommit => () => alice.Volume.Files.WriteAllBytesAsync("/t/f", data, Ct),
                CheckedOp.AppendCommit => () => alice.Volume.Files.AppendAllBytesAsync("/t/f", data, Ct),
                _ => (Func<Task>)(async () =>
                {
                    await using DfsTransaction again = await alice.Volume.Transactions.BeginAsync(null, Ct);
                    await again.Files.WriteAllBytesAsync("/t/f", data, Ct);
                    await again.CommitAsync(Ct);
                }),
            };
            await AssertDeniedAsync(retry, $"{op} after the revocation");

            await SetAclAsync(admin, "/t/f", false, false, aliceWrite, bobModify);
            if (op == CheckedOp.AppendCommit)
            {
                await alice.Volume.Files.AppendAllBytesAsync("/t/f", data, Ct);
                await AssertBytesAsync([.. original, .. data], admin.Files.ReadAllBytesAsync("/t/f", Ct), "append after re-grant");
            }
            else
            {
                await alice.Volume.Files.WriteAllBytesAsync("/t/f", data, Ct);
                await AssertBytesAsync(data, admin.Files.ReadAllBytesAsync("/t/f", Ct), "write after re-grant");
            }

            Assert.Equal(inode, (await admin.Files.GetInfoAsync("/t/f", Ct)).InodeId);
        });

    // =============================================================================================================
    // ACL-027
    // =============================================================================================================

    /// <summary>
    /// SetAccessControl's reply is lost after apply (held at reply-send, then the client's connections are reset); the SDK
    /// retries with the same operation id and receives the stored outcome without a second effect (a later change by
    /// another client, committed before the retry, is not overwritten); the result survives leader failover and a cold
    /// restart of every node.
    /// </summary>
    [Fact]
    [Catalog("ACL-027", "S", "INV-06", "INV-08", "INV-12")]
    public Task ACL_027_AclChangeIdempotentAndDurable() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient a = await cluster.ConnectAsync(new TestClientOptions { Name = "a" }, Ct);
            await using DfsClient b = await cluster.ConnectAsync(new TestClientOptions { Name = "b" }, Ct);
            DfsVolume va = a.GetVolume(Vol), vb = b.GetVolume(Vol);
            SimUser alice = await AddUserAsync(cluster, "alice");
            SimUser bob = await AddUserAsync(cluster, "bob");
            await va.Directories.CreateDirectoryAsync("/t", Ct);
            await SetAclAsync(va, "/t", true, true, AdminsFull);
            byte[] content = TestData.Bytes(2701, 5000);
            await va.Files.WriteAllBytesAsync("/t/f", content, Ct);
            DfsAccessRule[] d1 = [Allow(alice.Identity, Read)];
            DfsAccessRule[] d2 = [Allow(bob.Identity, Read), Deny(alice.Identity, FC)];

            // a's change is applied; its reply is held at the server while b's later change commits.
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Volume: Vol, PathPrefix: "/t/f"), FaultAction.Block());
            Task<CommitToken> first = va.Files.SetAccessControlAsync("/t/f", new DfsSecurityDescriptor(AdminId, d1), Ct);
            await arm.WaitTriggeredAsync(Wait);
            DfsOperationId opId = arm.Contexts[0].OperationId ?? throw new InvalidOperationException("the reply-send fault context carries no operation id");
            CommitToken t2 = await vb.Files.SetAccessControlAsync("/t/f", new DfsSecurityDescriptor(AdminId, d2), Ct);

            // Lose the held reply: a's connections are closed while the server still holds it, so it can never arrive and
            // the SDK has to retry with the same operation id — after b's change, so a re-application would overwrite it.
            // (A DropReplies rule cannot be used here: it matches requests, and this request was delivered already.)
            cluster.Network.ResetConnections("client:a");
            arm.Release();
            CommitToken t1 = await first;
            cluster.Faults.AssertReached(arm);
            int sameIdRequests = cluster.Faults.Coverage.Hits.Count(h => h.Site == FaultSites.RpcRequestAccepted && h.OperationId == opId);
            Assert.True(sameIdRequests >= 2, $"the lost reply must be retried with operation id {opId} (requests seen with it: {sameIdRequests})");

            DfsOperationOutcome outcome = await va.QueryOperationAsync(opId, false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(outcome.Token, t1);
            Assert.True(t1.IsComparableWith(t2) && t1.CompareTo(t2) < 0, $"the retried change {t1} must keep its original position before {t2}");

            async Task AssertFinalStateAsync(string when)
            {
                DfsSecurityDescriptor acl = await va.Files.GetAccessControlAsync("/t/f", Ct);
                Assert.Equal(AdminId, acl.Owner);
                Assert.Equal(Describe(new DfsSecurityDescriptor(AdminId, [d2[1], d2[0]])), Describe(acl.WithoutInheritedRules()));
                await AssertBytesAsync(content, bob.Volume.Files.ReadAllBytesAsync("/t/f", Ct), $"bob reads {when}");
                await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), $"alice must stay denied {when} (no re-application of the first change)");
            }

            await AssertFinalStateAsync("after the retry");
            ulong revision = (await cluster.Inspect.GetFileAsync(Vol, "/t/f"))!.Revision;
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            Assert.Equal(revision, (await cluster.Inspect.GetFileAsync(Vol, "/t/f"))!.Revision);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await leader.CrashAsync(CrashMode.LoseUnflushed);
            TestNode newLeader = await cluster.GetLeaderAsync(Vol, Ct);
            Assert.NotEqual(leader.Name, newLeader.Name);
            await AssertFinalStateAsync("after leader failover");
            await leader.RestartAsync();
            await cluster.WaitForStableAsync(Wait, Ct);

            foreach (TestNode n in cluster.Nodes.ToArray())
                await n.CrashAsync(CrashMode.LoseUnflushed);
            foreach (TestNode n in cluster.Nodes.ToArray())
                await n.RestartAsync();
            await cluster.WaitForStableAsync(Wait, Ct);
            await AssertFinalStateAsync("after a cold restart");
            Assert.Equal(revision, (await cluster.Inspect.GetFileAsync(Vol, "/t/f"))!.Revision);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        });

    // =============================================================================================================
    // ACL-028
    // =============================================================================================================

    /// <summary>The concurrent descriptor change of ACL-028 part 1.</summary>
    public enum DescriptorChange
    {
        /// <summary>SetAccessControl adding an entry (the reader keeps its access).</summary>
        AccessControl,

        /// <summary>SetOwner.</summary>
        Owner,
    }

    /// <summary>
    /// A transaction that read a file conflicts with a concurrent ACL or owner change on it (no effect of the
    /// transaction); a transaction changes an ACL and data atomically: no observer reads the new data under the old ACL
    /// or the old data under the new one; an aborted transaction changes neither.
    /// </summary>
    [Theory]
    [InlineData(DescriptorChange.AccessControl)]
    [InlineData(DescriptorChange.Owner)]
    [Catalog("ACL-028", "S", "INV-04", "INV-08", "INV-12")]
    public Task ACL_028_AclInTransactionsConflicts(DescriptorChange change) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            SimUser alice = await AddUserAsync(cluster, "alice");
            SimUser carol = await AddUserAsync(cluster, "carol");
            await admin.Directories.CreateDirectoryAsync("/t", Ct);
            const DfsFileSystemRights create = DfsFileSystemRights.CreateFiles | DfsFileSystemRights.ListDirectory | DfsFileSystemRights.ReadAttributes;
            await SetAclAsync(admin, "/t", true, true, AdminsFull, Allow(alice.Identity, create));
            byte[] fdata = TestData.Bytes(2801, 6000);
            await admin.Files.WriteAllBytesAsync("/t/f", fdata, Ct);
            await SetAclAsync(admin, "/t/f", false, false, Allow(alice.Identity, Read));

            // Positive control: without a concurrent change the read-write transaction commits.
            await using (DfsTransaction ok = await alice.Volume.Transactions.BeginAsync(null, Ct))
            {
                byte[] read = await ok.Files.ReadAllBytesAsync("/t/f", Ct);
                await ok.Files.WriteAllBytesAsync("/t/out-ok", read, Ct);
                await ok.CommitAsync(Ct);
            }

            await AssertBytesAsync(fdata, admin.Files.ReadAllBytesAsync("/t/out-ok", Ct), "control transaction");

            await using (DfsTransaction tx = await alice.Volume.Transactions.BeginAsync(null, Ct))
            {
                byte[] read = await tx.Files.ReadAllBytesAsync("/t/f", Ct);
                await tx.Files.WriteAllBytesAsync("/t/out", read, Ct);
                if (change == DescriptorChange.AccessControl)
                    await SetAclAsync(admin, "/t/f", false, false, Allow(alice.Identity, Read), Allow(carol.Identity, Read));
                else
                    await admin.Files.SetOwnerAsync("/t/f", carol.Identity, Ct);
                await Assert.ThrowsAnyAsync<DfsTransactionConflictException>(() => tx.CommitAsync(Ct));
            }

            Assert.False(await admin.Files.ExistsAsync("/t/out", Ct), "the conflicting transaction must have no effect");
            await AssertBytesAsync(fdata, alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "alice kept read access");

            // ACL and data change atomically in one transaction.
            byte[] oldData = TestData.Bytes(2802, 5000), newData = TestData.Bytes(2803, 7000);
            await admin.Files.WriteAllBytesAsync("/t/g", oldData, Ct);
            await SetAclAsync(admin, "/t/g", false, false, Deny(alice.Identity, DfsFileSystemRights.ReadData));
            DfsAccessRule[] granted = [Allow(alice.Identity, Read)];

            await using (DfsTransaction aborted = await adminClient.GetVolume(Vol).Transactions.BeginAsync(null, Ct))
            {
                await aborted.Files.SetAccessControlAsync("/t/g", new DfsSecurityDescriptor(AdminId, granted), Ct);
                await aborted.Files.WriteAllBytesAsync("/t/g", newData, Ct);
                await aborted.AbortAsync(Ct);
            }

            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/g", Ct), "an aborted transaction changes no ACL");
            await AssertBytesAsync(oldData, admin.Files.ReadAllBytesAsync("/t/g", Ct), "an aborted transaction changes no data");

            await using DfsTransaction both = await admin.Transactions.BeginAsync(null, Ct);
            await both.Files.SetAccessControlAsync("/t/g", new DfsSecurityDescriptor(AdminId, granted), Ct);
            await both.Files.WriteAllBytesAsync("/t/g", newData, Ct);
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/g", Ct), "transaction changes are invisible before commit");
            await AssertBytesAsync(oldData, admin.Files.ReadAllBytesAsync("/t/g", Ct), "data invisible before commit");

            bool committed = false;
            int successes = 0;
            async Task ObserveAsync()
            {
                for (int i = 0; i < 5000 && !(committed && successes > 2); i++)
                {
                    try
                    {
                        byte[] seen = await alice.Volume.Files.ReadAllBytesAsync("/t/g", Ct);
                        AssertEx.BytesEqual(newData, seen, "alice may only read /t/g under the new ACL, which must come with the new data");
                        successes++;
                    }
                    catch (UnauthorizedAccessException)
                    {
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(1), Ct);
                }
            }

            Task observer = ObserveAsync();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
            await both.CommitAsync(Ct);
            committed = true;
            await observer;
            Assert.True(successes > 0, "the observer saw the committed state");
            await AssertBytesAsync(newData, alice.Volume.Files.ReadAllBytesAsync("/t/g", Ct), "after commit");
            DfsSecurityDescriptor acl = await admin.Files.GetAccessControlAsync("/t/g", Ct);
            Assert.Equal(Describe(new DfsSecurityDescriptor(AdminId, granted)), Describe(acl.WithoutInheritedRules()));
        });

    // =============================================================================================================
    // ACL-029
    // =============================================================================================================

    /// <summary>
    /// A snapshot keeps owners and descriptors at its boundary: later ACL, owner, inheritance and placement (move)
    /// changes on the live volume do not change snapshot decisions, while group membership is evaluated with the current
    /// identity database.
    /// </summary>
    [Fact]
    [Catalog("ACL-029", "S", "INV-08", "INV-12", "INV-13")]
    public Task ACL_029_SnapshotPreservesDescriptors() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            await cluster.CreateGroupAsync("eng");
            SimUser alice = await AddUserAsync(cluster, "alice");
            SimUser bob = await AddUserAsync(cluster, "bob");
            SimUser carol = await AddUserAsync(cluster, "carol", "eng");
            SimUser dave = await AddUserAsync(cluster, "dave");
            await admin.Directories.CreateDirectoryAsync("/t/d", Ct);
            await SetAclAsync(admin, "/t", true, true, AdminsFull, Allow(G("eng"), Read, CIOI));
            await SetAclAsync(admin, "/t/d", true, false, Allow(alice.Identity, Read, CIOI));
            byte[] f = TestData.Bytes(2901, 6000), g = TestData.Bytes(2902, 300);
            await admin.Files.WriteAllBytesAsync("/t/d/f", f, Ct);
            await admin.Files.WriteAllBytesAsync("/t/d/g", g, Ct);
            await SetAclAsync(admin, "/t/d/f", false, false, Deny(bob.Identity, Read));
            var atBoundary = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach ((string p, bool dir) in new[] { ("/t", true), ("/t/d", true), ("/t/d/f", false), ("/t/d/g", false) })
                atBoundary[p] = Fingerprint(await GetAclAsync(admin, p, dir));

            await admin.Snapshots.CreateAsync("s1", null, Ct);

            await SetAclAsync(admin, "/t/d", true, false, Deny(alice.Identity, Read, CIOI), Allow(bob.Identity, Read, CIOI));
            await SetAclAsync(admin, "/t/d/f", false, false);
            await admin.Files.SetOwnerAsync("/t/d/f", bob.Identity, Ct);
            await admin.Files.MoveAsync("/t/d/g", "/t/g2", false, Ct);
            await cluster.Admin.RemoveGroupMemberAsync("eng", "carol", Ct);
            await cluster.Admin.AddGroupMemberAsync("eng", "dave", Ct);

            // Group changes bind decisions once the deciding volume leader has applied them (ACL-08): let every replica
            // catch up so the snapshot decisions below see the current identity database wherever they are made.
            await cluster.WaitForStableAsync(Wait, Ct);

            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/d/f", Ct), "live: alice denied after the change");
            await AssertBytesAsync(f, bob.Volume.Files.ReadAllBytesAsync("/t/d/f", Ct), "live: bob allowed after the change");

            await using (DfsSnapshotView view = await admin.Snapshots.OpenAsync("s1", Ct))
            {
                foreach ((string p, string expected) in atBoundary)
                {
                    DfsSecurityDescriptor got = p is "/t" or "/t/d" ? await view.Directories.GetAccessControlAsync(p, Ct) : await view.Files.GetAccessControlAsync(p, Ct);
                    Assert.Equal(expected, Fingerprint(got));
                }
            }

            async Task<bool> SnapshotReadAsync(SimUser u, string path)
            {
                await using DfsSnapshotView view = await u.Volume.Snapshots.OpenAsync("s1", Ct);
                return await AllowedAsync(async () =>
                {
                    byte[] bytes = await view.Files.ReadAllBytesAsync(path, Ct);
                    AssertEx.BytesEqual(path == "/t/d/f" ? f : g, bytes, $"{u.Name} snapshot read {path}");
                }, $"{u.Name} snapshot read {path}");
            }

            Assert.True(await SnapshotReadAsync(alice, "/t/d/f"), "alice was allowed at the boundary");
            Assert.False(await SnapshotReadAsync(bob, "/t/d/f"), "bob was denied at the boundary");
            Assert.True(await SnapshotReadAsync(alice, "/t/d/g"), "g is still at its boundary path in the snapshot");
            Assert.False(await SnapshotReadAsync(carol, "/t/d/g"), "carol left eng: snapshot decisions use the current group database");
            Assert.True(await SnapshotReadAsync(dave, "/t/d/g"), "dave joined eng: snapshot decisions use the current group database");
        });

    // =============================================================================================================
    // ACL-030
    // =============================================================================================================

    /// <summary>How the protected volume is restored in ACL-030.</summary>
    public enum RestoreKind
    {
        /// <summary>DfsSnapshotClient.RestoreAsync into a new volume.</summary>
        Snapshot,

        /// <summary>Backup to a late-sync backup node, then DfsAdminClient.RestoreFromBackupAsync.</summary>
        Backup,
    }

    /// <summary>
    /// Snapshot restore and backup restore reproduce owners, explicit entries (order, flags, types), protection flags and
    /// the resulting inherited entries exactly as at the snapshot, and the restored volume enforces them (every user's
    /// read/list decisions equal the source's at the snapshot), independent of later source changes.
    /// </summary>
    [Theory]
    [InlineData(RestoreKind.Snapshot)]
    [InlineData(RestoreKind.Backup)]
    [Catalog("ACL-030", "S", "INV-08", "INV-12", "INV-13")]
    public Task ACL_030_BackupAndRestorePreserveDescriptors(RestoreKind kind) =>
        ClusterTest.RunAsync(Options(backupNodes: kind == RestoreKind.Backup ? 1 : 0), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            await cluster.CreateGroupAsync("eng");
            SimUser[] users = [await AddUserAsync(cluster, "alice"), await AddUserAsync(cluster, "bob", "eng"), await AddUserAsync(cluster, "dave")];
            SimUser alice = users[0], bob = users[1], dave = users[2];

            var tree = new (string Path, bool Dir)[] { ("/t", true), ("/t/a", true), ("/t/a/f", false), ("/t/p", true), ("/t/p/x", false), ("/t/p/y", false) };
            var contents = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            await admin.Directories.CreateDirectoryAsync("/t/a", Ct);
            await admin.Directories.CreateDirectoryAsync("/t/p", Ct);
            await SetAclAsync(admin, "/t", true, true, AdminsFull, Allow(G("eng"), Read, CIOI), Allow(DfsIdentity.CreatorOwner, Modify, CIOI));
            await admin.Directories.SetOwnerAsync("/t/a", alice.Identity, Ct);
            await SetAclAsync(admin, "/t/a", true, false, Deny(bob.Identity, Read, CIOI, NP), Allow(dave.Identity, DfsFileSystemRights.ListDirectory));
            foreach (string file in new[] { "/t/a/f", "/t/p/x", "/t/p/y" })
            {
                contents[file] = TestData.Bytes((ulong)(3000 + contents.Count), 5000 + contents.Count * 1000);
                await admin.Files.WriteAllBytesAsync(file, contents[file], Ct);
            }

            await admin.Files.WriteAllBytesAsync("/t/p/y:s", TestData.Bytes(3010, 2000), Ct);
            await admin.Files.SetOwnerAsync("/t/a/f", alice.Identity, Ct);
            await SetAclAsync(admin, "/t/a/f", false, false, Allow(dave.Identity, DfsFileSystemRights.ReadData));
            await SetAclAsync(admin, "/t/p", true, true, AdminsFull, Allow(dave.Identity, FC, OI, IO), Deny(bob.Identity, DfsFileSystemRights.ListDirectory));
            await admin.Files.SetOwnerAsync("/t/p/x", bob.Identity, Ct);

            await cluster.WaitForStableAsync(Wait, Ct);   // bob's eng membership is applied by every replica before deciding
            var descriptors = new Dictionary<string, string>(StringComparer.Ordinal);
            var decisions = new Dictionary<(string User, string Path), bool>();
            async Task<bool> DecideAsync(SimUser u, DfsVolume v, string path, bool dir) =>
                await AllowedAsync(dir
                    ? (Func<Task>)(() => ToListAsync(v.Directories.EnumerateAsync(path, null, Ct)))
                    : () => AssertBytesAsync(contents[path], v.Files.ReadAllBytesAsync(path, Ct), $"{u.Name} reads {path}"),
                    $"{u.Name} on {path}");
            descriptors["/"] = Fingerprint(await admin.Directories.GetAccessControlAsync("/", Ct));
            foreach ((string path, bool dir) in tree)
            {
                descriptors[path] = Fingerprint(await GetAclAsync(admin, path, dir));
                foreach (SimUser u in users)
                    decisions[(u.Name, path)] = await DecideAsync(u, u.Volume, path, dir);
            }

            Assert.Contains(decisions.Values, d => d);
            Assert.Contains(decisions.Values, d => !d);
            await admin.Snapshots.CreateAsync("s1", null, Ct);

            // Later source changes must not leak into the restored volume.
            await SetAclAsync(admin, "/t/a", true, false);
            await admin.Directories.SetOwnerAsync("/t/a", dave.Identity, Ct);
            await SetAclAsync(admin, "/t/p", true, false, Allow(bob.Identity, FC, CIOI));

            const string restored = "restored";
            if (kind == RestoreKind.Snapshot)
            {
                await admin.Snapshots.RestoreAsync("s1", restored, Ct);
            }
            else
            {
                TestBackupNode b0 = cluster.BackupNodes[0];
                await b0.RegisterAsync(Vol);
                await admin.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
                DfsBackupCheckpoint checkpoint = await admin.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", TimeSpan.FromMinutes(2), Ct);
                Assert.Equal("s1", checkpoint.SnapshotName);
                await adminClient.Admin.RestoreFromBackupAsync(new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Vol, "s1", restored), Ct);
            }

            foreach (SimUser u in users)
                await cluster.Admin.GrantAsync(restored, u.Name, DfsPermissions.Read | DfsPermissions.Write, Ct);
            DfsVolume r = adminClient.GetVolume(restored);
            Assert.Equal(descriptors["/"], Fingerprint(await r.Directories.GetAccessControlAsync("/", Ct)));
            foreach ((string path, bool dir) in tree)
            {
                Assert.Equal(descriptors[path], Fingerprint(await GetAclAsync(r, path, dir)));
                foreach (SimUser u in users)
                {
                    bool now = await DecideAsync(u, u.Client.GetVolume(restored), path, dir);
                    Assert.True(decisions[(u.Name, path)] == now, $"{u.Name} on {path} in the restored volume: {now}, at the snapshot: {decisions[(u.Name, path)]}");
                }
            }

            await AssertBytesAsync(TestData.Bytes(3010, 2000), r.Files.ReadAllBytesAsync("/t/p/y:s", Ct), "stream restored");
        });

    // =============================================================================================================
    // ACL-033
    // =============================================================================================================

    private sealed record Interval(string Kind, long Invoke, long Complete, long Effective);

    /// <summary>
    /// While alice's membership in the group that grants CreateFiles flips repeatedly, her creations commit; a lagging
    /// follower (isolated, crashed, restarted and replaying) and every other replica apply identical decisions from the
    /// identity sets carried in the commands: replica digests and per-file outcomes agree everywhere, and every outcome
    /// is consistent with the membership in force around its interval.
    /// </summary>
    [Fact]
    [Catalog("ACL-033", "S", "INV-08", "INV-12")]
    public Task ACL_033_ReplicatedApplyIsDeterministic() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient adminClient = await ConnectAdminAsync(cluster);
            DfsVolume admin = adminClient.GetVolume(Vol);
            await cluster.CreateGroupAsync("eng");
            SimUser alice = await AddUserAsync(cluster, "alice");
            await admin.Directories.CreateDirectoryAsync("/t/w", Ct);
            await SetAclAsync(admin, "/t", true, true, AdminsFull);
            await SetAclAsync(admin, "/t/w", true, false, Allow(G("eng"), DfsFileSystemRights.CreateFiles));

            TestNode volumeLeader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode controlLeader = await cluster.GetControlLeaderAsync(Ct);
            TestNode lagging = cluster.Nodes.First(n => n.Name != volumeLeader.Name && n.Name != controlLeader.Name);
            cluster.Network.Isolate(lagging.Name);
            MessageRule slow = cluster.Network.DelayNext(new MessageFilter(Type: MessageType.RaftBatch), TimeSpan.FromMilliseconds(15), 400);

            var toggles = new List<Interval>();
            var writes = new List<(int Index, long Invoke, long Complete, OpOutcome Outcome)>();
            bool togglingDone = false;

            async Task ToggleAsync()
            {
                for (int i = 0; i < 8; i++)
                {
                    bool add = i % 2 == 0;
                    long invoke = cluster.Time.GetTimestamp();
                    if (add)
                        await cluster.Admin.AddGroupMemberAsync("eng", "alice", Ct);
                    else
                        await cluster.Admin.RemoveGroupMemberAsync("eng", "alice", Ct);
                    long complete = cluster.Time.GetTimestamp();
                    ulong controlCommit = (await cluster.Inspect.GetControlGroupAsync()).CommitIndex;
                    TestNode decider = await cluster.GetLeaderAsync(Vol, Ct);
                    await cluster.WaitUntilAsync(async () => (await decider.Inspect.GetGroupAsync("control"))?.AppliedIndex >= controlCommit,
                        Wait, "the deciding volume leader applied the membership change", Ct);
                    toggles.Add(new Interval(add ? "add" : "remove", invoke, complete, cluster.Time.GetTimestamp()));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(40 + 7 * i), Ct);
                }

                togglingDone = true;
            }

            async Task WriteAsync()
            {
                for (int i = 0; !togglingDone || i < 4; i++)
                {
                    if (i >= 200)
                        break;
                    long invoke = cluster.Time.GetTimestamp();
                    OpOutcome outcome;
                    try
                    {
                        await alice.Volume.Files.WriteAllBytesAsync($"/t/w/f{i}", TestData.Bytes((ulong)(3300 + i), 100 + i), Ct);
                        outcome = OpOutcome.Ok;
                    }
                    catch (UnauthorizedAccessException e) when (DfsErrors.ToCode(e) == DfsErrorCode.AccessDenied)
                    {
                        outcome = OpOutcome.Failed;
                    }
                    catch (Exception e) when (HistoryRecorder.Classify(e) == OpOutcome.Unknown)
                    {
                        outcome = OpOutcome.Unknown;
                    }

                    writes.Add((i, invoke, cluster.Time.GetTimestamp(), outcome));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(9), Ct);
                }
            }

            Task toggler = ToggleAsync();
            Task writer = WriteAsync();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(150), Ct);
            await lagging.CrashAsync(CrashMode.LoseUnflushed);
            await lagging.RestartAsync();
            await Task.WhenAll(toggler, writer);
            slow.Cancel();
            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Wait, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            Assert.Contains(writes, w => w.Outcome == OpOutcome.Ok);
            Assert.Contains(writes, w => w.Outcome == OpOutcome.Failed);

            // Every replica applied the same decisions.
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            var digests = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (TestNode n in cluster.Nodes)
                digests[n.Name] = await n.Inspect.ComputeStateDigestAsync();
            Assert.Single(digests.Values.Distinct(StringComparer.Ordinal));
            var perNode = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (TestNode n in cluster.Nodes)
            {
                VolumeStateInspection state = (await n.Inspect.GetVolumeStateAsync(Vol))!;
                perNode[n.Name] = string.Join(",", writes.Select(w => state.GetFile($"/t/w/f{w.Index}") is { } fi ? $"{w.Index}:{fi.Revision}" : $"{w.Index}:-"));
            }

            Assert.Single(perNode.Values.Distinct(StringComparer.Ordinal));

            // Outcomes match the namespace (acknowledged => present with its bytes, denied => absent).
            var created = new Dictionary<int, bool>();
            foreach ((int index, _, _, OpOutcome outcome) in writes)
            {
                bool exists = await admin.Files.ExistsAsync($"/t/w/f{index}", Ct);
                created[index] = exists;
                if (outcome == OpOutcome.Ok)
                    Assert.True(exists, $"f{index} acknowledged but absent");
                if (outcome == OpOutcome.Failed)
                    Assert.False(exists, $"f{index} denied but present");
                if (exists)
                    await AssertBytesAsync(TestData.Bytes((ulong)(3300 + index), 100 + index), admin.Files.ReadAllBytesAsync($"/t/w/f{index}", Ct), $"f{index}");
            }

            // ...and the membership in force: within a period where the membership was definitely known to the deciding
            // leader (from a change's application by it until the next change was invoked), every decision follows it.
            var periods = new List<(long From, long To, bool Member)> { (long.MinValue, toggles[0].Invoke, false) };
            for (int t = 0; t < toggles.Count; t++)
                periods.Add((toggles[t].Effective, t + 1 < toggles.Count ? toggles[t + 1].Invoke : long.MaxValue, toggles[t].Kind == "add"));
            foreach ((long from, long to, bool member) in periods)
            {
                foreach ((int index, long invoke, long complete, _) in writes)
                {
                    if (invoke < from || complete > to)
                        continue;
                    Assert.True(created[index] == member,
                        $"f{index} [{invoke}, {complete}] {(created[index] ? "created" : "denied")} while alice was definitely {(member ? "in" : "out of")} eng [{from}, {to}]");
                }
            }

            Assert.Contains(writes, w => w.Invoke > toggles[0].Invoke && w.Complete < toggles[^1].Complete);
        });
}
