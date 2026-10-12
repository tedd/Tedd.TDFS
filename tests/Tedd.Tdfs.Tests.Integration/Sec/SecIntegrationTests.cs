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
public sealed class SecIntegrationTests
{
    private const string Vol = SecHelpers.Vol;
    private const string Vol2 = SecHelpers.Vol2;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-001
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The peer identity presented by the untrusted node in SEC-001.</summary>
    public enum PeerIdentityCase
    {
        /// <summary>A configured member restarts with a wrong cluster secret.</summary>
        KnownNodeWrongSecret,

        /// <summary>An unknown node with a wrong cluster secret tries to join.</summary>
        UnknownNodeWrongSecret,

        /// <summary>A node of another cluster (right secret, foreign cluster id) answers at a member's name and endpoint.</summary>
        ForeignClusterIdentity,

        /// <summary>A member removed by the administrator returns with its old disk and credentials.</summary>
        RemovedNodeReturns,

        /// <summary>A member restarts presenting an expired, unknown node certificate.</summary>
        ExpiredNodeCertificate,
    }

    /// <summary>
    /// Unknown, wrong-secret, foreign-cluster, revoked and expired-certificate node identities are never trusted: they do
    /// not become reachable members or leaders, and their storage receipts never count — a policy needing the third zone
    /// that only the untrusted identity could supply fails with PlacementUnavailable, while ordinary commits are backed by
    /// trusted holders only. Restoring the correct identity (where possible) restores the third zone (positive control).
    /// </summary>
    [Theory]
    [InlineData(PeerIdentityCase.KnownNodeWrongSecret)]
    [InlineData(PeerIdentityCase.UnknownNodeWrongSecret)]
    [InlineData(PeerIdentityCase.ForeignClusterIdentity)]
    [InlineData(PeerIdentityCase.RemovedNodeReturns)]
    [InlineData(PeerIdentityCase.ExpiredNodeCertificate)]
    [Catalog("SEC-001", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_001_MutualPeerIdentity(PeerIdentityCase identity) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume vol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            await vol.Files.WriteAllBytesAsync("/control-all3.bin", TestData.Bytes(101, 3 * chunk), new DfsMutationOptions { StoragePolicy = SecHelpers.AllThreePolicy }, Ct);

            TestNode victim = cluster[2];
            ulong incarnationBefore = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == victim.Name).Incarnation;
            byte[] wrongSecret = TestData.Bytes(0x5EC001, 32);
            ReadOnlyMemory<byte> originalSecret = default;
            string? originalCertificate = null;
            string untrusted = victim.Name;
            switch (identity)
            {
                case PeerIdentityCase.KnownNodeWrongSecret:
                    await victim.StopAsync();
                    await victim.RestartAsync(o =>
                    {
                        originalSecret = o.ClusterSecret;
                        return o with { ClusterSecret = wrongSecret };
                    });
                    break;
                case PeerIdentityCase.UnknownNodeWrongSecret:
                    await victim.CrashAsync();
                    TestNode intruder = await cluster.AddNodeAsync(new TestNodeSpec("n3", victim.Labels.Zone), o => o with { ClusterSecret = wrongSecret }, join: true, Ct);
                    untrusted = intruder.Name;
                    break;
                case PeerIdentityCase.ForeignClusterIdentity:
                    await victim.StopAsync();
                    await victim.ReplaceDiskWithEmptyAsync();
                    var foreignId = new Guid(TestData.Bytes(0x5EC0F0, 16));
                    await victim.RestartAsync(o => o with
                    {
                        Bootstrap = new ClusterBootstrap(foreignId, "foreign",
                            [new BootstrapMember(victim.Id, victim.Name, victim.Labels, [victim.Endpoint])],
                            [victim.Name],
                            [new BootstrapPrincipal("admin", "foreign-admin-secret", true)]),
                        JoinSeeds = [],
                    });
                    break;
                case PeerIdentityCase.RemovedNodeReturns:
                    await victim.StopAsync();
                    await cluster.Admin.RemoveNodeAsync(victim.Name, force: true, Ct);
                    await victim.RestartAsync();
                    break;
                default:
                    string pfx = SecHelpers.WriteExpiredCertificate(Path.Combine(cluster.DataRoot, "expired-node.pfx"), victim.Name);
                    await victim.StopAsync();
                    await victim.RestartAsync(o =>
                    {
                        originalCertificate = o.CertificatePath;
                        return o with { CertificatePath = pfx };
                    });
                    break;
            }

            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);

            IReadOnlyList<DfsNodeInfo> nodes = await cluster.Admin.ListNodesAsync(Ct);
            DfsNodeInfo? entry = nodes.SingleOrDefault(n => n.Name == untrusted);
            if (identity == PeerIdentityCase.UnknownNodeWrongSecret)
            {
                Assert.Null(entry);
            }
            else
            {
                Assert.NotNull(entry);
                Assert.False(entry.IsReachable, $"{untrusted} must not be a reachable trusted member ({identity})");
                if (identity == PeerIdentityCase.RemovedNodeReturns)
                    Assert.Equal(DfsNodeStatus.Removed, entry.Status);
                else
                    Assert.Equal(incarnationBefore, entry.Incarnation);
            }

            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            GroupInspection volumeGroup = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            Assert.NotEqual(untrusted, control.Leader);
            Assert.NotEqual(untrusted, volumeGroup.Leader);
            if (identity == PeerIdentityCase.UnknownNodeWrongSecret)
            {
                Assert.DoesNotContain(untrusted, control.Voters);
                Assert.DoesNotContain(untrusted, control.Learners);
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();

            DfsPlacementUnavailableException placement = await Assert.ThrowsAsync<DfsPlacementUnavailableException>(() =>
                vol.Files.WriteAllBytesAsync("/needs-three.bin", TestData.Bytes(102, 3 * chunk), new DfsMutationOptions { StoragePolicy = SecHelpers.AllThreePolicy }, Ct));
            Assert.Equal(DfsErrorCode.PlacementUnavailable, placement.ErrorCode);
            Assert.Equal(3, placement.RequiredCopies);
            Assert.True(placement.AchievedCopies <= 2 && placement.AchievedZones <= 2,
                $"a receipt from the untrusted identity was counted: achieved {placement.AchievedCopies} copies in {placement.AchievedZones} zones");
            Assert.False(await vol.Files.ExistsAsync("/needs-three.bin", Ct));

            byte[] ordinary = TestData.Bytes(103, 3 * chunk);
            await vol.Files.WriteAllBytesAsync("/ordinary.bin", ordinary, Ct);
            FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, "/ordinary.bin");
            Assert.NotNull(file);
            foreach (ExtentInspection extent in file.Extents)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(extent.Object);
                int trusted = holders.Count(h => h != untrusted && h != victim.Name);
                Assert.True(trusted >= 2, $"object {extent.Object} must be durably held by two trusted nodes; holders [{string.Join(", ", holders)}]");
            }

            await RecoveryScenario.AssertFileBytesAsync(vol.Files, "/ordinary.bin", ordinary, "ordinary commit with an untrusted peer present", Ct);

            if (identity is PeerIdentityCase.KnownNodeWrongSecret or PeerIdentityCase.ExpiredNodeCertificate)
            {
                await victim.StopAsync();
                await victim.RestartAsync(o => identity == PeerIdentityCase.KnownNodeWrongSecret
                    ? o with { ClusterSecret = originalSecret }
                    : o with { CertificatePath = originalCertificate });
                await cluster.WaitUntilAsync(async () => (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == victim.Name).IsReachable,
                    TimeSpan.FromSeconds(30), "the node is trusted again with its correct identity");
                await vol.Files.WriteAllBytesAsync("/after-fix-all3.bin", TestData.Bytes(104, 3 * chunk), new DfsMutationOptions { StoragePolicy = SecHelpers.AllThreePolicy }, Ct);
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-002
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Credential presented in SEC-002.</summary>
    public enum CredentialCase
    {
        /// <summary>A principal that does not exist.</summary>
        UnknownPrincipal,

        /// <summary>An existing principal with a wrong secret.</summary>
        WrongSecret,

        /// <summary>An existing principal with another principal's secret.</summary>
        OtherPrincipalsSecret,

        /// <summary>A case variant of an existing principal name (names are ordinal).</summary>
        PrincipalNameCaseVariant,

        /// <summary>A principal deleted while a client is connected.</summary>
        DeletedPrincipal,

        /// <summary>A raw connection that never authenticates.</summary>
        AnonymousRawConnection,

        /// <summary>A raw connection whose authentication failed.</summary>
        RawFailedAuthentication,
    }

    /// <summary>
    /// Anonymous and invalid clients cannot reach protected namespace, data, snapshot or administrative endpoints on any
    /// node: SDK connects fail with AuthenticationFailed, a deleted principal's live session loses access, raw requests
    /// before/without successful authentication are refused, and nothing changes.
    /// </summary>
    [Theory]
    [InlineData(CredentialCase.UnknownPrincipal)]
    [InlineData(CredentialCase.WrongSecret)]
    [InlineData(CredentialCase.OtherPrincipalsSecret)]
    [InlineData(CredentialCase.PrincipalNameCaseVariant)]
    [InlineData(CredentialCase.DeletedPrincipal)]
    [InlineData(CredentialCase.AnonymousRawConnection)]
    [InlineData(CredentialCase.RawFailedAuthentication)]
    [Catalog("SEC-002", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_002_ClientAuthenticationRequired(CredentialCase credential) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice",
                new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write | DfsPermissions.Snapshot });
            TestPrincipal bob = await cluster.CreatePrincipalAsync("bob", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read });
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            byte[] protectedBytes = TestData.Bytes(201, 3 * admin.Capabilities.ChunkSize);
            await adminVol.Files.WriteAllBytesAsync("/protected.bin", protectedBytes, Ct);
            await adminVol.Snapshots.CreateAsync("snap-protected", null, Ct);
            ObjectId protectedObject = (await cluster.Inspect.GetFileAsync(Vol, "/protected.bin"))!.Extents[0].Object;
            string digestBefore = await SecHelpers.SubtreeDigestAsync(adminVol, "/", Ct);
            string globalBefore = await SecHelpers.GlobalDigestAsync(adminVol, admin.Admin, Ct);

            async Task AssertConnectRefusedAsync(DfsCredentials credentials)
            {
                Exception? e = await RecoveryScenario.CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions { Name = "intruder", Credentials = credentials }, Ct));
                DfsAuthenticationException auth = Assert.IsType<DfsAuthenticationException>(e);
                Assert.Equal(DfsErrorCode.AuthenticationFailed, auth.ErrorCode);
                Assert.DoesNotContain(credentials.Secret, auth.Message, StringComparison.Ordinal);
            }

            switch (credential)
            {
                case CredentialCase.UnknownPrincipal:
                    await AssertConnectRefusedAsync(new DfsCredentials("ghost", alice.Credentials.Secret));
                    break;
                case CredentialCase.WrongSecret:
                    await AssertConnectRefusedAsync(new DfsCredentials("alice", alice.Credentials.Secret + "x"));
                    break;
                case CredentialCase.OtherPrincipalsSecret:
                    await AssertConnectRefusedAsync(new DfsCredentials("alice", bob.Credentials.Secret));
                    break;
                case CredentialCase.PrincipalNameCaseVariant:
                    await AssertConnectRefusedAsync(new DfsCredentials("ALICE", alice.Credentials.Secret));
                    break;
                case CredentialCase.DeletedPrincipal:
                {
                    await using DfsClient a = await SecHelpers.ConnectAsAsync(cluster, alice.Credentials, "alice-c", Ct);
                    DfsVolume av = a.GetVolume(Vol);
                    await RecoveryScenario.AssertFileBytesAsync(av.Files, "/protected.bin", protectedBytes, "alice before deletion", Ct);
                    await cluster.Admin.DeletePrincipalAsync("alice", Ct);
                    await cluster.WaitUntilAsync(async () => await RecoveryScenario.CaptureAsync(() => av.Files.ReadAllBytesAsync("/protected.bin", Ct)) is UnauthorizedAccessException,
                        TimeSpan.FromSeconds(15), "a deleted principal's existing session must lose access");
                    var families = new (string Name, Func<Task> Action)[]
                    {
                        ("namespace read", () => av.Files.GetInfoAsync("/protected.bin", Ct)),
                        ("namespace write", () => av.Files.WriteAllBytesAsync("/by-deleted.bin", new byte[] { 1 }, Ct)),
                        ("data stream read", async () => { await using DfsStream s = await av.Files.OpenReadAsync("/protected.bin", Ct); _ = await s.ReadAsync(new byte[16], Ct); }),
                        ("directory enumerate", async () => { await foreach (DfsEntryInfo _ in av.Directories.EnumerateAsync("/", null, Ct)) { } }),
                        ("snapshot list", () => av.Snapshots.ListAsync(Ct)),
                        ("snapshot open", async () => { await using DfsSnapshotView v = await av.Snapshots.OpenAsync("snap-protected", Ct); }),
                        ("snapshot create", () => av.Snapshots.CreateAsync("by-deleted", null, Ct)),
                        ("admin", () => a.Admin.ListVolumesAsync(Ct)),
                    };
                    foreach ((string name, Func<Task> action) in families)
                    {
                        Exception? e = await RecoveryScenario.CaptureAsync(action);
                        Assert.True(e is UnauthorizedAccessException, $"{name} by a deleted principal must be refused, got {(e is null ? "success" : e.GetType().Name)}");
                    }

                    await AssertConnectRefusedAsync(alice.Credentials);
                    break;
                }

                default:
                    foreach (TestNode node in cluster.Nodes)
                    {
                        await using RawProtocolClient raw = await cluster.ConnectRawAsync(node.Name, Ct);
                        if (credential == CredentialCase.RawFailedAuthentication)
                            Assert.Equal(DfsErrorCode.AuthenticationFailed, await raw.AuthenticateAsync(new DfsCredentials("alice", "not-the-secret")));
                        string at = $"{credential} at {node.Name}";
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.GetInfo, "/protected.bin"), at + " GetInfo");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.CreateDirectory, "/anon-dir"), at + " CreateDirectory");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.WriteAll, "/anon.bin"), at + " WriteAll");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.Open, "/anon-open.bin"), at + " Open");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.Move, "/protected.bin", "/stolen.bin"), at + " Move");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.SendPathOperationAsync(Vol, DfsOperationKind.Delete, "/protected.bin"), at + " Delete");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.GetObjectDirectAsync(Vol, protectedObject), at + " GetObject");
                        SecHelpers.AssertUnauthenticatedRefused(await raw.PutObjectDirectAsync(Vol, ObjectId.New(new SeededRandom(202)), TestData.Bytes(203, 4096)), at + " PutObject");
                        foreach (MessageType type in new[] { MessageType.ListSnapshots, MessageType.OpenSnapshot, MessageType.CreateSnapshot, MessageType.AdminRequest, MessageType.BackupRegister, MessageType.OpenSession, MessageType.TxBegin })
                            SecHelpers.AssertUnauthenticatedRefused(await raw.SendFrameAsync(SecHelpers.Frame(type, [0])), $"{at} {type}");
                    }

                    break;
            }

            Assert.Equal(digestBefore, await SecHelpers.SubtreeDigestAsync(adminVol, "/", Ct));
            Assert.Equal(globalBefore, await SecHelpers.GlobalDigestAsync(adminVol, admin.Admin, Ct));
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-003
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Runs every operation of the file/directory/transaction/snapshot catalog as a principal of the given kind: allowed
    /// operations succeed with verified results/effects, denied ones fail with AccessDenied and change nothing. The same
    /// decisions are then checked through raw protocol requests that bypass all SDK validation.
    /// </summary>
    [Theory]
    [InlineData(PrincipalKind.Reader)]
    [InlineData(PrincipalKind.Writer)]
    [InlineData(PrincipalKind.Snapshotter)]
    [InlineData(PrincipalKind.NoGrants)]
    [InlineData(PrincipalKind.OtherVolumeOnly)]
    [InlineData(PrincipalKind.ClusterAdmin)]
    [Catalog("SEC-003", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_003_NamespaceAuthorizationEveryOperation(PrincipalKind kind) =>
        ClusterTest.RunAsync(SecHelpers.Options() with { ScenarioTimeout = TimeSpan.FromMinutes(5) }, async cluster =>
        {
            string tag = kind.ToString().ToLowerInvariant();
            TestPrincipal principal = await SecHelpers.CreatePrincipalAsync(cluster, kind, "p-" + tag);
            DfsPermissions effective = SecHelpers.EffectiveOnVol(kind);
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            await SecOperationCatalog.SetupBaseAsync(adminVol, Ct);
            foreach (SecOperation op in SecOperationCatalog.All)
                await SecOperationCatalog.SetupAsync(adminVol, $"/sec3/{tag}/{op.Name}", op, Ct);

            await using DfsClient client = await SecHelpers.ConnectAsAsync(cluster, principal.Credentials, "p-" + tag, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            var failures = new List<string>();
            int allowedCount = 0, deniedCount = 0;
            foreach (SecOperation op in SecOperationCatalog.All)
            {
                string root = $"/sec3/{tag}/{op.Name}";
                bool holds = kind == PrincipalKind.ClusterAdmin || (effective & op.Required) == op.Required;
                bool ambiguous = holds && op.AmbiguousForNonAdmin && kind != PrincipalKind.ClusterAdmin;
                string before = await SecHelpers.SubtreeDigestAsync(adminVol, root, Ct) + await SecHelpers.GlobalDigestAsync(adminVol, admin.Admin, Ct);
                Exception? e = await RecoveryScenario.CaptureAsync(() => op.Act(volume, root, Ct));
                string after = await SecHelpers.SubtreeDigestAsync(adminVol, root, Ct) + await SecHelpers.GlobalDigestAsync(adminVol, admin.Admin, Ct);
                bool denied = e is UnauthorizedAccessException && DfsErrors.ToCode(e) == DfsErrorCode.AccessDenied;
                if (holds && !ambiguous)
                {
                    allowedCount++;
                    if (e is not null)
                        failures.Add($"{op.Name}: must be allowed for {kind} but failed with {e.GetType().Name} ({DfsErrors.ToCode(e)}): {e.Message}");
                    else if (op.Mutates && before == after)
                        failures.Add($"{op.Name}: allowed mutation for {kind} had no observable effect");
                    else if (!op.Mutates && before != after)
                        failures.Add($"{op.Name}: read operation changed state");
                }
                else if (ambiguous)
                {
                    if (e is not null && !denied)
                        failures.Add($"{op.Name}: must either succeed or be AccessDenied for {kind}, got {e.GetType().Name} ({DfsErrors.ToCode(e)})");
                    if (denied && before != after)
                        failures.Add($"{op.Name}: denied for {kind} but state changed");
                }
                else
                {
                    deniedCount++;
                    if (!denied)
                        failures.Add($"{op.Name}: must be AccessDenied for {kind} but {(e is null ? "succeeded" : $"failed with {e.GetType().Name} ({DfsErrors.ToCode(e)})")}");
                    if (before != after)
                        failures.Add($"{op.Name}: denied for {kind} but state changed");
                }
            }

            // Server-side enforcement independent of SDK validation.
            string rawRoot = $"/sec3/{tag}/raw";
            await adminVol.Files.WriteAllTextAsync(rawRoot + "/a.txt", "a", null, Ct);
            await adminVol.Files.WriteAllTextAsync(rawRoot + "/b.txt", "b", null, Ct);
            await adminVol.Files.WriteAllTextAsync(rawRoot + "/c.txt", "c", null, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(principal.Credentials));
            var rawChecks = new (DfsOperationKind Kind, string Path, string? Path2, DfsPermissions Required, DfsErrorCode WhenAllowed, bool Mutates)[]
            {
                (DfsOperationKind.GetInfo, rawRoot + "/a.txt", null, DfsPermissions.Read, DfsErrorCode.None, false),
                (DfsOperationKind.GetInfo, rawRoot + "/missing.txt", null, DfsPermissions.Read, DfsErrorCode.FileNotFound, false),
                (DfsOperationKind.Open, rawRoot + "/raw-new.bin", null, DfsPermissions.Write, DfsErrorCode.None, true),
                (DfsOperationKind.WriteAll, rawRoot + "/b.txt", null, DfsPermissions.Write, DfsErrorCode.None, true),
                (DfsOperationKind.Move, rawRoot + "/c.txt", rawRoot + "/c-moved.txt", DfsPermissions.Write, DfsErrorCode.None, true),
                (DfsOperationKind.CreateDirectory, rawRoot + "/raw-dir", null, DfsPermissions.Write, DfsErrorCode.None, true),
                (DfsOperationKind.Delete, rawRoot + "/a.txt", null, DfsPermissions.Write, DfsErrorCode.None, true),
            };
            foreach ((DfsOperationKind opKind, string path, string? path2, DfsPermissions required, DfsErrorCode whenAllowed, bool mutates) in rawChecks)
            {
                bool holds = kind == PrincipalKind.ClusterAdmin || (effective & required) == required;
                string before = await SecHelpers.SubtreeDigestAsync(adminVol, rawRoot, Ct);
                DfsErrorCode code = await raw.SendPathOperationAsync(Vol, opKind, path, path2);
                string after = await SecHelpers.SubtreeDigestAsync(adminVol, rawRoot, Ct);
                if (holds)
                {
                    if (code != whenAllowed)
                        failures.Add($"raw {opKind} {path}: expected {whenAllowed} for {kind}, got {code}");
                    if (mutates && before == after)
                        failures.Add($"raw {opKind} {path}: allowed but no effect");
                }
                else
                {
                    if (code != DfsErrorCode.AccessDenied)
                        failures.Add($"raw {opKind} {path}: expected AccessDenied for {kind} (independent of existence), got {code}");
                    if (before != after)
                        failures.Add($"raw {opKind} {path}: denied but state changed");
                }
            }

            TestContext.Current.TestOutputHelper?.WriteLine($"{kind}: {allowedCount} allowed, {deniedCount} denied, {SecOperationCatalog.All.Count - allowedCount - deniedCount} ambiguous operations checked");
            Assert.True(failures.Count == 0, $"{failures.Count} authorization violation(s) for {kind}:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-004
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Who attempts direct storage reads in SEC-004.</summary>
    public enum DirectAccessor
    {
        /// <summary>No authentication.</summary>
        Unauthenticated,

        /// <summary>Authenticated, no grants on either volume.</summary>
        NoGrants,

        /// <summary>Read on "vol2" only.</summary>
        OtherVolumeReader,

        /// <summary>Read on "vol" but no data capability.</summary>
        SameVolumeReaderWithoutCapability,

        /// <summary>Cluster administrator without a data capability.</summary>
        ClusterAdminWithoutCapability,
    }

    /// <summary>
    /// Guessing or learning object ids grants nothing: direct storage reads of a protected file's objects (and of a
    /// guessed id) on every holder are refused for every principal without a valid capability, under either volume name,
    /// without revealing existence; the SDK path serves the bytes only to authorized principals.
    /// </summary>
    [Theory]
    [InlineData(DirectAccessor.Unauthenticated)]
    [InlineData(DirectAccessor.NoGrants)]
    [InlineData(DirectAccessor.OtherVolumeReader)]
    [InlineData(DirectAccessor.SameVolumeReaderWithoutCapability)]
    [InlineData(DirectAccessor.ClusterAdminWithoutCapability)]
    [Catalog("SEC-004", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_004_DirectObjectReadCannotBypassAcl(DirectAccessor accessor) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            int chunk = admin.Capabilities.ChunkSize;
            byte[] secret = TestData.Bytes(401, 3 * chunk + 5);
            await admin.GetVolume(Vol).Files.WriteAllBytesAsync("/secret.bin", secret, Ct);
            await admin.GetVolume(Vol2).Files.WriteAllBytesAsync("/other.bin", TestData.Bytes(402, chunk + 1), Ct);
            FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, "/secret.bin");
            Assert.NotNull(inspected);
            ObjectId[] objects = inspected.Extents.Select(e => e.Object).Distinct().ToArray();
            Assert.NotEmpty(objects);
            ObjectId guessed = ObjectId.New(new SeededRandom(403));

            TestPrincipal? principal = accessor switch
            {
                DirectAccessor.Unauthenticated => null,
                DirectAccessor.NoGrants => await cluster.CreatePrincipalAsync("p-none", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.None }),
                DirectAccessor.OtherVolumeReader => await cluster.CreatePrincipalAsync("p-vol2", new Dictionary<string, DfsPermissions> { [Vol2] = DfsPermissions.Read }),
                DirectAccessor.SameVolumeReaderWithoutCapability => await cluster.CreatePrincipalAsync("p-reader", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read }),
                _ => await cluster.CreatePrincipalAsync("p-admin", new Dictionary<string, DfsPermissions>(), clusterAdmin: true),
            };

            int attempts = 0;
            foreach (ObjectId id in objects.Append(guessed))
            {
                IReadOnlyList<string> targets = id == guessed ? cluster.Nodes.Select(n => n.Name).ToArray() : await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.NotEmpty(targets);
                foreach (string holder in targets)
                {
                    await using RawProtocolClient raw = await cluster.ConnectRawAsync(holder, Ct);
                    if (principal is not null)
                        Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(principal.Credentials));
                    foreach (string volumeName in new[] { Vol, Vol2 })
                    {
                        DfsErrorCode code = await raw.GetObjectDirectAsync(volumeName, id);
                        attempts++;
                        string at = $"{accessor}: direct read of {(id == guessed ? "guessed" : "existing")} object {id} as {volumeName} on {holder}";
                        if (principal is null)
                            SecHelpers.AssertUnauthenticatedRefused(code, at);
                        else
                            Assert.True(code == DfsErrorCode.AccessDenied, $"{at} must be AccessDenied (no capability; no existence oracle), got {code}");
                    }
                }
            }

            Assert.True(attempts >= 2 * (objects.Length + cluster.Nodes.Count));
            if (principal is not null)
            {
                await using DfsClient c = await SecHelpers.ConnectAsAsync(cluster, principal.Credentials, "p-sdk", Ct);
                byte[]? read = null;
                Exception? e = await RecoveryScenario.CaptureAsync(async () => read = await c.GetVolume(Vol).Files.ReadAllBytesAsync("/secret.bin", Ct));
                if (accessor is DirectAccessor.SameVolumeReaderWithoutCapability or DirectAccessor.ClusterAdminWithoutCapability)
                {
                    Assert.Null(e);
                    AssertEx.BytesEqual(secret, read, "the authorized path (metadata + capability) serves the bytes");
                }
                else
                {
                    SecHelpers.AssertAccessDenied(e, $"{accessor} reading /secret.bin through the SDK");
                }
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-005
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Uploading arbitrary objects without namespace authority and a staging capability is refused on every node for every
    /// principal; crafted publication frames naming the injected object are rejected; the object is never stored, never
    /// reachable and the namespace of both volumes is unchanged.
    /// </summary>
    [Theory]
    [InlineData(DirectAccessor.Unauthenticated)]
    [InlineData(DirectAccessor.NoGrants)]
    [InlineData(DirectAccessor.OtherVolumeReader)]
    [InlineData(DirectAccessor.SameVolumeReaderWithoutCapability)]
    [InlineData(DirectAccessor.ClusterAdminWithoutCapability)]
    [Catalog("SEC-005", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_005_DirectWriteCannotPublishNamespace(DirectAccessor accessor) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            await VolumeContentFixture.PopulateAsync(adminVol, 501, Ct);
            string before = await SecHelpers.SubtreeDigestAsync(adminVol, "/", Ct);
            string before2 = await SecHelpers.SubtreeDigestAsync(admin.GetVolume(Vol2), "/", Ct);

            TestPrincipal? principal = accessor switch
            {
                DirectAccessor.Unauthenticated => null,
                DirectAccessor.NoGrants => await cluster.CreatePrincipalAsync("w-none", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.None }),
                DirectAccessor.OtherVolumeReader => await cluster.CreatePrincipalAsync("w-vol2", new Dictionary<string, DfsPermissions> { [Vol2] = DfsPermissions.All }),
                DirectAccessor.SameVolumeReaderWithoutCapability => await cluster.CreatePrincipalAsync("w-writer", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read | DfsPermissions.Write }),
                _ => await cluster.CreatePrincipalAsync("w-admin", new Dictionary<string, DfsPermissions>(), clusterAdmin: true),
            };

            var random = new SeededRandom(502);
            ObjectId injected = ObjectId.New(random);
            byte[] payload = TestData.Bytes(503, 70_000);
            byte[] idBytes = new byte[16];
            BitConverter.TryWriteBytes(idBytes, (ulong)injected.Value);
            BitConverter.TryWriteBytes(idBytes.AsSpan(8), (ulong)(injected.Value >> 64));
            foreach (TestNode node in cluster.Nodes)
            {
                await using RawProtocolClient raw = await cluster.ConnectRawAsync(node.Name, Ct);
                if (principal is not null)
                    Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(principal.Credentials));
                foreach (string volumeName in new[] { Vol, Vol2 })
                {
                    DfsErrorCode code = await raw.PutObjectDirectAsync(volumeName, injected, payload);
                    string at = $"{accessor}: direct upload as {volumeName} to {node.Name}";
                    if (principal is null)
                        SecHelpers.AssertUnauthenticatedRefused(code, at);
                    else
                        Assert.True(code == DfsErrorCode.AccessDenied, $"{at} without a staging capability must be AccessDenied, got {code}");
                }

                byte[] pathBytes = Encoding.UTF8.GetBytes("/injected.bin");
                foreach (MessageType type in new[] { MessageType.CommitFile, MessageType.WriteAll, MessageType.Append, MessageType.ReplicateObject, MessageType.CopyObjects, MessageType.TxCommit })
                {
                    DfsErrorCode code = await raw.SendFrameAsync(SecHelpers.Frame(type, [.. idBytes, .. pathBytes]));
                    Assert.True(code != DfsErrorCode.None, $"{accessor}: crafted {type} naming the injected object was accepted on {node.Name}");
                }
            }

            foreach (TestNode node in cluster.Nodes)
                Assert.False(await node.Inspect.HasObjectAsync(injected), $"{node.Name} stored the injected object");
            Assert.DoesNotContain(injected, await cluster.Inspect.GetAllStoredObjectsAsync());
            Assert.DoesNotContain(injected, (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects());
            Assert.DoesNotContain(injected, (await cluster.Inspect.GetVolumeStateAsync(Vol2)).ReachableObjects());
            Assert.False(await adminVol.Files.ExistsAsync("/injected.bin", Ct));
            Assert.Equal(before, await SecHelpers.SubtreeDigestAsync(adminVol, "/", Ct));
            Assert.Equal(before2, await SecHelpers.SubtreeDigestAsync(admin.GetVolume(Vol2), "/", Ct));

            if (accessor is DirectAccessor.SameVolumeReaderWithoutCapability && principal is not null)
            {
                // Positive control: the same principal publishes through the authorized path.
                await using DfsClient c = await SecHelpers.ConnectAsAsync(cluster, principal.Credentials, "w-sdk", Ct);
                await c.GetVolume(Vol).Files.WriteAllBytesAsync("/legit.bin", payload, Ct);
                FileInspection? legit = await cluster.Inspect.GetFileAsync(Vol, "/legit.bin");
                Assert.NotNull(legit);
                Assert.DoesNotContain(legit.Extents, e => e.Object == injected);
                await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/legit.bin", payload, "authorized publication", Ct);
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-008
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Credentials attempting to erase protected backup history in SEC-008.</summary>
    public enum RetentionAttacker
    {
        /// <summary>The source's export credentials for the target.</summary>
        ExportCredentials,

        /// <summary>The source cluster administrator.</summary>
        SourceClusterAdmin,

        /// <summary>The backup host's reader principal.</summary>
        BackupReader,
    }

    /// <summary>
    /// Export, source-admin and reader credentials can neither delete nor shorten/release protected recovery points on the
    /// backup host; removing the target or deleting snapshots on the source erases nothing at the destination; export
    /// credentials still transfer new snapshots and the retention admin can extend retention (positive controls).
    /// </summary>
    [Theory]
    [InlineData(RetentionAttacker.ExportCredentials)]
    [InlineData(RetentionAttacker.SourceClusterAdmin)]
    [InlineData(RetentionAttacker.BackupReader)]
    [Catalog("SEC-008", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_008_BackupRetentionPermissionsSeparated(RetentionAttacker attacker) =>
        ClusterTest.RunAsync(SecHelpers.Options(backups: 1), async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume vol = admin.GetVolume(Vol);
            ExpectedVolumeContent expected = await VolumeContentFixture.PopulateAsync(vol, 801, Ct);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DateTimeOffset expiry = backup.Clock.GetUtcNow() + TimeSpan.FromHours(1);
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("S1", new DfsSnapshotOptions { Retention = new DfsRetention(null, Hold: true) }, Ct);
            await RecoveryScenario.PublishAsync(vol, backup, s1, Ct);
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("S2", new DfsSnapshotOptions { Retention = new DfsRetention(expiry) }, Ct);
            await RecoveryScenario.PublishAsync(vol, backup, s2, Ct);

            DfsCredentials credentials = attacker switch
            {
                RetentionAttacker.ExportCredentials => backup.ExportCredentials,
                RetentionAttacker.SourceClusterAdmin => cluster.AdminCredentials,
                _ => backup.ReaderCredentials,
            };
            DfsClient? attackerClient = null;
            Exception? connect = await RecoveryScenario.CaptureAsync(async () =>
                attackerClient = await cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = "attacker", Credentials = credentials }, Ct));
            if (connect is not null)
            {
                Assert.IsAssignableFrom<UnauthorizedAccessException>(connect);
            }
            else
            {
                Assert.NotNull(attackerClient);
                await using DfsClient attackerOwned = attackerClient;
                DfsSnapshotClient snapshots = attackerOwned.GetVolume(Vol).Snapshots;
                var attempts = new (string Name, Func<Task> Action)[]
                {
                    ("delete held S1", () => snapshots.DeleteAsync("S1", Ct)),
                    ("delete unexpired S2", () => snapshots.DeleteAsync("S2", Ct)),
                    ("release hold on S1", () => snapshots.SetRetentionAsync("S1", new DfsRetention(null, false), Ct)),
                    ("shorten S2", () => snapshots.SetRetentionAsync("S2", new DfsRetention(backup.Clock.GetUtcNow() + TimeSpan.FromMinutes(1)), Ct)),
                    ("expire S2 in the past", () => snapshots.SetRetentionAsync("S2", new DfsRetention(backup.Clock.GetUtcNow() - TimeSpan.FromDays(1)), Ct)),
                };
                foreach ((string name, Func<Task> action) in attempts)
                {
                    Exception? e = await RecoveryScenario.CaptureAsync(action);
                    Assert.True(e is UnauthorizedAccessException, $"{attacker}: '{name}' must be denied, got {(e is null ? "success" : e.GetType().Name)}");
                }
            }

            await vol.Backups.RemoveTargetAsync(backup.TargetName, Ct);
            await vol.Snapshots.DeleteAsync("S1", Ct);
            await vol.Snapshots.DeleteAsync("S2", Ct);

            await using DfsClient retentionAdmin = await cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = "retention", Credentials = backup.RetentionAdminCredentials }, Ct);
            DfsSnapshotClient protectedView = retentionAdmin.GetVolume(Vol).Snapshots;
            DfsSnapshotInfo held = await protectedView.GetAsync("S1", Ct);
            Assert.True(held.Retention.Hold);
            DfsSnapshotInfo timed = await protectedView.GetAsync("S2", Ct);
            Assert.True(timed.Retention.ExpiresUtc is { } e2 && (e2 - expiry).Duration() <= TimeSpan.FromSeconds(1), "S2's retention must be unchanged");
            foreach (string name in new[] { "S1", "S2" })
            {
                await using DfsSnapshotView view = await protectedView.OpenAsync(name, Ct);
                VolumeContentFixture.AssertMatches(expected, await VolumeContentFixture.CaptureAsync(view, Ct), $"{name} survives source-side removal and denied attempts");
            }

            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DfsSnapshotInfo s3 = await vol.Snapshots.CreateAsync("S3", null, Ct);
            await RecoveryScenario.PublishAsync(vol, backup, s3, Ct);
            DateTimeOffset extended = expiry + TimeSpan.FromDays(1);
            await protectedView.SetRetentionAsync("S2", new DfsRetention(extended), Ct);
            Assert.True((await protectedView.GetAsync("S2", Ct)).Retention.ExpiresUtc is { } e3 && (e3 - extended).Duration() <= TimeSpan.FromSeconds(1),
                "the retention admin may extend protection");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-009
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The backup host authorizes readers by its own local principals and grants: source principals are not accepted, and
    /// source ACL changes made while the backup is disconnected (revoke, delete, new principal) neither propagate nor are
    /// claimed to; the source reports the target offline; local access is unchanged during and after reconnection.
    /// </summary>
    [Fact]
    [Catalog("SEC-009", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_009_BackupOfflineAuthorizationIsExplicit() =>
        ClusterTest.RunAsync(SecHelpers.Options(backups: 1), async cluster =>
        {
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.Read });
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume vol = admin.GetVolume(Vol);
            ExpectedVolumeContent expected = await VolumeContentFixture.PopulateAsync(vol, 901, Ct);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DfsSnapshotInfo s = await vol.Snapshots.CreateAsync("S", null, Ct);
            await RecoveryScenario.PublishAsync(vol, backup, s, Ct);

            async Task AssertLocalPolicyAsync(string phase, params DfsCredentials[] refused)
            {
                await using (DfsClient reader = await cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = "local-reader", Credentials = backup.ReaderCredentials }, Ct))
                await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
                    VolumeContentFixture.AssertMatches(expected, await VolumeContentFixture.CaptureAsync(view, Ct), $"local reader {phase}");
                foreach (DfsCredentials credentials in refused)
                {
                    Exception? e = await RecoveryScenario.CaptureAsync(async () =>
                    {
                        await using DfsClient c = await cluster.ConnectBackupAsync(backup.Index, new TestClientOptions { Name = "foreign-reader", Credentials = credentials }, Ct);
                        await using DfsSnapshotView view = await c.GetVolume(Vol).Snapshots.OpenAsync("S", Ct);
                        _ = await view.Files.ReadAllBytesAsync(VolumeContentFixture.SparsePath, Ct);
                    });
                    Assert.True(e is UnauthorizedAccessException, $"source principal {credentials.Principal} must not be authorized by the backup host {phase}");
                }
            }

            await AssertLocalPolicyAsync("while connected", alice.Credentials, cluster.AdminCredentials);

            foreach (TestNode node in cluster.Nodes)
                cluster.Network.CutBoth(node.Name, backup.Name);
            await cluster.Admin.RevokeAsync(Vol, "alice", DfsPermissions.Read, Ct);
            await cluster.Admin.DeletePrincipalAsync("alice", Ct);
            TestPrincipal mallory = await cluster.CreatePrincipalAsync("mallory", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.All });
            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(backup.TargetName, Ct)).State is DfsBackupState.Offline,
                TimeSpan.FromSeconds(30), "the source reports the disconnected target as offline, not as synchronised");
            await AssertLocalPolicyAsync("while disconnected", alice.Credentials, mallory.Credentials, cluster.AdminCredentials);

            cluster.Network.Heal();
            await vol.Backups.RequestSyncAsync(backup.TargetName, "S", Ct);
            await AssertLocalPolicyAsync("after reconnection", alice.Credentials, mallory.Credentials, cluster.AdminCredentials);
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-010
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Identical names, commit-token integers, operation ids, object ids and identical content (dedup hints) across two
    /// volumes with disjoint principals never cross the volume/principal security boundary.
    /// </summary>
    [Fact]
    [Catalog("SEC-010", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_010_CrossVolumeObjectIsolation() =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            TestPrincipal pa = await cluster.CreatePrincipalAsync("pa", new Dictionary<string, DfsPermissions> { [Vol] = DfsPermissions.All });
            TestPrincipal pb = await cluster.CreatePrincipalAsync("pb", new Dictionary<string, DfsPermissions> { [Vol2] = DfsPermissions.All });
            await using DfsClient a = await SecHelpers.ConnectAsAsync(cluster, pa.Credentials, "pa", Ct);
            await using DfsClient b = await SecHelpers.ConnectAsAsync(cluster, pb.Credentials, "pb", Ct);
            DfsVolume va = a.GetVolume(Vol);
            DfsVolume vb = b.GetVolume(Vol2);
            int chunk = a.Capabilities.ChunkSize;
            byte[] secretA = TestData.Bytes(1001, 3 * chunk + 1);
            byte[] contentB = TestData.Bytes(1002, 3 * chunk + 1);
            DfsOperationId idA = await va.NewOperationIdAsync(Ct);
            CommitToken ta = await va.Files.WriteAllBytesAsync("/shared/name.bin", secretA, new DfsMutationOptions { OperationId = idA }, Ct);
            CommitToken tb = await vb.Files.WriteAllBytesAsync("/shared/name.bin", contentB, Ct);

            await RecoveryScenario.AssertFileBytesAsync(va.Files, "/shared/name.bin", secretA, "same name, volume vol", Ct);
            await RecoveryScenario.AssertFileBytesAsync(vb.Files, "/shared/name.bin", contentB, "same name, volume vol2", Ct);

            foreach ((DfsClient client, string volume, string who) in new[] { (b, Vol, "pb on vol"), (a, Vol2, "pa on vol2") })
            {
                DfsVolume foreign = client.GetVolume(volume);
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => foreign.Files.ReadAllBytesAsync("/shared/name.bin", Ct)), who + " read");
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => foreign.Files.ExistsAsync("/shared/name.bin", Ct)), who + " exists");
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => foreign.Files.WriteAllBytesAsync("/shared/name.bin", new byte[] { 0 }, Ct)), who + " overwrite");
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => foreign.Snapshots.ListAsync(Ct)), who + " snapshot list");
            }

            RecoveryScenario.AssertNotComparable(ta, tb, "tokens of different volumes never compare, even with equal sequence numbers");
            Exception? minimum = await RecoveryScenario.CaptureAsync(() => vb.Snapshots.CreateAsync("min-from-vol", new DfsSnapshotOptions { MinimumCommit = ta }, Ct));
            RecoveryScenario.AssertFailedWith(minimum, "a MinimumCommit token of another volume", DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidArgument);
            Assert.DoesNotContain(await vb.Snapshots.ListAsync(Ct), s => s.Name == "min-from-vol");

            SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => b.GetVolume(Vol).QueryOperationAsync(idA, false, Ct)), "pb querying pa's operation");
            Exception? reuse = await RecoveryScenario.CaptureAsync(() => vb.Files.WriteAllBytesAsync("/reuse.bin", TestData.Bytes(1003, 10), new DfsMutationOptions { OperationId = idA }, Ct));
            RecoveryScenario.AssertFailedWith(reuse, "pb reusing pa's operation id in another volume",
                DfsErrorCode.OperationExpired, DfsErrorCode.OperationIdMismatch, DfsErrorCode.InvalidArgument, DfsErrorCode.AccessDenied, DfsErrorCode.EpochMismatch);
            Assert.False(await vb.Files.ExistsAsync("/reuse.bin", Ct));

            FileInspection? fa = await cluster.Inspect.GetFileAsync(Vol, "/shared/name.bin");
            Assert.NotNull(fa);
            ObjectId[] objectsA = fa.Extents.Select(e => e.Object).Distinct().ToArray();
            foreach (ObjectId id in objectsA)
            {
                foreach (string holder in await cluster.Inspect.GetObjectHoldersAsync(id))
                {
                    await using RawProtocolClient raw = await cluster.ConnectRawAsync(holder, Ct);
                    Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(pb.Credentials));
                    Assert.Equal(DfsErrorCode.AccessDenied, await raw.GetObjectDirectAsync(Vol2, id));
                    Assert.Equal(DfsErrorCode.AccessDenied, await raw.GetObjectDirectAsync(Vol, id));
                    Assert.Equal(DfsErrorCode.AccessDenied, await raw.PutObjectDirectAsync(Vol2, id, contentB.AsMemory(0, 1000)));
                }
            }

            await RecoveryScenario.AssertFileBytesAsync(va.Files, "/shared/name.bin", secretA, "an identical object id in another domain cannot alter vol's data", Ct);

            await vb.Files.WriteAllBytesAsync("/dup.bin", secretA, Ct);
            FileInspection? fdup = await cluster.Inspect.GetFileAsync(Vol2, "/dup.bin");
            Assert.NotNull(fdup);
            Assert.Empty(objectsA.Intersect(fdup.Extents.Select(e => e.Object)));
            VolumeId volBId = (await vb.GetInfoAsync(Ct)).Id;
            VolumeId volAId = (await va.GetInfoAsync(Ct)).Id;
            foreach (TestNode node in cluster.Nodes)
            {
                foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol2))
                {
                    Assert.Equal(volBId, record.Volume);
                    Assert.DoesNotContain(record.Id, objectsA);
                }

                foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
                    Assert.Equal(volAId, record.Volume);
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-011
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The protected exchange tampered with in SEC-011.</summary>
    public enum TamperTarget
    {
        /// <summary>The client's handshake.</summary>
        Handshake,

        /// <summary>A WriteAll request.</summary>
        WriteAllRequest,

        /// <summary>An Append request.</summary>
        AppendRequest,

        /// <summary>An object upload to a storage node.</summary>
        PutObjectRequest,

        /// <summary>A stream's CommitFile request.</summary>
        CommitFileRequest,

        /// <summary>An object read reply from a storage node.</summary>
        GetObjectReply,

        /// <summary>A metadata read reply (inline data) from the leader.</summary>
        ReadRangeReply,
    }

    /// <summary>
    /// Tampers with protected transmissions below the transport's integrity protection; the receiver rejects them (the
    /// exchange is retried/closed) and no altered data or metadata is ever accepted: final contents are exactly the old or
    /// the intended new state (resolved by operation id when uncertain), never duplicated, and reads never return altered bytes.
    /// </summary>
    [Theory]
    [InlineData(TamperTarget.Handshake, TransportTamper.FlipBit)]
    [InlineData(TamperTarget.Handshake, TransportTamper.Truncate)]
    [InlineData(TamperTarget.WriteAllRequest, TransportTamper.FlipBit)]
    [InlineData(TamperTarget.WriteAllRequest, TransportTamper.Scramble)]
    [InlineData(TamperTarget.AppendRequest, TransportTamper.FlipBit)]
    [InlineData(TamperTarget.AppendRequest, TransportTamper.Replay)]
    [InlineData(TamperTarget.PutObjectRequest, TransportTamper.FlipBit)]
    [InlineData(TamperTarget.PutObjectRequest, TransportTamper.AppendGarbage)]
    [InlineData(TamperTarget.CommitFileRequest, TransportTamper.Truncate)]
    [InlineData(TamperTarget.CommitFileRequest, TransportTamper.Replay)]
    [InlineData(TamperTarget.GetObjectReply, TransportTamper.FlipBit)]
    [InlineData(TamperTarget.ReadRangeReply, TransportTamper.Scramble)]
    [Catalog("SEC-011", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_011_TransportTamperingDetected(TamperTarget target, TransportTamper mode) =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const string client = "client:t0";
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            byte[] original = TestData.Bytes(1101, 3 * chunk + 3);
            byte[] small = TestData.Bytes(1102, 100);
            await adminVol.Files.WriteAllBytesAsync("/t.bin", original, Ct);
            await adminVol.Files.WriteAllBytesAsync("/inline.bin", small, Ct);

            if (target == TamperTarget.Handshake)
            {
                MessageRule hello = cluster.Network.TamperNext(new MessageFilter(From: client, Type: MessageType.Hello), mode);
                DfsClient? connected = null;
                Exception? e = await RecoveryScenario.CaptureAsync(async () => connected = await cluster.ConnectAsync(new TestClientOptions { Name = "t0" }, Ct));
                Assert.True(hello.Matched >= 1, "the tamper point was not reached");
                if (e is null)
                {
                    Assert.NotNull(connected);
                    await using DfsClient c = connected;
                    Assert.Equal(cluster.AdminCredentials.Principal, c.Principal);
                    await RecoveryScenario.AssertFileBytesAsync(c.GetVolume(Vol).Files, "/t.bin", original, "after a rejected tampered handshake packet", Ct);
                }
                else
                {
                    RecoveryScenario.AssertFailedWith(e, "a tampered handshake fails closed", DfsErrorCode.AuthenticationFailed, DfsErrorCode.ProtocolError,
                        DfsErrorCode.Timeout, DfsErrorCode.NodeUnavailable, DfsErrorCode.Unavailable);
                }

                return;
            }

            await using DfsClient t0 = await cluster.ConnectAsync(new TestClientOptions { Name = "t0" }, Ct);
            DfsVolume volume = t0.GetVolume(Vol);
            byte[] replacement = TestData.Bytes(1103, 3 * chunk + 9);
            byte[] suffix = TestData.Bytes(1104, chunk + 17);
            MessageRule rule;
            switch (target)
            {
                case TamperTarget.WriteAllRequest:
                case TamperTarget.PutObjectRequest:
                {
                    MessageType type = target == TamperTarget.WriteAllRequest ? MessageType.WriteAll : MessageType.PutObject;
                    rule = cluster.Network.TamperNext(new MessageFilter(From: client, Type: type, Direction: Tedd.Tdfs.Transport.LinkDirection.Request), mode);
                    DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                    Exception? e = await RecoveryScenario.CaptureAsync(() => volume.Files.WriteAllBytesAsync("/t.bin", replacement, new DfsMutationOptions { OperationId = id }, Ct));
                    bool committed = e is null || await ResolveAsync(adminVol, id);
                    await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/t.bin", committed ? replacement : original, $"tampered {type}", Ct);
                    break;
                }

                case TamperTarget.AppendRequest:
                {
                    rule = cluster.Network.TamperNext(new MessageFilter(From: client, Type: MessageType.Append, Direction: Tedd.Tdfs.Transport.LinkDirection.Request), mode);
                    DfsOperationId id = await volume.NewOperationIdAsync(Ct);
                    Exception? e = await RecoveryScenario.CaptureAsync(() => volume.Files.AppendAllBytesAsync("/t.bin", suffix, new DfsMutationOptions { OperationId = id }, Ct));
                    bool committed = e is null || await ResolveAsync(adminVol, id);
                    await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/t.bin", committed ? [.. original, .. suffix] : original,
                        "tampered/replayed append applies at most once", Ct);
                    break;
                }

                case TamperTarget.CommitFileRequest:
                {
                    rule = cluster.Network.TamperNext(new MessageFilter(From: client, Type: MessageType.CommitFile, Direction: Tedd.Tdfs.Transport.LinkDirection.Request), mode);
                    DfsStream stream = await volume.Files.OpenAsync("/t.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                    await using (stream)
                    {
                        await stream.WriteAsync(replacement.AsMemory(0, 4096), Ct);
                        Exception? e = await RecoveryScenario.CaptureAsync(() => stream.CommitAsync(Ct));
                        byte[] intended = [.. replacement.AsSpan(0, 4096), .. original.AsSpan(4096)];
                        bool committed = e is null || (e is DfsCommitOutcomeUnknownException unknown && await ResolveAsync(adminVol, unknown.OperationId));
                        await RecoveryScenario.AssertFileBytesAsync(adminVol.Files, "/t.bin", committed ? intended : original, "tampered/replayed commit", Ct);
                    }

                    break;
                }

                case TamperTarget.GetObjectReply:
                {
                    rule = cluster.Network.TamperNext(new MessageFilter(To: client, Type: MessageType.GetObjectResult, Direction: Tedd.Tdfs.Transport.LinkDirection.Reply), mode);
                    byte[]? read = null;
                    Exception? e = await RecoveryScenario.CaptureAsync(async () => read = await volume.Files.ReadAllBytesAsync("/t.bin", Ct));
                    if (e is null)
                        AssertEx.BytesEqual(original, read, "a read that succeeds after a tampered reply returns verified bytes");
                    else
                        Assert.True(e is IOException, $"a tampered object reply must fail as an I/O/integrity error, got {e.GetType().Name}");
                    break;
                }

                default:
                {
                    rule = cluster.Network.TamperNext(new MessageFilter(To: client, Type: MessageType.ReadRangeResult, Direction: Tedd.Tdfs.Transport.LinkDirection.Reply), mode);
                    byte[]? read = null;
                    Exception? e = await RecoveryScenario.CaptureAsync(async () => read = await volume.Files.ReadAllBytesAsync("/inline.bin", Ct));
                    if (e is null)
                        AssertEx.BytesEqual(small, read, "a read that succeeds after a tampered reply returns verified bytes");
                    else
                        Assert.True(e is IOException, $"a tampered metadata reply must fail as an I/O/integrity error, got {e.GetType().Name}");
                    break;
                }
            }

            Assert.True(rule.Matched >= 1, $"the tamper point ({target}, {mode}) was not reached");
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);

    private static async Task<bool> ResolveAsync(DfsVolume volume, DfsOperationId id)
    {
        DfsOperationOutcome outcome = await volume.QueryOperationAsync(id, resolve: true, Ct);
        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve must give a definite outcome, got {outcome.Status}");
        return outcome.Status == DfsOutcomeStatus.Committed;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-012
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Writes distinctive plaintext canaries into payloads (extent and inline), names, snapshot names, a principal secret and
    /// a recovery-key passphrase, forces journal checkpoints and a backup, stops every node and scans every byte (and file
    /// name) under every live and backup node's data directory: with encryption at rest nothing leaks; without it the
    /// payload canary is found (proving the scan works) while secrets still never appear.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Catalog("SEC-012", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_012_AtRestEncryptionCoversRequiredData(bool encryptAtRest) =>
        ClusterTest.RunAsync(SecHelpers.Options(backups: 1, encryptAtRest: encryptAtRest), async cluster =>
        {
            const string dataCanary = "TDFS-PLAINTEXT-CANARY-DATA-7f3a";
            const string inlineCanary = "TDFS-INLINE-CANARY-91c2";
            const string nameCanary = "tdfs-name-canary-55e1";
            const string snapshotCanary = "tdfs-snap-canary-0b7d";
            const string secretCanary = "TDFS-SECRET-CANARY-3d9e";
            const string passphraseCanary = "TDFS-PASSPHRASE-CANARY-a4f0";

            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume vol = admin.GetVolume(Vol);
            int chunk = admin.Capabilities.ChunkSize;
            byte[] data = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(dataCanary + "|", 3 * chunk / (dataCanary.Length + 1) + 1)));
            await vol.Files.WriteAllBytesAsync("/" + nameCanary + "/payload.bin", data, Ct);
            await vol.Files.WriteAllTextAsync("/inline.txt", inlineCanary, null, Ct);
            await admin.Admin.CreatePrincipalAsync("canary-user", secretCanary, false, Ct);
            await admin.Admin.GrantAsync(Vol, "canary-user", DfsPermissions.Read, Ct);
            for (int i = 0; i < 250; i++)
                await vol.Files.WriteAllTextAsync("/inline.txt", inlineCanary + i.ToString(CultureInfo.InvariantCulture), null, Ct);
            byte[] recoveryKey = await admin.Admin.ExportRecoveryKeyAsync(passphraseCanary, Ct);
            Assert.NotEmpty(recoveryKey);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DfsSnapshotInfo snapshot = await vol.Snapshots.CreateAsync(snapshotCanary, null, Ct);
            await RecoveryScenario.PublishAsync(vol, backup, snapshot, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            foreach (TestBackupNode b in cluster.BackupNodes)
                await b.StopAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.StopAsync();

            var roots = cluster.Nodes.Select(n => n.Disk.RootDirectory).Concat(cluster.BackupNodes.Select(b => b.Disk.RootDirectory)).ToArray();
            var found = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            string[] canaries = [dataCanary, inlineCanary, nameCanary, snapshotCanary, secretCanary, passphraseCanary];
            long scannedBytes = 0;
            int scannedFiles = 0;
            foreach (string? root in roots)
            {
                Assert.NotNull(root);
                foreach (string file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
                {
                    byte[] raw = await File.ReadAllBytesAsync(file, Ct);
                    scannedBytes += raw.LongLength;
                    scannedFiles++;
                    foreach (string canary in canaries)
                    {
                        bool hit = file.Contains(canary, StringComparison.OrdinalIgnoreCase)
                            || SecHelpers.Contains(raw, Encoding.UTF8.GetBytes(canary))
                            || SecHelpers.Contains(raw, Encoding.Unicode.GetBytes(canary));
                        if (hit)
                        {
                            if (!found.TryGetValue(canary, out List<string>? list))
                                found[canary] = list = [];
                            list.Add(Path.GetRelativePath(root, file));
                        }
                    }
                }
            }

            TestContext.Current.TestOutputHelper?.WriteLine($"scanned {scannedFiles} files, {scannedBytes} bytes; hits: {string.Join("; ", found.Select(f => f.Key + "=" + string.Join(",", f.Value)))}");
            Assert.True(scannedFiles > 0 && scannedBytes > data.Length, "the scan must cover the stored data");
            Assert.False(found.ContainsKey(secretCanary), "a principal secret is stored in plaintext");
            Assert.False(found.ContainsKey(passphraseCanary), "a recovery-key passphrase is stored in plaintext");
            if (encryptAtRest)
            {
                foreach (string canary in new[] { dataCanary, inlineCanary, nameCanary, snapshotCanary })
                    Assert.False(found.ContainsKey(canary), $"plaintext '{canary}' found at rest in [{string.Join(", ", found.GetValueOrDefault(canary) ?? [])}]");
            }
            else
            {
                Assert.True(found.ContainsKey(dataCanary), "control: with encryption at rest disabled the payload canary must be visible, otherwise the scan is ineffective");
            }
        }, Ct);

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
    /// allocation, without crashing any node and without stalling unrelated authenticated work; idle unauthenticated
    /// connections are eventually closed by the server.
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
                                foreach (ulong claimed in new[] { 1UL << 31, 1UL << 40, ulong.MaxValue >> 1 })
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

                for (int i = 0; i < 5; i++)
                {
                    byte[] payload = TestData.Bytes(1400 + (ulong)i, 20_000);
                    var watch = Stopwatch.StartNew();
                    await volume.Files.WriteAllBytesAsync("/legit-" + SecHelpers.Inv(i) + ".bin", payload, Ct);
                    await RecoveryScenario.AssertFileBytesAsync(volume.Files, "/legit-" + SecHelpers.Inv(i) + ".bin", payload, "unrelated authenticated work under abuse", Ct);
                    Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"unrelated work stalled for {watch.Elapsed} under {abuse}");
                }

                Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
                long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
                TestContext.Current.TestOutputHelper?.WriteLine($"{abuse}: frames={frames} connections={raws.Count} refused={refusedConnections} reconnects={reconnects} allocated={allocated / (1 << 20)} MiB");
                Assert.True(allocated < 512L << 20, $"{abuse} caused {allocated} bytes of allocation; claimed lengths must not be pre-allocated");

                if (abuse == UnauthenticatedAbuse.ConnectionFlood)
                {
                    await cluster.WaitUntilAsync(() => Task.FromResult(raws.All(r => !r.IsConnected)), TimeSpan.FromSeconds(30),
                        "idle unauthenticated connections must be closed by the server (handshake deadline)");
                }
            }
            finally
            {
                foreach (RawProtocolClient raw in raws)
                    await RecoveryScenario.CaptureAsync(async () => await raw.DisposeAsync());
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-015
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Parser family fuzzed in SEC-015.</summary>
    public enum FuzzedParser
    {
        /// <summary>The wire codec, locally, for every message type.</summary>
        WireCodec,

        /// <summary>Every node's request parser, before authentication.</summary>
        WireRemoteUnauthenticated,

        /// <summary>Every node's request parser, authenticated as a principal without grants.</summary>
        WireRemoteLowPrivilege,

        /// <summary>Journal segment recovery.</summary>
        JournalSegments,

        /// <summary>Atomic checkpoint/superblock files.</summary>
        AtomicFiles,

        /// <summary>Object store segments and index rebuild.</summary>
        ObjectStoreSegments,

        /// <summary>Backup host manifests, pointers and object segments on disk.</summary>
        BackupHostFiles,
    }

    /// <summary>
    /// Fuzzes every wire and disk parser with seeded valid/invalid framing, lengths and counts: malformed input is rejected
    /// with the parser's typed error (never a crash or another exception type), allocation stays bounded, nothing
    /// unauthorized happens, and corrupted persistent state is either detected or recovered to an exact prefix/verified
    /// subset — never silently accepted as different data.
    /// </summary>
    [Theory]
    [InlineData(FuzzedParser.WireCodec)]
    [InlineData(FuzzedParser.WireRemoteUnauthenticated)]
    [InlineData(FuzzedParser.WireRemoteLowPrivilege)]
    [InlineData(FuzzedParser.JournalSegments)]
    [InlineData(FuzzedParser.AtomicFiles)]
    [InlineData(FuzzedParser.ObjectStoreSegments)]
    [InlineData(FuzzedParser.BackupHostFiles)]
    [Catalog("SEC-015", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_015_ParserAndManifestFuzzing(FuzzedParser parser) => parser switch
    {
        FuzzedParser.WireCodec => Task.Run(SecFuzzing.FuzzWireCodec, Ct),
        FuzzedParser.JournalSegments => SecFuzzing.FuzzJournalAsync(0x15_0001, Ct),
        FuzzedParser.AtomicFiles => SecFuzzing.FuzzAtomicFileAsync(0x15_0002, Ct),
        FuzzedParser.ObjectStoreSegments => SecFuzzing.FuzzObjectStoreAsync(0x15_0003, Ct),
        FuzzedParser.BackupHostFiles => ClusterTest.RunAsync(SecHelpers.Options(backups: 1), cluster => SecFuzzing.FuzzBackupHostAsync(cluster, 0x15_0004, Ct), Ct),
        _ => ClusterTest.RunAsync(SecHelpers.Options(), cluster => SecFuzzing.FuzzRemoteAsync(cluster, parser == FuzzedParser.WireRemoteLowPrivilege, 0x15_0005, Ct), Ct),
    };

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-016
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Security and administrative actions (principal creation, grant, revoke, failed authentication, denied access, key
    /// rotation, recovery-key export, volume and snapshot lifecycle, denied policy change) are audited with principal,
    /// operation, outcome and scope, while payload bytes, secrets, passphrases and key material never appear in the audit
    /// log or client diagnostics.
    /// </summary>
    [Fact]
    [Catalog("SEC-016", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_016_AuditContainsIdentityNotPayload() =>
        ClusterTest.RunAsync(SecHelpers.Options(), async cluster =>
        {
            const string payloadCanary = "TDFS-AUDIT-PAYLOAD-CANARY-2f8b";
            const string secretCanary = "TDFS-AUDIT-SECRET-CANARY-6c1d";
            const string passphraseCanary = "TDFS-AUDIT-PASSPHRASE-CANARY-90ae";
            const string user = "audited-user";
            var diagnostics = new RecordingDiagnostics();
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "auditor", Diagnostics = diagnostics }, Ct);
            DfsVolume vol = admin.GetVolume(Vol);

            await admin.Admin.CreatePrincipalAsync(user, secretCanary, false, Ct);
            await admin.Admin.GrantAsync(Vol, user, DfsPermissions.Read, Ct);
            byte[] payload = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat(payloadCanary, 300)));
            await vol.Files.WriteAllBytesAsync("/audited/payload.bin", payload, Ct);
            await vol.Files.WriteAllTextAsync("/audited/inline.txt", payloadCanary, null, Ct);

            await using (DfsClient userClient = await cluster.ConnectAsync(new TestClientOptions { Name = "user-c", Credentials = new DfsCredentials(user, secretCanary), Diagnostics = diagnostics }, Ct))
            {
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => userClient.GetVolume(Vol).Files.WriteAllBytesAsync("/audited/denied.bin", payload, Ct)), "write without Write");
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(() => userClient.Admin.SetDefaultPolicyAsync(Vol, VolumeContentFixture.PairPolicyDefinition, false, Ct)), "policy change without admin rights");
            }

            Exception? badLogin = await RecoveryScenario.CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions { Name = "bad-login", Credentials = new DfsCredentials(user, secretCanary + "-wrong"), Diagnostics = diagnostics }, Ct));
            Assert.IsType<DfsAuthenticationException>(badLogin);
            await admin.Admin.RevokeAsync(Vol, user, DfsPermissions.Read, Ct);
            await admin.Admin.RotateDataKeyAsync(Vol, Ct);
            byte[] key = await admin.Admin.ExportRecoveryKeyAsync(passphraseCanary, Ct);
            await admin.Admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "audited-vol" }, Ct);
            await admin.Admin.DeleteVolumeAsync("audited-vol", Ct);
            await vol.Snapshots.CreateAsync("audited-snap", null, Ct);
            await vol.Snapshots.DeleteAsync("audited-snap", Ct);

            IReadOnlyList<string> log = await admin.Admin.GetAuditLogAsync(1000, Ct);
            string[] success = ["ok", "success", "succeeded", "allowed", "completed", "committed", "granted"];
            string[] denied = ["denied", "unauthorized", "refused", "rejected", "fail"];
            AssertAudited(log, "principal creation", [user, "admin"], ["principal", "create"], success);
            AssertAudited(log, "grant", [user, Vol, "admin"], ["grant"], success);
            AssertAudited(log, "revoke", [user, Vol, "admin"], ["revoke"], success);
            AssertAudited(log, "denied write", [user, Vol], ["write"], denied);
            AssertAudited(log, "denied policy change", [user, Vol], ["polic"], denied);
            AssertAudited(log, "failed authentication", [user], ["auth", "login", "handshake"], denied);
            AssertAudited(log, "key rotation", ["admin", Vol], ["rotat"], success);
            AssertAudited(log, "recovery key export", ["admin"], ["recovery"], success);
            AssertAudited(log, "volume creation", ["admin", "audited-vol"], ["creat"], success);
            AssertAudited(log, "volume deletion", ["admin", "audited-vol"], ["delet"], success);
            AssertAudited(log, "snapshot creation", ["admin", Vol, "audited-snap"], ["snapshot"], success);

            string[] forbidden = [payloadCanary, secretCanary, passphraseCanary, Convert.ToBase64String(key), Convert.ToHexString(key), Convert.ToHexString(key.AsSpan(0, Math.Min(16, key.Length))),
                Convert.ToHexString(payload.AsSpan(0, 32)), Convert.ToBase64String(payload.AsSpan(0, 48))];
            foreach (string entry in log)
            {
                foreach (string secret in forbidden)
                    Assert.False(entry.Contains(secret, StringComparison.OrdinalIgnoreCase), $"audit entry leaks payload/secret/key material: {entry}");
            }

            foreach (DfsDiagnosticEvent e in diagnostics.Events)
            {
                string text = e.Name + " " + e.Message + " " + e.Exception;
                foreach (string secret in forbidden)
                    Assert.False(text.Contains(secret, StringComparison.OrdinalIgnoreCase), $"client diagnostics leak payload/secret/key material: {e.Name}");
            }
        }, Ct);

    private static void AssertAudited(IReadOnlyList<string> log, string action, string[] mustContainAll, string[] operationAnyOf, string[] outcomeAnyOf)
    {
        bool found = log.Any(entry =>
            mustContainAll.All(t => entry.Contains(t, StringComparison.OrdinalIgnoreCase))
            && operationAnyOf.Any(t => entry.Contains(t, StringComparison.OrdinalIgnoreCase))
            && outcomeAnyOf.Any(t => entry.Contains(t, StringComparison.OrdinalIgnoreCase)));
        Assert.True(found,
            $"no audit entry for {action} containing [{string.Join(", ", mustContainAll)}], one of [{string.Join(", ", operationAnyOf)}] and an outcome [{string.Join(", ", outcomeAnyOf)}]. Last entries:{Environment.NewLine}{string.Join(Environment.NewLine, log.TakeLast(30))}");
    }

    private sealed class RecordingDiagnostics : IDfsDiagnostics
    {
        private readonly ConcurrentQueue<DfsDiagnosticEvent> _events = new();

        public IReadOnlyCollection<DfsDiagnosticEvent> Events => _events;

        public void OnEvent(DfsDiagnosticEvent e) => _events.Enqueue(e);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // SEC-018
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Non-administrative principals attempt replication-policy, topology, retention/backup and promotion/authority
    /// changes: each is denied with AccessDenied, nothing changes (policies, nodes, volumes, targets, retention, principals,
    /// features) and every denial is audited. An administrator's impossible policy is rejected rather than approximated.
    /// </summary>
    [Theory]
    [InlineData(PrincipalKind.Writer)]
    [InlineData(PrincipalKind.Snapshotter)]
    [InlineData(PrincipalKind.BackupAdmin)]
    [InlineData(PrincipalKind.NoGrants)]
    [InlineData(PrincipalKind.OtherVolumeOnly)]
    [Catalog("SEC-018", "I", "INV-08", "INV-12", "INV-13", "INV-14")]
    public Task SEC_018_AdminCannotSilentlyWeakenPolicy(PrincipalKind kind) =>
        ClusterTest.RunAsync(SecHelpers.Options(backups: 1), async cluster =>
        {
            string name = "weak-" + kind.ToString().ToLowerInvariant();
            TestPrincipal principal = await SecHelpers.CreatePrincipalAsync(cluster, kind, name);
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "admin-c" }, Ct);
            DfsVolume adminVol = admin.GetVolume(Vol);
            await adminVol.Files.WriteAllBytesAsync("/f.bin", TestData.Bytes(1801, 5000), Ct);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DfsSnapshotInfo snap = await adminVol.Snapshots.CreateAsync("protected-snap", new DfsSnapshotOptions { Retention = new DfsRetention(null, Hold: true) }, Ct);
            await RecoveryScenario.PublishAsync(adminVol, backup, snap, Ct);

            async Task<string> StateAsync()
            {
                DfsVolumeInfo info = (await admin.Admin.GetVolumeStatusAsync(Vol, Ct)).Info;
                IReadOnlyList<DfsNodeInfo> nodes = await admin.Admin.ListNodesAsync(Ct);
                DfsClusterStatus status = await admin.Admin.GetClusterStatusAsync(Ct);
                IReadOnlyList<string> targets = await adminVol.Backups.ListTargetsAsync(Ct);
                DfsSnapshotInfo s = await adminVol.Snapshots.GetAsync("protected-snap", Ct);
                return string.Join("|",
                    info.DefaultPolicy, string.Join(",", info.NamedPolicies.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Key + "=" + p.Value)),
                    string.Join(",", nodes.OrderBy(n => n.Name, StringComparer.Ordinal).Select(n => $"{n.Name}:{n.Labels}:{n.Status}")),
                    string.Join(",", status.Volumes.Order(StringComparer.Ordinal)), string.Join(",", status.ActiveFeatures.Order(StringComparer.Ordinal)),
                    string.Join(",", targets.Order(StringComparer.Ordinal)), s.Retention.ToString(), status.PlacementEpoch.ToString(CultureInfo.InvariantCulture));
            }

            string before = await StateAsync();
            await using DfsClient weak = await SecHelpers.ConnectAsAsync(cluster, principal.Credentials, name, Ct);
            DfsAdminClient weakAdmin = weak.Admin;
            DfsVolume weakVol = weak.GetVolume(Vol);
            var weaker = new DfsStoragePolicy { TargetCopies = 1, TargetDistinctZones = 1, DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1 };
            TestNode n0 = cluster[0];
            var attempts = new List<(string Category, string Name, Func<Task> Action)>
            {
                ("replication", "SetDefaultPolicy", () => weakAdmin.SetDefaultPolicyAsync(Vol, weaker, true, Ct)),
                ("replication", "DefinePolicy", () => weakAdmin.DefinePolicyAsync(Vol, "weaker", weaker, Ct)),
                ("topology", "SetNodeLabels", () => weakAdmin.SetNodeLabelsAsync(n0.Name, n0.Labels with { Zone = "z9" }, Ct)),
                ("topology", "DrainNode", () => weakAdmin.DrainNodeAsync(cluster[1].Name, Ct)),
                ("topology", "RemoveNode", () => weakAdmin.RemoveNodeAsync(cluster[2].Name, true, Ct)),
                ("topology", "AddNode", () => weakAdmin.AddNodeAsync("n9", new DfsTopologyLabels("r1", "z9", "rk1", "h9", "d1"), ["quic://127.0.0.1:1"], Ct)),
                ("promotion", "RestoreFromBackup", () => weakAdmin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "protected-snap", "promoted"), Ct)),
                ("promotion", "CreateVolume", () => weakAdmin.CreateVolumeAsync(new DfsVolumeSpec { Name = "rogue-vol" }, Ct)),
                ("promotion", "DeleteVolume", () => weakAdmin.DeleteVolumeAsync(Vol, Ct)),
                ("promotion", "GrantSelf", () => weakAdmin.GrantAsync(Vol, name, DfsPermissions.All, Ct)),
                ("promotion", "CreateAdminPrincipal", () => weakAdmin.CreatePrincipalAsync("rogue-admin", "rogue-secret", true, Ct)),
                ("promotion", "SetFeature", () => weakAdmin.SetFeatureAsync("rogue-feature", true, Ct)),
                ("promotion", "RotateDataKey", () => weakAdmin.RotateDataKeyAsync(Vol, Ct)),
                ("promotion", "ExportRecoveryKey", () => weakAdmin.ExportRecoveryKeyAsync("rogue-passphrase", Ct)),
            };
            if (kind != PrincipalKind.Snapshotter)
            {
                attempts.Add(("retention", "ReleaseSnapshotHold", () => weakVol.Snapshots.SetRetentionAsync("protected-snap", new DfsRetention(null, false), Ct)));
                attempts.Add(("retention", "DeleteHeldSnapshot", () => weakVol.Snapshots.DeleteAsync("protected-snap", Ct)));
                attempts.Add(("promotion", "SnapshotRestore", () => weakVol.Snapshots.RestoreAsync("protected-snap", "promoted2", Ct)));
            }

            if (kind != PrincipalKind.BackupAdmin)
            {
                attempts.Add(("retention", "ConfigureBackupBudgets", () => weakVol.Backups.ConfigureAsync(backup.TargetName, new DfsBackupSchedule(),
                    new DfsBackupBudgets(TimeSpan.FromSeconds(1), 1), Ct)));
                attempts.Add(("retention", "RemoveBackupTarget", () => weakVol.Backups.RemoveTargetAsync(backup.TargetName, Ct)));
                attempts.Add(("retention", "RegisterRogueTarget", () => weakVol.Backups.RegisterTargetAsync(new DfsBackupTargetRegistration("rogue", ["quic://127.0.0.1:1"],
                    new DfsCredentials("x", "y"), new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30)), Ct)));
            }

            foreach ((string category, string attempt, Func<Task> action) in attempts)
                SecHelpers.AssertAccessDenied(await RecoveryScenario.CaptureAsync(action), $"{kind}: {category} change '{attempt}'");

            Assert.Equal(before, await StateAsync());
            Assert.IsType<DfsAuthenticationException>(await RecoveryScenario.CaptureAsync(() =>
                cluster.ConnectAsync(new TestClientOptions { Name = "rogue", Credentials = new DfsCredentials("rogue-admin", "rogue-secret") }, Ct)));

            IReadOnlyList<string> log = await admin.Admin.GetAuditLogAsync(1000, Ct);
            int deniedEntries = log.Count(e => e.Contains(name, StringComparison.OrdinalIgnoreCase)
                && (e.Contains("denied", StringComparison.OrdinalIgnoreCase) || e.Contains("unauthorized", StringComparison.OrdinalIgnoreCase) || e.Contains("refused", StringComparison.OrdinalIgnoreCase)));
            Assert.True(deniedEntries >= attempts.Count, $"{attempts.Count} denied attempts by {name} but only {deniedEntries} denial audit entries");

            DfsVolumeInfo infoBefore = (await admin.Admin.GetVolumeStatusAsync(Vol, Ct)).Info;
            var impossible = new DfsStoragePolicy { TargetCopies = 5, TargetDistinctZones = 5, DurableCopiesBeforeAck = 5, DurableZonesBeforeAck = 5 };
            Exception? rejected = await RecoveryScenario.CaptureAsync(() => admin.Admin.SetDefaultPolicyAsync(Vol, impossible, true, Ct));
            RecoveryScenario.AssertFailedWith(rejected, "a policy the topology cannot satisfy must be rejected, not approximated", DfsErrorCode.InvalidPolicy, DfsErrorCode.PlacementUnavailable);
            Assert.Equal(infoBefore.DefaultPolicy, (await admin.Admin.GetVolumeStatusAsync(Vol, Ct)).Info.DefaultPolicy);
        }, Ct);
}
