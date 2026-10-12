using System.Reflection;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Sdk;

/// <summary>
/// SDK suite, level U (test plan §7.2, FSD §§1, 5, 13): client options, isolation, thread safety, genuinely
/// asynchronous I/O, disposal, cancellation, advertised capabilities and secret hygiene.
/// </summary>
public sealed class SdkUnitTests
{
    private const string Volume = SdkHelpers.Volume;

    private static CancellationToken Ct => SdkHelpers.Ct;

    // =====================================================================================================
    // SDK-001 OptionsRejectInvalidValues
    // =====================================================================================================

    private const string Sdk001Secret = "S3cret-sdk001-must-never-appear";
    private const string ExistingPath = "/sdk001/existing.bin";
    private const string NewPath = "/sdk001/new.bin";

    private static DfsClientOptions ValidClientOptions() => new()
    {
        SeedEndpoints = ["quic://127.0.0.1:7100", "quic://[::1]:7101", "mem://n0"],
        Credentials = new DfsCredentials("admin", Sdk001Secret),
    };

    private static readonly Dictionary<string, (Func<DfsClientOptions, DfsClientOptions> Mutate, string Parameter)> InvalidClientOptionTable =
        new(StringComparer.Ordinal)
        {
            ["NoSeeds"] = (o => o with { SeedEndpoints = [] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["NullSeedList"] = (o => o with { SeedEndpoints = null! }, nameof(DfsClientOptions.SeedEndpoints)),
            ["EmptySeedEndpoint"] = (o => o with { SeedEndpoints = [""] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["NullSeedEndpoint"] = (o => o with { SeedEndpoints = [null!] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["SeedWithoutScheme"] = (o => o with { SeedEndpoints = ["127.0.0.1:7100"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["SeedUnsupportedScheme"] = (o => o with { SeedEndpoints = ["http://127.0.0.1:7100"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["QuicSeedWithoutPort"] = (o => o with { SeedEndpoints = ["quic://127.0.0.1"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["QuicSeedPortZero"] = (o => o with { SeedEndpoints = ["quic://127.0.0.1:0"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["QuicSeedPortOutOfRange"] = (o => o with { SeedEndpoints = ["quic://127.0.0.1:70000"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["QuicSeedUnbracketedIpv6"] = (o => o with { SeedEndpoints = ["quic://::1:7100"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["MemorySeedWithoutName"] = (o => o with { SeedEndpoints = ["mem://"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["SecondSeedInvalid"] = (o => o with { SeedEndpoints = ["mem://n0", "not an endpoint"] }, nameof(DfsClientOptions.SeedEndpoints)),
            ["EmptyExpectedClusterId"] = (o => o with { ExpectedClusterId = Guid.Empty }, nameof(DfsClientOptions.ExpectedClusterId)),
            ["NullCredentials"] = (o => o with { Credentials = null! }, nameof(DfsClientOptions.Credentials)),
            ["EmptyPrincipal"] = (o => o with { Credentials = new DfsCredentials("", Sdk001Secret) }, nameof(DfsClientOptions.Credentials)),
            ["WhitespacePrincipal"] = (o => o with { Credentials = new DfsCredentials("   ", Sdk001Secret) }, nameof(DfsClientOptions.Credentials)),
            ["NullPrincipal"] = (o => o with { Credentials = new DfsCredentials(null!, Sdk001Secret) }, nameof(DfsClientOptions.Credentials)),
            ["EmptySecret"] = (o => o with { Credentials = new DfsCredentials("admin", "") }, nameof(DfsClientOptions.Credentials)),
            ["NullSecret"] = (o => o with { Credentials = new DfsCredentials("admin", null!) }, nameof(DfsClientOptions.Credentials)),
            ["EmptyClientName"] = (o => o with { ClientName = "" }, nameof(DfsClientOptions.ClientName)),
            ["WhitespaceClientName"] = (o => o with { ClientName = " \t" }, nameof(DfsClientOptions.ClientName)),
            ["ZeroConnectTimeout"] = (o => o with { ConnectTimeout = TimeSpan.Zero }, nameof(DfsClientOptions.ConnectTimeout)),
            ["NegativeConnectTimeout"] = (o => o with { ConnectTimeout = TimeSpan.FromSeconds(-1) }, nameof(DfsClientOptions.ConnectTimeout)),
            ["ZeroOperationTimeout"] = (o => o with { OperationTimeout = TimeSpan.Zero }, nameof(DfsClientOptions.OperationTimeout)),
            ["NegativeOperationTimeout"] = (o => o with { OperationTimeout = TimeSpan.FromTicks(-1) }, nameof(DfsClientOptions.OperationTimeout)),
            ["ZeroShutdownTimeout"] = (o => o with { ShutdownTimeout = TimeSpan.Zero }, nameof(DfsClientOptions.ShutdownTimeout)),
            ["NegativeShutdownTimeout"] = (o => o with { ShutdownTimeout = TimeSpan.MinValue }, nameof(DfsClientOptions.ShutdownTimeout)),
            ["ZeroBufferSize"] = (o => o with { DefaultBufferSize = 0 }, nameof(DfsClientOptions.DefaultBufferSize)),
            ["NegativeBufferSize"] = (o => o with { DefaultBufferSize = -1 }, nameof(DfsClientOptions.DefaultBufferSize)),
            ["ZeroMaxMaterializeBytes"] = (o => o with { MaxMaterializeBytes = 0 }, nameof(DfsClientOptions.MaxMaterializeBytes)),
            ["NegativeMaxMaterializeBytes"] = (o => o with { MaxMaterializeBytes = long.MinValue }, nameof(DfsClientOptions.MaxMaterializeBytes)),
            ["NegativeRetryAttempts"] = (o => o with { MaxRetryAttempts = -1 }, nameof(DfsClientOptions.MaxRetryAttempts)),
            ["NegativeRetryBaseDelay"] = (o => o with { RetryBaseDelay = TimeSpan.FromMilliseconds(-1) }, nameof(DfsClientOptions.RetryBaseDelay)),
            ["NegativeRetryMaxDelay"] = (o => o with { RetryBaseDelay = TimeSpan.Zero, RetryMaxDelay = TimeSpan.FromMilliseconds(-1) }, nameof(DfsClientOptions.RetryMaxDelay)),
            ["RetryBaseAboveMax"] = (o => o with { RetryBaseDelay = TimeSpan.FromSeconds(2), RetryMaxDelay = TimeSpan.FromSeconds(1) }, nameof(DfsClientOptions.RetryBaseDelay)),
            ["NullSecurity"] = (o => o with { Security = null! }, nameof(DfsClientOptions.Security)),
            ["NullPinList"] = (o => o with { Security = new DfsTransportSecurity { PinnedSpkiSha256 = null! } }, nameof(DfsClientOptions.Security)),
            ["ShortPin"] = (o => o with { Security = new DfsTransportSecurity { PinnedSpkiSha256 = [new byte[31]] } }, nameof(DfsClientOptions.Security)),
            ["LongPin"] = (o => o with { Security = new DfsTransportSecurity { PinnedSpkiSha256 = [new byte[32], new byte[33]] } }, nameof(DfsClientOptions.Security)),
            ["NullPin"] = (o => o with { Security = new DfsTransportSecurity { PinnedSpkiSha256 = [null!] } }, nameof(DfsClientOptions.Security)),
            ["NullTimeProvider"] = (o => o with { TimeProvider = null! }, nameof(DfsClientOptions.TimeProvider)),
        };

    public static TheoryData<string> InvalidClientOptionCases => new(InvalidClientOptionTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(InvalidClientOptionCases))]
    [Catalog("SDK-001", "U", "INV-08")]
    public async Task SDK_001_OptionsRejectInvalidValues(string caseName)
    {
        (Func<DfsClientOptions, DfsClientOptions> mutate, string parameter) = InvalidClientOptionTable[caseName];
        DfsClientOptions valid = ValidClientOptions();
        valid.Validate(); // the baseline is accepted, so the failure below is caused by the mutation alone
        DfsClientOptions invalid = mutate(valid);

        ArgumentException ex = Assert.ThrowsAny<ArgumentException>(invalid.Validate);
        Assert.Equal(parameter, ex.ParamName);
        Assert.DoesNotContain(Sdk001Secret, ex.ToString(), StringComparison.Ordinal);

        // Connecting validates first, before any network activity: nothing listens at the seeds, so a connection
        // attempt would surface as an availability failure or a timeout rather than an ArgumentException.
        long started = TimeProvider.System.GetTimestamp();
        ArgumentException asyncEx = await Assert.ThrowsAnyAsync<ArgumentException>(() => DfsClient.ConnectAsync(invalid, Ct));
        Assert.Equal(parameter, asyncEx.ParamName);
        ArgumentException syncEx = Assert.ThrowsAny<ArgumentException>(() => DfsClient.Connect(invalid));
        Assert.Equal(parameter, syncEx.ParamName);
        Assert.DoesNotContain(Sdk001Secret, asyncEx.ToString(), StringComparison.Ordinal);
        Assert.True(TimeProvider.System.GetElapsedTime(started) < TimeSpan.FromSeconds(5), "invalid options must be rejected without waiting for the network");
    }

    private sealed record OptionCase(string Name, Func<OptionContext, Task> Invoke, Func<Exception, bool> IsExpected, string Expected);

    private sealed class OptionContext
    {
        public required DfsVolume Volume { get; init; }
        public required DfsTransaction Transaction { get; init; }
        public required CommitToken Token { get; init; }
        public DfsFileClient Files => Volume.Files;
    }

    private static bool IsArgument(Exception e) => e is ArgumentException;

    private static bool IsNotSupported(Exception e) => e is NotSupportedException;

    private static bool IsArgumentOrNotSupported(Exception e) => e is ArgumentException or NotSupportedException;

    private static OptionCase OpenCase(string name, DfsOpenOptions options, bool existing = false, Func<Exception, bool>? expected = null,
        string expectedText = "ArgumentException") =>
        new(name, async c =>
        {
            await using DfsStream s = await c.Files.OpenAsync(existing ? ExistingPath : NewPath, options, Ct);
        }, expected ?? IsArgument, expectedText);

    private static OptionCase Case(string name, Func<OptionContext, Task> invoke, Func<Exception, bool>? expected = null, string expectedText = "ArgumentException") =>
        new(name, invoke, expected ?? IsArgument, expectedText);

    private static readonly Dictionary<string, OptionCase[]> OperationOptionCategories = new(StringComparer.Ordinal)
    {
        ["OpenOptionEnumValues"] =
        [
            OpenCase("UnknownFileMode", new DfsOpenOptions { Mode = (FileMode)99, Access = FileAccess.Write }),
            OpenCase("ZeroFileMode", new DfsOpenOptions { Mode = 0, Access = FileAccess.Write }),
            OpenCase("UnknownFileAccess", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = (FileAccess)8 }),
            OpenCase("ZeroFileAccess", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = 0 }),
            OpenCase("UnknownFileShareBits", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = (FileShare)0x40 }, existing: true),
            OpenCase("InheritableShare", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read | FileShare.Inheritable }, existing: true,
                IsArgumentOrNotSupported, "ArgumentException or NotSupportedException"),
            OpenCase("UnknownWriteMode", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, WriteMode = (DfsWriteMode)7 }),
            OpenCase("UnknownReadView", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, ReadView = (DfsReadView)9 }, existing: true),
            OpenCase("ZeroBufferSize", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, BufferSize = 0 }),
            OpenCase("NegativeBufferSize", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, BufferSize = -1 }),
        ],
        ["OpenUnsupportedFileOptions"] =
        [
            OpenCase("WriteThrough", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Options = FileOptions.WriteThrough }, false, IsNotSupported, "NotSupportedException"),
            OpenCase("DeleteOnClose", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Options = FileOptions.DeleteOnClose }, false, IsNotSupported, "NotSupportedException"),
            OpenCase("Encrypted", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Options = FileOptions.Encrypted }, false, IsNotSupported, "NotSupportedException"),
            OpenCase("UnknownOptionBits", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Options = (FileOptions)0x10 }, true, IsNotSupported, "NotSupportedException"),
            OpenCase("AsynchronousPlusWriteThrough", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Options = FileOptions.Asynchronous | FileOptions.WriteThrough }, false, IsNotSupported, "NotSupportedException"),
        ],
        ["OpenIncompatibleCombinations"] =
        [
            OpenCase("CreateNewWithReadAccess", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Read }),
            OpenCase("CreateWithReadAccess", new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.Read }),
            OpenCase("TruncateWithReadAccess", new DfsOpenOptions { Mode = FileMode.Truncate, Access = FileAccess.Read }, existing: true),
            OpenCase("TruncateWithReadWriteAccess", new DfsOpenOptions { Mode = FileMode.Truncate, Access = FileAccess.ReadWrite }, existing: true),
            OpenCase("AppendWithReadAccess", new DfsOpenOptions { Mode = FileMode.Append, Access = FileAccess.Read }, existing: true),
            OpenCase("AppendWithReadWriteAccess", new DfsOpenOptions { Mode = FileMode.Append, Access = FileAccess.ReadWrite }, existing: true),
            OpenCase("VersionPinnedWithWriteAccess", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, ReadView = DfsReadView.VersionPinned }),
            OpenCase("VersionPinnedWithReadWriteAccess", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, ReadView = DfsReadView.VersionPinned }, existing: true),
            OpenCase("WriterGrantingShareReadWrite", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.ReadWrite }, existing: true,
                IsArgumentOrNotSupported, "ArgumentException or NotSupportedException"),
            OpenCase("WriterGrantingShareWrite", new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.Write }, false,
                IsArgumentOrNotSupported, "ArgumentException or NotSupportedException"),
            OpenCase("EmptyStoragePolicyName", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, StoragePolicy = "" }),
            OpenCase("EmptyOperationId", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, OperationId = default(DfsOperationId) }),
            Case("VersionPinnedInsideTransaction", async c =>
            {
                await using DfsStream s = await c.Transaction.Files.OpenAsync(ExistingPath, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, ReadView = DfsReadView.VersionPinned }, Ct);
            }, e => e is ArgumentException or NotSupportedException or InvalidOperationException, "ArgumentException, NotSupportedException or InvalidOperationException"),
        ],
        ["MutationOptions"] =
        [
            // Only empty names are rejected here: the FSD/API define no character rules for policy or snapshot names,
            // so a whitespace or '/'-containing name is not an invalid *option* (an unknown policy is a server error).
            Case("EmptyStoragePolicy", c => c.Files.WriteAllBytesAsync(NewPath, new byte[] { 1 }, new DfsMutationOptions { StoragePolicy = "" }, Ct)),
            Case("EmptyCommitSnapshotName", c => c.Files.WriteAllBytesAsync(NewPath, new byte[] { 1 }, new DfsMutationOptions { CommitSnapshotName = "" }, Ct)),
            Case("EmptyCommitSnapshotNameOnAppend", c => c.Files.AppendAllBytesAsync(ExistingPath, new byte[] { 1 }, new DfsMutationOptions { CommitSnapshotName = "" }, Ct)),
            Case("EmptyOperationIdOnWrite", c => c.Files.WriteAllBytesAsync(NewPath, new byte[] { 1 }, new DfsMutationOptions { OperationId = default(DfsOperationId) }, Ct)),
            Case("EmptyOperationIdOnMove", c => c.Files.MoveAsync(ExistingPath, NewPath, false, new DfsMutationOptions { OperationId = default(DfsOperationId) }, Ct)),
            Case("EmptyOperationIdOnCreateDirectory", c => c.Volume.Directories.CreateDirectoryAsync("/sdk001/newdir", new DfsMutationOptions { OperationId = default(DfsOperationId) }, Ct)),
            Case("SyncEmptyStoragePolicy", c => { _ = c.Files.WriteAllBytes(NewPath, new byte[] { 1 }, new DfsMutationOptions { StoragePolicy = "" }); return Task.CompletedTask; }),
            Case("NullMutationOptions", c => c.Files.WriteAllBytesAsync(NewPath, new byte[] { 1 }, null!, Ct)),
        ],
        ["EnumerationOptions"] =
        [
            Case("PageSizeZero", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { PageSize = 0 }, null, Ct)),
            Case("PageSizeNegative", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { PageSize = -5 }, null, Ct)),
            Case("PageSizeAboveMaximum", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { PageSize = 10_001 }, null, Ct)),
            Case("UnknownEntryKinds", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { Kinds = (DfsEntryKinds)4 }, null, Ct)),
            Case("NoEntryKinds", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { Kinds = 0 }, null, Ct)),
            Case("NullPattern", c => c.Volume.Directories.ListPageAsync("/sdk001", new DfsEnumerationOptions { Pattern = null! }, null, Ct)),
            Case("EnumeratePageSizeZero", async c =>
            {
                await foreach (DfsEntryInfo _ in c.Volume.Directories.EnumerateAsync("/sdk001", new DfsEnumerationOptions { PageSize = 0 }, Ct))
                    break;
            }),
        ],
        ["TransactionAndSnapshotOptions"] =
        [
            Case("ZeroTransactionTimeout", async c => { await using DfsTransaction t = await c.Volume.Transactions.BeginAsync(new DfsTransactionOptions { Timeout = TimeSpan.Zero }, Ct); }),
            Case("NegativeTransactionTimeout", async c => { await using DfsTransaction t = await c.Volume.Transactions.BeginAsync(new DfsTransactionOptions { Timeout = TimeSpan.FromSeconds(-3) }, Ct); }),
            Case("EmptyTransactionOperationId", async c => { await using DfsTransaction t = await c.Volume.Transactions.BeginAsync(new DfsTransactionOptions { OperationId = default(DfsOperationId) }, Ct); }),
            Case("ZeroAdapterTimeout", c => c.Volume.Snapshots.CreateAsync("sdk001-snap", new DfsSnapshotOptions { AdapterTimeout = TimeSpan.Zero }, Ct)),
            Case("NegativeAdapterTimeout", c => c.Volume.Snapshots.CreateAsync("sdk001-snap", new DfsSnapshotOptions { AdapterTimeout = TimeSpan.FromSeconds(-1) }, Ct)),
            Case("NullAdapterList", c => c.Volume.Snapshots.CreateAsync("sdk001-snap", new DfsSnapshotOptions { Adapters = null! }, Ct)),
            Case("EmptySnapshotName", c => c.Volume.Snapshots.CreateAsync("", null, Ct)),
            // (CommitToken.None as MinimumCommit / await token is not listed: the API does not say whether None means
            // "no minimum" or is invalid, so either behaviour is legal.)
            Case("EmptyCommitWithSnapshotName", c => c.Transaction.CommitWithSnapshotAsync("", Ct)),
        ],
        ["BackupOptions"] =
        [
            Case("NegativeAwaitTimeout", c => c.Volume.Backups.AwaitBackupAsync("b0", c.Token, TimeSpan.FromSeconds(-1), Ct)),
            Case("ZeroAwaitTimeout", c => c.Volume.Backups.AwaitBackupAsync("b0", c.Token, TimeSpan.Zero, Ct)),
            Case("NegativePinnedBytesBudget", c => c.Volume.Backups.ConfigureAsync("b0", new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), -1), Ct)),
            Case("ZeroPinAgeBudget", c => c.Volume.Backups.ConfigureAsync("b0", new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.Zero, 1L << 30), Ct)),
            Case("ZeroConcurrentObjects", c => c.Volume.Backups.ConfigureAsync("b0", new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30, long.MaxValue, 0), Ct)),
            Case("ZeroBandwidthBudget", c => c.Volume.Backups.ConfigureAsync("b0", new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30, 0), Ct)),
            Case("NegativeScheduleInterval", c => c.Volume.Backups.ConfigureAsync("b0", new DfsBackupSchedule(TimeSpan.FromMinutes(-1)), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30), Ct)),
            Case("EmptyTargetName", c => c.Volume.Backups.GetStatusAsync("", Ct)),
        ],
    };

    public static TheoryData<string> OperationOptionCategoryCases => new(OperationOptionCategories.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(OperationOptionCategoryCases))]
    [Catalog("SDK-001", "U", "INV-08")]
    public async Task SDK_001_OptionsRejectInvalidValues_OperationOptions(string category)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/sdk001", Ct);
            byte[] existing = TestData.Bytes(1001, 100);
            CommitToken token = await volume.Files.WriteAllBytesAsync(ExistingPath, existing, Ct);
            DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
            var context = new OptionContext { Volume = volume, Transaction = tx, Token = token };
            string me = SdkHelpers.NetworkName();
            int inodesBefore = await SdkHelpers.InodeCountAsync(cluster);
            int snapshotsBefore = (await volume.Snapshots.ListAsync(Ct)).Count;
            long requestsBefore = SdkHelpers.ForegroundRequests(cluster, me);

            var failures = new List<string>();
            foreach (OptionCase c in OperationOptionCategories[category])
            {
                long before = SdkHelpers.ForegroundRequests(cluster, me);
                Exception? ex = await Record.ExceptionAsync(() => c.Invoke(context));
                if (ex is null)
                {
                    failures.Add($"{c.Name}: accepted (expected {c.Expected})");
                }
                else
                {
                    ex = SdkHelpers.Unwrap(ex);
                    if (!c.IsExpected(ex))
                        failures.Add($"{c.Name}: {ex.GetType().Name} ({DfsErrors.ToCode(ex)}) instead of {c.Expected}: {ex.Message}");
                }

                long after = SdkHelpers.ForegroundRequests(cluster, me);
                if (after != before)
                    failures.Add($"{c.Name}: {after - before} request(s) were sent before the invalid option was rejected");
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            if (SdkHelpers.ForegroundRequests(cluster, me) != requestsBefore)
                failures.Add("requests reached the cluster during the sweep of invalid options");

            // Nothing was mutated: same namespace, existing bytes intact, nothing created, no snapshot.
            Assert.Equal(inodesBefore, await SdkHelpers.InodeCountAsync(cluster));
            Assert.Equal(snapshotsBefore, (await volume.Snapshots.ListAsync(Ct)).Count);
            await SdkHelpers.AssertBytesEqualAsync(existing, volume.Files.ReadAllBytesAsync(ExistingPath, Ct));
            Assert.False(await volume.Files.ExistsAsync(NewPath, Ct));
            Assert.False(await volume.Directories.ExistsAsync("/sdk001/newdir", Ct));
            await tx.AbortAsync(Ct);
            Assert.True(failures.Count == 0, $"{category}:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
        }, Ct);
    }

    // =====================================================================================================
    // SDK-003 MultipleClientsAreIsolated
    // =====================================================================================================

    [Fact]
    [Catalog("SDK-003", "U", "INV-12", "INV-08")]
    public async Task SDK_003_MultipleClientsAreIsolated()
    {
        // Explicit, different seeds: with Seed = 0 both clusters would derive the same seed from the test name, and every
        // seeded identity (cluster id, secrets, node ids) would be identical, hiding any cross-cluster mix-up.
        TestClusterOptions optionsA = TestClusterOptions.ThreeZones with { Seed = 0x5D_0003_0A, Volumes = [new TestVolumeSpec("va"), new TestVolumeSpec("shared")] };
        TestClusterOptions optionsB = TestClusterOptions.ThreeZones with { Seed = 0x5D_0003_0B, Volumes = [new TestVolumeSpec("vb"), new TestVolumeSpec("shared")] };
        await ClusterTest.RunAsync(optionsA, async clusterA =>
        {
            await ClusterTest.RunAsync(optionsB, async clusterB =>
            {
                Assert.NotEqual(clusterA.ClusterId, clusterB.ClusterId);
                TestPrincipal alice = await clusterA.CreatePrincipalAsync("alice", new Dictionary<string, DfsPermissions>
                {
                    ["va"] = DfsPermissions.Read | DfsPermissions.Write,
                    ["shared"] = DfsPermissions.Read,
                });
                TestPrincipal carol = await clusterA.CreatePrincipalAsync("carol", new Dictionary<string, DfsPermissions>
                {
                    ["shared"] = DfsPermissions.Read | DfsPermissions.Write,
                });
                TestPrincipal bob = await clusterB.CreatePrincipalAsync("bob", new Dictionary<string, DfsPermissions>
                {
                    ["vb"] = DfsPermissions.Read | DfsPermissions.Write,
                    ["shared"] = DfsPermissions.Read | DfsPermissions.Write,
                });

                await using DfsClient a = await clusterA.ConnectAsync(new TestClientOptions { Name = "alice", Credentials = alice.Credentials, MaxMaterializeBytes = 1024, BufferSize = 4096 }, Ct);
                await using DfsClient c = await clusterA.ConnectAsync(new TestClientOptions { Name = "carol", Credentials = carol.Credentials }, Ct);
                await using DfsClient b = await clusterB.ConnectAsync(new TestClientOptions { Name = "bob", Credentials = bob.Credentials }, Ct);

                // Identity: each client is bound to its own cluster and principal.
                Assert.Equal(clusterA.ClusterId, a.Cluster.ClusterId);
                Assert.Equal(clusterA.ClusterId, c.Cluster.ClusterId);
                Assert.Equal(clusterB.ClusterId, b.Cluster.ClusterId);
                Assert.Equal("alice", a.Principal);
                Assert.Equal("carol", c.Principal);
                Assert.Equal("bob", b.Principal);
                Assert.Same(a.GetVolume("shared"), a.GetVolume("shared"));
                Assert.NotSame(a.GetVolume("shared"), c.GetVolume("shared"));
                Assert.Same(a, a.GetVolume("shared").Client);
                Assert.Same(b, b.GetVolume("shared").Client);

                // The same path in a same-named volume of two clusters holds two independent files.
                byte[] bytesB = TestData.Bytes(3001, 2048);
                byte[] bytesC = TestData.Bytes(3002, 512);
                CommitToken tokenB = await b.GetVolume("shared").Files.WriteAllBytesAsync("/same.bin", bytesB, Ct);
                CommitToken tokenC = await c.GetVolume("shared").Files.WriteAllBytesAsync("/same.bin", bytesC, Ct);
                Assert.Equal(clusterB.ClusterId, tokenB.ClusterId);
                Assert.Equal(clusterA.ClusterId, tokenC.ClusterId);
                Assert.False(tokenB.IsComparableWith(tokenC));
                await SdkHelpers.AssertBytesEqualAsync(bytesB, b.GetVolume("shared").Files.ReadAllBytesAsync("/same.bin", Ct));
                await SdkHelpers.AssertBytesEqualAsync(bytesC, c.GetVolume("shared").Files.ReadAllBytesAsync("/same.bin", Ct));
                await SdkHelpers.AssertBytesEqualAsync(bytesC, a.GetVolume("shared").Files.ReadAllBytesAsync("/same.bin", Ct));
                Assert.Equal(bytesC.Length, (await clusterA.Inspect.GetFileAsync("shared", "/same.bin"))!.Length);
                Assert.Equal(bytesB.Length, (await clusterB.Inspect.GetFileAsync("shared", "/same.bin"))!.Length);

                // Per-client defaults are not shared: Alice's 1 KiB materialisation limit applies to Alice only.
                byte[] big = TestData.Bytes(3003, 2048);
                await a.GetVolume("va").Files.WriteAllBytesAsync("/big.bin", big, Ct);
                await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(() => a.GetVolume("va").Files.ReadAllBytesAsync("/big.bin", Ct), DfsErrorCode.LimitExceeded);
                await using (DfsClient adminA = await clusterA.ConnectAsync(new TestClientOptions { Name = "adminA" }, Ct))
                    await SdkHelpers.AssertBytesEqualAsync(big, adminA.GetVolume("va").Files.ReadAllBytesAsync("/big.bin", Ct));
                await using (DfsStream s = await a.GetVolume("va").Files.OpenReadAsync("/big.bin", Ct))
                {
                    var streamed = new MemoryStream();
                    await s.CopyToAsync(streamed, Ct);
                    AssertEx.BytesEqual(big, streamed.ToArray(), "large files remain readable through streams");
                }

                // Authorization state is per principal even though clients share a process (and pools).
                await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => c.GetVolume("va").Files.ReadAllBytesAsync("/big.bin", Ct));
                await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => c.GetVolume("va").Files.WriteAllBytesAsync("/carol.bin", bytesC, Ct));
                await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => a.GetVolume("shared").Files.WriteAllBytesAsync("/alice.bin", big, Ct));
                Assert.False(await c.GetVolume("shared").Files.ExistsAsync("/alice.bin", Ct));
                Assert.True(await a.GetVolume("va").Files.ExistsAsync("/big.bin", Ct), "Alice keeps her own access after Carol's denials");

                // Routing never crosses clusters: a volume that exists only in the other cluster is not found.
                foreach ((DfsClient client, string volume) in new[] { (a, "vb"), (b, "va") })
                {
                    Exception ex = await SdkHelpers.CaptureAsync(() => client.GetVolume(volume).Files.ExistsAsync("/same.bin", Ct), $"{client.Principal} on {volume}");
                    Exception unwrapped = SdkHelpers.Unwrap(ex);
                    Assert.True(unwrapped is UnauthorizedAccessException || DfsErrors.ToCode(unwrapped) == DfsErrorCode.VolumeNotFound,
                        $"{client.Principal} on '{volume}': expected VolumeNotFound or AccessDenied but got {unwrapped.GetType().Name} ({DfsErrors.ToCode(unwrapped)})");
                }

                Assert.Equal(0, clusterB.Network.Log.CountMessages(new MessageFilter(From: "client:alice")));
                Assert.Equal(0, clusterB.Network.Log.CountMessages(new MessageFilter(From: "client:carol")));
                Assert.Equal(0, clusterA.Network.Log.CountMessages(new MessageFilter(From: "client:bob")));
            }, Ct);
        }, Ct);
    }

    // =====================================================================================================
    // SDK-008 FacadesAreThreadSafe
    // =====================================================================================================

    private const string SharedKey = "/shared/register.bin";

    private sealed record WorkerResult(List<DfsOperationId> OperationIds, List<InodeId> Inodes, List<CommitToken> Tokens);

    [Fact]
    [Catalog("SDK-008", "U", "INV-06", "INV-08")]
    public async Task SDK_008_FacadesAreThreadSafe()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/shared", Ct);
            const int Workers = 16;
            const int Rounds = 3;
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task<WorkerResult>[] workers = Enumerable.Range(0, Workers)
                .Select(w => Task.Run(async () =>
                {
                    await gate.Task;
                    return await RunWorkerAsync(cluster, volume, w, Rounds);
                }))
                .ToArray();
            gate.SetResult();
            WorkerResult[] results = await Task.WhenAll(workers);

            // Request ids are unique across threads.
            List<DfsOperationId> opIds = results.SelectMany(r => r.OperationIds).ToList();
            Assert.Equal(Workers * Rounds, opIds.Count);
            Assert.Equal(opIds.Count, opIds.Distinct().Count());

            // Every created file and directory is its own inode.
            List<InodeId> inodes = results.SelectMany(r => r.Inodes).ToList();
            Assert.Equal(inodes.Count, inodes.Distinct().Count());

            // Every returned token belongs to this volume's history.
            CommitToken reference = results[0].Tokens[0];
            Assert.All(results.SelectMany(r => r.Tokens), t => Assert.True(t.IsComparableWith(reference), $"token {t} is not from the volume's history"));

            // Concurrent writes/reads of one shared file form a linearizable register history.
            List<HistoryOp> shared = cluster.History.Operations.Where(o => o.Key == SharedKey).ToList();
            Assert.Equal(Workers * Rounds * 2, shared.Count);
            Assert.All(shared, o => Assert.Equal(OpOutcome.Ok, o.Outcome));
            LinearizabilityChecker.Check(shared, new RegisterModel()).AssertValid();

            // Pooled connections are reused, not opened per call: every worker issued well over a dozen calls, yet the
            // client holds at most one connection per concurrent caller per node (and at least one: it is still live).
            int open = cluster.Network.CountOpenConnections(SdkHelpers.NetworkName());
            Assert.InRange(open, 1, cluster.Nodes.Count * Workers);
        }, Ct);
    }

    private static async Task<WorkerResult> RunWorkerAsync(TestCluster cluster, DfsVolume volume, int worker, int rounds)
    {
        var result = new WorkerResult([], [], []);
        string dir = $"/w{worker}";
        DfsDirectoryInfo dirInfo = await volume.Directories.CreateDirectoryAsync(dir, Ct);
        Assert.Equal(dir, dirInfo.Path);
        result.Inodes.Add(dirInfo.InodeId);
        for (int round = 0; round < rounds; round++)
        {
            ulong seed = (ulong)(worker * 100 + round);
            int size = round switch { 0 => 100, 1 => 5_000, _ => 70_000 }; // inline, single extent, multi-chunk
            byte[] data = TestData.Bytes(80_000 + seed, size);
            string file = $"{dir}/f{round}.bin";

            DfsOperationId op = await volume.NewOperationIdAsync(Ct);
            result.OperationIds.Add(op);
            CommitToken written = await volume.Files.WriteAllBytesAsync(file, data, new DfsMutationOptions { OperationId = op }, Ct);
            result.Tokens.Add(written);
            DfsFileInfo info = await volume.Files.GetInfoAsync(file, Ct);
            Assert.Equal(written.Sequence, info.Revision); // this call's result is this call's commit
            Assert.Equal(size, info.Length);
            result.Inodes.Add(info.InodeId);
            byte[] readBack = await volume.Files.ReadAllBytesAsync(file, Ct);
            AssertEx.BytesEqual(data, readBack, $"worker {worker} round {round}");
            DfsOperationOutcome outcome = await volume.QueryOperationAsync(op, false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(written, outcome.Token);

            string streamed = $"{dir}/s{round}.bin";
            await using (DfsStream s = await volume.Files.CreateAsync(streamed, Ct))
            {
                await s.WriteAsync(data, Ct);
                result.Tokens.Add(await s.CommitAsync(Ct));
                result.Inodes.Add(s.InodeId);
                s.Position = 0;
                byte[] back = new byte[data.Length];
                await s.ReadExactlyAsync(back, Ct);
                AssertEx.BytesEqual(data, back, $"worker {worker} stream round {round}");
            }

            byte[] suffix = TestData.Bytes(90_000 + seed, 33);
            result.Tokens.Add(await volume.Files.AppendAllBytesAsync(file, suffix, Ct));
            Assert.Equal(size + suffix.Length, (await volume.Files.GetInfoAsync(file, Ct)).Length);
            result.Tokens.Add(await volume.Files.MoveAsync(file, $"{dir}/m{round}.bin", false, Ct));
            Assert.False(await volume.Files.ExistsAsync(file, Ct));
            result.Tokens.Add(await volume.Files.DeleteAsync(streamed, Ct));

            // 16 threads hit one file: WriteAll's implicit writer reservation may legally conflict with another WriteAll
            // or a ReadAll pin (typed sharing violation, no effect, no automatic waiting — LOCK-03). Such rejections are
            // retried inside the recorded operation; they are not evidence about the facades' thread safety.
            byte[] value = Encoding.UTF8.GetBytes($"w{worker}-r{round}");
            await cluster.History.RecordAsync($"w{worker}", "write", SharedKey, value,
                () => SdkHelpers.RetryOnSharingViolationAsync(cluster, () => volume.Files.WriteAllBytesAsync(SharedKey, value, Ct)));
            await cluster.History.RecordAsync<byte[]?>($"w{worker}", "read", SharedKey, null, () => SdkHelpers.RetryOnSharingViolationAsync<byte[]?>(cluster, async () =>
            {
                try
                {
                    return await volume.Files.ReadAllBytesAsync(SharedKey, Ct);
                }
                catch (FileNotFoundException)
                {
                    return null;
                }
            }));
        }

        string[] names = (await SdkHelpers.ListNamesAsync(volume.Directories, dir)).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(Enumerable.Range(0, rounds).Select(r => $"m{r}.bin").Order(StringComparer.Ordinal), names);
        return result;
    }

    // =====================================================================================================
    // SDK-009 AsyncMethodsAreActuallyAsync
    // =====================================================================================================

    private const string DataPath = "/sdk009/data.bin";

    private sealed class AsyncCallContext
    {
        public required DfsVolume Volume { get; init; }
        public required DfsVolume Observer { get; init; }
        public required byte[] Data { get; init; }
        public required DfsStream ReadStream { get; init; }
        public required DfsStream WriteThroughStream { get; init; }
        public required DfsStream OnFlushStream { get; init; }
        public required byte[] Staged { get; init; }
        public required DfsTransaction Transaction { get; init; }
        public required DfsOperationId KnownOperation { get; init; }
        public required long LogLength { get; init; }
        public required byte[] WriteThroughOriginal { get; init; }
        public DfsFileClient Files => Volume.Files;
    }

    private sealed record AsyncCall(bool Mutates, Func<AsyncCallContext, CancellationToken, Task> Invoke, Func<AsyncCallContext, Task<bool>>? EffectVisible = null);

    private static readonly Dictionary<string, AsyncCall> AsyncCalls = new(StringComparer.Ordinal)
    {
        ["Files.ExistsAsync"] = new(false, (c, ct) => c.Files.ExistsAsync(DataPath, ct)),
        ["Files.GetInfoAsync"] = new(false, (c, ct) => c.Files.GetInfoAsync(DataPath, ct)),
        ["Files.ReadAllBytesAsync"] = new(false, (c, ct) => c.Files.ReadAllBytesAsync(DataPath, ct)),
        ["Files.OpenReadAsync"] = new(false, async (c, ct) => { await using DfsStream s = await c.Files.OpenReadAsync(DataPath, ct); }),
        ["Directories.EnumerateAsync"] = new(false, async (c, ct) =>
        {
            await foreach (DfsEntryInfo _ in c.Volume.Directories.EnumerateAsync("/sdk009/dir", null, ct))
                break;
        }),
        ["Directories.ListPageAsync"] = new(false, (c, ct) => c.Volume.Directories.ListPageAsync("/sdk009/dir", null, null, ct)),
        ["Directories.GetInfoAsync"] = new(false, (c, ct) => c.Volume.Directories.GetInfoAsync("/sdk009/dir", ct)),
        ["Stream.ReadAsync"] = new(false, (c, ct) => c.ReadStream.ReadAsync(new byte[16], ct).AsTask()),
        ["Stream.GetLengthAsync"] = new(false, (c, ct) => c.ReadStream.GetLengthAsync(ct).AsTask()),
        ["Volume.GetInfoAsync"] = new(false, (c, ct) => c.Volume.GetInfoAsync(ct)),
        ["Volume.QueryOperationAsync"] = new(false, (c, ct) => c.Volume.QueryOperationAsync(c.KnownOperation, false, ct)),
        ["Snapshots.ListAsync"] = new(false, (c, ct) => c.Volume.Snapshots.ListAsync(ct)),
        ["Transactions.BeginAsync"] = new(true, (c, ct) => c.Volume.Transactions.BeginAsync(null, ct)),
        ["Files.WriteAllBytesAsync"] = new(true, (c, ct) => c.Files.WriteAllBytesAsync("/sdk009/new.bin", c.Data, ct),
            c => c.Observer.Files.ExistsAsync("/sdk009/new.bin", Ct)),
        ["Files.AppendAllBytesAsync"] = new(true, (c, ct) => c.Files.AppendAllBytesAsync("/sdk009/log.bin", new byte[] { 1, 2, 3 }, ct),
            async c => (await c.Observer.Files.GetInfoAsync("/sdk009/log.bin", Ct)).Length != c.LogLength),
        ["Files.CopyAsync"] = new(true, (c, ct) => c.Files.CopyAsync(DataPath, "/sdk009/copy.bin", false, ct),
            c => c.Observer.Files.ExistsAsync("/sdk009/copy.bin", Ct)),
        ["Files.MoveAsync"] = new(true, (c, ct) => c.Files.MoveAsync("/sdk009/move-src.bin", "/sdk009/move-dst.bin", false, ct),
            c => c.Observer.Files.ExistsAsync("/sdk009/move-dst.bin", Ct)),
        ["Files.DeleteAsync"] = new(true, (c, ct) => c.Files.DeleteAsync("/sdk009/delete.bin", ct),
            async c => !await c.Observer.Files.ExistsAsync("/sdk009/delete.bin", Ct)),
        ["Directories.CreateDirectoryAsync"] = new(true, (c, ct) => c.Volume.Directories.CreateDirectoryAsync("/sdk009/newdir", ct),
            c => c.Observer.Directories.ExistsAsync("/sdk009/newdir", Ct)),
        // The probed files have an open writer (Share.Read), so the observer reads them with a compatible
        // Share.ReadWrite stream instead of ReadAllBytes (whose internal handle may legally conflict with the writer).
        ["Stream.WriteAsync(WriteThrough)"] = new(true, (c, ct) => c.WriteThroughStream.WriteAsync(new byte[] { 9, 9, 9 }, ct).AsTask(),
            async c => !(await SdkHelpers.ReadSharedAsync(c.Observer.Files, "/sdk009/wt.bin", Ct)).AsSpan().SequenceEqual(c.WriteThroughOriginal)),
        ["Stream.FlushAsync(OnFlush)"] = new(true, (c, ct) => c.OnFlushStream.FlushAsync(ct),
            async c => (await SdkHelpers.ReadSharedAsync(c.Observer.Files, "/sdk009/of.bin", Ct)).AsSpan().SequenceEqual(c.Staged)),
        ["Transaction.CommitAsync"] = new(true, (c, ct) => c.Transaction.CommitAsync(ct),
            c => c.Observer.Files.ExistsAsync("/sdk009/tx.bin", Ct)),
        ["Snapshots.CreateAsync"] = new(true, (c, ct) => c.Volume.Snapshots.CreateAsync("sdk009-snap", null, ct),
            async c => (await c.Observer.Snapshots.ListAsync(Ct)).Any(s => s.Name == "sdk009-snap")),
        ["Client.ConnectAsync"] = new(false, (_, _) => Task.CompletedTask),
    };

    public static TheoryData<string> AsyncCallCases => new(AsyncCalls.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(AsyncCallCases))]
    [Catalog("SDK-009", "U", "INV-17", "INV-06")]
    public async Task SDK_009_AsyncMethodsAreActuallyAsync(string callName)
    {
        // Deterministic mode runs the whole cluster and client on one simulation thread: there is no worker thread a
        // synchronous network wait (or a Task.Run substitute) could block on, so such an SDK would hang here.
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            if (callName == "Client.ConnectAsync")
            {
                await ConnectIsAsynchronousAsync(cluster);
                return;
            }

            AsyncCall call = AsyncCalls[callName];
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" });
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" });
            AsyncCallContext context = await PrepareAsyncCallContextAsync(client.GetVolume(Volume), observer.GetVolume(Volume));
            if (call.EffectVisible is not null)
                Assert.False(await call.EffectVisible(context), $"{callName}: effect visible before the call");

            MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c0", Direction: LinkDirection.Request));
            using var cts = new CancellationTokenSource();
            Task task = call.Invoke(context, cts.Token);
            Assert.False(task.IsCompleted, $"{callName} completed synchronously while the transport was blocked");
            await hold.WaitForAsync(1, TimeSpan.FromSeconds(5));
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500));
            Assert.False(task.IsCompleted, $"{callName} completed while all its requests were held");

            cts.Cancel();
            await Task.WhenAny(task, cluster.DelayAsync(TimeSpan.FromSeconds(1)));
            Assert.True(task.IsCompleted, $"{callName} did not honor cancellation within 1 s while the transport was blocked");
            Exception? error = await Record.ExceptionAsync(() => task);
            Assert.NotNull(error);
            error = SdkHelpers.Unwrap(error);

            hold.ReleaseAll();
            hold.Dispose();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2));

            if (!call.Mutates)
            {
                Assert.True(error is OperationCanceledException, $"{callName}: expected OperationCanceledException but got {error}");
                if (callName == "Stream.ReadAsync")
                    Assert.Equal(0, context.ReadStream.Position); // FSD §6.2: a failed read leaves Position unchanged
            }
            else if (error is OperationCanceledException)
            {
                // Reporting a definite cancellation is legal only if the held request can never commit afterwards.
                if (call.EffectVisible is not null)
                    Assert.False(await call.EffectVisible(context), $"{callName} reported cancellation, but its request committed after the transport was released");
            }
            else
            {
                DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(error);
                Assert.False(unknown.OperationId.IsEmpty);
                DfsOperationOutcome outcome = await context.Observer.QueryOperationAsync(unknown.OperationId, resolve: true);
                Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Aborted });
                if (call.EffectVisible is not null)
                    Assert.Equal(outcome.Status == DfsOutcomeStatus.Committed, await call.EffectVisible(context));
            }

            // The client is still usable after a cancelled call.
            await SdkHelpers.AssertBytesEqualAsync(context.Data, context.Files.ReadAllBytesAsync(DataPath));
        });
    }

    private static async Task<AsyncCallContext> PrepareAsyncCallContextAsync(DfsVolume volume, DfsVolume observer)
    {
        byte[] data = TestData.Bytes(9009, 8192);
        await volume.Directories.CreateDirectoryAsync("/sdk009/dir", default);
        for (int i = 0; i < 3; i++)
            await volume.Files.WriteAllBytesAsync($"/sdk009/dir/e{i}", new byte[] { (byte)i }, default);
        DfsOperationId known = await volume.NewOperationIdAsync(default);
        await volume.Files.WriteAllBytesAsync(DataPath, data, new DfsMutationOptions { OperationId = known }, default);
        await volume.Files.WriteAllBytesAsync("/sdk009/log.bin", new byte[] { 0 }, default);
        await volume.Files.WriteAllBytesAsync("/sdk009/move-src.bin", new byte[] { 1 }, default);
        await volume.Files.WriteAllBytesAsync("/sdk009/delete.bin", new byte[] { 2 }, default);
        byte[] wtOriginal = TestData.Bytes(9010, 64);
        await volume.Files.WriteAllBytesAsync("/sdk009/wt.bin", wtOriginal, default);
        await volume.Files.WriteAllBytesAsync("/sdk009/of.bin", TestData.Bytes(9011, 64), default);

        DfsStream read = await volume.Files.OpenAsync(DataPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, default);
        DfsStream writeThrough = await volume.Files.OpenAsync("/sdk009/wt.bin",
            new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read, WriteMode = DfsWriteMode.WriteThrough }, default);
        DfsStream onFlush = await volume.Files.OpenAsync("/sdk009/of.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, default);
        byte[] staged = TestData.Bytes(9012, 64);
        await onFlush.WriteAsync(staged, default);
        DfsTransaction tx = await volume.Transactions.BeginAsync(null, default);
        await tx.Files.WriteAllBytesAsync("/sdk009/tx.bin", new byte[] { 7 }, default);
        _ = await read.ReadAsync(new byte[1], default); // routes, sessions and connections are established before the hold
        read.Position = 0;

        return new AsyncCallContext
        {
            Volume = volume,
            Observer = observer,
            Data = data,
            ReadStream = read,
            WriteThroughStream = writeThrough,
            OnFlushStream = onFlush,
            Staged = staged,
            Transaction = tx,
            KnownOperation = known,
            LogLength = 1,
            WriteThroughOriginal = wtOriginal,
        };
    }

    private static async Task ConnectIsAsynchronousAsync(TestCluster cluster)
    {
        MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c5"));
        using var cts = new CancellationTokenSource();
        Task<DfsClient> connect = cluster.ConnectAsync(new TestClientOptions { Name = "c5" }, cts.Token);
        Assert.False(connect.IsCompleted, "ConnectAsync completed synchronously while the transport was blocked");
        await hold.WaitForAsync(1, TimeSpan.FromSeconds(5));
        await cluster.DelayAsync(TimeSpan.FromMilliseconds(500));
        Assert.False(connect.IsCompleted);
        cts.Cancel();
        await Task.WhenAny(connect, cluster.DelayAsync(TimeSpan.FromSeconds(1)));
        Assert.True(connect.IsCompleted, "ConnectAsync did not honor cancellation within 1 s");
        Exception? error = await Record.ExceptionAsync(() => connect);
        Assert.NotNull(error);
        Assert.True(SdkHelpers.Unwrap(error) is OperationCanceledException, $"expected OperationCanceledException but got {error}");

        hold.ReleaseAll();
        hold.Dispose();
        await cluster.DelayAsync(TimeSpan.FromSeconds(1)); // let the released frames drain
        cluster.Network.Log.Reset();
        await cluster.DelayAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c5")));
        Assert.Equal(0, cluster.Network.CountOpenConnections("client:c5"));
    }

    // =====================================================================================================
    // SDK-011 ClientDisposalIsIdempotent
    // =====================================================================================================

    public static TheoryData<string> DisposalSequences => new("DisposeAsync+DisposeAsync", "Dispose+Dispose", "Dispose+DisposeAsync", "DisposeAsync+Dispose", "ConcurrentDisposeAsync");

    [Theory, MemberData(nameof(DisposalSequences))]
    [Catalog("SDK-011", "U", "INV-17", "INV-12")]
    public async Task SDK_011_ClientDisposalIsIdempotent(string sequence)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            string me = SdkHelpers.NetworkName();
            DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            DfsAdminClient admin = client.Admin;
            byte[] bytes = TestData.Bytes(1101, 3000);
            await volume.Files.WriteAllBytesAsync("/sdk011.bin", bytes, Ct);
            Assert.False(client.IsDisposed);
            Assert.True(cluster.Network.CountOpenConnections(me) > 0);

            switch (sequence)
            {
                case "DisposeAsync+DisposeAsync":
                case "DisposeAsync+Dispose":
                    await client.DisposeAsync();
                    break;
                case "ConcurrentDisposeAsync":
                    await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => client.DisposeAsync().AsTask())));
                    break;
                default:
                    client.Dispose();
                    break;
            }

            Assert.True(client.IsDisposed);
            await cluster.WaitUntilAsync(() => Task.FromResult(cluster.Network.CountOpenConnections(me) == 0), TimeSpan.FromSeconds(5),
                "the disposed client's connections are closed", Ct);
            cluster.Network.Log.Reset();

            // Second disposal: no exception, no second release.
            switch (sequence)
            {
                case "DisposeAsync+DisposeAsync":
                case "Dispose+DisposeAsync":
                    await client.DisposeAsync();
                    break;
                case "ConcurrentDisposeAsync":
                    await client.DisposeAsync();
                    client.Dispose();
                    break;
                default:
                    client.Dispose();
                    break;
            }

            Assert.True(client.IsDisposed);

            // New work is rejected locally on every facade, async and sync. GetVolume does no I/O, so it may either throw
            // ObjectDisposedException itself or hand out a facade whose operations do (the FSD only requires that new
            // work is rejected).
            foreach (string name in new[] { Volume, "another" })
            {
                DfsVolume? late = null;
                Exception? getVolume = Record.Exception(() => late = client.GetVolume(name));
                if (getVolume is not null)
                    Assert.IsAssignableFrom<ObjectDisposedException>(getVolume);
                else
                    await Assert.ThrowsAsync<ObjectDisposedException>(() => late!.Files.ExistsAsync("/sdk011.bin", Ct));
            }

            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Files.ReadAllBytesAsync("/sdk011.bin", Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Files.WriteAllBytesAsync("/sdk011-new.bin", bytes, Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Files.OpenReadAsync("/sdk011.bin", Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Directories.CreateDirectoryAsync("/sdk011-dir", Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Transactions.BeginAsync(null, Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Snapshots.ListAsync(Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.GetInfoAsync(Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.NewOperationIdAsync(Ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListVolumesAsync(Ct));
            Assert.Throws<ObjectDisposedException>(() => volume.Files.ReadAllBytes("/sdk011.bin"));
            Assert.Throws<ObjectDisposedException>(() => volume.Directories.Exists("/"));

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: me)));
            Assert.Equal(0, cluster.Network.CountOpenConnections(me));

            // Disposal changed nothing in the volume.
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            await SdkHelpers.AssertBytesEqualAsync(bytes, other.GetVolume(Volume).Files.ReadAllBytesAsync("/sdk011.bin", Ct));
            Assert.False(await other.GetVolume(Volume).Files.ExistsAsync("/sdk011-new.bin", Ct));
        }, Ct);
    }

    // =====================================================================================================
    // SDK-013 CancellationTokenPropagation
    // =====================================================================================================

    private static readonly Type[] PublicSdkTypes =
    [
        typeof(DfsClient), typeof(DfsVolume), typeof(DfsFileClient), typeof(DfsDirectoryClient), typeof(DfsTransactionClient),
        typeof(DfsTransaction), typeof(DfsSnapshotClient), typeof(DfsSnapshotView), typeof(DfsBackupClient), typeof(DfsAdminClient), typeof(DfsStream),
    ];

    private sealed class SweepContext
    {
        public required DfsClient Client { get; init; }
        public required DfsVolume Volume { get; init; }
        public required DfsTransaction Transaction { get; init; }
        public required DfsStream Stream { get; init; }
        public required DfsOperationId OperationId { get; init; }
        public required CommitToken Token { get; init; }
        public required DfsClientOptions ClientOptions { get; init; }
    }

    private const string SweepFile = "/sdk013/existing.bin";

    [Fact]
    [Catalog("SDK-013", "U", "INV-06", "INV-17")]
    public async Task SDK_013_CancellationTokenPropagation_PreCancelledEveryOverload()
    {
        MethodInfo[] methods = CancellableAsyncMethods().OrderBy(Describe, StringComparer.Ordinal).ToArray();
        Assert.True(methods.Length >= 90, $"only {methods.Length} cancellable async overloads were found; the reflection inventory is broken");
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/sdk013", Ct);
            byte[] existing = TestData.Bytes(1313, 5000);
            CommitToken token = await volume.Files.WriteAllBytesAsync(SweepFile, existing, Ct);
            await volume.Snapshots.CreateAsync("sdk013-snap", null, Ct);
            DfsOperationId reserved = await volume.NewOperationIdAsync(Ct);
            DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
            DfsStream stream = await volume.Files.OpenAsync(SweepFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            var context = new SweepContext
            {
                Client = client,
                Volume = volume,
                Transaction = tx,
                Stream = stream,
                OperationId = reserved,
                Token = token,
                ClientOptions = cluster.CreateClientOptions(),
            };

            string me = SdkHelpers.NetworkName();
            int inodesBefore = await SdkHelpers.InodeCountAsync(cluster);
            int snapshotsBefore = (await volume.Snapshots.ListAsync(Ct)).Count;
            IReadOnlyList<string> volumesBefore = await client.Admin.ListVolumesAsync(Ct);
            IReadOnlyList<string> targetsBefore = await volume.Backups.ListTargetsAsync(Ct);
            long requestsBefore = SdkHelpers.ForegroundRequests(cluster, me);

            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            var failures = new List<string>();
            foreach (MethodInfo method in methods)
            {
                object? target = method.IsStatic ? null : TargetFor(method.DeclaringType!, context);
                object?[] args;
                try
                {
                    args = method.GetParameters().Select(p => ArgumentFor(p, method, context, cancelled.Token)).ToArray();
                }
                catch (NotSupportedException ex)
                {
                    failures.Add(ex.Message);
                    continue;
                }

                long before = SdkHelpers.ForegroundRequests(cluster, me);
                Exception? error = await InvokeAndAwaitAsync(method, target, args);
                error = error is null ? null : SdkHelpers.Unwrap(error);
                if (error is not OperationCanceledException)
                    failures.Add($"{Describe(method)}: {(error is null ? "completed" : $"{error.GetType().Name}: {error.Message}")} instead of OperationCanceledException");
                long after = SdkHelpers.ForegroundRequests(cluster, me);
                if (after != before)
                    failures.Add($"{Describe(method)}: sent {after - before} request(s) despite a cancelled token");
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            if (SdkHelpers.ForegroundRequests(cluster, me) != requestsBefore)
                failures.Add("requests reached the cluster during the pre-cancelled sweep");
            Assert.True(failures.Count == 0, $"{failures.Count} of {methods.Length} overloads misbehaved:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");

            // Nothing changed and nothing was committed or aborted implicitly.
            Assert.Equal(inodesBefore, await SdkHelpers.InodeCountAsync(cluster));
            Assert.Equal(snapshotsBefore, (await volume.Snapshots.ListAsync(Ct)).Count);
            Assert.Equal(volumesBefore.Order(StringComparer.Ordinal), (await client.Admin.ListVolumesAsync(Ct)).Order(StringComparer.Ordinal));
            Assert.Equal(targetsBefore.Order(StringComparer.Ordinal), (await volume.Backups.ListTargetsAsync(Ct)).Order(StringComparer.Ordinal));
            Assert.Equal(DfsTransactionState.Active, tx.State);
            Assert.False(stream.IsFaulted);
            await stream.DisposeAsync();
            await tx.AbortAsync(Ct);
            await SdkHelpers.AssertBytesEqualAsync(existing, volume.Files.ReadAllBytesAsync(SweepFile, Ct));
        }, Ct);
    }

    public static TheoryData<string> CancellationStages => new(
        "F00_OperationStart", "F01_Validated", "F02_ConnectionAcquisition", "F03_UploadChunk", "F03_RequestSend", "F14_ReplyReceived",
        "RoutingAndSession", "BackupWait");

    [Theory, MemberData(nameof(CancellationStages))]
    [Catalog("SDK-013", "U", "INV-06", "INV-17")]
    public async Task SDK_013_CancellationTokenPropagation(string stage)
    {
        TestClusterOptions options = stage == "BackupWait" ? TestClusterOptions.ThreeZones with { BackupNodeCount = 1 } : TestClusterOptions.ThreeZones;
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            const string Target = "/sdk013/target.bin";
            byte[] original = TestData.Bytes(1330, 3000);
            byte[] replacement = TestData.Bytes(1331, 200_000); // above the inline threshold, four 64 KiB chunks
            await volume.Directories.CreateDirectoryAsync("/sdk013", Ct);
            await volume.Files.WriteAllBytesAsync(Target, original, Ct);
            string me = SdkHelpers.NetworkName();

            if (stage == "RoutingAndSession")
            {
                await CancelDuringRoutingAsync(cluster, Target, original);
                return;
            }

            if (stage == "BackupWait")
            {
                await CancelDuringBackupWaitAsync(cluster, volume);
                return;
            }

            (string site, int occurrence, bool beforeTransmission) = stage switch
            {
                "F00_OperationStart" => (FaultSites.ClientOperationStart, 1, true),
                "F01_Validated" => (FaultSites.ClientValidated, 1, true),
                "F02_ConnectionAcquisition" => (FaultSites.ClientConnect, 1, true),
                "F03_UploadChunk" => (FaultSites.ClientUploadChunk, 2, true), // staging: the commit request is never sent
                "F03_RequestSend" => (FaultSites.ClientRequestSend, 1, false),
                "F14_ReplyReceived" => (FaultSites.ClientReplyReceived, 1, false),
                _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, null),
            };
            if (stage == "F02_ConnectionAcquisition")
                cluster.Network.ResetConnections(me);
            if (stage == "F03_RequestSend")
                replacement = TestData.Bytes(1333, 1000); // inline: no uploads, so the first send of the operation is (close to) the commit

            long commitsBefore = SdkHelpers.RequestsOfKind(cluster, me, DfsOperationKind.WriteAll);
            using var cts = new CancellationTokenSource();
            bool cancelledAfterCommitReply = false;
            FaultArm arm = stage == "F14_ReplyReceived"
                // A large WriteAll receives upload (PutObject/staging) replies before the commit reply, so cancel at the
                // first reply received after the WriteAll commit request itself was sent: only then is the outcome known.
                ? cluster.Faults.Arm(new FaultMatch(site, Operation: DfsOperationKind.WriteAll, Count: 100_000), FaultAction.Custom(_ =>
                {
                    if (!cancelledAfterCommitReply && SdkHelpers.RequestsOfKind(cluster, me, DfsOperationKind.WriteAll) > commitsBefore)
                    {
                        cancelledAfterCommitReply = true;
                        cts.Cancel();
                    }

                    return Task.CompletedTask;
                }))
                : cluster.Faults.Arm(new FaultMatch(site, Operation: DfsOperationKind.WriteAll, Occurrence: occurrence), FaultAction.CancelCaller());
            Exception? error = await Record.ExceptionAsync(() => volume.Files.WriteAllBytesAsync(Target, replacement, cts.Token));
            cluster.Faults.AssertReached(arm);
            if (stage == "F14_ReplyReceived")
                Assert.True(cancelledAfterCommitReply, "F14: no reply was received for the WriteAll after its commit request was sent");
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            error = error is null ? null : SdkHelpers.Unwrap(error);
            byte[] current = await volume.Files.ReadAllBytesAsync(Target, Ct);

            if (beforeTransmission)
            {
                // Nothing was transmitted for publication, so the SDK knows nothing committed.
                Assert.True(error is OperationCanceledException, $"{stage}: expected OperationCanceledException but got {error?.ToString() ?? "success"}");
                Assert.Equal(commitsBefore, SdkHelpers.RequestsOfKind(cluster, me, DfsOperationKind.WriteAll));
                AssertEx.BytesEqual(original, current, $"{stage}: cancellation before publication leaves committed state unchanged");
            }
            else if (stage == "F03_RequestSend")
            {
                switch (error)
                {
                    case OperationCanceledException:
                        AssertEx.BytesEqual(original, current, "a cancellation reported as definite must not commit later");
                        await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                        await SdkHelpers.AssertBytesEqualAsync(original, volume.Files.ReadAllBytesAsync(Target, Ct));
                        break;
                    case DfsCommitOutcomeUnknownException unknown:
                        DfsOperationOutcome outcome = await volume.QueryOperationAsync(unknown.OperationId, resolve: true, Ct);
                        Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Aborted });
                        await SdkHelpers.AssertBytesEqualAsync(outcome.Status == DfsOutcomeStatus.Committed ? replacement : original, volume.Files.ReadAllBytesAsync(Target, Ct));
                        break;
                    case null:
                        AssertEx.BytesEqual(replacement, current, "a successful return implies the replacement committed");
                        break;
                    default:
                        Assert.Fail($"{stage}: unexpected {error}");
                        break;
                }
            }
            else
            {
                // F14: the reply (commit) was received; cancellation racing completion must not be reported as failure.
                Assert.False(error is OperationCanceledException, "a known commit must not be reported as cancelled");
                if (error is not null)
                {
                    DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(error);
                    DfsOperationOutcome outcome = await volume.QueryOperationAsync(unknown.OperationId, resolve: true, Ct);
                    Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                }

                AssertEx.BytesEqual(replacement, current, "the commit acknowledged by the received reply stands");
            }

            // The client remains usable after any of these cancellations.
            byte[] after = TestData.Bytes(1332, 100);
            await volume.Files.WriteAllBytesAsync(Target, after, Ct);
            await SdkHelpers.AssertBytesEqualAsync(after, volume.Files.ReadAllBytesAsync(Target, Ct));
        }, Ct);
    }

    private static async Task CancelDuringRoutingAsync(TestCluster cluster, string path, byte[] expected)
    {
        // A fresh client's first volume call needs routing/session establishment; every request is held, so the call
        // is cancelled while acquiring its route or session (or, at the latest, its read).
        await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c7" }, Ct);
        MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c7", Direction: LinkDirection.Request));
        using var cts = new CancellationTokenSource();
        Task<byte[]> read = fresh.GetVolume(Volume).Files.ReadAllBytesAsync(path, cts.Token);
        await hold.WaitForAsync(1, TimeSpan.FromSeconds(5));
        SdkHelpers.Log($"first held request of the fresh client: {hold.Captured[0].Type}");
        Assert.False(read.IsCompleted);
        cts.Cancel();
        Exception? error = await Record.ExceptionAsync(() => read.WaitAsync(TimeSpan.FromSeconds(2), cluster.Time));
        Assert.True(error is OperationCanceledException, $"expected OperationCanceledException but got {error?.ToString() ?? "success"}");
        hold.ReleaseAll();
        hold.Dispose();
        byte[] readBack = await fresh.GetVolume(Volume).Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, readBack);
    }

    private static async Task CancelDuringBackupWaitAsync(TestCluster cluster, DfsVolume volume)
    {
        TestBackupNode backup = cluster.BackupNodes[0];
        await backup.RegisterAsync(Volume);
        await backup.PauseAsync(); // the barrier cannot be reached while the target is paused
        byte[] data = TestData.Bytes(1340, 500);
        CommitToken token = await volume.Files.WriteAllBytesAsync("/sdk013/backup.bin", data, Ct);
        using var cts = new CancellationTokenSource();
        Task<DfsBackupCheckpoint> wait = volume.Backups.AwaitBackupAsync(backup.TargetName, token, TimeSpan.FromSeconds(60), cts.Token);
        await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), Ct);
        Assert.False(wait.IsCompleted);
        cts.Cancel();
        Exception? error = await Record.ExceptionAsync(() => wait.WaitAsync(TimeSpan.FromSeconds(2), cluster.Time));
        Assert.True(error is OperationCanceledException, $"expected OperationCanceledException from the backup wait but got {error?.ToString() ?? "success"}");
        // Cancelling a wait never rolls back the live commit.
        byte[] readBack = await volume.Files.ReadAllBytesAsync("/sdk013/backup.bin", Ct);
        AssertEx.BytesEqual(data, readBack);
        await backup.ResumeAsync();
    }

    private static IEnumerable<MethodInfo> CancellableAsyncMethods() =>
        PublicSdkTypes
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Where(m => !m.IsSpecialName && m.GetParameters().Any(p => p.ParameterType == typeof(CancellationToken)) && IsAsyncReturn(m.ReturnType));

    private static bool IsAsyncReturn(Type t) =>
        typeof(Task).IsAssignableFrom(t) || t == typeof(ValueTask)
        || (t.IsGenericType && (t.GetGenericTypeDefinition() == typeof(ValueTask<>) || t.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)));

    private static string Describe(MethodInfo m) =>
        $"{m.DeclaringType!.Name}.{m.Name}({string.Join(", ", m.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"))})";

    private static object? TargetFor(Type type, SweepContext c)
    {
        if (type == typeof(DfsClient))
            return c.Client;
        if (type == typeof(DfsVolume))
            return c.Volume;
        if (type == typeof(DfsFileClient))
            return c.Volume.Files;
        if (type == typeof(DfsDirectoryClient))
            return c.Volume.Directories;
        if (type == typeof(DfsTransactionClient))
            return c.Volume.Transactions;
        if (type == typeof(DfsTransaction))
            return c.Transaction;
        if (type == typeof(DfsSnapshotClient))
            return c.Volume.Snapshots;
        if (type == typeof(DfsBackupClient))
            return c.Volume.Backups;
        if (type == typeof(DfsAdminClient))
            return c.Client.Admin;
        if (type == typeof(DfsStream))
            return c.Stream;
        throw new NotSupportedException($"No target instance for {type.Name}; extend SDK-013.");
    }

    private static readonly byte[] SweepPayload = TestData.Bytes(1314, 128);

    private static object? ArgumentFor(ParameterInfo p, MethodInfo m, SweepContext c, CancellationToken cancelled)
    {
        Type t = p.ParameterType;
        string name = p.Name ?? string.Empty;
        if (t == typeof(CancellationToken))
            return cancelled;
        if (t == typeof(string))
        {
            return name switch
            {
                "path" or "sourcePath" => SweepFile,
                "destinationPath" => "/sdk013/destination.bin",
                "contents" => "text",
                "snapshotName" => "sdk013-snap",
                "name" when m.DeclaringType == typeof(DfsSnapshotClient) => "sdk013-snap",
                "name" => "n9",
                "newVolumeName" => "sdk013-restored",
                "target" => "b0",
                "volume" => Volume,
                "principal" => "sdk013-principal",
                "secret" => "sdk013-secret",
                "policyName" => "sdk013-policy",
                "feature" => "sdk013-feature",
                "passphrase" => "sdk013-passphrase",
                "node" or "continuationToken" => null,
                _ => "sdk013-" + name,
            };
        }

        if (t == typeof(ReadOnlyMemory<byte>))
            return new ReadOnlyMemory<byte>(SweepPayload);
        if (t == typeof(Memory<byte>))
            return new Memory<byte>(new byte[16]);
        if (t == typeof(byte[]))
            return new byte[16];
        if (t == typeof(int))
            return name switch { "offset" => 0, "count" => 16, "bufferSize" => 4096, "max" => 10, _ => 1 };
        if (t == typeof(long))
            return 0L;
        if (t == typeof(bool))
            return false;
        if (t == typeof(TimeSpan))
            return TimeSpan.FromSeconds(10);
        if (t == typeof(Encoding))
            return null;
        if (t == typeof(IEnumerable<string>))
            return new[] { "line" };
        if (t == typeof(IReadOnlyList<string>))
            return new[] { "mem://n9" };
        if (t == typeof(Stream))
            return new MemoryStream();
        if (t == typeof(FileMode))
            return FileMode.OpenOrCreate;
        if (t == typeof(FileAccess))
            return FileAccess.ReadWrite;
        if (t == typeof(FileShare))
            return FileShare.None;
        if (t == typeof(DfsOpenOptions))
            return new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None };
        if (t == typeof(DfsMutationOptions))
            return new DfsMutationOptions();
        if (t == typeof(DfsInodeAccess))
            return new DfsInodeAccess();
        if (t == typeof(DfsEnumerationOptions) || t == typeof(DfsTransactionOptions) || t == typeof(DfsSnapshotOptions))
            return null;
        if (t == typeof(DfsOperationId))
            return c.OperationId;
        if (t == typeof(DfsTransactionId))
            return c.Transaction.Id;
        if (t == typeof(DfsRetention))
            return new DfsRetention();
        if (t == typeof(DfsBackupSchedule))
            return new DfsBackupSchedule();
        if (t == typeof(DfsBackupBudgets))
            return new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30);
        if (t == typeof(DfsBackupTargetRegistration))
            return new DfsBackupTargetRegistration("b0", ["mem://b0"], new DfsCredentials("sdk013-export", "sdk013-export-secret"), new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));
        if (t == typeof(CommitToken))
            return c.Token;
        if (t == typeof(DfsTopologyLabels))
            return new DfsTopologyLabels("r1", "z9", "rk9", "h9", "d9");
        if (t == typeof(DfsVolumeSpec))
            return new DfsVolumeSpec { Name = "sdk013-volume" };
        if (t == typeof(DfsStoragePolicy))
            return DfsStoragePolicy.Default;
        if (t == typeof(DfsPermissions))
            return DfsPermissions.Read;
        if (t == typeof(DfsRestoreFromBackupRequest))
            return new DfsRestoreFromBackupRequest(["mem://b0"], new DfsCredentials("sdk013-reader", "sdk013-reader-secret"), Volume, "sdk013-snap", "sdk013-restored-2");
        if (t == typeof(DfsClientOptions))
            return c.ClientOptions;
        throw new NotSupportedException($"No argument factory for parameter '{name}' of type {t.Name} in {Describe(m)}; extend SDK-013 to cover the new overload.");
    }

    private static async Task<Exception?> InvokeAndAwaitAsync(MethodInfo method, object? target, object?[] args)
    {
        object? result;
        try
        {
            result = method.Invoke(target, args);
        }
        catch (TargetInvocationException tie)
        {
            return tie.InnerException ?? tie;
        }

        try
        {
            await AwaitResultAsync(result);
        }
        catch (Exception ex)
        {
            return ex;
        }

        return null;
    }

    private static async Task AwaitResultAsync(object? result)
    {
        switch (result)
        {
            case null:
                return;
            case Task task:
                await task;
                return;
            case ValueTask valueTask:
                await valueTask;
                return;
        }

        Type type = result.GetType();
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            var asTask = (Task)type.GetMethod(nameof(ValueTask<int>.AsTask))!.Invoke(result, null)!;
            await asTask;
            return;
        }

        Type? sequence = type.GetInterfaces().Append(type)
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>));
        if (sequence is null)
            throw new InvalidOperationException($"Unexpected async return type {type}.");
        object enumerator = sequence.GetMethod(nameof(IAsyncEnumerable<int>.GetAsyncEnumerator))!.Invoke(result, [CancellationToken.None])!;
        Type enumeratorType = typeof(IAsyncEnumerator<>).MakeGenericType(sequence.GetGenericArguments()[0]);
        try
        {
            var moveNext = (ValueTask<bool>)enumeratorType.GetMethod(nameof(IAsyncEnumerator<int>.MoveNextAsync))!.Invoke(enumerator, null)!;
            await moveNext;
        }
        finally
        {
            await ((IAsyncDisposable)enumerator).DisposeAsync();
        }
    }

    // =====================================================================================================
    // SDK-014 CapabilitiesAreAdvertised
    // =====================================================================================================

    [Fact]
    [Catalog("SDK-014", "U", "INV-08", "INV-17")]
    public async Task SDK_014_CapabilitiesAreAdvertised()
    {
        var strict = new DfsVolumeLimits
        {
            MaxFileLength = 1L << 20,
            MaxPublishBytes = 256L << 10,
            MaxTransactionTouchedInodes = 4,
            MaxHandlesPerSession = 3,
        };
        TestClusterOptions options = TestClusterOptions.ThreeZones with
        {
            Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("strict") { Limits = strict, InlineThreshold = 1024 }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsServerCapabilities caps = client.Capabilities;
            Assert.True(caps.ProtocolVersion >= 1);
            Assert.NotNull(caps.Features);
            Assert.Equal(DfsPath.MaxPathBytes, caps.MaxPathBytes);
            Assert.Equal(DfsPath.MaxComponentBytes, caps.MaxComponentBytes);
            Assert.Equal(NodeTimings.Fast.ChunkSize, caps.ChunkSize);
            Assert.Equal(NodeTimings.Fast.MaxObjectSize, caps.MaxObjectSize);
            Assert.Equal(NodeTimings.Fast.LeaseDuration, caps.LeaseDuration);
            Assert.InRange(caps.MaxFileLength, 1, long.MaxValue);
            Assert.InRange(caps.MaxPublishBytes, 1, long.MaxValue);
            Assert.NotNull(caps.TransactionLimits);
            Assert.Equal(cluster.ClusterId, client.Cluster.ClusterId);
            DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
            Assert.True(status.ActiveFeatures.IsSubsetOf(caps.Features), "every cluster-wide active feature is advertised to clients");

            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume strictVolume = client.GetVolume("strict");
            DfsVolumeInfo volInfo = await vol.GetInfoAsync(Ct);
            DfsVolumeInfo strictInfo = await strictVolume.GetInfoAsync(Ct);
            Assert.Equal(new DfsVolumeLimits(), volInfo.Limits);
            Assert.Equal(strict, strictInfo.Limits);
            Assert.Equal(4096, volInfo.InlineThreshold);
            Assert.Equal(1024, strictInfo.InlineThreshold);

            // 1. Publication size: exactly at the limit accepted, one byte more rejected without partial publication.
            byte[] atLimit = TestData.Bytes(1401, (int)strict.MaxPublishBytes);
            byte[] overLimit = TestData.Bytes(1402, (int)strict.MaxPublishBytes + 1);
            await strictVolume.Files.WriteAllBytesAsync("/publish.bin", atLimit, Ct);
            await SdkHelpers.AssertBytesEqualAsync(atLimit, strictVolume.Files.ReadAllBytesAsync("/publish.bin", Ct));
            DfsLimitExceededException publishEx = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(
                () => strictVolume.Files.WriteAllBytesAsync("/publish.bin", overLimit, Ct), DfsErrorCode.LimitExceeded);
            Assert.Equal(strict.MaxPublishBytes, publishEx.Limit);
            Assert.Equal(overLimit.Length, publishEx.Requested);
            await SdkHelpers.AssertBytesEqualAsync(atLimit, strictVolume.Files.ReadAllBytesAsync("/publish.bin", Ct), "rejected WriteAll leaves the old contents");
            await vol.Files.WriteAllBytesAsync("/publish.bin", overLimit, Ct); // another volume's stricter limit does not apply here
            await SdkHelpers.AssertBytesEqualAsync(overLimit, vol.Files.ReadAllBytesAsync("/publish.bin", Ct));

            DfsStream big = await strictVolume.Files.OpenAsync("/publish.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
            try
            {
                await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(async () =>
                {
                    await big.WriteAsync(overLimit, Ct);
                    await big.FlushAsync(Ct);
                }, DfsErrorCode.LimitExceeded);
            }
            finally
            {
                await Record.ExceptionAsync(() => big.AbortAsync(Ct));
                await Record.ExceptionAsync(() => big.DisposeAsync().AsTask());
            }

            await SdkHelpers.AssertBytesEqualAsync(atLimit, strictVolume.Files.ReadAllBytesAsync("/publish.bin", Ct), "rejected flush leaves the old contents");

            // 2. File length: the advertised maximum is reachable, one more byte is not.
            DfsStream sparse = await strictVolume.Files.CreateAsync("/length.bin", Ct);
            try
            {
                await sparse.SetLengthAsync(strict.MaxFileLength, Ct);
                await sparse.FlushAsync(Ct);
                Assert.Equal(strict.MaxFileLength, (await strictVolume.Files.GetInfoAsync("/length.bin", Ct)).Length);
                DfsLimitExceededException lengthEx = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(async () =>
                {
                    await sparse.SetLengthAsync(strict.MaxFileLength + 1, Ct);
                    await sparse.FlushAsync(Ct);
                }, DfsErrorCode.LimitExceeded);
                Assert.Equal(strict.MaxFileLength, lengthEx.Limit);
            }
            finally
            {
                await Record.ExceptionAsync(() => sparse.AbortAsync(Ct));
                await Record.ExceptionAsync(() => sparse.DisposeAsync().AsTask());
            }

            Assert.Equal(strict.MaxFileLength, (await strictVolume.Files.GetInfoAsync("/length.bin", Ct)).Length);

            // 3. Handles per session.
            await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetVolumeStateAsync("strict")).Handles.Count(h => !h.Expired) == 0,
                TimeSpan.FromSeconds(5), "earlier handles of the strict volume are closed", Ct);
            var handles = new List<DfsStream>();
            try
            {
                for (int i = 0; i < strict.MaxHandlesPerSession; i++)
                    handles.Add(await strictVolume.Files.OpenAsync("/publish.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct));
                DfsLimitExceededException handleEx = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(async () =>
                {
                    await using DfsStream extra = await strictVolume.Files.OpenAsync("/publish.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
                }, DfsErrorCode.LimitExceeded);
                Assert.Equal(strict.MaxHandlesPerSession, handleEx.Limit);
                await handles[0].DisposeAsync();
                handles.RemoveAt(0);
                handles.Add(await strictVolume.Files.OpenAsync("/publish.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct));
                for (int i = 0; i <= strict.MaxHandlesPerSession; i++)
                    handles.Add(await vol.Files.OpenAsync("/publish.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct));
            }
            finally
            {
                foreach (DfsStream h in handles)
                    await h.DisposeAsync();
            }

            // 4. Transaction touched inodes: at the limit commits, above it nothing is applied.
            foreach (DfsVolume v in new[] { vol, strictVolume })
            {
                await v.Directories.CreateDirectoryAsync("/tx", Ct);
                for (int i = 0; i < 5; i++)
                    await v.Files.WriteAllBytesAsync($"/tx/f{i}", Encoding.UTF8.GetBytes($"before-{i}"), Ct);
            }

            // Within the limit under any counting of "touched" inodes (2 written files; with the root and /tx read for
            // path resolution still 4). The limit-sized commit is not used as the accepted boundary because the FSD does
            // not say whether path-resolution reads count as touched.
            const int WithinLimit = 2;
            await using (DfsTransaction ok = await strictVolume.Transactions.BeginAsync(null, Ct))
            {
                for (int i = 0; i < WithinLimit; i++)
                    await ok.Files.WriteAllBytesAsync($"/tx/f{i}", Encoding.UTF8.GetBytes($"after-{i}"), Ct);
                Assert.False((await ok.CommitAsync(Ct)).IsNone);
            }

            DfsTransaction tooBig = await strictVolume.Transactions.BeginAsync(null, Ct);
            try
            {
                DfsLimitExceededException txEx = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(async () =>
                {
                    for (int i = 0; i < 5; i++)
                        await tooBig.Files.WriteAllBytesAsync($"/tx/f{i}", Encoding.UTF8.GetBytes($"third-{i}"), Ct);
                    await tooBig.CommitAsync(Ct);
                }, DfsErrorCode.LimitExceeded);
                Assert.Equal(strict.MaxTransactionTouchedInodes, txEx.Limit);
            }
            finally
            {
                await Record.ExceptionAsync(() => tooBig.AbortAsync(Ct));
                await tooBig.DisposeAsync();
            }

            for (int i = 0; i < 5; i++)
                Assert.Equal(i < WithinLimit ? $"after-{i}" : $"before-{i}", await strictVolume.Files.ReadAllTextAsync($"/tx/f{i}", null, Ct));

            await using (DfsTransaction wide = await vol.Transactions.BeginAsync(null, Ct))
            {
                for (int i = 0; i < 5; i++)
                    await wide.Files.WriteAllBytesAsync($"/tx/f{i}", Encoding.UTF8.GetBytes($"wide-{i}"), Ct);
                await wide.CommitAsync(Ct);
            }

            for (int i = 0; i < 5; i++)
                Assert.Equal($"wide-{i}", await vol.Files.ReadAllTextAsync($"/tx/f{i}", null, Ct));
        }, Ct);
    }

    // =====================================================================================================
    // SDK-016 SecretsAreNotLogged
    // =====================================================================================================

    [Fact]
    [Catalog("SDK-016", "U", "INV-12")]
    public async Task SDK_016_SecretsAreNotLogged()
    {
        const string Principal = "sdk016user";
        const string UserSecret = "S3cr3t-SDK016-7f3a9c41e2";
        const string WrongSecret = "Wr0ng-SDK016-b2e8d07c55";
        const string Marker = "PAYLOAD-SDK016-c94e1a77";
        byte[] payload = [.. Encoding.UTF8.GetBytes(Marker + new string('x', 48)), .. TestData.Bytes(1601, 4096)];
        var diagnostics = new RecordingDiagnostics();
        var thrown = new List<Exception>();
        string adminSecret = string.Empty;

        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            adminSecret = cluster.AdminCredentials.Secret;
            await cluster.Admin.CreatePrincipalAsync(Principal, UserSecret, false, Ct);
            await cluster.Admin.GrantAsync(Volume, Principal, DfsPermissions.Read, Ct);
            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "sdk016admin", Diagnostics = diagnostics }, Ct);
            DfsFileClient adminFiles = admin.GetVolume(Volume).Files;
            await adminFiles.WriteAllBytesAsync("/existing.bin", payload, Ct);

            // Authentication failure (wrong secret).
            Exception auth = await SdkHelpers.CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions
            {
                Name = "sdk016wrong",
                Credentials = new DfsCredentials(Principal, WrongSecret),
                Diagnostics = diagnostics,
            }, Ct), "connect with a wrong secret");
            Assert.IsAssignableFrom<DfsAuthenticationException>(SdkHelpers.Unwrap(auth));
            thrown.Add(auth);

            // Connection failure (the only seed is unreachable).
            cluster.Network.CutBoth("client:sdk016cut", "n0");
            Exception connect = await SdkHelpers.CaptureAsync(() => cluster.ConnectAsync(new TestClientOptions
            {
                Name = "sdk016cut",
                SeedNodes = ["n0"],
                ConnectTimeout = TimeSpan.FromSeconds(2),
                Credentials = new DfsCredentials(Principal, UserSecret),
                Diagnostics = diagnostics,
            }, Ct), "connect to an unreachable seed");
            Exception connectInner = SdkHelpers.Unwrap(connect);
            Assert.True(connectInner is DfsUnavailableException or TimeoutException, $"unexpected connection failure type {connectInner.GetType().Name}");
            thrown.Add(connect);
            cluster.Network.Heal();

            // Authorization failures of operations carrying payloads.
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions
            {
                Name = "sdk016reader",
                Credentials = new DfsCredentials(Principal, UserSecret),
                Diagnostics = diagnostics,
            }, Ct);
            DfsFileClient files = reader.GetVolume(Volume).Files;
            await SdkHelpers.AssertBytesEqualAsync(payload, files.ReadAllBytesAsync("/existing.bin", Ct));
            foreach (Func<Task> denied in new Func<Task>[]
            {
                () => files.WriteAllBytesAsync("/denied.bin", payload, Ct),
                () => files.AppendAllTextAsync("/existing.bin", Marker, null, Ct),
                () => files.WriteAllTextAsync("/denied.txt", Marker + " text", null, Ct),
                async () =>
                {
                    await using DfsStream s = await files.OpenAsync("/existing.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                    await s.WriteAsync(payload, Ct);
                    await s.FlushAsync(Ct);
                },
            })
            {
                Exception ex = await SdkHelpers.CaptureAsync(denied, "write without permission");
                Assert.IsAssignableFrom<UnauthorizedAccessException>(SdkHelpers.Unwrap(ex));
                thrown.Add(ex);
            }

            await SdkHelpers.AssertBytesEqualAsync(payload, adminFiles.ReadAllBytesAsync("/existing.bin", Ct));

            // Serialization failure of a payload: text the requested encoding cannot represent.
            Encoding strictAscii = Encoding.GetEncoding("us-ascii", EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback);
            Exception encode = await SdkHelpers.CaptureAsync(
                () => adminFiles.WriteAllTextAsync("/encoding.txt", Marker + " \u00E6\u00F8\u00E5", strictAscii, Ct), "unencodable text");
            Assert.IsAssignableFrom<ArgumentException>(SdkHelpers.Unwrap(encode));
            Assert.False(await adminFiles.ExistsAsync("/encoding.txt", Ct));
            thrown.Add(encode);
        }, Ct);

        string[] forbidden =
        [
            UserSecret, WrongSecret, adminSecret, Marker,
            Convert.ToHexString(payload, 0, 24), Convert.ToHexString(payload, 0, 24).ToLowerInvariant(), Convert.ToBase64String(payload, 0, 24),
            Convert.ToHexString(payload, 100, 24), Convert.ToBase64String(payload, 102, 24),
        ];
        Assert.False(string.IsNullOrEmpty(adminSecret));
        IReadOnlyList<DfsDiagnosticEvent> events = diagnostics.Events;
        Assert.NotEmpty(events);
        var leaks = new List<string>();
        foreach (DfsDiagnosticEvent e in events)
        {
            string rendered = $"{e.Name}|{e.Message}|{e.OperationId}|{e.TransactionId}|{e.Volume}|{e.Token}|{(e.Exception is null ? string.Empty : SdkHelpers.Render(e.Exception))}";
            foreach (string secret in forbidden.Where(f => rendered.Contains(f, StringComparison.Ordinal)))
                leaks.Add($"diagnostic event '{e.Name}' contains '{secret[..Math.Min(8, secret.Length)]}\u2026'");
        }

        foreach (Exception ex in thrown)
        {
            string rendered = SdkHelpers.Render(ex);
            foreach (string secret in forbidden.Where(f => rendered.Contains(f, StringComparison.Ordinal)))
                leaks.Add($"{ex.GetType().Name} surfaced to the caller contains '{secret[..Math.Min(8, secret.Length)]}\u2026'");
        }

        SdkHelpers.Log($"SDK-016 inspected {events.Count} diagnostic events and {thrown.Count} exceptions.");
        Assert.True(leaks.Count == 0, string.Join(Environment.NewLine, leaks));
    }
}
