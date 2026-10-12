using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

public abstract class SuperblockContractTests : StorageContractTests
{
    protected static readonly Guid Cluster = new("11111111-2222-4333-8444-555555555555");
    protected static readonly NodeId Node = new(new Guid("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"));

    [Fact]
    public async Task LoadAbsent_ReturnsNull()
    {
        Assert.Null(await Superblock.LoadAsync(Medium, "node/superblock"));
    }

    [Fact]
    public async Task CreateLoadBump_PersistIdentityAndIncarnation()
    {
        Superblock created = await Superblock.CreateAsync(Medium, "node/superblock", Cluster, Node, "node-1");
        Assert.Equal(0ul, created.Incarnation);
        Assert.Equal(Superblock.CurrentFormatVersion, created.FormatVersion);

        Superblock bumped = await Superblock.BumpIncarnationAsync(Medium, "node/superblock");
        Assert.Equal(1ul, bumped.Incarnation);
        bumped = await Superblock.BumpIncarnationAsync(Medium, "node/superblock");
        Assert.Equal(2ul, bumped.Incarnation);

        Superblock? loaded = await Superblock.LoadAsync(Host.Restart(), "node/superblock");
        Assert.Equal(bumped, loaded);
        Assert.Equal(Cluster, loaded!.ClusterId);
        Assert.Equal(Node, loaded.NodeId);
        Assert.Equal("node-1", loaded.NodeName);
    }

    [Fact]
    public async Task CreateTwice_Throws()
    {
        await Superblock.CreateAsync(Medium, "sb", Cluster, Node, "n");
        await Assert.ThrowsAnyAsync<IOException>(async () => await Superblock.CreateAsync(Medium, "sb", Cluster, Node, "n"));
    }

    [Fact]
    public async Task CreateValidatesArguments()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await Superblock.CreateAsync(Medium, "sb", Guid.Empty, Node, "n"));
        await Assert.ThrowsAsync<ArgumentException>(async () => await Superblock.CreateAsync(Medium, "sb", Cluster, default, "n"));
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await Superblock.CreateAsync(Medium, "sb", Cluster, Node, " "));
    }

    [Fact]
    public async Task BumpWithoutSuperblock_ThrowsFileNotFound()
    {
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await Superblock.BumpIncarnationAsync(Medium, "missing"));
    }

    [Fact]
    public async Task UnicodeNodeName_RoundTrips()
    {
        await Superblock.CreateAsync(Medium, "sb", Cluster, Node, "nœud-東京-🚀");
        Assert.Equal("nœud-東京-🚀", (await Superblock.LoadAsync(Medium, "sb"))!.NodeName);
    }

    [Fact]
    public async Task RaiseMinSoftwareVersion_CannotExceedThisSoftware()
    {
        await Superblock.CreateAsync(Medium, "sb", Cluster, Node, "n");
        Superblock same = await Superblock.RaiseMinSoftwareVersionAsync(Medium, "sb", Superblock.SoftwareVersion);
        Assert.Equal(Superblock.SoftwareVersion, same.MinSoftwareVersion);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await Superblock.RaiseMinSoftwareVersionAsync(Medium, "sb", Superblock.SoftwareVersion + 1));
    }

    [Fact]
    public async Task NewerFormatOrSoftwareRequirement_IsRefused()
    {
        var newer = new Superblock(Superblock.CurrentFormatVersion + 1, Cluster, Node, "n", 3, 1);
        await AtomicFile.WriteAsync(Medium, "sb", newer.Encode());
        DfsProtocolException ex = await Assert.ThrowsAsync<DfsProtocolException>(async () => await Superblock.LoadAsync(Medium, "sb"));
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);

        var downgradeBlocked = new Superblock(Superblock.CurrentFormatVersion, Cluster, Node, "n", 3, Superblock.SoftwareVersion + 1);
        await AtomicFile.WriteAsync(Medium, "sb", downgradeBlocked.Encode());
        ex = await Assert.ThrowsAsync<DfsProtocolException>(async () => await Superblock.LoadAsync(Medium, "sb"));
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);
    }

    [Fact]
    public async Task MalformedPayload_ThrowsIntegrity()
    {
        await AtomicFile.WriteAsync(Medium, "sb", new byte[] { 1, 2, 3 });
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Superblock.LoadAsync(Medium, "sb"));
        byte[] valid = new Superblock(1, Cluster, Node, "n", 1, 1).Encode();
        await AtomicFile.WriteAsync(Medium, "sb", (byte[])[.. valid, 0x55]);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Superblock.LoadAsync(Medium, "sb"));
    }

    [Fact]
    public async Task CorruptFile_ThrowsIntegrity()
    {
        await Superblock.CreateAsync(Medium, "sb", Cluster, Node, "n");
        Host.Corrupt("sb", 30);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Superblock.LoadAsync(Medium, "sb"));
    }
}

public sealed class SuperblockSimulatedTests : SuperblockContractTests
{
    protected override MediumHost CreateHost() => new SimulatedHost();

    [Fact]
    public async Task CrashMatrix_BumpYieldsOldOrNewIncarnation()
    {
        await CrashMatrix.RunAsync(
            async medium =>
            {
                await Superblock.CreateAsync(medium, "n/sb", Cluster, Node, "node");
                await Superblock.BumpIncarnationAsync(medium, "n/sb");
            },
            async medium => await Superblock.BumpIncarnationAsync(medium, "n/sb"),
            async (medium, crash) =>
            {
                Superblock? sb = await Superblock.LoadAsync(medium, "n/sb");
                Assert.NotNull(sb);
                if (crash.Completed)
                    Assert.Equal(2ul, sb.Incarnation);
                else
                    Assert.InRange(sb.Incarnation, 1ul, 2ul);
                Assert.Equal(Node, sb.NodeId);
                return sb.Incarnation.ToString(System.Globalization.CultureInfo.InvariantCulture);
            });
    }

    [Fact]
    public async Task CrashMatrix_CreateYieldsAbsentOrComplete()
    {
        await CrashMatrix.RunAsync(
            _ => Task.CompletedTask,
            async medium => await Superblock.CreateAsync(medium, "data/node/superblock", Cluster, Node, "node"),
            async (medium, crash) =>
            {
                Superblock? sb = await Superblock.LoadAsync(medium, "data/node/superblock");
                if (crash.Completed)
                    Assert.NotNull(sb);
                if (sb is not null)
                    Assert.Equal(new Superblock(1, Cluster, Node, "node", 0, 1), sb);
                return sb is null ? "absent" : "present";
            });
    }
}

public sealed class SuperblockFileSystemTests : SuperblockContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
