using System.Buffers;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Tests.Unit.Internal.Volumes;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Open;

public sealed class ExportContractTests
{
    private const FileShare All = FileShare.ReadWrite | FileShare.Delete;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ExpiryAfterCheckpointHonorsTheOpenedIdentity(bool replace)
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        var header = new RequestHeader("vol", Session: session);
        var admin = SmDriver.Op(session: session, admin: true);
        d.Ok<ExportControlResultMessage>(new ExportControlCommand(new ExportControlMessage(header, ExportControlKind.Configure,
            Profile: DfsExportProfile.Windows)) { Op = admin });
        var binding = d.Ok<ExportControlResultMessage>(new ExportControlCommand(new ExportControlMessage(header, ExportControlKind.Bind,
            Destination: "/")) { Op = admin });
        var root = new DfsExportRoot(binding.Token, binding.Directory);
        var request = new ExportOpenMessage(new OpenHandleMessage(header, "/Name", FileMode.CreateNew, 0, All, 0, false, false, null),
            root, DfsOpenAccess.Delete, false, true);
        var opened = d.Ok<ExportOpenResultMessage>(new ExportOpenCommand(request, TimeSpan.FromSeconds(2)) { Op = admin });
        if (replace)
        {
            d.Ok<DeleteResultMessage>(new DeleteCommand("/name", false, false, null) { Op = admin });
            ulong native = d.OpenSession(nonce: 42);
            var replacement = d.Open(native, "/name", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            Assert.NotEqual(opened.Open.Info.Inode, replacement.Info.Inode);
        }
        byte[] checkpoint = d.Checkpoint();
        d.Machine.RestoreCheckpoint(new ReadOnlySequence<byte>(checkpoint), d.Index, 1);
        d.Apply(new ExpireSessionsCommand([session]));
        Assert.Equal(replace, d.State.Root.Children.ContainsKey("NAME"));
        Assert.Null(d.State.Find(opened.Open.Info.Inode.Value));
    }

    [Fact]
    public Task MultipleClientsShareWritesAndFollowDirectoryIdentity() =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic }, async cluster =>
        {
            await using var a = await cluster.ConnectAsync(new TestClientOptions { Name = "gateway-a" });
            await using var b = await cluster.ConnectAsync(new TestClientOptions { Name = "gateway-b" });
            var volume = a.GetVolume("vol");
            var native = b.GetVolume("vol");
            await Assert.ThrowsAsync<NotSupportedException>(() => volume.ConfigureExportsAsync(DfsExportProfile.Windows));
            await a.Admin.SetFeatureAsync(DfsExportRoot.ClusterFeature, true);
            await volume.Directories.CreateDirectoryAsync("/share");
            var root = await volume.BindExportAsync("/share");
            var options = new DfsOpenOptions { ExportRoot = root, Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = All };
            await Assert.ThrowsAsync<NotSupportedException>(() => volume.Files.OpenAsync("/data", options));
            await volume.ConfigureExportsAsync(DfsExportProfile.Windows);
            await volume.Directories.MoveAsync("/share", "/mapped");
            await volume.Directories.CreateDirectoryAsync("/share");
            await volume.Directories.CreateDirectoryAsync("/mapped/private");
            await volume.BindExportAsync("/mapped/private");
            await Assert.ThrowsAsync<DfsCrossVolumeException>(() => native.Files.OpenAsync("/private/data", options));
            await using var source = await native.Files.OpenAsync("/source", options with { DesiredAccess = DfsOpenAccess.Delete });
            await Assert.ThrowsAsync<DfsCrossVolumeException>(() => source.RenameAsync("/private/target"));
            await volume.Files.MoveAsync("/mapped/source", "/mapped/private/source");
            await Assert.ThrowsAsync<DfsCrossVolumeException>(() => source.RenameAsync("/returned"));
            await using var first = await volume.Files.OpenAsync("/Data", options);
            await using var second = await native.Files.OpenAsync("/DATA", options);
            Assert.Equal(DfsCreateResult.Created, first.CreateResult);
            Assert.Equal(DfsCreateResult.Opened, second.CreateResult);
            Assert.Equal(first.InodeId, second.InodeId);
            second.Position = 8;
            await Task.WhenAll(first.WriteAsync(new byte[] { 1, 2 }).AsTask(), second.WriteAsync(new byte[] { 3, 4 }).AsTask());
            Assert.Equal(10, await first.GetLengthAsync());
            first.Position = 8;
            var read = new byte[2];
            Assert.Equal(2, await first.ReadAsync(read));
            Assert.Equal(new byte[] { 3, 4 }, read);
            Assert.Equal(DfsWriteMode.WriteThrough, first.WriteMode);
            await second.SetLengthAsync(1);
            first.Position = 1;
            await first.WriteAsync(new byte[] { 2 });
            await using var append = await native.Files.OpenAsync("/data", options with { Mode = FileMode.Append, Access = FileAccess.Write });
            await append.WriteAsync(new byte[] { 3 });
            await first.FlushAsync();
            Assert.Equal(new byte[] { 1, 2, 3 }, await native.Files.ReadAllBytesAsync("/mapped/Data"));
            Assert.False(await native.Files.ExistsAsync("/share/Data"));
            await first.SetLengthAsync(20_000);
            second.Position = 10_000;
            await second.WriteAsync(new byte[8_192]);
            Assert.Equal(20_000, await second.GetLengthAsync());
            await using var secondAppend = await volume.Files.OpenAsync("/DATA", options with { Mode = FileMode.Append, Access = FileAccess.Write });
            await Task.WhenAll(append.WriteAsync(new byte[] { 42 }).AsTask(), secondAppend.WriteAsync(new byte[] { 43 }).AsTask());
            Assert.Equal(20_002, await first.GetLengthAsync());
            first.Position = 20_000;
            Assert.Equal(2, await first.ReadAsync(read));
            Assert.Equal(new byte[] { 42, 43 }, read.Order().ToArray());
        });

    [Fact]
    public Task DeletePendingNamesAndMetadataAreInodeBound() =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic }, async cluster =>
        {
            await using var client = await cluster.ConnectAsync(new TestClientOptions());
            await client.Admin.SetFeatureAsync(DfsExportRoot.ClusterFeature, true);
            var volume = client.GetVolume("vol");
            await volume.Files.WriteAllBytesAsync("/A", new byte[] { 1 });
            await volume.Files.WriteAllBytesAsync("/a", new byte[] { 2 });
            await Assert.ThrowsAsync<DfsAlreadyExistsException>(() => volume.ConfigureExportsAsync(DfsExportProfile.Windows));
            await volume.Files.DeleteAsync("/a");
            await volume.ConfigureExportsAsync(DfsExportProfile.Windows);
            var root = await volume.BindExportAsync("/");
            var options = new DfsOpenOptions { ExportRoot = root, DesiredAccess = DfsOpenAccess.Delete, Share = All };
            await using var handle = await volume.Files.OpenAsync("/a", options);
            await Assert.ThrowsAsync<DfsSharingViolationException>(() => volume.Files.OpenReadAsync("/A"));
            await using var metadata = await volume.Files.OpenAsync("/A", options with { DesiredAccess = DfsOpenAccess.None });
            var time = DateTimeOffset.UnixEpoch.AddHours(1);
            await metadata.SetBasicInformationAsync(new DfsBasicInformation(AccessedUtc: time, Attributes: FileAttributes.Hidden));
            Assert.Equal(time, (await handle.GetInfoAsync()).BasicInformation!.AccessedUtc);
            await volume.Snapshots.CreateAsync("metadata");
            await using (var snapshot = await volume.Snapshots.OpenAsync("metadata"))
            {
                Assert.Equal(FileAttributes.Hidden, (await snapshot.Files.GetInfoAsync("/a")).BasicInformation!.Attributes);
                await Assert.ThrowsAsync<NotSupportedException>(() => snapshot.Files.OpenAsync("/a", options));
            }
            await Assert.ThrowsAsync<DfsAlreadyExistsException>(() => volume.Files.OpenAsync("/a", FileMode.CreateNew, FileAccess.Write, All & ~FileShare.Write));
            await handle.RenameAsync("/b");
            await volume.Files.WriteAllBytesAsync("/A", new byte[] { 3 });
            await handle.SetDeletePendingAsync(true);
            await handle.SetDeletePendingAsync(false);
            await handle.SetDeletePendingAsync(true);
            await handle.DisposeAsync();
            var leader = await cluster.GetLeaderAsync("vol");
            await cluster.TransferLeadershipAsync("vol", leader.Name == "n0" ? "n1" : "n0");
            await Assert.ThrowsAsync<DfsSharingViolationException>(() => volume.Files.OpenAsync("/B", options));
            await metadata.DisposeAsync();
            Assert.False(await volume.Files.ExistsAsync("/b"));
            Assert.Equal(new byte[] { 3 }, await volume.Files.ReadAllBytesAsync("/a"));
            await using var dir = await volume.Files.OpenAsync("/dir", options with { Mode = FileMode.CreateNew, Directory = true });
            Assert.Equal(DfsCreateResult.Created, dir.CreateResult);
            await dir.SetDeletePendingAsync(true);
            await Assert.ThrowsAsync<DfsSharingViolationException>(() => volume.Files.WriteAllBytesAsync("/DIR/child", new byte[] { 1 }));
            await dir.DisposeAsync();
            Assert.False(await volume.Directories.ExistsAsync("/dir"));
        });
}
