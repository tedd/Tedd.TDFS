using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>
/// Per-volume data keys of a node's object store (ARCHITECTURE §11): keys come from the control record (wrapped by the KEK),
/// are durable in the node-local key file before a record uses them, and stay available when the record is gone.
/// </summary>
public sealed class VolumeDataKeysTests
{
    private const string Path = "volume-keys";
    private static readonly VolumeId Volume = new(new Guid("0d0d0d0d-0000-4000-8000-000000000004"));
    private static readonly byte[] Kek = TestData.Bytes(80, 32);

    [Fact]
    public async Task KeysFollowTheControlRecord_AndPersistBeforeUse()
    {
        var host = new SimulatedHost();
        var random = new SeededRandom(5);
        VolumeRecord? control = Record(random, 1);
        using (VolumeDataKeys keys = await OpenAsync(host.Medium, () => control))
        {
            Assert.Equal(1u, keys.GetCipher(Volume)!.CurrentKeyVersion);
            Assert.Null(await host.Medium.TryReadAllBytesAsync(Path));   // nothing sealed yet: nothing persisted
            Assert.Equal(1u, await keys.PrepareWriteAsync(control!, 1, null, default));
            Assert.NotNull(await host.Medium.TryReadAllBytesAsync(Path));

            control = Record(random, 2, control);
            Assert.Equal(2u, await keys.PrepareWriteAsync(control, 2, null, default));
            Assert.Equal([1u, 2u], keys.VersionsOf(Volume));
        }

        // The volume's control record is gone (or not loaded yet at start): the key file still opens both versions.
        using VolumeDataKeys reopened = await OpenAsync(host.Medium, () => null);
        IRecordCipher cipher = reopened.GetCipher(Volume)!;
        Assert.True(cipher.CanDecrypt(1) && cipher.CanDecrypt(2));
        Assert.Equal(2u, cipher.CurrentKeyVersion);
    }

    [Fact]
    public async Task AnUnknownRequiredVersion_IsRetryable_AfterCatchUp()
    {
        var host = new SimulatedHost();
        var random = new SeededRandom(6);
        VolumeRecord control = Record(random, 1);
        VolumeRecord rotated = Record(random, 2, control);
        VolumeRecord current = control;
        using VolumeDataKeys keys = await OpenAsync(host.Medium, () => current);
        await Assert.ThrowsAsync<DfsUnavailableException>(() => keys.PrepareWriteAsync(control, 2, _ => Task.CompletedTask, default));
        Assert.Equal(2u, await keys.PrepareWriteAsync(control, 2, _ =>
        {
            current = rotated;   // the control barrier brought the local replica up to date
            return Task.CompletedTask;
        }, default));
    }

    [Fact]
    public async Task WrongKek_OrDamagedFile_LeaveNoKeyMaterial()
    {
        var host = new SimulatedHost();
        var random = new SeededRandom(7);
        VolumeRecord control = Record(random, 1);
        using (VolumeDataKeys keys = await OpenAsync(host.Medium, () => control))
            await keys.PrepareWriteAsync(control, 0, null, default);

        var warnings = new List<string>();
        using (VolumeDataKeys foreign = await VolumeDataKeys.OpenAsync(host.Medium, Path, TestData.Bytes(81, 32), new SeededRandom(1), _ => null, warnings.Add, default))
        {
            Assert.Null(foreign.GetCipher(Volume));
            Assert.NotEmpty(warnings);
        }

        host.Simulated.CorruptByte(Path, 30);
        warnings.Clear();
        using VolumeDataKeys damaged = await VolumeDataKeys.OpenAsync(host.Medium, Path, Kek, new SeededRandom(1), _ => null, warnings.Add, default);
        Assert.Null(damaged.GetCipher(Volume));
        Assert.NotEmpty(warnings);
        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(() => damaged.PrepareWriteAsync(control with { DataKeys = [] }, 0, null, default));
    }

    private static Task<VolumeDataKeys> OpenAsync(IStorageMedium medium, Func<VolumeRecord?> control)
        => VolumeDataKeys.OpenAsync(medium, Path, Kek, new SeededRandom(1), id => id == Volume ? control() : null, _ => { }, default);

    private static VolumeRecord Record(IRandomSource random, uint version, VolumeRecord? previous = null)
    {
        byte[] wrapped = DataKeys.Wrap(Kek, Volume, version, DataKeys.NewDataKey(random), random);
        var key = new DataKeyVersion(version, wrapped, version);
        if (previous is not null)
            return previous with { DataKeys = [.. previous.DataKeys, key] };
        var creation = new VolumeCreation(Volume, "v", 1, "admin", DateTimeOffset.UnixEpoch, 1, new DfsMetadataPolicy(), DfsStoragePolicy.Default,
            new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal), 0, new DfsVolumeLimits(), "default", null, false, []);
        return new VolumeRecord(creation, Tedd.Tdfs.Protocol.Messages.WireVolumeState.Active, [], [], DfsStoragePolicy.Default,
            new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal), null, [key], new SortedDictionary<string, BackupTargetRecord>(StringComparer.Ordinal));
    }
}
