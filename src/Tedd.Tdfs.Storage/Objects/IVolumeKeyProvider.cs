namespace Tedd.Tdfs.Storage;

/// <summary>
/// Per-volume data-encryption keys of an <see cref="ObjectStore"/> (ARCHITECTURE §11): object payloads are sealed with a
/// key version of their owning volume (<see cref="ObjectEncoding.SealedVolumeUnits"/>) and decrypted with the version
/// recorded in their record header.
/// </summary>
/// <remarks>Called synchronously by the store (also under its write gate); implementations must not block.</remarks>
public interface IVolumeKeyProvider
{
    /// <summary>
    /// The cipher holding the key versions of <paramref name="volume"/> known to this node (its
    /// <see cref="IRecordCipher.CurrentKeyVersion"/> is the newest), or null when no key material of the volume is available.
    /// </summary>
    IRecordCipher? GetCipher(VolumeId volume);
}
