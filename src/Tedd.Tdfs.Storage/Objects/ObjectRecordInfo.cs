namespace Tedd.Tdfs.Storage;

/// <summary>Index entry of an object held by an <see cref="ObjectStore"/>.</summary>
/// <param name="Id">Object id (immutable: same id always means the same bytes).</param>
/// <param name="Volume">Owning volume.</param>
/// <param name="Staging">Staging session the object was uploaded under (GC protection).</param>
/// <param name="Length">Plaintext length in bytes.</param>
/// <param name="Checksum">XxHash128 of the plaintext.</param>
/// <param name="Encoding">Stored encoding (0 = plain; other values identify encryption/compression formats).</param>
/// <param name="KeyVersion">Data-key version used for encryption at rest (0 when not encrypted).</param>
public sealed record ObjectRecordInfo(ObjectId Id, VolumeId Volume, StagingId Staging, long Length, UInt128 Checksum, byte Encoding, uint KeyVersion);
