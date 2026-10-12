namespace Tedd.Tdfs.Storage;

/// <summary>How <see cref="IStorageMedium.OpenAsync"/> treats an existing or absent file.</summary>
public enum StorageOpenMode
{
    /// <summary>Create a new file; fails with <see cref="IOException"/> when the file already exists.</summary>
    CreateNew,

    /// <summary>Open an existing file; fails with <see cref="FileNotFoundException"/> when it is absent.</summary>
    OpenExisting,

    /// <summary>Open the file when it exists, otherwise create it.</summary>
    OpenOrCreate,

    /// <summary>Create the file when absent; truncate it to zero length when it exists (O_CREAT | O_TRUNC).</summary>
    Truncate,
}
