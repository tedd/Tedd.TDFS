namespace Tedd.Tdfs.Storage;

/// <summary>Classification and construction of local storage errors shared by all media.</summary>
public static class StorageErrors
{
    /// <summary>HRESULT of a disk-full error (HRESULT_FROM_WIN32(ERROR_DISK_FULL = 0x70)).</summary>
    public const int DiskFullHResult = unchecked((int)0x80070070);

    /// <summary>HRESULT of a write-protected (read-only) device error (HRESULT_FROM_WIN32(ERROR_WRITE_PROTECT = 0x13)).</summary>
    public const int ReadOnlyHResult = unchecked((int)0x80070013);

    /// <summary>HRESULT of a file-exists error (HRESULT_FROM_WIN32(ERROR_FILE_EXISTS = 0x50)).</summary>
    public const int FileExistsHResult = unchecked((int)0x80070050);

    /// <summary>HRESULT of a generic device I/O error (HRESULT_FROM_WIN32(ERROR_IO_DEVICE = 0x45D)).</summary>
    public const int IoDeviceHResult = unchecked((int)0x8007045D);

    private const int ErrorDiskFull = 0x70;
    private const int ErrorHandleDiskFull = 0x27;
    private const int ErrorWriteProtect = 0x13;
    private const int UnixEnospc = 28;
    private const int UnixEdquot = 122;
    private const int UnixErofs = 30;

    /// <summary>True when <paramref name="ex"/> (or an inner exception) reports that the device is full (ENOSPC / ERROR_DISK_FULL).</summary>
    public static bool IsDiskFull(Exception? ex)
    {
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            if (e is IOException)
            {
                int code = e.HResult & 0xFFFF;
                if (e.HResult == DiskFullHResult || (IsWin32(e.HResult) && (code == ErrorDiskFull || code == ErrorHandleDiskFull)))
                    return true;
                if (e.HResult == UnixEnospc || e.HResult == UnixEdquot)
                    return true;
            }
        }

        return false;
    }

    /// <summary>True when <paramref name="ex"/> (or an inner exception) reports a read-only device or denied write access.</summary>
    public static bool IsReadOnly(Exception? ex)
    {
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            if (e is UnauthorizedAccessException)
                return true;
            if (e is IOException && (e.HResult == ReadOnlyHResult || (IsWin32(e.HResult) && (e.HResult & 0xFFFF) == ErrorWriteProtect) || e.HResult == UnixErofs))
                return true;
        }

        return false;
    }

    /// <summary>Creates the disk-full exception thrown by the media in this assembly.</summary>
    public static IOException DiskFull(string path) => new($"disk full: no space left on device while writing '{path}'.", DiskFullHResult);

    /// <summary>Creates the read-only-device exception thrown by the media in this assembly.</summary>
    public static IOException ReadOnlyDevice(string path) => new($"read-only file system: cannot modify '{path}'.", ReadOnlyHResult);

    /// <summary>Creates a generic device I/O error (EIO).</summary>
    public static IOException DeviceError(string operation, string path) => new($"I/O error during {operation} of '{path}'.", IoDeviceHResult);

    /// <summary>Creates a file-exists error.</summary>
    public static IOException FileExists(string path) => new($"The file '{path}' already exists.", FileExistsHResult);

    private static bool IsWin32(int hresult) => (hresult & unchecked((int)0xFFFF0000)) == unchecked((int)0x80070000);
}
