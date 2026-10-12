using System.Runtime.InteropServices;

namespace Tedd.Tdfs.Storage;

/// <summary>Best-effort directory fsync on Unix-like systems (persists entry creation, rename and removal).</summary>
internal static partial class DirectorySync
{
    private const int ORdOnly = 0;

    /// <summary>Opens the directory read-only and fsyncs it. Missing libc or an open failure is ignored; an fsync failure throws.</summary>
    /// <exception cref="IOException">fsync reported an error.</exception>
    public static void TryFsyncDirectory(string fullPath)
    {
        if (OperatingSystem.IsWindows())
            return;

        int fd;
        try
        {
            fd = Open(fullPath, ORdOnly);
        }
        catch (DllNotFoundException)
        {
            return;
        }
        catch (EntryPointNotFoundException)
        {
            return;
        }

        if (fd < 0)
            return;

        try
        {
            if (Fsync(fd) != 0)
            {
                int errno = Marshal.GetLastPInvokeError();
                throw new IOException($"fsync of directory '{fullPath}' failed (errno {errno}).", errno);
            }
        }
        finally
        {
            _ = Close(fd);
        }
    }

    [LibraryImport("libc", EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int Open(string path, int flags);

    [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static partial int Fsync(int fd);

    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    private static partial int Close(int fd);
}
