using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Recursive removal of a node-local directory (for example the data of a group this node no longer hosts).</summary>
internal static class StorageCleanup
{
    /// <summary>Deletes <paramref name="directory"/> and everything below it (best effort; missing paths are ignored).</summary>
    public static async Task DeleteTreeAsync(IStorageMedium medium, string directory, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        string dir = StoragePath.Normalize(directory);
        if (dir.Length == 0 || !await medium.ExistsAsync(dir, ct))
            return;
        await DeleteChildrenAsync(medium, dir, ct);
        await medium.DeleteAsync(dir, ct);
        await medium.FlushDirectoryAsync(StoragePath.GetParent(dir), ct);
    }

    private static async Task DeleteChildrenAsync(IStorageMedium medium, string dir, CancellationToken ct)
    {
        IReadOnlyList<string> names;
        try
        {
            names = await medium.ListAsync(dir, ct);
        }
        catch (DirectoryNotFoundException)
        {
            return;
        }

        foreach (string name in names)
        {
            string path = StoragePath.Combine(dir, name);
            if (await IsDirectoryAsync(medium, path, ct))
            {
                await DeleteChildrenAsync(medium, path, ct);
                await medium.DeleteAsync(path, ct);
            }
            else
            {
                await medium.DeleteAsync(path, ct);
            }
        }

        await medium.FlushDirectoryAsync(dir, ct);
    }

    private static async Task<bool> IsDirectoryAsync(IStorageMedium medium, string path, CancellationToken ct)
    {
        try
        {
            await medium.ListAsync(path, ct);
            return true;
        }
        catch (Exception ex) when (ex is DirectoryNotFoundException or IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }
}
