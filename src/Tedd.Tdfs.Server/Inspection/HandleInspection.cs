namespace Tedd.Tdfs.Server;

/// <summary>An open handle.</summary>
/// <param name="Id">Handle id.</param>
/// <param name="Inode">Opened inode.</param>
/// <param name="Session">Owning session id.</param>
/// <param name="Access">Requested access.</param>
/// <param name="Share">Granted sharing.</param>
/// <param name="IsWriter">True for a writable handle.</param>
/// <param name="Generation">Writer generation bound to the handle (0 for readers).</param>
/// <param name="Expired">True when the lease expired.</param>
public sealed record HandleInspection(HandleId Id, InodeId Inode, ulong Session, FileAccess Access, FileShare Share, bool IsWriter, ulong Generation, bool Expired);
