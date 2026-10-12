namespace Tedd.Tdfs;

/// <summary>
/// Access rights of an access control entry (Addendum A, ACL-02). Values are the NTFS access mask bits and equal
/// <c>System.Security.AccessControl.FileSystemRights</c> exactly, including the composites (which, as in .NET, do not
/// include <see cref="Synchronize"/> except <see cref="FullControl"/>).
/// </summary>
/// <remarks>
/// Directory and file meanings share bits: <see cref="ListDirectory"/> = <see cref="ReadData"/>,
/// <see cref="CreateFiles"/> = <see cref="WriteData"/>, <see cref="CreateDirectories"/> = <see cref="AppendData"/>,
/// <see cref="Traverse"/> = <see cref="ExecuteFile"/>. <see cref="Synchronize"/> carries no meaning in TDFS: access
/// decisions ignore it (see <see cref="DfsAccessEvaluator"/>).
/// </remarks>
[Flags]
public enum DfsFileSystemRights
{
    /// <summary>Read file data (files). 0x1.</summary>
    ReadData = 0x1,
    /// <summary>List directory entries (directories). 0x1.</summary>
    ListDirectory = 0x1,
    /// <summary>Write file data (files). 0x2.</summary>
    WriteData = 0x2,
    /// <summary>Create files in a directory (directories). 0x2.</summary>
    CreateFiles = 0x2,
    /// <summary>Append data to a file (files). 0x4.</summary>
    AppendData = 0x4,
    /// <summary>Create subdirectories (directories). 0x4.</summary>
    CreateDirectories = 0x4,
    /// <summary>Read extended attributes. 0x8.</summary>
    ReadExtendedAttributes = 0x8,
    /// <summary>Write extended attributes. 0x10.</summary>
    WriteExtendedAttributes = 0x10,
    /// <summary>Execute a file (files). 0x20.</summary>
    ExecuteFile = 0x20,
    /// <summary>Traverse a directory (directories). 0x20.</summary>
    Traverse = 0x20,
    /// <summary>Delete children of a directory regardless of their own <see cref="Delete"/> right. 0x40.</summary>
    DeleteSubdirectoriesAndFiles = 0x40,
    /// <summary>Read basic attributes (length, times, kind). 0x80.</summary>
    ReadAttributes = 0x80,
    /// <summary>Write basic attributes. 0x100.</summary>
    WriteAttributes = 0x100,
    /// <summary>Delete the object. 0x10000.</summary>
    Delete = 0x10000,
    /// <summary>Read the security descriptor. 0x20000.</summary>
    ReadPermissions = 0x20000,
    /// <summary>Change the discretionary ACL. 0x40000.</summary>
    ChangePermissions = 0x40000,
    /// <summary>Take ownership. 0x80000.</summary>
    TakeOwnership = 0x80000,
    /// <summary>NTFS synchronize bit; accepted for compatibility and ignored by access decisions. 0x100000.</summary>
    Synchronize = 0x100000,

    /// <summary><see cref="ReadData"/> | <see cref="ReadExtendedAttributes"/> | <see cref="ReadAttributes"/> | <see cref="ReadPermissions"/> = 0x20089.</summary>
    Read = ReadData | ReadExtendedAttributes | ReadAttributes | ReadPermissions,
    /// <summary><see cref="WriteData"/> | <see cref="AppendData"/> | <see cref="WriteExtendedAttributes"/> | <see cref="WriteAttributes"/> = 0x116.</summary>
    Write = WriteData | AppendData | WriteExtendedAttributes | WriteAttributes,
    /// <summary><see cref="Read"/> | <see cref="ExecuteFile"/> = 0x200A9.</summary>
    ReadAndExecute = Read | ExecuteFile,
    /// <summary><see cref="ReadAndExecute"/> | <see cref="Write"/> | <see cref="Delete"/> = 0x301BF.</summary>
    Modify = ReadAndExecute | Write | Delete,
    /// <summary>
    /// <see cref="Modify"/> | <see cref="DeleteSubdirectoriesAndFiles"/> | <see cref="ChangePermissions"/> |
    /// <see cref="TakeOwnership"/> | <see cref="Synchronize"/> = 0x1F01FF (every defined bit).
    /// </summary>
    FullControl = Modify | DeleteSubdirectoriesAndFiles | ChangePermissions | TakeOwnership | Synchronize,
}
