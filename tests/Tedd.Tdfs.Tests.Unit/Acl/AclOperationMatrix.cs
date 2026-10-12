using System.Text;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Acl.AclHelpers;

namespace Tedd.Tdfs.Tests.Unit.Acl;

/// <summary>Every SDK operation of the ACL-05 table exercised by ACL-015.</summary>
public enum MatrixOp
{
    OpenRead, LiveRead, PinnedRead, ReadAllBytes, ReadAllText, ReadAllLines, ReadLines,
    OpenWrite, OpenReadWrite, TruncateOpen, WriteAllBytesExisting, WriteAllTextExisting, SetLength,
    AppendOpen, AppendAllBytes, AppendAllText,
    CreateNewOpen, WriteAllBytesNew, CreateDirectory,
    DeleteFile, DeleteDirectory,
    MoveFile, RenameFile, MoveDirectory, MoveFileOverwrite,
    Enumerate, ListPage,
    FileGetInfo, FileExists, DirectoryGetInfo, DirectoryExists,
    GetAccessControl, DirectoryGetAccessControl,
    SetAccessControl, DirectorySetAccessControl,
    SetOwnerSelf, DirectorySetOwnerSelf, SetOwnerOther,
    CopyFile,
    StreamReadAll, StreamOpenRead, StreamWriteAll, StreamOpenWrite, StreamSetLength, StreamAppend, StreamGetInfo, StreamExists, StreamDelete,
}

/// <summary>Nodes of a matrix case tree (relative to its case root R).</summary>
internal enum MNode
{
    /// <summary>R/src (directory).</summary>
    Src,

    /// <summary>R/src/f.bin (6000-byte file with named stream "s").</summary>
    F,

    /// <summary>R/src/t.txt (text file).</summary>
    T,

    /// <summary>R/src/d (empty directory).</summary>
    D,

    /// <summary>R/dst (directory).</summary>
    Dst,

    /// <summary>R/dst/g.bin (file overwritten by MoveFileOverwrite).</summary>
    G,
}

/// <summary>An explicit, non-inheritable Allow entry for the case principal on one node.</summary>
internal readonly record struct MGrant(MNode Node, DfsFileSystemRights Rights);

/// <summary>An additional explicit entry installed on one node of every tree of a case (exact and lacking alike).</summary>
internal readonly record struct MExtra(MNode Node, DfsAccessRule Rule);

/// <summary>
/// One ACL-015 row: the operation, the ACL-05 table rows it needs (all must be allowed), the grant sets that hold
/// exactly the required rights (alternatives of an OR are separate sets), and grant sets lacking the required right
/// while holding every other right. <paramref name="Extra"/> entries are installed on every tree of the case.
/// </summary>
internal sealed record MatrixCase(MatrixOp Op, AclTableRow[] Rows, MGrant[][] Exact, MGrant[][] Lacking, bool ExactIsAdministrator = false,
    MExtra[]? Extra = null)
{
    /// <summary>The extra entries (never null).</summary>
    public MExtra[] Extras => Extra ?? [];
}

/// <summary>Definitions, execution and effect checks of the ACL-015 operation matrix.</summary>
internal static class AclOperationMatrix
{
    public const DfsFileSystemRights RD = DfsFileSystemRights.ReadData;
    public const DfsFileSystemRights LD = DfsFileSystemRights.ListDirectory;
    public const DfsFileSystemRights WD = DfsFileSystemRights.WriteData;
    public const DfsFileSystemRights CF = DfsFileSystemRights.CreateFiles;
    public const DfsFileSystemRights AD = DfsFileSystemRights.AppendData;
    public const DfsFileSystemRights CD = DfsFileSystemRights.CreateDirectories;
    public const DfsFileSystemRights RA = DfsFileSystemRights.ReadAttributes;
    public const DfsFileSystemRights DE = DfsFileSystemRights.Delete;
    public const DfsFileSystemRights DC = DfsFileSystemRights.DeleteSubdirectoriesAndFiles;
    public const DfsFileSystemRights RP = DfsFileSystemRights.ReadPermissions;
    public const DfsFileSystemRights CP = DfsFileSystemRights.ChangePermissions;
    public const DfsFileSystemRights TO = DfsFileSystemRights.TakeOwnership;

    /// <summary>Principal used as the new owner by SetOwnerOther and as the extra ACE identity of SetAccessControl.</summary>
    public const string Bystander = "bystander";

    public static readonly byte[] ContentA = TestData.Bytes(1501, 6000);
    public static readonly byte[] StreamS = TestData.Bytes(1502, 5000);
    public static readonly byte[] ContentB = TestData.Bytes(1503, 3000);
    public static readonly byte[] NewData = TestData.Bytes(1504, 2500);
    public static readonly byte[] Suffix = TestData.Bytes(1505, 700);
    public const string Text = "alpha\nbeta\ngamma\n";

    private static MGrant[] On(params (MNode Node, DfsFileSystemRights Rights)[] grants) => grants.Select(g => new MGrant(g.Node, g.Rights)).ToArray();

    private static DfsFileSystemRights All(DfsFileSystemRights without) => FC & ~without;

    private static MatrixCase Single(MatrixOp op, AclTableRow row, MNode node, DfsFileSystemRights right, bool fullParent = true) =>
        new(op, [row], [On((node, right))], [fullParent ? On((node, All(right)), (MNode.Src, FC)) : On((node, All(right)))]);

    private static MatrixCase ReadCase(MatrixOp op, MNode node) => Single(op, AclTableRow.Read, node, RD);

    private static MatrixCase WriteCase(MatrixOp op, MNode node) => Single(op, AclTableRow.Write, node, WD);

    private static MatrixCase AppendCase(MatrixOp op, MNode node) =>
        new(op, [AclTableRow.Append], [On((node, AD)), On((node, WD))], [On((node, All(AD | WD)), (MNode.Src, FC))]);

    private static MatrixCase InfoCase(MatrixOp op, MNode node) =>
        new(op, [AclTableRow.GetInfo], [On((node, RA)), On((MNode.Src, LD))], [On((node, All(RA)), (MNode.Src, All(LD)))]);

    private static MatrixCase DeleteCase(MatrixOp op, MNode node) =>
        new(op, [AclTableRow.Delete], [On((node, DE)), On((MNode.Src, DC))], [On((node, All(DE)), (MNode.Src, All(DC)))]);

    /// <summary>Every ACL-015 case.</summary>
    public static IReadOnlyList<MatrixCase> Cases { get; } =
    [
        ReadCase(MatrixOp.OpenRead, MNode.F),
        ReadCase(MatrixOp.LiveRead, MNode.F),
        ReadCase(MatrixOp.PinnedRead, MNode.F),
        ReadCase(MatrixOp.ReadAllBytes, MNode.F),
        ReadCase(MatrixOp.ReadAllText, MNode.T),
        ReadCase(MatrixOp.ReadAllLines, MNode.T),
        ReadCase(MatrixOp.ReadLines, MNode.T),
        WriteCase(MatrixOp.OpenWrite, MNode.F),
        new(MatrixOp.OpenReadWrite, [AclTableRow.Read, AclTableRow.Write], [On((MNode.F, RD | WD))],
            [On((MNode.F, All(RD)), (MNode.Src, FC)), On((MNode.F, All(WD)), (MNode.Src, FC))]),
        WriteCase(MatrixOp.TruncateOpen, MNode.F),
        WriteCase(MatrixOp.WriteAllBytesExisting, MNode.F),
        WriteCase(MatrixOp.WriteAllTextExisting, MNode.T),
        WriteCase(MatrixOp.SetLength, MNode.F),
        AppendCase(MatrixOp.AppendOpen, MNode.F),
        AppendCase(MatrixOp.AppendAllBytes, MNode.F),
        AppendCase(MatrixOp.AppendAllText, MNode.T),
        Single(MatrixOp.CreateNewOpen, AclTableRow.CreateFile, MNode.Src, CF, fullParent: false),
        Single(MatrixOp.WriteAllBytesNew, AclTableRow.CreateFile, MNode.Src, CF, fullParent: false),
        Single(MatrixOp.CreateDirectory, AclTableRow.CreateDirectory, MNode.Src, CD, fullParent: false),
        DeleteCase(MatrixOp.DeleteFile, MNode.F),
        DeleteCase(MatrixOp.DeleteDirectory, MNode.D),
        new(MatrixOp.MoveFile, [AclTableRow.Move],
            [On((MNode.F, DE), (MNode.Dst, CF)), On((MNode.Src, DC), (MNode.Dst, CF))],
            [On((MNode.F, All(DE)), (MNode.Src, All(DC)), (MNode.Dst, FC)), On((MNode.F, FC), (MNode.Src, FC), (MNode.Dst, All(CF)))]),
        new(MatrixOp.RenameFile, [AclTableRow.Move],
            [On((MNode.F, DE), (MNode.Src, CF)), On((MNode.Src, DC | CF))],
            [On((MNode.F, All(DE)), (MNode.Src, All(DC))), On((MNode.F, FC), (MNode.Src, All(CF)))]),
        new(MatrixOp.MoveDirectory, [AclTableRow.Move],
            [On((MNode.D, DE), (MNode.Dst, CD)), On((MNode.Src, DC), (MNode.Dst, CD))],
            [On((MNode.D, All(DE)), (MNode.Src, All(DC)), (MNode.Dst, FC)), On((MNode.D, FC), (MNode.Src, FC), (MNode.Dst, All(CD)))]),
        new(MatrixOp.MoveFileOverwrite, [AclTableRow.Move],
            [On((MNode.F, DE), (MNode.Dst, CF), (MNode.G, DE)), On((MNode.F, DE), (MNode.Dst, CF | DC))],
            [
                On((MNode.F, DE), (MNode.Dst, All(DC)), (MNode.G, All(DE))),
                On((MNode.F, All(DE)), (MNode.Src, All(DC)), (MNode.Dst, FC), (MNode.G, FC)),
                On((MNode.F, FC), (MNode.Src, FC), (MNode.Dst, All(CF)), (MNode.G, FC)),
            ]),
        Single(MatrixOp.Enumerate, AclTableRow.Enumerate, MNode.Src, LD, fullParent: false),
        Single(MatrixOp.ListPage, AclTableRow.Enumerate, MNode.Src, LD, fullParent: false),
        InfoCase(MatrixOp.FileGetInfo, MNode.F),
        InfoCase(MatrixOp.FileExists, MNode.F),
        InfoCase(MatrixOp.DirectoryGetInfo, MNode.D),
        InfoCase(MatrixOp.DirectoryExists, MNode.D),
        Single(MatrixOp.GetAccessControl, AclTableRow.GetAccessControl, MNode.F, RP),
        Single(MatrixOp.DirectoryGetAccessControl, AclTableRow.GetAccessControl, MNode.D, RP),
        Single(MatrixOp.SetAccessControl, AclTableRow.SetAccessControl, MNode.F, CP),
        Single(MatrixOp.DirectorySetAccessControl, AclTableRow.SetAccessControl, MNode.D, CP),
        Single(MatrixOp.SetOwnerSelf, AclTableRow.SetOwnerSelf, MNode.F, TO),
        Single(MatrixOp.DirectorySetOwnerSelf, AclTableRow.SetOwnerSelf, MNode.D, TO),
        // The administrator gets nothing from the file's own entries here: an explicit Deny of TakeOwnership to
        // Administrators on the file shows that the success comes from the Administrators rule (any owner), not the ACL.
        new(MatrixOp.SetOwnerOther, [AclTableRow.SetOwnerOther], [On()], [On((MNode.F, FC), (MNode.Src, FC))], ExactIsAdministrator: true,
            Extra: [new MExtra(MNode.F, Deny(DfsIdentity.Administrators, TO))]),
        new(MatrixOp.CopyFile, [AclTableRow.Copy],
            [On((MNode.F, RD), (MNode.Dst, CF))],
            [On((MNode.F, All(RD)), (MNode.Src, FC), (MNode.Dst, FC)), On((MNode.F, FC), (MNode.Src, FC), (MNode.Dst, All(CF)))]),
        ReadCase(MatrixOp.StreamReadAll, MNode.F),
        ReadCase(MatrixOp.StreamOpenRead, MNode.F),
        WriteCase(MatrixOp.StreamWriteAll, MNode.F),
        WriteCase(MatrixOp.StreamOpenWrite, MNode.F),
        WriteCase(MatrixOp.StreamSetLength, MNode.F),
        AppendCase(MatrixOp.StreamAppend, MNode.F),
        InfoCase(MatrixOp.StreamGetInfo, MNode.F),
        InfoCase(MatrixOp.StreamExists, MNode.F),
        DeleteCase(MatrixOp.StreamDelete, MNode.F),
    ];

    public static MatrixCase Get(MatrixOp op) => Cases.Single(c => c.Op == op);

    /// <summary>
    /// The ACL-05 table rows (operation cell text exactly as in the addendum) and the matrix operations that exercise
    /// each of them. ACL-015 fails if the specification gains a row that is not mapped here.
    /// </summary>
    public static IReadOnlyDictionary<string, MatrixOp[]> TableCoverage { get; } = new Dictionary<string, MatrixOp[]>(StringComparer.Ordinal)
    {
        ["Open for read / ReadAll / live read"] =
            [MatrixOp.OpenRead, MatrixOp.ReadAllBytes, MatrixOp.ReadAllText, MatrixOp.ReadAllLines, MatrixOp.ReadLines, MatrixOp.LiveRead, MatrixOp.PinnedRead, MatrixOp.OpenReadWrite],
        ["Open for write, truncate, WriteAll over existing, SetLength"] =
            [MatrixOp.OpenWrite, MatrixOp.OpenReadWrite, MatrixOp.TruncateOpen, MatrixOp.WriteAllBytesExisting, MatrixOp.WriteAllTextExisting, MatrixOp.SetLength],
        ["Append mode / Append helper"] = [MatrixOp.AppendOpen, MatrixOp.AppendAllBytes, MatrixOp.AppendAllText],
        ["Create file / directory"] = [MatrixOp.CreateNewOpen, MatrixOp.WriteAllBytesNew, MatrixOp.CreateDirectory],
        ["Delete file or directory"] = [MatrixOp.DeleteFile, MatrixOp.DeleteDirectory],
        ["Rename/move"] = [MatrixOp.MoveFile, MatrixOp.RenameFile, MatrixOp.MoveDirectory, MatrixOp.MoveFileOverwrite],
        ["Enumerate"] = [MatrixOp.Enumerate, MatrixOp.ListPage],
        ["GetInfo / Exists"] = [MatrixOp.FileGetInfo, MatrixOp.FileExists, MatrixOp.DirectoryGetInfo, MatrixOp.DirectoryExists],
        ["GetAccessControl"] = [MatrixOp.GetAccessControl, MatrixOp.DirectoryGetAccessControl],
        ["SetAccessControl"] = [MatrixOp.SetAccessControl, MatrixOp.DirectorySetAccessControl],
        ["SetOwner"] = [MatrixOp.SetOwnerSelf, MatrixOp.DirectorySetOwnerSelf, MatrixOp.SetOwnerOther],
        ["Copy"] = [MatrixOp.CopyFile],
        ["Named stream operations"] =
            [
                MatrixOp.StreamReadAll, MatrixOp.StreamOpenRead, MatrixOp.StreamWriteAll, MatrixOp.StreamOpenWrite, MatrixOp.StreamSetLength,
                MatrixOp.StreamAppend, MatrixOp.StreamGetInfo, MatrixOp.StreamExists, MatrixOp.StreamDelete,
            ],
    };

    /// <summary>The operation cells of the ACL-05 table, parsed from the embedded addendum.</summary>
    public static IReadOnlyList<string> SpecTableOperations()
    {
        using Stream? s = typeof(TestCatalog).Assembly.GetManifestResourceStream("fsd-addendum-acl-pools-streams.md");
        Assert.NotNull(s);
        using var reader = new StreamReader(s, Encoding.UTF8);
        string[] lines = reader.ReadToEnd().Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
        int header = Array.FindIndex(lines, l => l.StartsWith("| Operation | Required |", StringComparison.Ordinal));
        Assert.True(header >= 0, "ACL-05 table header not found in the addendum");
        var ops = new List<string>();
        for (int i = header + 2; i < lines.Length && lines[i].StartsWith('|'); i++)
            ops.Add(lines[i].Split('|', StringSplitOptions.TrimEntries)[1]);
        return ops;
    }

    // ------------------------------------------------------------------------------------------------------------
    // Case tree
    // ------------------------------------------------------------------------------------------------------------

    public static string PathOf(string root, MNode node) => node switch
    {
        MNode.Src => root + "/src",
        MNode.F => root + "/src/f.bin",
        MNode.T => root + "/src/t.txt",
        MNode.D => root + "/src/d",
        MNode.Dst => root + "/dst",
        MNode.G => root + "/dst/g.bin",
        _ => throw new ArgumentOutOfRangeException(nameof(node)),
    };

    /// <summary>The primary path the operation acts on (a stream path for stream operations).</summary>
    public static string TargetOf(MatrixOp op, string root) => op switch
    {
        MatrixOp.ReadAllText or MatrixOp.ReadAllLines or MatrixOp.ReadLines or MatrixOp.WriteAllTextExisting or MatrixOp.AppendAllText => PathOf(root, MNode.T),
        MatrixOp.CreateNewOpen or MatrixOp.WriteAllBytesNew => root + "/src/new.bin",
        MatrixOp.CreateDirectory => root + "/src/newdir",
        MatrixOp.DeleteDirectory or MatrixOp.MoveDirectory or MatrixOp.DirectoryGetInfo or MatrixOp.DirectoryExists
            or MatrixOp.DirectoryGetAccessControl or MatrixOp.DirectorySetAccessControl or MatrixOp.DirectorySetOwnerSelf => PathOf(root, MNode.D),
        MatrixOp.Enumerate or MatrixOp.ListPage => PathOf(root, MNode.Src),
        MatrixOp.StreamReadAll or MatrixOp.StreamOpenRead or MatrixOp.StreamWriteAll or MatrixOp.StreamOpenWrite or MatrixOp.StreamSetLength
            or MatrixOp.StreamAppend or MatrixOp.StreamGetInfo or MatrixOp.StreamExists or MatrixOp.StreamDelete => PathOf(root, MNode.F) + ":s",
        _ => PathOf(root, MNode.F),
    };

    /// <summary>The destination path of move/copy operations, otherwise null.</summary>
    public static string? DestinationOf(MatrixOp op, string root) => op switch
    {
        MatrixOp.MoveFile => root + "/dst/moved.bin",
        MatrixOp.RenameFile => root + "/src/renamed.bin",
        MatrixOp.MoveDirectory => root + "/dst/moved",
        MatrixOp.MoveFileOverwrite => PathOf(root, MNode.G),
        MatrixOp.CopyFile => root + "/dst/copy.bin",
        _ => null,
    };

    /// <summary>
    /// Builds the case tree under <paramref name="root"/> (protected: Administrators only), grants the principal and
    /// installs the case's extra entries.
    /// </summary>
    public static async Task BuildAsync(AclWorld w, string root, string principal, MGrant[] grants, MExtra[] extras)
    {
        await w.MkdirAsync(root);
        await w.ProtectAsync(root);
        await w.MkdirAsync(PathOf(root, MNode.Src));
        await w.MkfileAsync(PathOf(root, MNode.F), ContentA);
        await w.Admin.Files.WriteAllBytesAsync(PathOf(root, MNode.F) + ":s", StreamS, Ct);
        await w.MkfileAsync(PathOf(root, MNode.T), Encoding.UTF8.GetBytes(Text));
        await w.MkdirAsync(PathOf(root, MNode.D));
        await w.MkdirAsync(PathOf(root, MNode.Dst));
        await w.MkfileAsync(PathOf(root, MNode.G), ContentB);
        foreach ((MNode node, DfsAccessRule[] rules) in RulesByNode(principal, grants, extras))
            await w.SetAclAsync(PathOf(root, node), false, rules);
    }

    /// <summary>The explicit entries of each granted node: one Allow for the principal (union of its grants) plus extras.</summary>
    public static IEnumerable<(MNode Node, DfsAccessRule[] Rules)> RulesByNode(string principal, MGrant[] grants, MExtra[] extras)
    {
        foreach (MNode node in grants.Select(g => g.Node).Concat(extras.Select(e => e.Node)).Distinct().Order())
        {
            var rules = new List<DfsAccessRule>();
            MGrant[] mine = grants.Where(g => g.Node == node).ToArray();
            if (mine.Length > 0)
                rules.Add(Allow(U(principal), mine.Aggregate((DfsFileSystemRights)0, (acc, g) => acc | g.Rights)));
            rules.AddRange(extras.Where(e => e.Node == node).Select(e => e.Rule));
            yield return (node, rules.ToArray());
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Reads a handle to its end with plain reads (no Length/Position queries, which Stream.CopyToAsync makes).</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream s)
    {
        using var ms = new MemoryStream();
        byte[] buffer = new byte[16 * 1024];
        int n;
        while ((n = await s.ReadAsync(buffer, Ct)) > 0)
            ms.Write(buffer, 0, n);
        return ms.ToArray();
    }

    /// <summary>Performs the operation as the principal of <paramref name="v"/>; returns data read (when any).</summary>
    public static async Task<object?> PerformAsync(MatrixOp op, DfsVolume v, string root, string principal)
    {
        string target = TargetOf(op, root);
        string? dest = DestinationOf(op, root);
        switch (op)
        {
            case MatrixOp.OpenRead:
            case MatrixOp.StreamOpenRead:
            {
                await using DfsStream s = await v.Files.OpenReadAsync(target, Ct);
                return await ReadToEndAsync(s);
            }

            case MatrixOp.LiveRead:
            case MatrixOp.PinnedRead:
            {
                await using DfsStream s = await v.Files.OpenAsync(target, new DfsOpenOptions
                {
                    Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read,
                    ReadView = op == MatrixOp.LiveRead ? DfsReadView.Live : DfsReadView.VersionPinned,
                }, Ct);
                return await ReadToEndAsync(s);
            }

            case MatrixOp.ReadAllBytes:
            case MatrixOp.StreamReadAll:
                return await v.Files.ReadAllBytesAsync(target, Ct);
            case MatrixOp.ReadAllText:
                return await v.Files.ReadAllTextAsync(target, null, Ct);
            case MatrixOp.ReadAllLines:
                return await v.Files.ReadAllLinesAsync(target, null, Ct);
            case MatrixOp.ReadLines:
                return await ToListAsync(v.Files.ReadLinesAsync(target, null, Ct));
            case MatrixOp.OpenWrite:
            {
                await using (DfsStream s = await v.Files.OpenWriteAsync(target, Ct))
                    await s.WriteAsync(NewData.AsMemory(0, 100), Ct);
                return null;
            }

            case MatrixOp.StreamOpenWrite:
            {
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
                    await s.WriteAsync(NewData.AsMemory(0, 100), Ct);
                return null;
            }

            case MatrixOp.OpenReadWrite:
            {
                byte[] head = new byte[10];
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct))
                {
                    await s.ReadExactlyAsync(head, Ct);
                    s.Seek(0, SeekOrigin.Begin);
                    await s.WriteAsync(NewData.AsMemory(0, 100), Ct);
                }

                return head;
            }

            case MatrixOp.TruncateOpen:
            {
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.Truncate, FileAccess.Write, FileShare.None, Ct))
                    await s.WriteAsync(NewData.AsMemory(0, 50), Ct);
                return null;
            }

            case MatrixOp.WriteAllBytesExisting:
            case MatrixOp.WriteAllBytesNew:
            case MatrixOp.StreamWriteAll:
                return await v.Files.WriteAllBytesAsync(target, NewData, Ct);
            case MatrixOp.WriteAllTextExisting:
                return await v.Files.WriteAllTextAsync(target, "omega\n", null, Ct);
            case MatrixOp.SetLength:
            case MatrixOp.StreamSetLength:
            {
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
                    await s.SetLengthAsync(1234, Ct);
                return null;
            }

            case MatrixOp.AppendOpen:
            {
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.Append, FileAccess.Write, FileShare.Read, Ct))
                    await s.WriteAsync(Suffix, Ct);
                return null;
            }

            case MatrixOp.AppendAllBytes:
            case MatrixOp.StreamAppend:
                return await v.Files.AppendAllBytesAsync(target, Suffix, Ct);
            case MatrixOp.AppendAllText:
                return await v.Files.AppendAllTextAsync(target, "delta\n", null, Ct);
            case MatrixOp.CreateNewOpen:
            {
                await using (DfsStream s = await v.Files.OpenAsync(target, FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct))
                    await s.WriteAsync(NewData, Ct);
                return null;
            }

            case MatrixOp.CreateDirectory:
                return await v.Directories.CreateDirectoryAsync(target, Ct);
            case MatrixOp.DeleteFile:
            case MatrixOp.StreamDelete:
                return await v.Files.DeleteAsync(target, Ct);
            case MatrixOp.DeleteDirectory:
                return await v.Directories.DeleteAsync(target, false, Ct);
            case MatrixOp.MoveFile:
            case MatrixOp.RenameFile:
                return await v.Files.MoveAsync(target, dest!, false, Ct);
            case MatrixOp.MoveFileOverwrite:
                return await v.Files.MoveAsync(target, dest!, true, Ct);
            case MatrixOp.MoveDirectory:
                return await v.Directories.MoveAsync(target, dest!, Ct);
            case MatrixOp.Enumerate:
                return await ToListAsync(v.Directories.EnumerateAsync(target, null, Ct));
            case MatrixOp.ListPage:
                return await v.Directories.ListPageAsync(target, null, null, Ct);
            case MatrixOp.FileGetInfo:
            case MatrixOp.StreamGetInfo:
                return await v.Files.GetInfoAsync(target, Ct);
            case MatrixOp.FileExists:
            case MatrixOp.StreamExists:
                return await v.Files.ExistsAsync(target, Ct);
            case MatrixOp.DirectoryGetInfo:
                return await v.Directories.GetInfoAsync(target, Ct);
            case MatrixOp.DirectoryExists:
                return await v.Directories.ExistsAsync(target, Ct);
            case MatrixOp.GetAccessControl:
                return await v.Files.GetAccessControlAsync(target, Ct);
            case MatrixOp.DirectoryGetAccessControl:
                return await v.Directories.GetAccessControlAsync(target, Ct);
            case MatrixOp.SetAccessControl:
                return await v.Files.SetAccessControlAsync(target, NewDescriptor(principal), Ct);
            case MatrixOp.DirectorySetAccessControl:
                return await v.Directories.SetAccessControlAsync(target, NewDescriptor(principal), Ct);
            case MatrixOp.SetOwnerSelf:
                return await v.Files.SetOwnerAsync(target, U(principal), Ct);
            case MatrixOp.DirectorySetOwnerSelf:
                return await v.Directories.SetOwnerAsync(target, U(principal), Ct);
            case MatrixOp.SetOwnerOther:
                return await v.Files.SetOwnerAsync(target, U(Bystander), Ct);
            case MatrixOp.CopyFile:
                return await v.Files.CopyAsync(target, dest!, false, Ct);
            default:
                throw new ArgumentOutOfRangeException(nameof(op));
        }
    }

    /// <summary>The explicit entries SetAccessControl installs (the caller keeps ChangePermissions, the bystander gets Read).</summary>
    public static DfsAccessRule[] NewRules(string principal) => [Allow(U(principal), CP), Allow(U(Bystander), DfsFileSystemRights.Read)];

    private static DfsSecurityDescriptor NewDescriptor(string principal) => new(AdminId, NewRules(principal));

    /// <summary>
    /// Asserts the effect of a successful operation (bytes, identities, owners, entries) and applies it to the reference.
    /// <paramref name="before"/> holds the inode ids of the case tree captured before the operation.
    /// </summary>
    public static async Task AssertEffectAndApplyAsync(MatrixOp op, AclWorld w, string root, string principal, object? result,
        IReadOnlyDictionary<string, InodeId> before)
    {
        DfsVolume admin = w.Admin;
        string target = TargetOf(op, root);
        string? dest = DestinationOf(op, root);
        string fPath = PathOf(root, MNode.F);
        string because = $"{op} by {principal}";
        switch (op)
        {
            case MatrixOp.OpenRead:
            case MatrixOp.LiveRead:
            case MatrixOp.PinnedRead:
            case MatrixOp.ReadAllBytes:
                AssertEx.BytesEqual(ContentA, (byte[])result!, because);
                break;
            case MatrixOp.StreamOpenRead:
            case MatrixOp.StreamReadAll:
                AssertEx.BytesEqual(StreamS, (byte[])result!, because);
                break;
            case MatrixOp.ReadAllText:
                Assert.Equal(Text, (string)result!);
                break;
            case MatrixOp.ReadAllLines:
                Assert.Equal(new[] { "alpha", "beta", "gamma" }, (string[])result!);
                break;
            case MatrixOp.ReadLines:
                Assert.Equal(new[] { "alpha", "beta", "gamma" }, (List<string>)result!);
                break;
            case MatrixOp.OpenWrite:
            case MatrixOp.OpenReadWrite:
            {
                if (op == MatrixOp.OpenReadWrite)
                    AssertEx.BytesEqual(ContentA.AsSpan(0, 10), (byte[])result!, because);
                byte[] expected = [.. NewData.AsSpan(0, 100), .. ContentA.AsSpan(100)];
                await AssertBytesAsync(expected, admin.Files.ReadAllBytesAsync(target, Ct), because);
                Assert.Equal(before[fPath], (await admin.Files.GetInfoAsync(target, Ct)).InodeId);
                break;
            }

            case MatrixOp.StreamOpenWrite:
            {
                byte[] expected = [.. NewData.AsSpan(0, 100), .. StreamS.AsSpan(100)];
                await AssertBytesAsync(expected, admin.Files.ReadAllBytesAsync(target, Ct), because);
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(fPath, Ct), "default stream unchanged");
                break;
            }

            case MatrixOp.TruncateOpen:
                await AssertBytesAsync(NewData[..50], admin.Files.ReadAllBytesAsync(target, Ct), because);
                break;
            case MatrixOp.WriteAllBytesExisting:
                await AssertBytesAsync(NewData, admin.Files.ReadAllBytesAsync(target, Ct), because);
                Assert.Equal(before[fPath], (await admin.Files.GetInfoAsync(target, Ct)).InodeId);
                break;
            case MatrixOp.StreamWriteAll:
                await AssertBytesAsync(NewData, admin.Files.ReadAllBytesAsync(target, Ct), because);
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(fPath, Ct), "default stream unchanged");
                break;
            case MatrixOp.WriteAllTextExisting:
                Assert.Equal("omega\n", await admin.Files.ReadAllTextAsync(target, null, Ct));
                break;
            case MatrixOp.SetLength:
                await AssertBytesAsync(ContentA[..1234], admin.Files.ReadAllBytesAsync(target, Ct), because);
                break;
            case MatrixOp.StreamSetLength:
                await AssertBytesAsync(StreamS[..1234], admin.Files.ReadAllBytesAsync(target, Ct), because);
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(fPath, Ct), "default stream unchanged");
                break;
            case MatrixOp.AppendOpen:
            case MatrixOp.AppendAllBytes:
                await AssertBytesAsync([.. ContentA, .. Suffix], admin.Files.ReadAllBytesAsync(target, Ct), because);
                break;
            case MatrixOp.StreamAppend:
                await AssertBytesAsync([.. StreamS, .. Suffix], admin.Files.ReadAllBytesAsync(target, Ct), because);
                break;
            case MatrixOp.AppendAllText:
                Assert.Equal(Text + "delta\n", await admin.Files.ReadAllTextAsync(target, null, Ct));
                break;
            case MatrixOp.CreateNewOpen:
            case MatrixOp.WriteAllBytesNew:
                await AssertBytesAsync(NewData, admin.Files.ReadAllBytesAsync(target, Ct), because);
                w.Ref.AddFile(target, U(principal));
                Assert.Equal(U(principal), (await admin.Files.GetAccessControlAsync(target, Ct)).Owner);
                break;
            case MatrixOp.CreateDirectory:
                Assert.True(await admin.Directories.ExistsAsync(target, Ct), because);
                w.Ref.AddDirectory(target, U(principal));
                Assert.Equal(U(principal), (await admin.Directories.GetAccessControlAsync(target, Ct)).Owner);
                break;
            case MatrixOp.DeleteFile:
                Assert.False(await admin.Files.ExistsAsync(target, Ct), because);
                w.Ref.Remove(target);
                break;
            case MatrixOp.DeleteDirectory:
                Assert.False(await admin.Directories.ExistsAsync(target, Ct), because);
                w.Ref.Remove(target);
                break;
            case MatrixOp.StreamDelete:
            {
                // Only the stream goes; the file (same inode, same default stream) stays, so the reference is unchanged.
                Assert.False(await admin.Files.ExistsAsync(target, Ct), because + ": stream still present");
                Assert.Equal(before[fPath], (await admin.Files.GetInfoAsync(fPath, Ct)).InodeId);
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(fPath, Ct), "default stream unchanged");
                Assert.Empty(await admin.Files.ListStreamsAsync(fPath, Ct));
                break;
            }

            case MatrixOp.MoveFile:
            case MatrixOp.RenameFile:
            case MatrixOp.MoveFileOverwrite:
                Assert.False(await admin.Files.ExistsAsync(target, Ct), because + ": source still present");
                Assert.Equal(before[target], (await admin.Files.GetInfoAsync(dest!, Ct)).InodeId);
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(dest!, Ct), because);
                await AssertBytesAsync(StreamS, admin.Files.ReadAllBytesAsync(dest! + ":s", Ct), "streams travel with the inode");
                w.Ref.Move(target, dest!);
                break;
            case MatrixOp.MoveDirectory:
                Assert.False(await admin.Directories.ExistsAsync(target, Ct), because + ": source still present");
                Assert.Equal(before[target], (await admin.Directories.GetInfoAsync(dest!, Ct)).InodeId);
                w.Ref.Move(target, dest!);
                break;
            case MatrixOp.Enumerate:
                Assert.Equal(new[] { "d", "f.bin", "t.txt" }, ((List<DfsEntryInfo>)result!).Select(e => e.Name).Order(StringComparer.Ordinal));
                break;
            case MatrixOp.ListPage:
                Assert.Equal(new[] { "d", "f.bin", "t.txt" }, ((DfsDirectoryPage)result!).Entries.Select(e => e.Name).Order(StringComparer.Ordinal));
                break;
            case MatrixOp.FileGetInfo:
            {
                var info = (DfsFileInfo)result!;
                Assert.Equal(before[target], info.InodeId);
                Assert.Equal(ContentA.Length, info.Length);
                break;
            }

            case MatrixOp.StreamGetInfo:
            {
                var info = (DfsFileInfo)result!;
                Assert.Equal(before[fPath], info.InodeId);
                Assert.True(info.IsStream, because);
                Assert.Equal("s", info.StreamName);
                Assert.Equal(StreamS.Length, info.Length);
                break;
            }

            case MatrixOp.FileExists:
            case MatrixOp.StreamExists:
            case MatrixOp.DirectoryExists:
                Assert.True((bool)result!, because + " returned false for an existing entry");
                break;
            case MatrixOp.DirectoryGetInfo:
                Assert.Equal(before[target], ((DfsDirectoryInfo)result!).InodeId);
                break;
            case MatrixOp.GetAccessControl:
            case MatrixOp.DirectoryGetAccessControl:
            {
                var got = (DfsSecurityDescriptor)result!;
                Assert.Equal(w.Ref.OwnerOf(target), got.Owner);
                Assert.Equal(w.Ref.DescribeAcl(target), NtfsAclReference.Describe(got));
                break;
            }

            case MatrixOp.SetAccessControl:
            case MatrixOp.DirectorySetAccessControl:
            {
                DfsSecurityDescriptor stored = await w.GetAclAsAsync(admin, target);
                w.Ref.SetExplicit(target, NewRules(principal), isProtected: false);
                Assert.Equal(NtfsAclReference.Describe(new DfsSecurityDescriptor(AdminId, NewRules(principal))), NtfsAclReference.Describe(stored.WithoutInheritedRules()));
                Assert.Equal(AdminId, stored.Owner);
                break;
            }

            case MatrixOp.SetOwnerSelf:
            case MatrixOp.DirectorySetOwnerSelf:
                Assert.Equal(U(principal), (await w.GetAclAsAsync(admin, target)).Owner);
                w.Ref.SetOwner(target, U(principal));
                break;
            case MatrixOp.SetOwnerOther:
                Assert.Equal(U(Bystander), (await w.GetAclAsAsync(admin, target)).Owner);
                w.Ref.SetOwner(target, U(Bystander));
                break;
            case MatrixOp.CopyFile:
            {
                await AssertBytesAsync(ContentA, admin.Files.ReadAllBytesAsync(dest!, Ct), because);
                DfsSecurityDescriptor copy = await admin.Files.GetAccessControlAsync(dest!, Ct);
                Assert.Equal(U(principal), copy.Owner);
                Assert.Empty(copy.ExplicitRules);
                Assert.False(copy.AreAccessRulesProtected);
                w.Ref.Copy(target, dest!, U(principal));
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(op));
        }
    }
}
