using System.Security.AccessControl;
using R = Tedd.Tdfs.DfsFileSystemRights;

// The .NET access-control enums are annotated Windows-only, but reading enum metadata (names and values) works on every
// platform; the cross-checks below only do that.
#pragma warning disable CA1416

namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>Rights and flag values equal the NTFS / .NET access-control values (Addendum A, ACL-02, ACL-002).</summary>
public sealed class DfsFileSystemRightsTests
{
    public static TheoryData<R, int> ExactValues => new()
    {
        { R.ReadData, 0x1 }, { R.ListDirectory, 0x1 }, { R.WriteData, 0x2 }, { R.CreateFiles, 0x2 },
        { R.AppendData, 0x4 }, { R.CreateDirectories, 0x4 }, { R.ReadExtendedAttributes, 0x8 },
        { R.WriteExtendedAttributes, 0x10 }, { R.ExecuteFile, 0x20 }, { R.Traverse, 0x20 },
        { R.DeleteSubdirectoriesAndFiles, 0x40 }, { R.ReadAttributes, 0x80 }, { R.WriteAttributes, 0x100 },
        { R.Delete, 0x10000 }, { R.ReadPermissions, 0x20000 }, { R.ChangePermissions, 0x40000 },
        { R.TakeOwnership, 0x80000 }, { R.Synchronize, 0x100000 },
        { R.Read, 0x20089 }, { R.Write, 0x116 }, { R.ReadAndExecute, 0x200A9 }, { R.Modify, 0x301BF },
        { R.FullControl, 0x1F01FF },
    };

    [Theory]
    [MemberData(nameof(ExactValues))]
    public void ValuesAreTheNtfsAccessMaskBits(R right, int expected)
    {
        Assert.Equal(expected, (int)right);
    }

    [Fact]
    public void CompositesAreTheDocumentedUnions()
    {
        Assert.Equal(R.ReadData | R.ReadExtendedAttributes | R.ReadAttributes | R.ReadPermissions, R.Read);
        Assert.Equal(R.WriteData | R.AppendData | R.WriteExtendedAttributes | R.WriteAttributes, R.Write);
        Assert.Equal(R.Read | R.ExecuteFile, R.ReadAndExecute);
        Assert.Equal(R.ReadAndExecute | R.Write | R.Delete, R.Modify);
        Assert.Equal(R.Modify | R.DeleteSubdirectoriesAndFiles | R.ChangePermissions | R.TakeOwnership | R.Synchronize, R.FullControl);
        // As in .NET, only FullControl includes Synchronize.
        Assert.Equal((R)0, (R.Read | R.Write | R.ReadAndExecute | R.Modify) & R.Synchronize);
    }

    [Fact]
    public void FullControlIsTheUnionOfEverySingleBit()
    {
        R union = 0;
        foreach (R value in Enum.GetValues<R>())
        {
            int v = (int)value;
            if (v != 0 && (v & (v - 1)) == 0)
                union |= value;
        }

        Assert.Equal(R.FullControl, union);
    }

    [Fact]
    public void RightsEqualSystemSecurityAccessControlFileSystemRights()
    {
        string[] ours = Enum.GetNames<R>().Order(StringComparer.Ordinal).ToArray();
        string[] theirs = Enum.GetNames<FileSystemRights>().Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(theirs, ours);
        foreach (string name in theirs)
            Assert.Equal((int)Enum.Parse<FileSystemRights>(name), (int)Enum.Parse<R>(name));
    }

    [Fact]
    public void FlagsAndTypeEqualSystemSecurityAccessControl()
    {
        AssertSameEnum<DfsInheritanceFlags, InheritanceFlags>();
        AssertSameEnum<DfsPropagationFlags, PropagationFlags>();
        AssertSameEnum<DfsAccessControlType, AccessControlType>();
    }

    private static void AssertSameEnum<TOurs, TTheirs>()
        where TOurs : struct, Enum
        where TTheirs : struct, Enum
    {
        string[] ours = Enum.GetNames<TOurs>().Order(StringComparer.Ordinal).ToArray();
        string[] theirs = Enum.GetNames<TTheirs>().Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(theirs, ours);
        foreach (string name in theirs)
            Assert.Equal(Convert.ToInt64(Enum.Parse<TTheirs>(name)), Convert.ToInt64(Enum.Parse<TOurs>(name)));
    }
}
