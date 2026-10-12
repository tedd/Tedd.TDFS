using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Smoke;

/// <summary>Placeholder checks that the unit test project builds, references the harness and runs.</summary>
public sealed class SmokeTests
{
    [Fact]
    public void DfsPath_Normalize_KeepsCanonicalPath()
    {
        Assert.Equal("/a/b", DfsPath.Normalize("/a/b"));
    }

    [Fact]
    public void TestCatalog_ContainsEveryNamedCase()
    {
        Assert.Equal(586, TestCatalog.All.Count);
        Assert.Equal(TestCatalog.ExpectedCount, TestCatalog.All.Count);
    }
}
