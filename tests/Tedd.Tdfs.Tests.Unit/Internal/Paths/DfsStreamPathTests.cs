namespace Tedd.Tdfs.Tests.Unit.Internal.Paths;

/// <summary><see cref="DfsStreamPath"/> helpers (Addendum A, ADS-01, ADS-02).</summary>
public sealed class DfsStreamPathTests
{
    [Fact]
    public void Constants()
    {
        Assert.Equal("$DATA", DfsStreamPath.DataType);
        Assert.Equal(1024, DfsStreamPath.MaxStreamsPerInode);
        Assert.Equal(0, DfsStreamPath.DefaultStreamName.Length);
        Assert.Equal(':', DfsPath.StreamSeparator);
    }

    [Theory]
    [InlineData("/a/f:s", "/a/f", "s")]
    [InlineData("/f:s", "/f", "s")]
    [InlineData("/a/f", "/a/f", null)]
    [InlineData("/", "/", null)]
    [InlineData("/d:Zone.Identifier", "/d", "Zone.Identifier")]
    public void SplitSeparatesFileAndStream(string path, string file, string? stream)
    {
        Assert.Equal((file, stream), DfsStreamPath.Split(path));
        Assert.Equal(file, DfsStreamPath.GetFilePath(path));
        Assert.Equal(stream, DfsStreamPath.GetStreamName(path));
        Assert.Equal(stream is not null, DfsStreamPath.HasStream(path));
        Assert.Equal(path, DfsStreamPath.Combine(file, stream));
    }

    [Fact]
    public void SplitRequiresNormalizedPaths()
    {
        foreach (string bad in new[] { "/a/f:s:$DATA", "/a/f::$DATA", "/a/f:", "a/f:s", "/a//f:s" })
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsStreamPath.Split(bad));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
            Assert.Throws<ArgumentException>(() => DfsStreamPath.HasStream(bad));
        }
    }

    [Fact]
    public void CombineBuildsNormalizedStreamPaths()
    {
        Assert.Equal("/a/f:s", DfsStreamPath.Combine("/a/f", "s"));
        Assert.Equal("/a/f", DfsStreamPath.Combine("/a/f", null));
        Assert.Equal("/a/f", DfsStreamPath.Combine("/a/f", ""));
        Assert.Equal("/a/f:\u00E9", DfsStreamPath.Combine("/a/f", "e\u0301"));
        Assert.Equal("/d:s", DfsStreamPath.Combine("/d", "s"));
        Assert.Equal("/", DfsStreamPath.Combine("/", null));

        foreach ((string file, string stream) in new[]
        {
            ("/", "s"), ("/a/f:s", "t"), ("/a/f", "a:b"), ("/a/f", "$x"), ("/a/f", "$DATA"), ("/a/f", "."), ("/a/f", ".."),
            ("/a/f", "x/y"), ("/a/f", "x\\y"), ("/a/f", new string('s', 256)), ("/a/f::$DATA", "s"),
        })
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsStreamPath.Combine(file, stream));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        }
    }

    [Fact]
    public void CombineEnforcesThePathLimit()
    {
        string file = string.Concat(Enumerable.Repeat("/" + new string('d', 200), 20)) + "/f";     // 4022 bytes
        Assert.Equal(file + ":" + new string('s', 73), DfsStreamPath.Combine(file, new string('s', 73)));
        Assert.Throws<ArgumentException>(() => DfsStreamPath.Combine(file, new string('s', 74)));
    }

    [Fact]
    public void StreamNameNormalization()
    {
        Assert.Equal("\u00E9", DfsStreamPath.NormalizeStreamName("e\u0301"));
        Assert.Equal("Zone.Identifier", DfsStreamPath.NormalizeStreamName("Zone.Identifier"));
        Assert.True(DfsStreamPath.TryNormalizeStreamName("s", out string normalized, out string? error));
        Assert.Equal("s", normalized);
        Assert.Null(error);

        foreach (string? bad in new[] { null, "", "a:b", "$x", ".", "..", "a/b", "a\\b", "a\0b", new string('x', 256) })
        {
            Assert.False(DfsStreamPath.TryNormalizeStreamName(bad, out string n, out string? e), bad);
            Assert.Equal(string.Empty, n);
            Assert.NotNull(e);
            if (bad is not null)
                Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(Assert.Throws<ArgumentException>(() => DfsStreamPath.NormalizeStreamName(bad))));
        }

        Assert.Throws<ArgumentNullException>(() => DfsStreamPath.NormalizeStreamName(null!));
    }

    [Fact]
    public void RoundTripOverManyNormalizedPaths()
    {
        string[] files = ["/a", "/a/b", "/\u00E9/\u00F8", "/x y/z"];
        string?[] streams = [null, "s", "Zone.Identifier", "\u00E5", "a b", "s$"];
        foreach (string file in files)
        {
            foreach (string? stream in streams)
            {
                string path = DfsStreamPath.Combine(file, stream);
                Assert.True(DfsPath.IsNormalized(path));
                Assert.Equal((file, stream), DfsStreamPath.Split(path));
                Assert.Equal(path, DfsPath.Normalize(path + (stream is null ? "::$DATA" : ":$DATA")));
                Assert.Equal(DfsPath.GetParent(file), DfsPath.GetParent(path));
            }
        }
    }
}
