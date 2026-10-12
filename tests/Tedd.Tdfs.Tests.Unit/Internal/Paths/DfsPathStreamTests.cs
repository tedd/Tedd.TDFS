using System.Text;

namespace Tedd.Tdfs.Tests.Unit.Internal.Paths;

/// <summary>Stream syntax in <see cref="DfsPath"/> (Addendum A, ADS-01, ADS-001).</summary>
public sealed class DfsPathStreamTests
{
    public static TheoryData<string, string> Accepted => new()
    {
        { "/f:s", "/f:s" },
        { "/a/f:s", "/a/f:s" },
        { "/a/f:s:$DATA", "/a/f:s" },
        { "/a/f::$DATA", "/a/f" },
        { "/f::$DATA", "/f" },
        { "/a/b.txt:Zone.Identifier", "/a/b.txt:Zone.Identifier" },
        { "/a/b.txt:Zone.Identifier:$DATA", "/a/b.txt:Zone.Identifier" },
        { "/a/f:with space", "/a/f:with space" },
        { "/a/f:S", "/a/f:S" },
        { "/a/f:s..", "/a/f:s.." },
        { "/a/f:%3A", "/a/f:%3A" },
        { "/a/f:s$", "/a/f:s$" },
        { "/a/f:e\u0301", "/a/f:\u00E9" },
        { "/a/e\u0301:s", "/a/\u00E9:s" },
        { "/a/e\u0301:e\u0301:$DATA", "/a/\u00E9:\u00E9" },
        { "/a/e\u0301::$DATA", "/a/\u00E9" },
        { "/d/e/f:\U0001F600", "/d/e/f:\U0001F600" },
    };

    [Theory]
    [MemberData(nameof(Accepted))]
    public void AcceptedStreamPathsNormalize(string input, string expected)
    {
        Assert.Equal(expected, DfsPath.Normalize(input));
        Assert.True(DfsPath.TryNormalize(input, out string normalized, out string? error));
        Assert.Null(error);
        Assert.Equal(expected, normalized);
        Assert.True(DfsPath.IsNormalized(expected));
        Assert.Equal(string.Equals(input, expected, StringComparison.Ordinal), DfsPath.IsNormalized(input));
        // Idempotent.
        Assert.Equal(expected, DfsPath.Normalize(expected));
    }

    public static TheoryData<string> Rejected => new()
    {
        "/:s",                       // root has no streams
        "/::$DATA",
        "/:s:$DATA",
        "/a:s/f",                    // colon in a non-final component
        "/a::$DATA/f",
        "/a/b:c/d/e",
        "/a/:s",                     // empty file name
        "/a/::$DATA",
        "/a/f:",                     // empty stream name
        "/a/f:s:",                   // empty type
        "/a/f:s:t",                  // unsupported type / extra separator
        "/a/f:s:$INDEX_ALLOCATION",
        "/a/f::$INDEX_ALLOCATION",
        "/a/f::$BITMAP",
        "/a/f:s:$data",              // type is case-sensitive
        "/a/f::$Data",
        "/a/f:s:$DATA:x",            // more separators
        "/a/f:::$DATA",
        "/a/f:s::$DATA",
        "/a/f:$I30",                 // '$' starts a stream type, never a stream name
        "/a/f:$DATA",
        "/a/f:$I30:$INDEX_ALLOCATION",
        "/a/f:.",                    // component rules apply to stream names
        "/a/f:..",
        "/a/.:s",
        "/a/..:s",
        "/a/f:s/",                   // trailing slash
        "/a/f:s/g",                  // stream path has no children
        "/a/f:s\\t",                 // backslash
        "a/f:s",                     // relative
        "/a/f:" + new string('x', 256),
        "/a/" + new string('x', 256) + ":s",
        "/a/f:" + string.Concat(Enumerable.Repeat("\u00E9", 128)),
    };

    [Theory]
    [MemberData(nameof(Rejected))]
    public void RejectedStreamPathsAreInvalidPaths(string input)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsPath.Normalize(input));
        Assert.Equal("path", ex.ParamName);
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        Assert.False(DfsPath.TryNormalize(input, out string normalized, out string? error));
        Assert.Equal(string.Empty, normalized);
        Assert.False(string.IsNullOrEmpty(error));
        Assert.False(DfsPath.IsNormalized(input));
    }

    [Fact]
    public void RootStreamMessageNamesTheRule()
    {
        Assert.False(DfsPath.TryNormalize("/:s", out _, out string? error));
        Assert.Contains("root", error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComponentAndStreamLimitsApplySeparately()
    {
        string name255 = new('n', 255);
        string stream255 = new('s', 255);
        string path = "/" + name255 + ":" + stream255;
        Assert.Equal(path, DfsPath.Normalize(path));
        Assert.Equal(path, DfsPath.Normalize(path + ":$DATA"));

        // Multi-byte: 127 x 'é' (254 bytes) + 'x' = 255 bytes is fine.
        string multi = string.Concat(Enumerable.Repeat("\u00E9", 127)) + "x";
        Assert.Equal("/f:" + multi, DfsPath.Normalize("/f:" + multi));
    }

    [Fact]
    public void PathLimitIncludesTheStreamSuffix()
    {
        // A 4096-byte path whose final component is a stream path; then exceed it by one byte.
        string prefix = string.Concat(Enumerable.Repeat("/" + new string('d', 200), 20));      // 4020 bytes
        int remaining = DfsPath.MaxPathBytes - Encoding.UTF8.GetByteCount(prefix) - "/f:".Length;
        Assert.Equal(73, remaining);
        string exact = prefix + "/f:" + new string('s', remaining);
        Assert.Equal(DfsPath.MaxPathBytes, Encoding.UTF8.GetByteCount(exact));
        Assert.Equal(exact, DfsPath.Normalize(exact));
        Assert.Equal(exact, DfsPath.Normalize(exact + ":$DATA"));    // the type suffix is removed before the limit check
        Assert.Throws<ArgumentException>(() => DfsPath.Normalize(exact + "s"));
    }

    [Fact]
    public void ExistingPathRulesStillHold()
    {
        foreach (string bad in new[] { "/a//b", "/a/./b", "/a/../b", "/a/", "", "a" })
            Assert.False(DfsPath.TryNormalize(bad, out _, out _), bad);
        Assert.Equal("/", DfsPath.Normalize("/"));
        Assert.Equal("/a/b", DfsPath.Normalize("/a/b"));
    }

    [Fact]
    public void MalformedCharactersInStreamPathsAreRejected()
    {
        foreach (string bad in new[] { "/f:\uD800", "/f:a\uDC00", "/f\uD800:s", "/a/f:s\0", "/a/f\0:s", "/a/f::$DATA\0" })
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(Assert.Throws<ArgumentException>(() => DfsPath.Normalize(bad))));
    }

    [Fact]
    public void StreamNamesAreNfcNormalisedAndCaseSensitive()
    {
        // ADS-018: NFC-equivalent names address the same stream; case-different names are distinct.
        Assert.Equal(DfsPath.Normalize("/f:\u00C5ngstr\u00F6m"), DfsPath.Normalize("/f:A\u030Angstro\u0308m"));
        Assert.NotEqual(DfsPath.Normalize("/f:Stream"), DfsPath.Normalize("/f:stream"));

        // Oracle: the platform's NFC (ICU) agrees for a set of decomposed names.
        foreach (string raw in new[] { "e\u0301", "A\u030A", "\u1100\u1161\u11A8", "o\u0308\u0304", "\u0041\u0301\u0323" })
            Assert.Equal("/f:" + raw.Normalize(NormalizationForm.FormC), DfsPath.Normalize("/f:" + raw));
    }

    // ---- Split / GetName / GetParent / Combine / IsAncestorOrSelf -------------------------------------------

    [Fact]
    public void SplitGetNameGetParentTreatTheStreamAsPartOfTheFinalComponent()
    {
        Assert.Equal(new[] { "a", "f:s" }, DfsPath.Split("/a/f:s"));
        Assert.Equal("f:s", DfsPath.GetName("/a/f:s"));
        Assert.Equal("/a", DfsPath.GetParent("/a/f:s"));
        Assert.Equal("/", DfsPath.GetParent("/f:s"));
        Assert.Equal("f:s", DfsPath.GetName("/f:s"));
        Assert.Throws<ArgumentException>(() => DfsPath.GetParent("/a/f:s:$DATA"));   // not normalized
        Assert.Throws<ArgumentException>(() => DfsPath.Split("/a/f::$DATA"));
    }

    [Fact]
    public void CombineAcceptsStreamQualifiedNames()
    {
        Assert.Equal("/a/f:s", DfsPath.Combine("/a", "f:s"));
        Assert.Equal("/a/f:s", DfsPath.Combine("/a", "f:s:$DATA"));
        Assert.Equal("/a/f", DfsPath.Combine("/a", "f::$DATA"));
        Assert.Equal("/f:s", DfsPath.Combine("/", "f:s"));
        Assert.Equal("/a/\u00E9:\u00E9", DfsPath.Combine("/a", "e\u0301:e\u0301"));

        foreach ((string parent, string name) in new[]
        {
            ("/", ":s"), ("/", "::$DATA"), ("/a", ":s"), ("/a", "f:"), ("/a", "f:s:t"), ("/a", "f:$x"), ("/a", "x/y:s"),
            ("/a", "f:s/"), ("/a", "."), ("/a", ""), ("/a/f:s", "x"), ("/a/f:s", "x:y"),
        })
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsPath.Combine(parent, name));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        }
    }

    [Theory]
    [InlineData("/a/f:s")]
    [InlineData("/f:s")]
    [InlineData("/a/b/c")]
    [InlineData("/x")]
    [InlineData("/a/\u00E9:\u00E9 t")]
    public void CombineGetParentGetNameRoundTrip(string path)
    {
        Assert.Equal(path, DfsPath.Combine(DfsPath.GetParent(path), DfsPath.GetName(path)));
    }

    [Fact]
    public void AFileContainsItsStreams()
    {
        Assert.True(DfsPath.IsAncestorOrSelf("/a/f", "/a/f:s"));
        Assert.True(DfsPath.IsAncestorOrSelf("/a", "/a/f:s"));
        Assert.True(DfsPath.IsAncestorOrSelf("/", "/f:s"));
        Assert.True(DfsPath.IsAncestorOrSelf("/a/f:s", "/a/f:s"));
        Assert.True(DfsPath.IsAncestorOrSelf("/d", "/d:s"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/f:s", "/a/f"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/f:s", "/a/f:s2"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/f:s", "/a/f:t"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/f", "/a/f2:s"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/f", "/a/fs"));
    }

    [Fact]
    public void EntryNamesNeverContainColons()
    {
        foreach (string name in new[] { "a:b", ":", "f::$DATA", "f:s" })
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsPath.ValidateComponent(name));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
            Assert.False(DfsPath.TryNormalizeComponent(name, out _, out string? error));
            Assert.Contains("':'", error!, StringComparison.Ordinal);
        }

        Assert.Equal("ok", DfsPath.NormalizeComponent("ok"));
    }
}
