using System.Globalization;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Sdk;

namespace Tedd.Tdfs.Tests.Unit.Path;

/// <summary>
/// PATH suite (test plan §7.3, FSD §4 NS-01..NS-04): path parsing and name identity. Pure rules are checked against
/// <see cref="DfsPath"/> directly; every rule is also driven through the SDK against a cluster (invalid input must be
/// rejected before any request is sent) and, where the row requires it, against the server with SDK validation
/// bypassed (<see cref="RawProtocolClient"/>).
/// </summary>
public sealed class PathUnitTests
{
    private const string Volume = PathHelpers.Volume;

    private static CancellationToken Ct => SdkHelpers.Ct;

    // =====================================================================================================
    // PATH-001 AbsoluteVolumeRelativePaths
    // =====================================================================================================

    private static readonly string[] RelativePathList =
    [
        "", "a", "a/b", "documents/report.json", "./a", "../a", ".", "..", "~/a", " /a", "\t/a", "C:/a", "C:\\a",
        "\\\\server\\share\\a", "vol:/a", "mem://n0/a", "quic://127.0.0.1:7100/a", "a/",
    ];

    public static TheoryData<string> RelativePaths => new(RelativePathList);

    public static TheoryData<string, string> ValidAbsolutePaths => new()
    {
        { "/", "/" },
        { "/a", "/a" },
        { "/documents/report.json", "/documents/report.json" },
        { "/a/b/c/d/e", "/a/b/c/d/e" },
        { "/with space/x y", "/with space/x y" },
        { "/\u00FCn\u00EFc\u00F8d\u00E9/\u65E5\u672C\u8A9E", "/\u00FCn\u00EFc\u00F8d\u00E9/\u65E5\u672C\u8A9E" },
        { "/a-b_c.d~e!$&'()+,;=@", "/a-b_c.d~e!$&'()+,;=@" },
        { "/" + new string('x', 255), "/" + new string('x', 255) },
    };

    [Theory, MemberData(nameof(ValidAbsolutePaths))]
    [Catalog("PATH-001", "U", "INV-07", "INV-08")]
    public void PATH_001_AbsoluteVolumeRelativePaths_AcceptsAbsolute(string input, string expected)
    {
        Assert.Equal(expected, DfsPath.Normalize(input));
        Assert.True(DfsPath.TryNormalize(input, out string normalized, out string? error), error);
        Assert.Equal(expected, normalized);
        Assert.Null(error);
        Assert.Equal(expected, DfsPath.Normalize(expected));

        string rebuilt = DfsPath.Root;
        foreach (string component in DfsPath.Split(expected))
            rebuilt = DfsPath.Combine(rebuilt, component);
        Assert.Equal(expected, rebuilt);
        if (expected != DfsPath.Root)
            Assert.Equal(expected, DfsPath.Combine(DfsPath.GetParent(expected), DfsPath.GetName(expected)));
    }

    [Theory, MemberData(nameof(RelativePaths))]
    [Catalog("PATH-001", "U", "INV-07", "INV-08")]
    public void PATH_001_AbsoluteVolumeRelativePaths_RejectsRelative(string input)
    {
        ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() => DfsPath.Normalize(input));
        Assert.Equal("path", ex.ParamName);
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        Assert.False(DfsPath.TryNormalize(input, out string normalized, out string? error));
        Assert.Equal(string.Empty, normalized);
        Assert.False(string.IsNullOrEmpty(error));
    }

    [Fact]
    [Catalog("PATH-001", "U", "INV-07", "INV-08")]
    public void PATH_001_AbsoluteVolumeRelativePaths_RejectsNull()
    {
        Assert.Throws<ArgumentNullException>(() => DfsPath.Normalize(null!));
        Assert.False(DfsPath.TryNormalize(null, out string normalized, out string? error));
        Assert.Equal(string.Empty, normalized);
        Assert.NotNull(error);
    }

    [Fact]
    [Catalog("PATH-001", "U", "INV-07", "INV-08")]
    public async Task PATH_001_AbsoluteVolumeRelativePaths()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);

            // The root and valid absolute volume-relative names work end to end.
            Assert.True(await volume.Directories.ExistsAsync("/", Ct));
            DfsDirectoryInfo root = await volume.Directories.GetInfoAsync("/", Ct);
            Assert.Equal("/", root.Path);
            Assert.Equal(InodeId.Root, root.InodeId);

            DfsDirectoryInfo documents = await volume.Directories.CreateDirectoryAsync("/documents", Ct);
            Assert.Equal("/documents", documents.Path);
            byte[] content = TestData.Bytes(101, 300);
            CommitToken token = await volume.Files.WriteAllBytesAsync("/documents/report.json", content, Ct);
            Assert.False(token.IsNone);
            DfsFileInfo info = await volume.Files.GetInfoAsync("/documents/report.json", Ct);
            Assert.Equal("/documents/report.json", info.Path);
            Assert.Equal(content.Length, info.Length);
            await SdkHelpers.AssertBytesEqualAsync(content, volume.Files.ReadAllBytesAsync("/documents/report.json", Ct));
            Assert.Equal(new[] { "report.json" }, await SdkHelpers.ListNamesAsync(volume.Directories, "/documents"));

            // Relative (and null) paths are rejected by the SDK before any request is sent, on every entry point.
            PathHelpers.PathOperationContext context = await PathHelpers.PrepareAsync(volume);
            await PathHelpers.AssertRejectedLocallyAsync(cluster, context, [.. RelativePathList, null]);

            // No relative input was resolved against an implicit working directory.
            Assert.False(await volume.Directories.ExistsAsync("/documents/documents", Ct));
            Assert.Equal(new[] { "documents", "path-valid" }, (await SdkHelpers.ListNamesAsync(volume.Directories, "/")).Order(StringComparer.Ordinal));
            await context.Transaction.AbortAsync(Ct);
        }, Ct);
    }

    // =====================================================================================================
    // PATH-002 RejectTraversalAndEmptyComponents
    // =====================================================================================================

    private static readonly string[] TraversalPathList =
    [
        "/.", "/..", "/a/.", "/a/..", "/./a", "/../a", "/a/./b", "/a/../b", "/a/b/..", "/../other/secret",
        "/a/../../other/secret", "/./../.", "/../", "/./",
    ];

    private static readonly string[] EmptyComponentPathList = ["//", "///", "//a", "/a//b", "/a/b//c", "/a/", "/a/b/", "/a//"];

    private static readonly string[] DotLookalikeList = ["/...", "/....", "/.a", "/a.", "/..a", "/a..", "/a..b", "/.hidden/..x", "/. ", "/ .", "/.. ", "/.%2E"];

    public static TheoryData<string> TraversalPaths => new(TraversalPathList);

    public static TheoryData<string> EmptyComponentPaths => new(EmptyComponentPathList);

    public static TheoryData<string> DotLookalikes => new(DotLookalikeList);

    [Theory, MemberData(nameof(TraversalPaths))]
    [Catalog("PATH-002", "U", "INV-07", "INV-12")]
    public void PATH_002_RejectTraversalAndEmptyComponents_Traversal(string input) => AssertInvalidPath(input);

    [Theory, MemberData(nameof(EmptyComponentPaths))]
    [Catalog("PATH-002", "U", "INV-07", "INV-12")]
    public void PATH_002_RejectTraversalAndEmptyComponents_EmptyComponents(string input) => AssertInvalidPath(input);

    [Theory, MemberData(nameof(DotLookalikes))]
    [Catalog("PATH-002", "U", "INV-07", "INV-12")]
    public void PATH_002_RejectTraversalAndEmptyComponents_DotLookalikesAreNames(string input)
    {
        Assert.Equal(input, DfsPath.Normalize(input));
        IReadOnlyList<string> components = DfsPath.Split(input);
        Assert.DoesNotContain(".", components);
        Assert.DoesNotContain("..", components);
        Assert.Equal(input[(input.LastIndexOf('/') + 1)..], DfsPath.GetName(input));
    }

    [Fact]
    [Catalog("PATH-002", "U", "INV-07", "INV-12")]
    public void PATH_002_RejectTraversalAndEmptyComponents_ComponentHelpers()
    {
        foreach (string name in new[] { ".", "..", "", "a/b", "/", "../x" })
        {
            ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() => DfsPath.ValidateComponent(name));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
            Assert.ThrowsAny<ArgumentException>(() => DfsPath.Combine("/a", name));
        }

        Assert.ThrowsAny<ArgumentException>(() => DfsPath.GetParent("/"));
        Assert.Equal("/", DfsPath.GetParent("/a"));
        Assert.Equal("/a", DfsPath.GetParent("/a/b"));
        Assert.True(DfsPath.IsAncestorOrSelf("/", "/a/b"));
        Assert.True(DfsPath.IsAncestorOrSelf("/a", "/a/b"));
        Assert.True(DfsPath.IsAncestorOrSelf("/a", "/a"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a", "/ab"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a/b", "/a"));
        Assert.False(DfsPath.IsAncestorOrSelf("/a", "/A/b"));
        // Non-normalized inputs to the helpers are rejected instead of being silently resolved.
        Assert.ThrowsAny<ArgumentException>(() => DfsPath.IsAncestorOrSelf("/a/..", "/b"));
        Assert.ThrowsAny<ArgumentException>(() => DfsPath.Split("/a/../b"));
    }

    [Fact]
    [Catalog("PATH-002", "U", "INV-07", "INV-12")]
    public async Task PATH_002_RejectTraversalAndEmptyComponents()
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("other")] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            DfsVolume other = client.GetVolume("other");
            byte[] secret = TestData.Bytes(201, 128);
            await other.Files.WriteAllBytesAsync("/secret", secret, Ct);
            await volume.Directories.CreateDirectoryAsync("/a/b", Ct);

            PathHelpers.PathOperationContext context = await PathHelpers.PrepareAsync(volume);
            await PathHelpers.AssertRejectedLocallyAsync(cluster, context, [.. TraversalPathList, .. EmptyComponentPathList]);

            // A volume cannot be addressed through a path or a path-like volume name.
            long before = SdkHelpers.ForegroundRequests(cluster, SdkHelpers.NetworkName());
            foreach (string name in new[] { "", ".", "..", "a/b", "/other", "other/..", "vol/../other", "../other", "other\\x" })
            {
                Assert.ThrowsAny<ArgumentException>(() => client.GetVolume(name));
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            Assert.Equal(before, SdkHelpers.ForegroundRequests(cluster, SdkHelpers.NetworkName()));

            // The other volume is untouched and nothing of it leaked into this volume.
            await SdkHelpers.AssertBytesEqualAsync(secret, other.Files.ReadAllBytesAsync("/secret", Ct));
            Assert.False(await volume.Files.ExistsAsync("/secret", Ct));
            Assert.False(await volume.Directories.ExistsAsync("/other", Ct));

            // Dot look-alikes are ordinary distinct names, not traversal.
            var inodes = new HashSet<InodeId>();
            foreach (string path in new[] { "/...", "/.a", "/a.", "/..a", "/a/b/..x" })
            {
                byte[] bytes = Encoding.UTF8.GetBytes(path);
                await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
                Assert.Equal(path, info.Path);
                Assert.True(inodes.Add(info.InodeId), $"{path} shares an inode with another look-alike");
                await SdkHelpers.AssertBytesEqualAsync(bytes, volume.Files.ReadAllBytesAsync(path, Ct));
            }

            List<string> rootNames = await SdkHelpers.ListNamesAsync(volume.Directories, "/");
            Assert.Contains("...", rootNames);
            Assert.Contains(".a", rootNames);
            Assert.DoesNotContain(".", rootNames);
            Assert.DoesNotContain("..", rootNames);
            Assert.Equal(new[] { "..x" }, await SdkHelpers.ListNamesAsync(volume.Directories, "/a/b"));
            await context.Transaction.AbortAsync(Ct);
        }, Ct);
    }

    // =====================================================================================================
    // PATH-003 RejectForbiddenCharacters
    // =====================================================================================================

    private static readonly Dictionary<string, string> ForbiddenCharacterPathTable = new(StringComparer.Ordinal)
    {
        ["NulInName"] = "/a\0b",
        ["NulOnly"] = "/\0",
        ["NulBeforeRoot"] = "\0/a",
        ["NulAtEnd"] = "/a/b\0",
        ["NulInDirectory"] = "/a\0/b",
        ["BackslashInName"] = "/a\\b",
        ["BackslashOnly"] = "/\\",
        ["BackslashRooted"] = "\\a",
        ["BackslashAtEnd"] = "/a/b\\",
        ["BackslashTraversal"] = "/a\\..\\b",
        ["UncLike"] = "/\\\\server\\share",
        ["LoneHighSurrogate"] = "/\uD800",
        ["LoneHighSurrogateMax"] = "/\uDBFF",
        ["LoneLowSurrogate"] = "/\uDC00",
        ["LowSurrogateInside"] = "/a\uDC00b",
        ["HighSurrogateAtEnd"] = "/x\uD83D",
        ["ReversedPair"] = "/\uDE00\uD83D",
        ["HighHighLow"] = "/\uD83D\uD83D\uDE00",
        ["LoneSurrogateInDirectory"] = "/ok/\uD83D/ok",
    };

    private static readonly Dictionary<string, string> WellFormedSurrogateTable = new(StringComparer.Ordinal)
    {
        ["Emoji"] = "/\U0001F600",
        ["EmojiInside"] = "/a\U0001F600b",
        ["CjkExtensionB"] = "/\U0002000B",
        ["MaxScalar"] = "/\U0010FFFD",
        ["EmojiDirectory"] = "/\U0001F4C1/\U0001F4C4",
    };

    /// <summary>Malformed UTF-8 path encodings (wire level).</summary>
    internal static readonly Dictionary<string, byte[]> MalformedUtf8Table = new(StringComparer.Ordinal)
    {
        ["TruncatedTwoByte"] = [0x2F, 0x61, 0xC3],
        ["InvalidLeadFF"] = [0x2F, 0xFF],
        ["InvalidLeadFE"] = [0x2F, 0x61, 0xFE, 0x62],
        ["LoneContinuation"] = [0x2F, 0x80],
        ["OverlongSlash"] = [0x2F, 0x61, 0xC0, 0xAF, 0x62],
        ["OverlongDotDot"] = [0x2F, 0xC0, 0xAE, 0xC0, 0xAE],
        ["OverlongNul"] = [0x2F, 0x61, 0xC0, 0x80],
        ["ThreeByteOverlong"] = [0x2F, 0xE0, 0x80, 0xAF],
        ["EncodedHighSurrogate"] = [0x2F, 0xED, 0xA0, 0x80],
        ["EncodedLowSurrogate"] = [0x2F, 0xED, 0xBF, 0xBF],
        ["CesuSurrogatePair"] = [0x2F, 0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80],
        ["AboveMaxScalar"] = [0x2F, 0xF4, 0x90, 0x80, 0x80],
        ["TruncatedFourByte"] = [0x2F, 0xF0, 0x9F, 0x98],
    };

    public static TheoryData<string> ForbiddenCharacterCases => new(ForbiddenCharacterPathTable.Keys.Order(StringComparer.Ordinal));

    public static TheoryData<string> WellFormedSurrogateCases => new(WellFormedSurrogateTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(ForbiddenCharacterCases))]
    [Catalog("PATH-003", "U", "INV-07", "INV-08")]
    public void PATH_003_RejectForbiddenCharacters_Normalize(string caseName)
    {
        string input = ForbiddenCharacterPathTable[caseName];
        AssertInvalidPath(input);
        int offending = 0;
        foreach (string component in input.Split('/'))
        {
            if (component.Contains('\0', StringComparison.Ordinal) || component.Contains('\\', StringComparison.Ordinal) || DfsUnicode.IndexOfUnpairedSurrogate(component) >= 0)
            {
                offending++;
                Assert.ThrowsAny<ArgumentException>(() => DfsPath.ValidateComponent(component));
                Assert.ThrowsAny<ArgumentException>(() => DfsPath.Combine("/ok", component));
            }
        }

        Assert.True(offending > 0, "the case must contain a forbidden character");
    }

    [Theory, MemberData(nameof(WellFormedSurrogateCases))]
    [Catalog("PATH-003", "U", "INV-07", "INV-08")]
    public void PATH_003_RejectForbiddenCharacters_WellFormedPairsAccepted(string caseName)
    {
        string input = WellFormedSurrogateTable[caseName];
        Assert.Equal(input, DfsPath.Normalize(input));
    }

    [Fact]
    [Catalog("PATH-003", "U", "INV-07", "INV-08")]
    public async Task PATH_003_RejectForbiddenCharacters()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            PathHelpers.PathOperationContext context = await PathHelpers.PrepareAsync(volume);
            await volume.Directories.CreateDirectoryAsync("/ok", Ct);
            List<string> rootBefore = await SdkHelpers.ListNamesAsync(volume.Directories, "/");

            await PathHelpers.AssertRejectedLocallyAsync(cluster, context, ForbiddenCharacterPathTable.Values);
            Assert.Equal(rootBefore, await SdkHelpers.ListNamesAsync(volume.Directories, "/"));
            Assert.Empty(await SdkHelpers.ListNamesAsync(volume.Directories, "/ok"));

            // Well-formed supplementary characters are ordinary names and round-trip exactly.
            await volume.Directories.CreateDirectoryAsync("/\U0001F4C1", Ct);
            foreach ((string caseName, string path) in WellFormedSurrogateTable)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(caseName);
                await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                Assert.Equal(path, (await volume.Files.GetInfoAsync(path, Ct)).Path);
                await SdkHelpers.AssertBytesEqualAsync(bytes, volume.Files.ReadAllBytesAsync(path, Ct));
            }

            Assert.Contains("\U0001F600", await SdkHelpers.ListNamesAsync(volume.Directories, "/"));
            Assert.Equal(new[] { "\U0001F4C4" }, await SdkHelpers.ListNamesAsync(volume.Directories, "/\U0001F4C1"));
            await context.Transaction.AbortAsync(Ct);
        }, Ct);
    }

    [Fact]
    [Catalog("PATH-003", "U", "INV-07", "INV-08")]
    public async Task PATH_003_RejectForbiddenCharacters_MalformedWireUtf8()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            try
            {
                // Control: the raw path really reaches the server and well-formed UTF-8 is accepted.
                Assert.Equal(DfsErrorCode.None, await raw.SendRawPathBytesAsync(Volume, DfsOperationKind.CreateDirectory, Encoding.UTF8.GetBytes("/raw-ok")));
                Assert.True(await volume.Directories.ExistsAsync("/raw-ok", Ct));

                int inodesBefore = await SdkHelpers.InodeCountAsync(cluster);
                List<string> rootBefore = await SdkHelpers.ListNamesAsync(volume.Directories, "/");
                var failures = new List<string>();
                DfsOperationKind[] kinds = [DfsOperationKind.CreateDirectory, DfsOperationKind.WriteAll, DfsOperationKind.Open, DfsOperationKind.GetInfo, DfsOperationKind.Delete];
                foreach ((string caseName, byte[] bytes) in MalformedUtf8Table)
                {
                    foreach (DfsOperationKind kind in kinds)
                    {
                        if (!raw.IsConnected)
                        {
                            await raw.DisposeAsync();
                            raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
                        }

                        DfsErrorCode code = await raw.SendRawPathBytesAsync(Volume, kind, bytes);
                        if (code is not (DfsErrorCode.InvalidPath or DfsErrorCode.ProtocolError))
                            failures.Add($"{kind} with {caseName} ({Convert.ToHexString(bytes)}) returned {code}");
                    }
                }

                Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
                Assert.Equal(inodesBefore, await SdkHelpers.InodeCountAsync(cluster));
                Assert.Equal(rootBefore, await SdkHelpers.ListNamesAsync(volume.Directories, "/"));
                // Overlong '/' and ".." encodings were not decoded into separators or traversal.
                Assert.False(await volume.Directories.ExistsAsync("/a", Ct));
                Assert.False(await volume.Files.ExistsAsync("/a", Ct));
                Assert.False(await volume.Files.ExistsAsync("/b", Ct));
            }
            finally
            {
                await raw.DisposeAsync();
            }
        }, Ct);
    }

    // =====================================================================================================
    // PATH-004 UnicodeNormalizationIdentity
    // =====================================================================================================

    private static readonly Dictionary<string, (string Variant, string Nfc)> NormalizationPairTable = new(StringComparer.Ordinal)
    {
        ["LatinAcute"] = ("cafe\u0301", "caf\u00E9"),
        ["HangulJamo"] = ("\u1100\u1161\u11A8", "\uAC01"),
        ["AngstromSign"] = ("\u212B", "\u00C5"),
        ["OhmSign"] = ("\u2126", "\u03A9"),
        ["KelvinSign"] = ("\u212A", "K"),
        ["CombiningOrderDotFirst"] = ("a\u0323\u0302", "\u1EAD"),
        ["CombiningOrderCircumflexFirst"] = ("a\u0302\u0323", "\u1EAD"),
        ["DiaeresisMacron"] = ("o\u0308\u0304", "\u022B"),
        ["GreekTonos"] = ("\u03B1\u0301", "\u03AC"),
        ["DevanagariCompositionExclusion"] = ("\u0958", "\u0915\u093C"),
        ["IWithDotAbove"] = ("I\u0307", "\u0130"),
    };

    private static readonly Dictionary<string, (string A, string B)> CompatibilityDistinctTable = new(StringComparer.Ordinal)
    {
        ["Ligature"] = ("\uFB01le", "file"),
        ["CircledDigit"] = ("\u2460", "1"),
        ["Fullwidth"] = ("\uFF21", "A"),
        ["MicroSign"] = ("\u00B5", "\u03BC"),
        ["Superscript"] = ("x\u00B2", "x2"),
        ["NoBreakSpace"] = ("a\u00A0b", "a b"),
    };

    public static TheoryData<string> NormalizationPairCases => new(NormalizationPairTable.Keys.Order(StringComparer.Ordinal));

    public static TheoryData<string> CompatibilityDistinctCases => new(CompatibilityDistinctTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(NormalizationPairCases))]
    [Catalog("PATH-004", "U", "INV-07")]
    public void PATH_004_UnicodeNormalizationIdentity_Normalize(string caseName)
    {
        (string variant, string nfc) = NormalizationPairTable[caseName];
        Assert.Equal(nfc, PathHelpers.OracleNfc(variant));
        Assert.Equal(nfc, PathHelpers.OracleNfc(nfc));
        Assert.Equal("/" + nfc, DfsPath.Normalize("/" + variant));
        Assert.Equal("/" + nfc, DfsPath.Normalize("/" + nfc));
        Assert.Equal("/d/" + nfc + "/x", DfsPath.Normalize("/d/" + variant + "/x"));
        Assert.Equal(nfc, DfsUnicode.ToNfc(variant));
        Assert.True(DfsUnicode.IsNfc(nfc));
    }

    [Theory, MemberData(nameof(CompatibilityDistinctCases))]
    [Catalog("PATH-004", "U", "INV-07")]
    public void PATH_004_UnicodeNormalizationIdentity_CompatibilityFormsStayDistinct(string caseName)
    {
        (string a, string b) = CompatibilityDistinctTable[caseName];
        Assert.Equal("/" + PathHelpers.OracleNfc(a), DfsPath.Normalize("/" + a));
        Assert.Equal("/" + a, DfsPath.Normalize("/" + a));
        Assert.NotEqual(DfsPath.Normalize("/" + a), DfsPath.Normalize("/" + b));
    }

    [Fact]
    [Catalog("PATH-004", "U", "INV-07")]
    public async Task PATH_004_UnicodeNormalizationIdentity()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v0 = c0.GetVolume(Volume);
            DfsVolume v1 = c1.GetVolume(Volume);
            await v0.Directories.CreateDirectoryAsync("/nfc", Ct);

            ulong seed = 400;
            var expectedNames = new List<string>();
            foreach ((string caseName, (string variant, string nfc)) in NormalizationPairTable)
            {
                string nfcPath = $"/nfc/{caseName}-{nfc}";
                string variantPath = $"/nfc/{caseName}-{variant}";
                expectedNames.Add($"{caseName}-{nfc}");
                byte[] first = TestData.Bytes(seed++, 200);
                await v0.Files.WriteAllBytesAsync(nfcPath, first, Ct);
                DfsFileInfo viaNfc = await v0.Files.GetInfoAsync(nfcPath, Ct);

                // The other form, from another client, resolves to the same inode and bytes.
                Assert.True(await v1.Files.ExistsAsync(variantPath, Ct), caseName);
                DfsFileInfo viaVariant = await v1.Files.GetInfoAsync(variantPath, Ct);
                Assert.Equal(viaNfc.InodeId, viaVariant.InodeId);
                Assert.Equal(nfcPath, viaVariant.Path);
                await SdkHelpers.AssertBytesEqualAsync(first, v1.Files.ReadAllBytesAsync(variantPath, Ct), caseName);

                // Creating the equivalent name is a duplicate create; replacing through it keeps the inode.
                await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(async () =>
                {
                    await using DfsStream s = await v1.Files.OpenAsync(variantPath, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None }, Ct);
                }, DfsErrorCode.AlreadyExists);
                byte[] second = TestData.Bytes(seed++, 64);
                await v1.Files.WriteAllBytesAsync(variantPath, second, Ct);
                DfsFileInfo afterReplace = await v0.Files.GetInfoAsync(nfcPath, Ct);
                Assert.Equal(viaNfc.InodeId, afterReplace.InodeId);
                await SdkHelpers.AssertBytesEqualAsync(second, v0.Files.ReadAllBytesAsync(nfcPath, Ct), caseName);
            }

            // Exactly one entry per name, stored in NFC.
            List<string> listed = await SdkHelpers.ListNamesAsync(v0.Directories, "/nfc");
            Assert.Equal(expectedNames.Order(StringComparer.Ordinal), listed.Order(StringComparer.Ordinal));
            Assert.All(listed, n => Assert.Equal(PathHelpers.OracleNfc(n), n));

            // Directories: a decomposed spelling of a directory name addresses the same directory.
            DfsDirectoryInfo dir = await v0.Directories.CreateDirectoryAsync("/nfc-dir\u00E9", Ct);
            await v1.Files.WriteAllBytesAsync("/nfc-dire\u0301/f.bin", TestData.Bytes(seed++, 10), Ct);
            Assert.Equal(dir.InodeId, (await v1.Directories.GetInfoAsync("/nfc-dire\u0301", Ct)).InodeId);
            Assert.Equal(new[] { "f.bin" }, await SdkHelpers.ListNamesAsync(v0.Directories, "/nfc-dir\u00E9"));

            // Move and delete through the other form act on the same entry.
            await v1.Files.MoveAsync("/nfc/LatinAcute-cafe\u0301", "/nfc/moved-cafe\u0301", false, Ct);
            Assert.False(await v0.Files.ExistsAsync("/nfc/LatinAcute-caf\u00E9", Ct));
            Assert.True(await v0.Files.ExistsAsync("/nfc/moved-caf\u00E9", Ct));
            await v1.Files.DeleteAsync("/nfc/moved-cafe\u0301", Ct);
            Assert.False(await v0.Files.ExistsAsync("/nfc/moved-caf\u00E9", Ct));

            // Compatibility (non-canonical) equivalents remain distinct entries.
            await v0.Directories.CreateDirectoryAsync("/compat", Ct);
            foreach ((string caseName, (string a, string b)) in CompatibilityDistinctTable)
            {
                await v0.Files.WriteAllBytesAsync("/compat/" + a, Encoding.UTF8.GetBytes(caseName + "-a"), Ct);
                await v0.Files.WriteAllBytesAsync("/compat/" + b, Encoding.UTF8.GetBytes(caseName + "-b"), Ct);
                Assert.NotEqual((await v0.Files.GetInfoAsync("/compat/" + a, Ct)).InodeId, (await v0.Files.GetInfoAsync("/compat/" + b, Ct)).InodeId);
                Assert.Equal(caseName + "-a", await v1.Files.ReadAllTextAsync("/compat/" + a, null, Ct));
            }

            Assert.Equal(CompatibilityDistinctTable.Count * 2, (await SdkHelpers.ListNamesAsync(v0.Directories, "/compat")).Count);

            // Create races between two clients using different normalization forms: one winner, one duplicate.
            await v0.Directories.CreateDirectoryAsync("/race", Ct);
            for (int round = 0; round < 5; round++)
            {
                string nfcName = $"/race/r{round}-x\u00E9";
                string nfdName = $"/race/r{round}-xe\u0301";
                byte[] bytes0 = TestData.Bytes(seed++, 100);
                byte[] bytes1 = TestData.Bytes(seed++, 100);
                Task<InodeId> t0 = CreateNewAndWriteAsync(v0.Files, round % 2 == 0 ? nfcName : nfdName, bytes0);
                Task<InodeId> t1 = CreateNewAndWriteAsync(v1.Files, round % 2 == 0 ? nfdName : nfcName, bytes1);
                Exception? e0 = await Record.ExceptionAsync(() => t0);
                Exception? e1 = await Record.ExceptionAsync(() => t1);
                Assert.True((e0 is null) ^ (e1 is null), $"round {round}: exactly one create must win (e0={e0?.GetType().Name}, e1={e1?.GetType().Name})");
                Exception loser = (e0 ?? e1)!;
                Assert.IsAssignableFrom<DfsAlreadyExistsException>(loser);
                byte[] winnerBytes = e0 is null ? bytes0 : bytes1;
                InodeId winnerInode = e0 is null ? await t0 : await t1;
                List<string> entries = (await SdkHelpers.ListNamesAsync(v0.Directories, "/race")).Where(n => n.StartsWith($"r{round}-", StringComparison.Ordinal)).ToList();
                Assert.Equal(new[] { $"r{round}-x\u00E9" }, entries);
                Assert.Equal(winnerInode, (await v1.Files.GetInfoAsync(nfdName, Ct)).InodeId);
                await SdkHelpers.AssertBytesEqualAsync(winnerBytes, v1.Files.ReadAllBytesAsync(nfcName, Ct), $"round {round}");
            }
        }, Ct);

        static async Task<InodeId> CreateNewAndWriteAsync(DfsFileClient files, string path, byte[] bytes)
        {
            await using DfsStream s = await files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None }, Ct);
            await s.WriteAsync(bytes, Ct);
            await s.FlushAsync(Ct);
            return s.InodeId;
        }
    }

    // =====================================================================================================
    // PATH-005 CaseSensitiveOrdinalIdentity
    // =====================================================================================================

    private static readonly string[] CaseVariantNames =
    [
        "Readme", "README", "readme", "ReadMe", "i", "I", "\u0130", "\u0131", "\u00DF", "SS", "ss", "\u1E9E", "\u03A3", "\u03C3", "\u03C2",
        "\u01C4", "\u01C5", "\u01C6", "\u0130stanbul", "istanbul", "ISTANBUL",
    ];

    public static TheoryData<string> Cultures => new("", "en-US", "tr-TR", "az-Latn-AZ", "lt-LT", "el-GR", "de-DE", "nl-NL", "ja-JP", "he-IL", "ar-SA");

    public static TheoryData<string> ServerCultures => new("", "en-US", "tr-TR", "az-Latn-AZ", "el-GR", "lt-LT");

    [Theory, MemberData(nameof(Cultures))]
    [Catalog("PATH-005", "U", "INV-07")]
    public void PATH_005_CaseSensitiveOrdinalIdentity_Normalize(string culture)
    {
        string[] normalized = PathHelpers.WithCulture(culture, () => CaseVariantNames.Select(n => DfsPath.Normalize("/" + n)).ToArray());
        for (int i = 0; i < CaseVariantNames.Length; i++)
            Assert.Equal("/" + CaseVariantNames[i], normalized[i]); // case preserved, NFC-stable inputs unchanged
        Assert.Equal(CaseVariantNames.Length, normalized.Distinct(StringComparer.Ordinal).Count());
        PathHelpers.WithCulture(culture, () =>
        {
            Assert.False(DfsPath.IsAncestorOrSelf("/Dir", "/dir/x"));
            Assert.False(DfsPath.IsAncestorOrSelf("/I", "/i/x"));
            Assert.False(DfsPath.IsAncestorOrSelf("/i", "/\u0130/x"));
            Assert.True(DfsPath.IsAncestorOrSelf("/\u0131", "/\u0131/x"));
            return 0;
        });
    }

    [Theory, MemberData(nameof(ServerCultures))]
    [Catalog("PATH-005", "U", "INV-07")]
    public async Task PATH_005_CaseSensitiveOrdinalIdentity(string culture)
    {
        // The culture is set before the cluster starts so that nodes started from this execution context and the
        // client both run under it.
        await PathHelpers.WithCultureAsync(culture, async () =>
        {
            await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
            {
                await using DfsClient client = await cluster.ConnectAsync(null, Ct);
                DfsFileClient files = client.GetVolume(Volume).Files;
                DfsDirectoryClient dirs = client.GetVolume(Volume).Directories;
                await dirs.CreateDirectoryAsync("/case", Ct);

                var inodes = new Dictionary<string, InodeId>(StringComparer.Ordinal);
                for (int i = 0; i < CaseVariantNames.Length; i++)
                {
                    string path = "/case/" + CaseVariantNames[i];
                    await files.WriteAllBytesAsync(path, TestData.Bytes(500 + (ulong)i, 32 + i), Ct);
                    inodes[CaseVariantNames[i]] = (await files.GetInfoAsync(path, Ct)).InodeId;
                }

                Assert.Equal(CaseVariantNames.Length, inodes.Values.Distinct().Count());
                List<string> listed = await SdkHelpers.ListNamesAsync(dirs, "/case");
                Assert.Equal(CaseVariantNames.Order(StringComparer.Ordinal), listed.Order(StringComparer.Ordinal));
                for (int i = 0; i < CaseVariantNames.Length; i++)
                {
                    string path = "/case/" + CaseVariantNames[i];
                    await SdkHelpers.AssertBytesEqualAsync(TestData.Bytes(500 + (ulong)i, 32 + i), files.ReadAllBytesAsync(path, Ct), path);
                    Assert.Equal(inodes[CaseVariantNames[i]], (await files.GetInfoAsync(path, Ct)).InodeId);
                }

                // A spelling that differs only by case and was never created does not exist.
                Assert.False(await files.ExistsAsync("/case/rEADME", Ct));
                Assert.False(await files.ExistsAsync("/case/\u0130STANBUL", Ct));
                Assert.False(await files.ExistsAsync("/case/\u0131stanbul", Ct));

                // Deleting one case variant leaves all others intact.
                await files.DeleteAsync("/case/readme", Ct);
                Assert.False(await files.ExistsAsync("/case/readme", Ct));
                foreach (string other in new[] { "Readme", "README", "ReadMe" })
                    Assert.Equal(inodes[other], (await files.GetInfoAsync("/case/" + other, Ct)).InodeId);

                // Directories differing only by case are distinct namespaces.
                await dirs.CreateDirectoryAsync("/case/Dir", Ct);
                await dirs.CreateDirectoryAsync("/case/dir", Ct);
                await files.WriteAllTextAsync("/case/Dir/x", "upper", null, Ct);
                await files.WriteAllTextAsync("/case/dir/x", "lower", null, Ct);
                Assert.Equal("upper", await files.ReadAllTextAsync("/case/Dir/x", null, Ct));
                Assert.Equal("lower", await files.ReadAllTextAsync("/case/dir/x", null, Ct));
                Assert.NotEqual((await dirs.GetInfoAsync("/case/Dir", Ct)).InodeId, (await dirs.GetInfoAsync("/case/dir", Ct)).InodeId);
            }, Ct);
        });
    }

    // =====================================================================================================
    // PATH-006 Utf8ComponentLimit
    // =====================================================================================================

    private sealed record ComponentCase(Func<string> Build, bool Valid, int NfcBytes);

    private static readonly Dictionary<string, ComponentCase> ComponentCaseTable = new(StringComparer.Ordinal)
    {
        ["Ascii254"] = new(() => PathHelpers.Ascii(254), true, 254),
        ["Ascii255"] = new(() => PathHelpers.Ascii(255), true, 255),
        ["Ascii256"] = new(() => PathHelpers.Ascii(256), false, 256),
        ["TwoByte254"] = new(() => PathHelpers.TwoByte(254), true, 254),
        ["TwoByte255"] = new(() => PathHelpers.TwoByte(255), true, 255),
        ["TwoByte256"] = new(() => PathHelpers.TwoByte(256), false, 256), // only 128 characters
        ["ThreeByte254"] = new(() => PathHelpers.ThreeByte(254), true, 254),
        ["ThreeByte255"] = new(() => PathHelpers.ThreeByte(255), true, 255),
        ["ThreeByte256"] = new(() => PathHelpers.ThreeByte(256), false, 256),
        ["FourByte254"] = new(() => PathHelpers.FourByte(254), true, 254),
        ["FourByte255"] = new(() => PathHelpers.FourByte(255), true, 255),
        ["FourByte256"] = new(() => PathHelpers.FourByte(256), false, 256), // 64 surrogate pairs
        // Raw input longer than the limit, NFC within it: accepted (limits apply after normalization).
        ["DecomposedNfc255"] = new(() => string.Concat(Enumerable.Repeat("o\u0308", 127)) + "a", true, 255),
        ["DecomposedNfc256"] = new(() => string.Concat(Enumerable.Repeat("o\u0308", 128)), false, 256),
        ["OhmSignNfc255"] = new(() => new string('\u2126', 127) + "a", true, 255),
        ["OhmSignNfc256"] = new(() => new string('\u2126', 128), false, 256),
        // Raw input well within the limit, NFC beyond it: rejected (U+0958 expands to U+0915 U+093C).
        ["ExpandingNfc252"] = new(() => new string('\u0958', 42), true, 252),
        ["ExpandingNfc255"] = new(() => new string('\u0958', 42) + "aaa", true, 255),
        ["ExpandingNfc256"] = new(() => new string('\u0958', 42) + "aaaa", false, 256),
        ["ExpandingNfc258"] = new(() => new string('\u0958', 43), false, 258),
    };

    public static TheoryData<string> ComponentCases => new(ComponentCaseTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(ComponentCases))]
    [Catalog("PATH-006", "U", "INV-07", "INV-08")]
    public void PATH_006_Utf8ComponentLimit_Normalize(string caseName)
    {
        ComponentCase c = ComponentCaseTable[caseName];
        string name = c.Build();
        string nfc = PathHelpers.OracleNfc(name);
        Assert.Equal(c.NfcBytes, Encoding.UTF8.GetByteCount(nfc)); // table sanity (independent oracle)
        if (c.Valid)
        {
            Assert.Equal("/" + nfc, DfsPath.Normalize("/" + name));
            Assert.Equal("/dir/" + nfc, DfsPath.Normalize("/dir/" + name));
            DfsPath.ValidateComponent(name);
            Assert.Equal("/" + nfc, DfsPath.Combine("/", name));
        }
        else
        {
            AssertInvalidPath("/" + name);
            AssertInvalidPath("/dir/" + name + "/x");
            ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() => DfsPath.ValidateComponent(name));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        }
    }

    [Fact]
    [Catalog("PATH-006", "U", "INV-07", "INV-08")]
    public async Task PATH_006_Utf8ComponentLimit()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/c", Ct);
            await volume.Directories.CreateDirectoryAsync("/raw", Ct);

            var expected = new List<string>();
            ulong seed = 600;
            foreach ((string caseName, ComponentCase c) in ComponentCaseTable.Where(kv => kv.Value.Valid))
            {
                string name = c.Build();
                string nfc = PathHelpers.OracleNfc(name);
                byte[] bytes = TestData.Bytes(seed++, 50);
                await volume.Files.WriteAllBytesAsync("/c/" + name, bytes, Ct);
                DfsFileInfo info = await volume.Files.GetInfoAsync("/c/" + nfc, Ct);
                Assert.Equal("/c/" + nfc, info.Path);
                await SdkHelpers.AssertBytesEqualAsync(bytes, volume.Files.ReadAllBytesAsync("/c/" + name, Ct), caseName);
                expected.Add(nfc);
            }

            List<string> listed = await SdkHelpers.ListNamesAsync(volume.Directories, "/c");
            Assert.Equal(expected.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal), listed.Order(StringComparer.Ordinal));
            Assert.All(listed, n => Assert.True(Encoding.UTF8.GetByteCount(n) <= DfsPath.MaxComponentBytes, $"stored name of {Encoding.UTF8.GetByteCount(n)} bytes"));

            // The SDK rejects every over-limit component before sending anything.
            PathHelpers.PathOperationContext context = await PathHelpers.PrepareAsync(volume);
            await PathHelpers.AssertRejectedLocallyAsync(cluster, context,
                ComponentCaseTable.Values.Where(c => !c.Valid).Select(c => "/c/" + c.Build()));

            // The server applies the same byte limit after normalization when SDK validation is bypassed.
            await using RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            foreach ((string caseName, ComponentCase c) in ComponentCaseTable)
            {
                DfsErrorCode code = await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, "/raw/" + c.Build());
                Assert.True(c.Valid ? code == DfsErrorCode.None : code == DfsErrorCode.InvalidPath, $"server returned {code} for {caseName} (valid={c.Valid})");
            }

            List<string> rawListed = await SdkHelpers.ListNamesAsync(volume.Directories, "/raw");
            Assert.Equal(ComponentCaseTable.Values.Where(c => c.Valid).Select(c => PathHelpers.OracleNfc(c.Build())).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal),
                rawListed.Order(StringComparer.Ordinal));
            await context.Transaction.AbortAsync(Ct);
        }, Ct);
    }

    // =====================================================================================================
    // PATH-007 Utf8PathLimit
    // =====================================================================================================

    private sealed record PathLengthCase(Func<string> Build, bool Valid, int NfcBytes, bool UseThroughSdk);

    private static string TwoByteO(int bytes) => new string('\u00F6', bytes / 2) + new string('a', bytes % 2);

    private static string Qa(int bytes) => string.Concat(Enumerable.Repeat("\u0915\u093C", bytes / 6)) + new string('a', bytes % 6);

    private static string DecomposeO(string nfc) => nfc.Replace("\u00F6", "o\u0308", StringComparison.Ordinal);

    private static string ComposeQa(string nfc) => nfc.Replace("\u0915\u093C", "\u0958", StringComparison.Ordinal);

    private static readonly Dictionary<string, PathLengthCase> PathLengthCaseTable = new(StringComparer.Ordinal)
    {
        ["Ascii4095"] = new(() => PathHelpers.BuildPath(4095, n => PathHelpers.Ascii(n, 'a')), true, 4095, true),
        ["Ascii4096"] = new(() => PathHelpers.BuildPath(4096, n => PathHelpers.Ascii(n, 'b')), true, 4096, true),
        ["Ascii4097"] = new(() => PathHelpers.BuildPath(4097, n => PathHelpers.Ascii(n, 'c')), false, 4097, true),
        ["TwoByte4095"] = new(() => PathHelpers.BuildPath(4095, PathHelpers.TwoByte), true, 4095, false),
        ["TwoByte4096"] = new(() => PathHelpers.BuildPath(4096, PathHelpers.TwoByte), true, 4096, true),
        ["TwoByte4097"] = new(() => PathHelpers.BuildPath(4097, PathHelpers.TwoByte), false, 4097, true),
        ["ThreeByte4095"] = new(() => PathHelpers.BuildPath(4095, PathHelpers.ThreeByte), true, 4095, false),
        ["ThreeByte4096"] = new(() => PathHelpers.BuildPath(4096, PathHelpers.ThreeByte), true, 4096, true),
        ["ThreeByte4097"] = new(() => PathHelpers.BuildPath(4097, PathHelpers.ThreeByte), false, 4097, true),
        ["FourByte4095"] = new(() => PathHelpers.BuildPath(4095, PathHelpers.FourByte), true, 4095, false),
        ["FourByte4096"] = new(() => PathHelpers.BuildPath(4096, PathHelpers.FourByte), true, 4096, true),
        ["FourByte4097"] = new(() => PathHelpers.BuildPath(4097, PathHelpers.FourByte), false, 4097, true),
        ["Decomposed4096"] = new(() => DecomposeO(PathHelpers.BuildPath(4096, TwoByteO)), true, 4096, true),
        ["Decomposed4097"] = new(() => DecomposeO(PathHelpers.BuildPath(4097, TwoByteO)), false, 4097, true),
        ["Expanding4096"] = new(() => ComposeQa(PathHelpers.BuildPath(4096, Qa)), true, 4096, true),
        ["Expanding4097"] = new(() => ComposeQa(PathHelpers.BuildPath(4097, Qa)), false, 4097, true),
        ["Depth2048Ascii4096"] = new(() => string.Concat(Enumerable.Repeat("/a", 2048)), true, 4096, false),
        ["Depth2048Ascii4097"] = new(() => string.Concat(Enumerable.Repeat("/a", 2048)) + "b", false, 4097, true),
    };

    public static TheoryData<string> PathLengthCases => new(PathLengthCaseTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(PathLengthCases))]
    [Catalog("PATH-007", "U", "INV-07", "INV-08")]
    public void PATH_007_Utf8PathLimit_Normalize(string caseName)
    {
        PathLengthCase c = PathLengthCaseTable[caseName];
        string path = c.Build();
        string nfc = PathHelpers.OracleNfc(path);
        Assert.Equal(c.NfcBytes, Encoding.UTF8.GetByteCount(nfc)); // table sanity (independent oracle)
        if (c.Valid)
        {
            Assert.Equal(nfc, DfsPath.Normalize(path));
            Assert.True(DfsPath.TryNormalize(path, out string normalized, out _));
            Assert.Equal(nfc, normalized);
        }
        else
        {
            AssertInvalidPath(path);
        }
    }

    [Fact]
    [Catalog("PATH-007", "U", "INV-07", "INV-08")]
    public void PATH_007_Utf8PathLimit_CombineRejectsOverflow()
    {
        string at4095 = PathHelpers.BuildPath(4095, n => PathHelpers.Ascii(n));
        string parent = DfsPath.GetParent(at4095);
        int parentBytes = Encoding.UTF8.GetByteCount(parent);
        // parent + "/" + name: exactly 4096 bytes is accepted, 4097 rejected.
        Assert.Equal(4096, Encoding.UTF8.GetByteCount(DfsPath.Combine(parent, PathHelpers.Ascii(4096 - parentBytes - 1, 'z'))));
        Assert.ThrowsAny<ArgumentException>(() => DfsPath.Combine(parent, PathHelpers.Ascii(4097 - parentBytes - 1, 'z')));
        // A name whose raw UTF-8 fits but whose NFC form (U+0958 -> U+0915 U+093C) makes the path 4097 bytes is rejected.
        int nameBytes = 4097 - parentBytes - 1;
        string expanding = new string('\u0958', nameBytes / 6) + PathHelpers.Ascii(nameBytes % 6, 'z');
        Assert.True(parentBytes + 1 + Encoding.UTF8.GetByteCount(expanding) <= 4096, "raw form must fit so only normalization exceeds the limit");
        Assert.Equal(4097, parentBytes + 1 + Encoding.UTF8.GetByteCount(PathHelpers.OracleNfc(expanding)));
        Assert.ThrowsAny<ArgumentException>(() => DfsPath.Combine(parent, expanding));
        AssertInvalidPath(parent + "/" + expanding);
    }

    [Fact]
    [Catalog("PATH-007", "U", "INV-07", "INV-08")]
    public async Task PATH_007_Utf8PathLimit()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            ulong seed = 700;
            string? asciiParent = null;
            foreach ((string caseName, PathLengthCase c) in PathLengthCaseTable.Where(kv => kv.Value.Valid && kv.Value.UseThroughSdk))
            {
                string path = c.Build();
                string nfc = PathHelpers.OracleNfc(path);
                byte[] bytes = TestData.Bytes(seed++, 40);
                await volume.Directories.CreateDirectoryAsync(DfsPath.GetParent(nfc), Ct);
                await volume.Files.WriteAllBytesAsync(path, bytes, Ct);
                DfsFileInfo info = await volume.Files.GetInfoAsync(nfc, Ct);
                Assert.Equal(nfc, info.Path);
                Assert.Equal(c.NfcBytes, Encoding.UTF8.GetByteCount(info.Path));
                await SdkHelpers.AssertBytesEqualAsync(bytes, volume.Files.ReadAllBytesAsync(path, Ct), caseName);
                Assert.Contains(DfsPath.GetName(nfc), await SdkHelpers.ListNamesAsync(volume.Directories, DfsPath.GetParent(nfc)));
                if (caseName == "Ascii4096")
                    asciiParent = DfsPath.GetParent(nfc);
            }

            // Client: excess rejected before any request.
            PathHelpers.PathOperationContext context = await PathHelpers.PrepareAsync(volume);
            await PathHelpers.AssertRejectedLocallyAsync(cluster, context,
                PathLengthCaseTable.Values.Where(c => !c.Valid && c.UseThroughSdk).Select(c => c.Build()));

            // Server: the same boundary, under an existing parent so only the total length decides.
            Assert.NotNull(asciiParent);
            int parentBytes = Encoding.UTF8.GetByteCount(asciiParent);
            string serverAt4096 = asciiParent + "/" + PathHelpers.Ascii(4096 - parentBytes - 1, 'd');
            string serverAt4097 = asciiParent + "/" + PathHelpers.Ascii(4097 - parentBytes - 1, 'e');
            string serverExpanding4097 = asciiParent + "/" + ComposeQa(Qa(4097 - parentBytes - 1));
            await using RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, serverAt4096));
            Assert.Equal(DfsErrorCode.InvalidPath, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, serverAt4097));
            Assert.Equal(DfsErrorCode.InvalidPath, await raw.SendPathOperationAsync(Volume, DfsOperationKind.WriteAll, serverAt4097));
            Assert.Equal(DfsErrorCode.InvalidPath, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, serverExpanding4097));
            Assert.True(await volume.Directories.ExistsAsync(serverAt4096, Ct));
            List<string> siblings = await SdkHelpers.ListNamesAsync(volume.Directories, asciiParent);
            Assert.DoesNotContain(siblings, n => n.StartsWith('e') || n.Contains('\u0915', StringComparison.Ordinal));
            await context.Transaction.AbortAsync(Ct);
        }, Ct);
    }

    // =====================================================================================================
    // PATH-008 CultureCannotChangeLookup
    // =====================================================================================================

    private static readonly string[] CultureSensitiveNames =
    [
        "I", "i", "\u0130", "\u0131", "I\u0307", "i\u0307", "Stra\u00DFe", "STRASSE", "strasse", "\u01C4", "\u01C5", "\u01C6",
        "\u03A3\u03C3\u03C2", "IJ", "\u0132", "e\u0301", "\u00E9", "\u00C9", "Ii", "iI", "\u00E6", "ae",
    ];

    [Theory, MemberData(nameof(Cultures))]
    [Catalog("PATH-008", "U", "INV-07")]
    public void PATH_008_CultureCannotChangeLookup_Normalize(string culture)
    {
        string[] invariant = PathHelpers.WithCulture("", () => CultureSensitiveNames.Select(n => DfsPath.Normalize("/" + n)).ToArray());
        string[] underCulture = PathHelpers.WithCulture(culture, () => CultureSensitiveNames.Select(n => DfsPath.Normalize("/" + n)).ToArray());
        Assert.Equal(invariant, underCulture);
        for (int i = 0; i < invariant.Length; i++)
            Assert.Equal("/" + PathHelpers.OracleNfc(CultureSensitiveNames[i]), underCulture[i]);

        // Identity (ordinal equality of normalized forms) is the same matrix under every culture.
        bool[,] matrix = PathHelpers.WithCulture(culture, () =>
        {
            var m = new bool[invariant.Length, invariant.Length];
            for (int a = 0; a < invariant.Length; a++)
            {
                for (int b = 0; b < invariant.Length; b++)
                    m[a, b] = DfsPath.IsAncestorOrSelf(DfsPath.Normalize("/" + CultureSensitiveNames[a]), DfsPath.Normalize("/" + CultureSensitiveNames[b]));
            }

            return m;
        });
        for (int a = 0; a < invariant.Length; a++)
        {
            for (int b = 0; b < invariant.Length; b++)
                Assert.Equal(string.Equals(invariant[a], invariant[b], StringComparison.Ordinal), matrix[a, b]);
        }
    }

    [Fact]
    [Catalog("PATH-008", "U", "INV-07")]
    public async Task PATH_008_CultureCannotChangeLookup()
    {
        string[] cultures = ["en-US", "tr-TR", "az-Latn-AZ", "lt-LT", "el-GR", "de-DE", ""];
        await PathHelpers.WithCultureAsync("en-US", async () =>
        {
            await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
            {
                await using DfsClient client = await cluster.ConnectAsync(null, Ct);
                DfsFileClient files = client.GetVolume(Volume).Files;
                DfsDirectoryClient dirs = client.GetVolume(Volume).Directories;
                await dirs.CreateDirectoryAsync("/cult", Ct);

                // Created under en-US; the NFC-distinct names among CultureSensitiveNames.
                var created = new Dictionary<string, InodeId>(StringComparer.Ordinal);
                ulong seed = 800;
                foreach (string name in CultureSensitiveNames)
                {
                    string nfc = PathHelpers.OracleNfc(name);
                    if (created.ContainsKey(nfc))
                        continue;
                    await files.WriteAllBytesAsync("/cult/" + name, TestData.Bytes(seed++, 16), Ct);
                    created[nfc] = (await files.GetInfoAsync("/cult/" + name, Ct)).InodeId;
                }

                string[] expectedOrder = created.Keys.Order(StringComparer.Ordinal).ToArray();
                List<string>? firstListing = null;
                foreach (string culture in cultures)
                {
                    await PathHelpers.WithCultureAsync(culture, async () =>
                    {
                        // Every spelling resolves to the inode of its own NFC form, never to a case-folded neighbour.
                        foreach (string name in CultureSensitiveNames)
                        {
                            InodeId inode = (await files.GetInfoAsync("/cult/" + name, Ct)).InodeId;
                            Assert.True(inode == created[PathHelpers.OracleNfc(name)], $"culture '{culture}': '/cult/{PathHelpers.Describe(name)}' resolved to the wrong inode");
                        }

                        List<string> listing = await SdkHelpers.ListNamesAsync(dirs, "/cult");
                        firstListing ??= listing;
                        Assert.Equal(firstListing, listing);
                        Assert.Equal(expectedOrder.Order(StringComparer.Ordinal), listing.Order(StringComparer.Ordinal));

                        // Wildcard matching is ordinal: "i*" never matches I, İ or ı.
                        var page = await dirs.ListPageAsync("/cult", new DfsEnumerationOptions { Pattern = "i*" }, null, Ct);
                        Assert.Equal(expectedOrder.Where(n => n.StartsWith('i')).Order(StringComparer.Ordinal), page.Entries.Select(e => e.Name).Order(StringComparer.Ordinal));
                        var upperPage = await dirs.ListPageAsync("/cult", new DfsEnumerationOptions { Pattern = "I*" }, null, Ct);
                        Assert.Equal(expectedOrder.Where(n => n.StartsWith('I')).Order(StringComparer.Ordinal), upperPage.Entries.Select(e => e.Name).Order(StringComparer.Ordinal));

                        Assert.False(await files.ExistsAsync("/cult/\u0131i", Ct));
                        Assert.False(await files.ExistsAsync("/cult/STRA\u00DFE", Ct));
                    });
                }
            }, Ct);
        });
    }

    // =====================================================================================================
    // PATH-009 NoUrlDoubleDecoding
    // =====================================================================================================

    private static readonly Dictionary<string, (string Path, string[] Components)> PercentCaseTable = new(StringComparer.Ordinal)
    {
        ["EncodedSlash"] = ("/a%2Fb", ["a%2Fb"]),
        ["EncodedSlashLower"] = ("/a%2fb", ["a%2fb"]),
        ["EncodedDotDot"] = ("/%2E%2E", ["%2E%2E"]),
        ["EncodedDotDotLowerDir"] = ("/%2e%2e/x", ["%2e%2e", "x"]),
        ["MixedDotDot"] = ("/.%2E/x", [".%2E", "x"]),
        ["EncodedBackslash"] = ("/a%5Cb", ["a%5Cb"]),
        ["EncodedNul"] = ("/a%00b", ["a%00b"]),
        ["DoubleEncodedSlash"] = ("/a%252Fb", ["a%252Fb"]),
        ["EncodedUtf8"] = ("/%C3%A9", ["%C3%A9"]),
        ["PercentAlone"] = ("/%", ["%"]),
        ["PlusSign"] = ("/a+b", ["a+b"]),
        ["EncodedTraversalEscape"] = ("/dir/%2E%2E%2F%2E%2E%2Fother", ["dir", "%2E%2E%2F%2E%2E%2Fother"]),
        ["QueryAndFragment"] = ("/a?b=c#d", ["a?b=c#d"]),
    };

    public static TheoryData<string> PercentCases => new(PercentCaseTable.Keys.Order(StringComparer.Ordinal));

    [Theory, MemberData(nameof(PercentCases))]
    [Catalog("PATH-009", "U", "INV-07", "INV-12")]
    public void PATH_009_NoUrlDoubleDecoding_Normalize(string caseName)
    {
        (string path, string[] components) = PercentCaseTable[caseName];
        Assert.Equal(path, DfsPath.Normalize(path));
        Assert.Equal(components, DfsPath.Split(path));
        Assert.Equal(components[^1], DfsPath.GetName(path));
    }

    [Fact]
    [Catalog("PATH-009", "U", "INV-07", "INV-12")]
    public async Task PATH_009_NoUrlDoubleDecoding()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsFileClient files = client.GetVolume(Volume).Files;
            DfsDirectoryClient dirs = client.GetVolume(Volume).Directories;
            await dirs.CreateDirectoryAsync("/dir", Ct);
            await dirs.CreateDirectoryAsync("/other-sentinel", Ct);

            foreach ((string caseName, (string path, _)) in PercentCaseTable)
            {
                if (DfsPath.GetParent(path) != "/")
                    await dirs.CreateDirectoryAsync(DfsPath.GetParent(path), Ct);
                await files.WriteAllBytesAsync(path, Encoding.UTF8.GetBytes(caseName), Ct);
            }

            foreach ((string caseName, (string path, string[] components)) in PercentCaseTable)
            {
                Assert.Equal(caseName, Encoding.UTF8.GetString(await files.ReadAllBytesAsync(path, Ct)));
                Assert.Contains(components[^1], await SdkHelpers.ListNamesAsync(dirs, DfsPath.GetParent(path)));
            }

            // Nothing was decoded into separators, traversal, NUL or other characters.
            Assert.False(await dirs.ExistsAsync("/a", Ct));
            Assert.False(await files.ExistsAsync("/a", Ct));
            Assert.False(await files.ExistsAsync("/\u00E9", Ct));
            Assert.False(await files.ExistsAsync("/other", Ct));
            Assert.NotEqual((await files.GetInfoAsync("/a%2Fb", Ct)).InodeId, (await files.GetInfoAsync("/a%2fb", Ct)).InodeId);
            Assert.Equal(new[] { "%2E%2E%2F%2E%2E%2Fother" }, await SdkHelpers.ListNamesAsync(dirs, "/dir"));
            Assert.Empty(await SdkHelpers.ListNamesAsync(dirs, "/other-sentinel"));

            // Through the wire API (no SDK involvement) the server treats percent sequences literally as well.
            await using RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, "/raw%2Fdir"));
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.GetInfo, "/%2E%2E"));
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, "/dir/%2E%2E%2F..%2Fescape"));
            Assert.Equal(DfsErrorCode.InvalidPath, await raw.SendPathOperationAsync(Volume, DfsOperationKind.GetInfo, "/.."));
            Assert.True(await dirs.ExistsAsync("/raw%2Fdir", Ct));
            Assert.False(await dirs.ExistsAsync("/raw", Ct));
            Assert.Contains("%2E%2E%2F..%2Fescape", await SdkHelpers.ListNamesAsync(dirs, "/dir"));
            Assert.False(await dirs.ExistsAsync("/escape", Ct));
        }, Ct);
    }

    // =====================================================================================================
    // PATH-010 RootProtection
    // =====================================================================================================

    [Fact]
    [Catalog("PATH-010", "U", "INV-07", "INV-12")]
    public async Task PATH_010_RootProtection()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            DfsFileClient files = volume.Files;
            DfsDirectoryClient dirs = volume.Directories;
            byte[] keep = TestData.Bytes(1001, 5000);
            await dirs.CreateDirectoryAsync("/keep", Ct);
            await dirs.CreateDirectoryAsync("/keep2", Ct);
            await files.WriteAllBytesAsync("/keep/file.bin", keep, Ct);
            await files.WriteAllBytesAsync("/f.bin", TestData.Bytes(1002, 10), Ct);
            string[] rootNames = (await SdkHelpers.ListNamesAsync(dirs, "/")).Order(StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "f.bin", "keep", "keep2" }, rootNames);
            InodeId keepInode = (await dirs.GetInfoAsync("/keep", Ct)).InodeId;
            InodeId fileInode = (await files.GetInfoAsync("/keep/file.bin", Ct)).InodeId;

            var write = new DfsOpenOptions { Access = FileAccess.Write, Share = FileShare.None };
            var attempts = new (string Name, Func<Task> Action, bool AllowNotEmpty)[]
            {
                ("Directories.Delete(/, nonrecursive)", () => dirs.DeleteAsync("/", false, Ct), true),
                ("Directories.Delete(/, recursive)", () => dirs.DeleteAsync("/", true, Ct), false),
                ("Directories.Move(/ -> /moved-root)", () => dirs.MoveAsync("/", "/moved-root", Ct), false),
                ("Directories.Move(/ -> /keep2/inside)", () => dirs.MoveAsync("/", "/keep2/inside", Ct), false),
                ("Directories.Move(/keep2 -> /)", () => dirs.MoveAsync("/keep2", "/", Ct), false),
                ("Files.Delete(/)", () => files.DeleteAsync("/", Ct), false),
                ("Files.Move(/ -> /x)", () => files.MoveAsync("/", "/x", false, Ct), false),
                ("Files.Move(/f.bin -> /, overwrite)", () => files.MoveAsync("/f.bin", "/", true, Ct), false),
                ("Files.Move(/f.bin -> /)", () => files.MoveAsync("/f.bin", "/", false, Ct), false),
                ("Files.Copy(/f.bin -> /, overwrite)", () => files.CopyAsync("/f.bin", "/", true, Ct), false),
                ("Files.Copy(/ -> /x)", () => files.CopyAsync("/", "/x", false, Ct), false),
                ("Files.WriteAllBytes(/)", () => files.WriteAllBytesAsync("/", keep, Ct), false),
                ("Files.WriteAllText(/)", () => files.WriteAllTextAsync("/", "x", null, Ct), false),
                ("Files.AppendAllBytes(/)", () => files.AppendAllBytesAsync("/", keep, Ct), false),
                ("Files.Create(/)", async () => { await using DfsStream s = await files.CreateAsync("/", Ct); }, false),
                ("Files.OpenWrite(/)", async () => { await using DfsStream s = await files.OpenWriteAsync("/", Ct); }, false),
                ("Files.Open(/, CreateNew)", async () => { await using DfsStream s = await files.OpenAsync("/", write with { Mode = FileMode.CreateNew }, Ct); }, false),
                ("Files.Open(/, Create)", async () => { await using DfsStream s = await files.OpenAsync("/", write with { Mode = FileMode.Create }, Ct); }, false),
                ("Files.Open(/, OpenOrCreate)", async () => { await using DfsStream s = await files.OpenAsync("/", write with { Mode = FileMode.OpenOrCreate }, Ct); }, false),
                ("Files.Open(/, Truncate)", async () => { await using DfsStream s = await files.OpenAsync("/", write with { Mode = FileMode.Truncate }, Ct); }, false),
                ("Files.Open(/, Append)", async () => { await using DfsStream s = await files.OpenAsync("/", write with { Mode = FileMode.Append }, Ct); }, false),
                ("Files.Delete(/) sync", () => { files.Delete("/"); return Task.CompletedTask; }, false),
            };

            var failures = new List<string>();
            foreach ((string name, Func<Task> action, bool allowNotEmpty) in attempts)
            {
                Exception? ex = await Record.ExceptionAsync(action);
                string? problem = DescribeRootRejectionProblem(ex, allowNotEmpty);
                if (problem is not null)
                    failures.Add($"{name}: {problem}");
            }

            // Transaction-bound attempts: rejected, and committing the transaction afterwards changes nothing.
            DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct);
            foreach ((string name, Func<Task> action) in new (string, Func<Task>)[]
            {
                ("tx.Directories.Delete(/, recursive)", () => tx.Directories.DeleteAsync("/", true, Ct)),
                ("tx.Directories.Move(/ -> /y)", () => tx.Directories.MoveAsync("/", "/y", Ct)),
                ("tx.Files.WriteAllBytes(/)", () => tx.Files.WriteAllBytesAsync("/", keep, Ct)),
                ("tx.Files.Delete(/)", () => tx.Files.DeleteAsync("/", Ct)),
            })
            {
                Exception? ex = await Record.ExceptionAsync(action);
                string? problem = DescribeRootRejectionProblem(ex, allowNotEmpty: false);
                if (problem is not null)
                    failures.Add($"{name}: {problem}");
            }

            Exception? commitError = await Record.ExceptionAsync(() => tx.CommitAsync(Ct));
            Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
            Assert.True(commitError is null || commitError is DfsTransactionAbortedException or DfsTransactionConflictException,
                $"committing the transaction after rejected root operations failed unexpectedly: {commitError}");

            await AssertRootIntactAsync();

            // Valid root operations keep working.
            DfsDirectoryInfo root = await dirs.GetInfoAsync("/", Ct);
            Assert.Equal(InodeId.Root, root.InodeId);
            Assert.Equal("/", root.Path);
            Assert.Equal(rootNames.Length, root.EntryCount);
            DfsDirectoryPage page = await dirs.ListPageAsync("/", new DfsEnumerationOptions { PageSize = 2 }, null, Ct);
            Assert.Equal(2, page.Entries.Count);
            Assert.NotNull(page.ContinuationToken);
            DfsDirectoryPage rest = await dirs.ListPageAsync("/", new DfsEnumerationOptions { PageSize = 2 }, page.ContinuationToken, Ct);
            Assert.Equal(rootNames, page.Entries.Concat(rest.Entries).Select(e => e.Name).Order(StringComparer.Ordinal));
            Exception? createRoot = await Record.ExceptionAsync(() => dirs.CreateDirectoryAsync("/", Ct));
            Assert.True(createRoot is null || createRoot is DfsAlreadyExistsException or ArgumentException, $"CreateDirectory(/) on the existing root: {createRoot}");

            // The server protects the root on its own when SDK validation is bypassed.
            await using RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            Assert.NotEqual(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Delete, "/"));
            Assert.NotEqual(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Move, "/", "/raw-moved-root"));
            Assert.NotEqual(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Move, "/keep2", "/"));
            Assert.NotEqual(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.WriteAll, "/"));
            Assert.NotEqual(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Open, "/"));
            await AssertRootIntactAsync();

            async Task AssertRootIntactAsync()
            {
                Assert.True(await dirs.ExistsAsync("/", Ct));
                Assert.Equal(InodeId.Root, (await dirs.GetInfoAsync("/", Ct)).InodeId);
                Assert.Equal(rootNames, (await SdkHelpers.ListNamesAsync(dirs, "/")).Order(StringComparer.Ordinal));
                Assert.Equal(keepInode, (await dirs.GetInfoAsync("/keep", Ct)).InodeId);
                Assert.Equal(fileInode, (await files.GetInfoAsync("/keep/file.bin", Ct)).InodeId);
                await SdkHelpers.AssertBytesEqualAsync(keep, files.ReadAllBytesAsync("/keep/file.bin", Ct));
                foreach (string ghost in new[] { "/moved-root", "/x", "/y", "/raw-moved-root", "/keep2/inside" })
                    Assert.False(await dirs.ExistsAsync(ghost, Ct) || await files.ExistsAsync(ghost, Ct), $"{ghost} must not exist");
            }
        }, Ct);
    }

    /// <summary>Null when <paramref name="ex"/> is an acceptable rejection of an operation on/over the root; otherwise the problem.</summary>
    private static string? DescribeRootRejectionProblem(Exception? ex, bool allowNotEmpty)
    {
        if (ex is null)
            return "completed instead of being rejected";
        ex = SdkHelpers.Unwrap(ex);
        if (ex is FileNotFoundException or DirectoryNotFoundException)
            return $"reported the root as missing: {ex.GetType().Name}";
        bool ok = ex switch
        {
            ArgumentException => true,
            UnauthorizedAccessException => true,
            DfsException { ErrorCode: DfsErrorCode.DirectoryNotEmpty } => allowNotEmpty,
            DfsException d => d.ErrorCode is DfsErrorCode.IsADirectory or DfsErrorCode.InvalidArgument or DfsErrorCode.InvalidPath
                or DfsErrorCode.AccessDenied or DfsErrorCode.AlreadyExists or DfsErrorCode.NotADirectory,
            _ => false,
        };
        return ok ? null : $"unexpected {ex.GetType().Name} ({DfsErrors.ToCode(ex)}): {ex.Message}";
    }

    // =====================================================================================================
    // PATH-012 ServerRevalidatesPaths
    // =====================================================================================================

    [Fact]
    [Catalog("PATH-012", "U", "INV-07", "INV-08", "INV-12")]
    public async Task PATH_012_ServerRevalidatesPaths()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            await volume.Directories.CreateDirectoryAsync("/raw-src", Ct);
            await volume.Directories.CreateDirectoryAsync("/a", Ct);
            RawProtocolClient raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
            try
            {
                // Control: the raw client is able to mutate with valid paths (so rejections below are meaningful).
                Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, "/raw-control"));
                Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.GetInfo, "/raw-control"));
                Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Move, "/raw-control", "/raw-control2"));
                Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Delete, "/raw-control2"));
                Assert.False(await volume.Directories.ExistsAsync("/raw-control2", Ct));

                int inodesBefore = await SdkHelpers.InodeCountAsync(cluster);
                List<string> rootBefore = await SdkHelpers.ListNamesAsync(volume.Directories, "/");
                string[] invalid =
                [
                    "", "a", "a/b", "//", "/a/", "/a//b", "/.", "/..", "/a/./b", "/a/../b", "/a/..", "/../vol2/x", "/a\0b",
                    "/a\\b", "\\a", "/" + PathHelpers.Ascii(256), "/a/" + PathHelpers.TwoByte(256), "/a/" + new string('\u0958', 43),
                    PathHelpers.BuildPath(4097, n => PathHelpers.Ascii(n)), PathHelpers.BuildPath(4097, PathHelpers.ThreeByte),
                    "/a/" + string.Concat(Enumerable.Repeat("e\u0301", 128)),
                ];
                DfsOperationKind[] singleKinds = [DfsOperationKind.CreateDirectory, DfsOperationKind.Delete, DfsOperationKind.Open, DfsOperationKind.GetInfo, DfsOperationKind.WriteAll];
                var failures = new List<string>();
                foreach (string path in invalid)
                {
                    foreach (DfsOperationKind kind in singleKinds)
                    {
                        DfsErrorCode code = await raw.SendPathOperationAsync(Volume, kind, path);
                        if (code != DfsErrorCode.InvalidPath)
                            failures.Add($"{kind}({PathHelpers.Describe(path)}) returned {code}");
                    }

                    DfsErrorCode asSource = await raw.SendPathOperationAsync(Volume, DfsOperationKind.Move, path, "/raw-dst");
                    if (asSource != DfsErrorCode.InvalidPath)
                        failures.Add($"Move({PathHelpers.Describe(path)} -> /raw-dst) returned {asSource}");
                    DfsErrorCode asDestination = await raw.SendPathOperationAsync(Volume, DfsOperationKind.Move, "/raw-src", path);
                    if (asDestination != DfsErrorCode.InvalidPath)
                        failures.Add($"Move(/raw-src -> {PathHelpers.Describe(path)}) returned {asDestination}");
                }

                // Malformed wire UTF-8 (including encoded lone surrogates) is rejected by the server as well.
                foreach ((string caseName, byte[] bytes) in MalformedUtf8Table)
                {
                    foreach (DfsOperationKind kind in singleKinds)
                    {
                        if (!raw.IsConnected)
                        {
                            await raw.DisposeAsync();
                            raw = await PathHelpers.ConnectRawToLeaderAsync(cluster);
                        }

                        DfsErrorCode code = await raw.SendRawPathBytesAsync(Volume, kind, bytes);
                        if (code is not (DfsErrorCode.InvalidPath or DfsErrorCode.ProtocolError))
                            failures.Add($"{kind}(malformed {caseName}) returned {code}");
                    }
                }

                Assert.True(failures.Count == 0, $"{failures.Count} invalid raw requests were not rejected:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");

                // Nothing changed: no partial effects of rejected requests.
                Assert.Equal(inodesBefore, await SdkHelpers.InodeCountAsync(cluster));
                Assert.Equal(rootBefore, await SdkHelpers.ListNamesAsync(volume.Directories, "/"));
                Assert.Empty(await SdkHelpers.ListNamesAsync(volume.Directories, "/a"));
                Assert.True(await volume.Directories.ExistsAsync("/raw-src", Ct));
                Assert.False(await volume.Directories.ExistsAsync("/raw-dst", Ct));
                Assert.False(await volume.Directories.ExistsAsync("/b", Ct));
                Assert.False(await volume.Directories.ExistsAsync("/vol2", Ct));
            }
            finally
            {
                await raw.DisposeAsync();
            }
        }, Ct);
    }

    // =====================================================================================================
    // PATH-013 OffsetsDoNotOverflow
    // =====================================================================================================

    private const long MaxFileLength = 1L << 20;

    public static TheoryData<string> OffsetChecks => new(
        "SeekBeforeBegin", "SeekOverflow", "PositionNegative", "InvalidSeekOrigin", "SetLengthNegative", "SetLengthAboveMax",
        "WriteBeyondMax", "WriteAtLongMaxPosition", "ReadAtLongMaxPosition", "BufferArguments", "CopyToBufferSize", "EnumerationPageSize");

    [Theory, MemberData(nameof(OffsetChecks))]
    [Catalog("PATH-013", "U", "INV-07", "INV-08")]
    public async Task PATH_013_OffsetsDoNotOverflow(string check)
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with
        {
            Volumes = [new TestVolumeSpec(Volume) { Limits = new DfsVolumeLimits { MaxFileLength = MaxFileLength } }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume volume = client.GetVolume(Volume);
            DfsFileClient files = volume.Files;
            const string FilePath = "/p013.bin";
            byte[] initial = TestData.Bytes(1300, 10_000);
            await files.WriteAllBytesAsync(FilePath, initial, Ct);
            Assert.Equal(MaxFileLength, (await volume.GetInfoAsync(Ct)).Limits.MaxFileLength);

            if (check == "ReadAtLongMaxPosition")
            {
                await CheckReadAtLongMaxAsync(files, FilePath, initial);
                return;
            }

            if (check == "EnumerationPageSize")
            {
                await CheckPageSizesAsync(volume.Directories);
                return;
            }

            var model = new ReferenceFileModel(initial);
            DfsStream s = await files.OpenAsync(FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            try
            {
                // A legal staged change first; it must be published exactly (or, if the stream faults, not at all).
                byte[] legal = TestData.Bytes(1301, 50);
                s.Position = 100;
                await s.WriteAsync(legal, Ct);
                model.Write(100, legal);
                var allowedFlushFailures = new List<Type>();

                switch (check)
                {
                    case "SeekBeforeBegin":
                    {
                        s.Position = 10;
                        AssertSeekRejected(() => s.Seek(-1, SeekOrigin.Begin));
                        AssertSeekRejected(() => s.Seek(-11, SeekOrigin.Current));
                        AssertSeekRejected(() => s.Seek(-(initial.Length + 1L), SeekOrigin.End));
                        AssertSeekRejected(() => s.Seek(long.MinValue, SeekOrigin.Current));
                        Assert.Equal(10, s.Position);
                        break;
                    }

                    case "SeekOverflow":
                    {
                        s.Position = 10;
                        AssertSeekRejected(() => s.Seek(long.MaxValue, SeekOrigin.Current));
                        Assert.Equal(10, s.Position);
                        AssertSeekRejected(() => s.Seek(long.MaxValue, SeekOrigin.End));
                        Assert.Equal(10, s.Position);
                        AssertSeekRejected(() => s.Seek(long.MaxValue - 5, SeekOrigin.Current));
                        Assert.Equal(10, s.Position);
                        break;
                    }

                    case "PositionNegative":
                    {
                        s.Position = 10;
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Position = -1);
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Position = long.MinValue);
                        Assert.Equal(10, s.Position);
                        break;
                    }

                    case "InvalidSeekOrigin":
                    {
                        s.Position = 10;
                        Assert.ThrowsAny<ArgumentException>(() => s.Seek(0, (SeekOrigin)3));
                        Assert.ThrowsAny<ArgumentException>(() => s.Seek(0, (SeekOrigin)(-1)));
                        Assert.Equal(10, s.Position);
                        break;
                    }

                    case "SetLengthNegative":
                    {
                        long lengthBefore = await s.GetLengthAsync(Ct);
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.SetLength(-1));
                        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => s.SetLengthAsync(long.MinValue, Ct));
                        Assert.Equal(lengthBefore, await s.GetLengthAsync(Ct));
                        break;
                    }

                    case "SetLengthAboveMax":
                    {
                        // Exactly the advertised maximum is legal (sparse extension); one more byte or long.MaxValue is not.
                        s.SetLength(MaxFileLength);
                        model.SetLength(MaxFileLength);
                        foreach (long excess in new[] { MaxFileLength + 1, long.MaxValue })
                        {
                            Exception? ex = await Record.ExceptionAsync(() => s.SetLengthAsync(excess, Ct));
                            if (ex is not null)
                                AssertLimitRejection(ex);
                            else
                                allowedFlushFailures.Add(typeof(DfsLimitExceededException));
                        }

                        break;
                    }

                    case "WriteBeyondMax":
                    {
                        s.Position = MaxFileLength - 1;
                        s.WriteByte(0x11);
                        model.Write(MaxFileLength - 1, [0x11]);
                        s.Position = MaxFileLength - 1;
                        Exception? two = await Record.ExceptionAsync(() => s.WriteAsync(new byte[] { 0x22, 0x33 }, Ct).AsTask());
                        if (two is not null)
                            AssertLimitRejection(two);
                        else
                            allowedFlushFailures.Add(typeof(DfsLimitExceededException));
                        if (!s.IsFaulted)
                        {
                            s.Position = MaxFileLength;
                            Exception? one = Record.Exception(() => s.WriteByte(0x44));
                            if (one is not null)
                                AssertLimitRejection(one);
                            else
                                allowedFlushFailures.Add(typeof(DfsLimitExceededException));
                        }

                        break;
                    }

                    case "WriteAtLongMaxPosition":
                    {
                        Exception? seek = Record.Exception(() => s.Seek(long.MaxValue - 1, SeekOrigin.Begin));
                        if (seek is not null)
                        {
                            AssertLimitRejection(seek); // rejecting the position itself is also legal
                            Assert.Equal(150, s.Position);
                        }
                        else
                        {
                            Assert.Equal(long.MaxValue - 1, s.Position);
                            Exception? write = await Record.ExceptionAsync(() => s.WriteAsync(new byte[] { 1, 2, 3, 4 }, Ct).AsTask());
                            Assert.NotNull(write);
                            AssertLimitRejection(write);
                            if (!s.IsFaulted)
                            {
                                s.Position = long.MaxValue;
                                AssertLimitRejection(Assert.ThrowsAny<Exception>(() => s.WriteByte(5)));
                            }
                        }

                        break;
                    }

                    case "BufferArguments":
                    {
                        s.Position = 20;
                        byte[] buffer = new byte[16];
                        Assert.Throws<ArgumentNullException>(() => s.Read(null!, 0, 1));
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Read(buffer, -1, 1));
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Read(buffer, 0, -1));
                        Assert.ThrowsAny<ArgumentException>(() => s.Read(buffer, 10, buffer.Length));
                        Assert.ThrowsAny<ArgumentException>(() => s.Read(buffer, int.MaxValue, 1));
                        Assert.ThrowsAny<ArgumentException>(() => s.Read(buffer, 1, int.MaxValue));
                        await Assert.ThrowsAnyAsync<ArgumentException>(() => s.ReadAsync(buffer, -1, 1, Ct));
                        await Assert.ThrowsAnyAsync<ArgumentException>(() => s.ReadAsync(buffer, 1, int.MaxValue, Ct));
                        Assert.Throws<ArgumentNullException>(() => s.Write(null!, 0, 1));
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Write(buffer, -1, 1));
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.Write(buffer, 0, -1));
                        Assert.ThrowsAny<ArgumentException>(() => s.Write(buffer, 10, buffer.Length));
                        Assert.ThrowsAny<ArgumentException>(() => s.Write(buffer, int.MaxValue, 1));
                        await Assert.ThrowsAnyAsync<ArgumentException>(() => s.WriteAsync(buffer, 1, int.MaxValue, Ct));
                        await Assert.ThrowsAnyAsync<ArgumentException>(() => s.WriteAsync(buffer, int.MaxValue, 1, Ct));
                        Assert.Equal(20, s.Position);
                        break;
                    }

                    case "CopyToBufferSize":
                    {
                        s.Position = 30;
                        using var destination = new MemoryStream();
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.CopyTo(destination, 0));
                        Assert.Throws<ArgumentOutOfRangeException>(() => s.CopyTo(destination, -1));
                        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => s.CopyToAsync(destination, 0, Ct));
                        await Assert.ThrowsAsync<ArgumentNullException>(() => s.CopyToAsync(null!, 4096, Ct));
                        Assert.Equal(30, s.Position);
                        Assert.Equal(0, destination.Length);
                        break;
                    }

                    default:
                        throw new ArgumentOutOfRangeException(nameof(check), check, "unknown check");
                }

                // Publish: either exactly the legal changes, or (when the stream faulted / the flush is rejected)
                // nothing at all. Never a partial or wrapped write.
                Exception? flushError = s.IsFaulted ? new InvalidOperationException("faulted") : await Record.ExceptionAsync(() => s.FlushAsync(Ct));
                byte[] committed = await files.ReadAllBytesAsync(FilePath, Ct);
                if (flushError is null)
                {
                    model.Flush();
                    AssertEx.BytesEqual(model.Committed, committed, $"{check}: committed bytes after a successful flush");
                }
                else
                {
                    Assert.True(s.IsFaulted || allowedFlushFailures.Any(t => t.IsInstanceOfType(SdkHelpers.Unwrap(flushError))),
                        $"{check}: unexpected flush failure {flushError}");
                    AssertEx.BytesEqual(initial, committed, $"{check}: a rejected publication must leave the previous revision");
                    await s.AbortAsync(Ct);
                }

                Assert.True(committed.LongLength <= MaxFileLength);
                Assert.Equal(committed.LongLength, (await files.GetInfoAsync(FilePath, Ct)).Length);
            }
            finally
            {
                if (s.IsFaulted)
                    await Record.ExceptionAsync(() => s.AbortAsync(Ct));
                await Record.ExceptionAsync(() => s.DisposeAsync().AsTask());
            }
        }, Ct);

        static void AssertSeekRejected(Func<long> seek)
        {
            Exception? ex = Record.Exception(() => seek());
            Assert.NotNull(ex);
            Assert.True(ex is IOException or ArgumentOutOfRangeException, $"seek failed with unexpected {ex.GetType().Name}: {ex.Message}");
        }

        static void AssertLimitRejection(Exception ex)
        {
            ex = SdkHelpers.Unwrap(ex);
            Assert.True(ex is DfsLimitExceededException or ArgumentOutOfRangeException or ArgumentException
                    || (ex is IOException && DfsErrors.ToCode(ex) is DfsErrorCode.LimitExceeded or DfsErrorCode.InvalidArgument),
                $"expected a limit/argument rejection but got {ex.GetType().Name} ({DfsErrors.ToCode(ex)}): {ex.Message}");
        }
    }

    private static async Task CheckReadAtLongMaxAsync(DfsFileClient files, string path, byte[] initial)
    {
        await using DfsStream reader = await files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
        foreach (long position in new[] { long.MaxValue - 1, long.MaxValue, (long)initial.Length + 1 })
        {
            Exception? seek = Record.Exception(() => reader.Seek(position, SeekOrigin.Begin));
            if (seek is not null)
            {
                Assert.True(seek is ArgumentOutOfRangeException or IOException, $"seek to {position}: {seek}");
                continue;
            }

            byte[] buffer = Enumerable.Repeat((byte)0xEE, 16).ToArray();
            Assert.Equal(0, reader.Read(buffer, 0, buffer.Length));
            Assert.All(buffer, b => Assert.Equal((byte)0xEE, b)); // nothing wrapped around to offset 0
            Assert.Equal(0, await reader.ReadAsync(buffer.AsMemory(), Ct));
            Assert.Equal(-1, reader.ReadByte());
            Assert.All(buffer, b => Assert.Equal((byte)0xEE, b));
            Assert.Equal(position, reader.Position);
        }

        reader.Position = 0;
        byte[] head = new byte[8];
        Assert.Equal(8, await reader.ReadAsync(head, Ct));
        AssertEx.BytesEqual(initial.AsSpan(0, 8), head);
    }

    private static async Task CheckPageSizesAsync(DfsDirectoryClient dirs)
    {
        await dirs.CreateDirectoryAsync("/pages", Ct);
        foreach (int bad in new[] { 0, -1, 10_001, int.MaxValue, int.MinValue })
        {
            await Assert.ThrowsAnyAsync<ArgumentException>(() => dirs.ListPageAsync("/pages", new DfsEnumerationOptions { PageSize = bad }, null, Ct));
            await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            {
                await foreach (DfsEntryInfo _ in dirs.EnumerateAsync("/pages", new DfsEnumerationOptions { PageSize = bad }, Ct))
                    break;
            });
        }

        Assert.Empty((await dirs.ListPageAsync("/pages", new DfsEnumerationOptions { PageSize = 1 }, null, Ct)).Entries);
        Assert.Empty((await dirs.ListPageAsync("/pages", new DfsEnumerationOptions { PageSize = 10_000 }, null, Ct)).Entries);
    }

    // =====================================================================================================
    // PATH-014 TokenIdentityCannotCrossVolumes
    // =====================================================================================================

    [Fact]
    [Catalog("PATH-014", "U", "INV-07", "INV-12")]
    public async Task PATH_014_TokenIdentityCannotCrossVolumes()
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with
        {
            Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("other")],
            BackupNodeCount = 1,
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume other = client.GetVolume("other");
            var failures = new List<string>();

            // Commit tokens: one foreign token below and one above this volume's sequence.
            CommitToken otherLow = await other.Files.WriteAllBytesAsync("/t0", TestData.Bytes(1400, 8), Ct);
            CommitToken volToken = default;
            for (int i = 0; i < 5; i++)
                volToken = await vol.Files.WriteAllBytesAsync("/v" + i, TestData.Bytes(1401 + (ulong)i, 8), Ct);
            CommitToken otherHigh = otherLow;
            for (int i = 0; i < 40; i++)
                otherHigh = await other.Files.WriteAllBytesAsync("/t" + i, TestData.Bytes(1410 + (ulong)i, 8), Ct);
            Assert.Equal(volToken.ClusterId, otherHigh.ClusterId);
            Assert.NotEqual(volToken.VolumeId, otherHigh.VolumeId);
            Assert.False(otherHigh.IsComparableWith(volToken));
            Assert.False(otherLow.IsComparableWith(volToken));
            Assert.Throws<ArgumentException>(() => otherHigh.CompareTo(volToken));
            Assert.Throws<ArgumentException>(() => volToken.IsAtOrAfter(otherLow));

            int snapshotsBefore = (await vol.Snapshots.ListAsync(Ct)).Count;
            var foreignTokens = new (string Name, CommitToken Token)[]
            {
                ("other volume, lower sequence", otherLow),
                ("other volume, higher sequence", otherHigh),
                ("same volume, other epoch", volToken with { VolumeEpoch = volToken.VolumeEpoch + 1 }),
                ("same volume, other cluster", volToken with { ClusterId = new Guid("6f1c2b7e-0d39-4a51-9a7e-2d3c4b5a6978") }),
            };
            for (int i = 0; i < foreignTokens.Length; i++)
            {
                (string name, CommitToken token) = foreignTokens[i];
                Exception? ex = await Record.ExceptionAsync(() =>
                    vol.Snapshots.CreateAsync("cross-" + i, new DfsSnapshotOptions { MinimumCommit = token }, Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct));
                if (!IsCrossIdentityRejection(ex))
                    failures.Add($"MinimumCommit with {name}: {(ex is null ? "snapshot was created" : ex.GetType().Name + ": " + ex.Message)}");
            }

            Assert.Equal(snapshotsBefore, (await vol.Snapshots.ListAsync(Ct)).Count);
            DfsSnapshotInfo control = await vol.Snapshots.CreateAsync("own-token", new DfsSnapshotOptions { MinimumCommit = volToken }, Ct);
            Assert.True(control.Boundary.IsAtOrAfter(volToken));

            // Backup barrier with a foreign token: rejected promptly instead of waiting or succeeding.
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume);
            long started = cluster.Time.GetTimestamp();
            Exception? awaitError = await Record.ExceptionAsync(() => vol.Backups.AwaitBackupAsync(backup.TargetName, otherHigh, TimeSpan.FromSeconds(5), Ct));
            if (!IsCrossIdentityRejection(awaitError) || cluster.Time.GetElapsedTime(started) >= TimeSpan.FromSeconds(4))
                failures.Add($"AwaitBackup with a foreign token: {awaitError?.GetType().Name ?? "returned a checkpoint"} after {cluster.Time.GetElapsedTime(started)}");

            // Continuation tokens: same directory name, same page position, different volume (or directory).
            await vol.Directories.CreateDirectoryAsync("/dir", Ct);
            await other.Directories.CreateDirectoryAsync("/dir", Ct);
            await vol.Directories.CreateDirectoryAsync("/dir2", Ct);
            for (int i = 0; i < 6; i++)
            {
                await vol.Files.WriteAllBytesAsync($"/dir/a{i}", new byte[] { 1 }, Ct);
                await vol.Files.WriteAllBytesAsync($"/dir2/c{i}", new byte[] { 1 }, Ct);
                await other.Files.WriteAllBytesAsync($"/dir/b{i}", new byte[] { 1 }, Ct);
            }

            var pageOptions = new DfsEnumerationOptions { PageSize = 2 };
            DfsDirectoryPage otherFirst = await other.Directories.ListPageAsync("/dir", pageOptions, null, Ct);
            DfsDirectoryPage volDir2First = await vol.Directories.ListPageAsync("/dir2", pageOptions, null, Ct);
            Assert.NotNull(otherFirst.ContinuationToken);
            Assert.NotNull(volDir2First.ContinuationToken);
            foreach ((string name, string token) in new[] { ("other volume", otherFirst.ContinuationToken!), ("other directory", volDir2First.ContinuationToken!) })
            {
                Exception? ex = await Record.ExceptionAsync(() => vol.Directories.ListPageAsync("/dir", pageOptions, token, Ct));
                if (!IsCrossIdentityRejection(ex))
                    failures.Add($"continuation token from {name}: {(ex is null ? "a page was returned" : ex.GetType().Name + ": " + ex.Message)}");
            }

            DfsDirectoryPage volFirst = await vol.Directories.ListPageAsync("/dir", pageOptions, null, Ct);
            DfsDirectoryPage volSecond = await vol.Directories.ListPageAsync("/dir", pageOptions, volFirst.ContinuationToken, Ct);
            Assert.All(volFirst.Entries.Concat(volSecond.Entries), e => Assert.StartsWith("a", e.Name, StringComparison.Ordinal));
            Assert.Equal(4, volFirst.Entries.Concat(volSecond.Entries).Select(e => e.Name).Distinct(StringComparer.Ordinal).Count());

            // Snapshot names and transaction ids of the other volume do not resolve here.
            await other.Snapshots.CreateAsync("only-in-other", null, Ct);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("only-in-other", Ct), DfsErrorCode.SnapshotNotFound);
            await AssertEx.ThrowsDfsAsync<DfsException>(async () => { await using DfsSnapshotView v = await vol.Snapshots.OpenAsync("only-in-other", Ct); }, DfsErrorCode.SnapshotNotFound);
            DfsTransactionId otherTx;
            await using (DfsTransaction tx = await other.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/tx.bin", new byte[] { 7 }, Ct);
                await tx.CommitAsync(Ct);
                otherTx = tx.Id;
            }

            string? txProblem = await DescribeForeignOutcomeAsync(() => vol.Transactions.QueryOutcomeAsync(otherTx, false, Ct));
            if (txProblem is not null)
                failures.Add($"transaction id of the other volume: {txProblem}");

            // Operation ids reserved in the other volume are not accepted or resolved here.
            DfsOperationId otherOp = await other.NewOperationIdAsync(Ct);
            await other.Files.AppendAllBytesAsync("/log", new byte[] { 1 }, new DfsMutationOptions { OperationId = otherOp }, Ct);
            Exception? reuse = await Record.ExceptionAsync(() => vol.Files.AppendAllBytesAsync("/log", new byte[] { 2 }, new DfsMutationOptions { OperationId = otherOp }, Ct));
            if (reuse is null || !(reuse is ArgumentException || SdkHelpers.Unwrap(reuse) is DfsException { ErrorCode: DfsErrorCode.OperationIdMismatch or DfsErrorCode.OperationExpired or DfsErrorCode.CrossVolume or DfsErrorCode.InvalidArgument }))
                failures.Add($"operation id of the other volume used for a mutation here: {reuse?.GetType().Name ?? "accepted"}");
            Assert.False(await vol.Files.ExistsAsync("/log", Ct));
            string? opProblem = await DescribeForeignOutcomeAsync(() => vol.QueryOperationAsync(otherOp, false, Ct));
            if (opProblem is not null)
                failures.Add($"operation id of the other volume queried here: {opProblem}");
            await SdkHelpers.AssertBytesEqualAsync(new byte[] { 1 }, other.Files.ReadAllBytesAsync("/log", Ct));

            // Handles: a handle issued by the other volume, presented to this volume with SDK checks bypassed.
            byte[] hBytes = TestData.Bytes(1460, 32);
            await vol.Files.WriteAllBytesAsync("/h.bin", hBytes, Ct);
            await other.Files.WriteAllBytesAsync("/h.bin", TestData.Bytes(1461, 32), Ct);
            await using DfsStream volHandle1 = await vol.Files.OpenAsync("/h.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
            await using DfsStream volHandle2 = await vol.Files.OpenAsync("/h.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
            TestNode otherLeader = await cluster.GetLeaderAsync("other", Ct);
            TestNode volLeader = await cluster.GetLeaderAsync(Volume, Ct);
            await using RawProtocolClient rawOther = await cluster.ConnectRawAsync(otherLeader.Name, Ct);
            await using RawProtocolClient rawVol = await cluster.ConnectRawAsync(volLeader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await rawOther.AuthenticateAsync(cluster.AdminCredentials));
            Assert.Equal(DfsErrorCode.None, await rawVol.AuthenticateAsync(cluster.AdminCredentials));
            (DfsErrorCode openCode, HandleId foreignHandle) = await rawOther.OpenHandleAsync("other", "/h.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Assert.Equal(DfsErrorCode.None, openCode);
            Assert.Equal(DfsErrorCode.None, await rawOther.SendHandleOperationAsync("other", DfsOperationKind.Read, foreignHandle)); // control
            VolumeStateInspection volState = await cluster.Inspect.GetVolumeStateAsync(Volume);
            bool numericCollision = volState.Handles.Any(h => h.Id == foreignHandle);
            SdkHelpers.Log($"PATH-014: foreign handle {foreignHandle.Value}; numerically equal handle exists in '{Volume}': {numericCollision}");
            foreach (DfsOperationKind kind in new[] { DfsOperationKind.Read, DfsOperationKind.KeepAlive, DfsOperationKind.CommitFile, DfsOperationKind.CloseHandle })
            {
                DfsErrorCode code = await rawVol.SendHandleOperationAsync(Volume, kind, foreignHandle);
                if (code == DfsErrorCode.None)
                    failures.Add($"handle {foreignHandle.Value} of 'other' was accepted by '{Volume}' for {kind}");
            }

            // This volume's own handles were not addressed by the foreign handle number.
            byte[] buffer = new byte[hBytes.Length];
            await volHandle1.ReadExactlyAsync(buffer, Ct);
            AssertEx.BytesEqual(hBytes, buffer);
            volHandle2.Position = 0;
            await volHandle2.ReadExactlyAsync(buffer, Ct);
            AssertEx.BytesEqual(hBytes, buffer);
            Assert.Equal(volState.Handles.Count, (await cluster.Inspect.GetVolumeStateAsync(Volume)).Handles.Count);

            Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
        }, Ct);

        static bool IsCrossIdentityRejection(Exception? ex)
        {
            if (ex is null)
                return false;
            ex = SdkHelpers.Unwrap(ex);
            return ex switch
            {
                TimeoutException => false,
                ArgumentException => true,
                DfsPinExpiredException => true,
                DfsException d => d.ErrorCode is DfsErrorCode.CrossVolume or DfsErrorCode.EpochMismatch or DfsErrorCode.ClusterMismatch
                    or DfsErrorCode.InvalidArgument or DfsErrorCode.ContinuationExpired,
                _ => false,
            };
        }

        static async Task<string?> DescribeForeignOutcomeAsync(Func<Task<DfsOperationOutcome>> query)
        {
            try
            {
                DfsOperationOutcome outcome = await query();
                return outcome.Status is DfsOutcomeStatus.NotFound or DfsOutcomeStatus.Expired
                    ? null
                    : $"resolved to {outcome.Status} (token {outcome.Token}) in the wrong volume";
            }
            catch (Exception ex) when (SdkHelpers.Unwrap(ex) is ArgumentException
                or DfsException { ErrorCode: DfsErrorCode.CrossVolume or DfsErrorCode.InvalidArgument or DfsErrorCode.OperationIdMismatch or DfsErrorCode.OperationExpired })
            {
                return null;
            }
        }
    }

    // =====================================================================================================

    private static void AssertInvalidPath(string input)
    {
        ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() => DfsPath.Normalize(input));
        Assert.Equal("path", ex.ParamName);
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        Assert.False(DfsPath.TryNormalize(input, out string normalized, out string? error));
        Assert.Equal(string.Empty, normalized);
        Assert.False(string.IsNullOrEmpty(error));
    }
}
