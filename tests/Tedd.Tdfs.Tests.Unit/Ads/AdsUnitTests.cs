using System.Globalization;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Ads.AdsHelpers;

namespace Tedd.Tdfs.Tests.Unit.Ads;

/// <summary>Every <see cref="DfsFileClient"/> entry point that takes a path (ADS-001: invalid stream syntax is rejected by all of them).</summary>
public enum PathApi
{
    OpenWithOptions, OpenWithModes, OpenRead, OpenWrite, Create, OpenSync,
    ReadAllBytes, ReadAllBytesSync, ReadAllText, ReadAllLines, ReadLinesAsync, ReadLinesSync,
    WriteAllBytes, WriteAllBytesWithOptions, WriteAllBytesSync, WriteAllText, WriteAllLines,
    AppendAllBytes, AppendAllBytesWithOptions, AppendAllText, AppendAllLines,
    CopySource, CopyDestination, CopyWithOptions, MoveSource, MoveDestination, MoveWithOptions,
    Delete, DeleteWithOptions, DeleteSync, Exists, ExistsSync, GetInfo, GetInfoSync, ListStreams,
    GetAccessControl, GetEffectiveRights, SetStoragePolicy, SetAccess,
}

/// <summary>Write entry points used to fill a named stream (ADS-002).</summary>
public enum StreamWriteApi
{
    WriteAllBytes, WriteAllBytesWithOptions, WriteAllBytesSync, WriteAllText, WriteAllLines,
    AppendAllBytes, AppendAllText, AppendAllLines, CreateStream, OpenCreateNew, OpenWrite, OpenWriteThrough, OpenSync,
}

/// <summary>Entry points that create a stream (and its absent file) (ADS-003).</summary>
public enum CreatingApi
{
    WriteAllBytes, WriteAllBytesWithOptions, WriteAllBytesSync, WriteAllText, AppendAllBytes, AppendAllText,
    OpenCreateNew, OpenCreate, OpenOrCreate, OpenAppend, CreateAsync, OpenWriteAsync,
}

/// <summary>Directory listing entry points (ADS-006).</summary>
public enum EnumerationApi { EnumerateAsync, EnumerateSync, ListPageOneEntryPerPage, PatternStar, PatternPrefix, PatternSingleChar, KindsFiles, KindsDirectories }

/// <summary>Stream deletion entry points (ADS-007).</summary>
public enum StreamDeleteApi { DeleteAsync, DeleteWithOptions, DeleteSync }

/// <summary>Shape of the deleted stream (ADS-007).</summary>
public enum StreamShape { Empty, Inline, MultiExtent }

/// <summary>Ways a file leaves the namespace (ADS-008).</summary>
public enum FileRemoval { DeleteAsync, DeleteWithOptions, DeleteSync, RenameOver, RecursiveParentDelete }

/// <summary>Ways a file is moved (ADS-009).</summary>
public enum MoveKind { FilesMoveAsync, FilesMoveWithOptions, FilesMoveSync, RenameInPlace, ParentDirectoryMove, MoveOverExisting }

/// <summary>Copy entry points (ADS-010).</summary>
public enum CopyKind { CopyAsync, CopyWithOptions, CopySync, CopyToOtherDirectory }

/// <summary>Whole-content replacement entry points (ADS-011).</summary>
public enum ReplaceApi { WriteAllBytes, WriteAllBytesWithOptions, WriteAllBytesSync, WriteAllText, WriteAllLines, CopyOverwrite }

/// <summary>The stream addressed by a replacement (ADS-011).</summary>
public enum ReplaceTarget { DefaultStream, ExistingNamedStream, NewNamedStream }

/// <summary>Spellings of one stream path (ADS-014).</summary>
public enum StreamAlias { Plain, DataSuffix, Decomposed }

/// <summary>Kind of handle held on a named stream (ADS-015).</summary>
public enum StreamHandleKind { LiveReader, PinnedReader, Writer }

/// <summary>
/// ADS — alternate data streams: the U-level rows (FSD Addendum A §A4, ADS-01..ADS-06; catalog §A5.3). Every row runs
/// against an in-memory cluster through the public SDK, comparing with deterministic source bytes held by the test
/// (an independent per-file stream model) and with the leader's applied state (<see cref="FileInspection.Streams"/>).
/// </summary>
public sealed class AdsUnitTests
{
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    private static readonly byte[] Payload = [1, 2, 3, 4, 5];

    // ------------------------------------------------------------------ ADS-001 StreamPathSyntax

    public static TheoryData<string, string, string, string?> AcceptedStreamPaths => new()
    {
        { "/f:s", "/f:s", "/f", "s" },
        { "/a/f:s", "/a/f:s", "/a/f", "s" },
        { "/a/f:s:$DATA", "/a/f:s", "/a/f", "s" },
        { "/a/f::$DATA", "/a/f", "/a/f", null },
        { "/a/f", "/a/f", "/a/f", null },
        { "/d:Zone.Identifier", "/d:Zone.Identifier", "/d", "Zone.Identifier" },
        { "/a/b.txt:Zone.Identifier:$DATA", "/a/b.txt:Zone.Identifier", "/a/b.txt", "Zone.Identifier" },
        { "/a/f:with space", "/a/f:with space", "/a/f", "with space" },
        { "/a/f:s$", "/a/f:s$", "/a/f", "s$" },
        { "/a/f:%3A", "/a/f:%3A", "/a/f", "%3A" },
        { "/a/e\u0301:e\u0301:$DATA", "/a/\u00E9:\u00E9", "/a/\u00E9", "\u00E9" },
        { "/a/e\u0301::$DATA", "/a/\u00E9", "/a/\u00E9", null },
        { "/x/y/f:\U0001F600", "/x/y/f:\U0001F600", "/x/y/f", "\U0001F600" },
        // File name and stream name are each limited to 255 UTF-8 bytes (the final component may exceed 255 in total).
        { "/a/f:" + new string('s', 255), "/a/f:" + new string('s', 255), "/a/f", new string('s', 255) },
        { "/a/" + new string('f', 255) + ":" + new string('s', 255), "/a/" + new string('f', 255) + ":" + new string('s', 255), "/a/" + new string('f', 255), new string('s', 255) },
        { "/a/f:" + string.Concat(Enumerable.Repeat("e\u0301", 127)), "/a/f:" + new string('\u00E9', 127), "/a/f", new string('\u00E9', 127) },
    };

    /// <summary>
    /// Accepted stream syntax: <c>f:s</c>, <c>f:s:$DATA</c> (→ <c>f:s</c>) and <c>f::$DATA</c> (→ <c>f</c>) normalise (NFC
    /// included); the normalised form splits into file and stream, recombines exactly, and a file contains its streams.
    /// </summary>
    [Theory]
    [MemberData(nameof(AcceptedStreamPaths))]
    [Catalog("ADS-001", "U", "INV-07")]
    public void ADS_001_StreamPathSyntax(string input, string normalized, string filePath, string? streamName)
    {
        Assert.Equal(normalized, DfsPath.Normalize(input));
        Assert.True(DfsPath.TryNormalize(input, out string tryNormalized, out string? error));
        Assert.Null(error);
        Assert.Equal(normalized, tryNormalized);
        Assert.True(DfsPath.IsNormalized(normalized));
        Assert.Equal(normalized, DfsPath.Normalize(normalized));

        Assert.Equal((filePath, streamName), DfsStreamPath.Split(normalized));
        Assert.Equal(streamName is not null, DfsStreamPath.HasStream(normalized));
        Assert.Equal(filePath, DfsStreamPath.GetFilePath(normalized));
        Assert.Equal(streamName, DfsStreamPath.GetStreamName(normalized));
        Assert.Equal(normalized, DfsStreamPath.Combine(filePath, streamName));
        Assert.Equal(DfsPath.GetParent(filePath), DfsPath.GetParent(normalized));
        Assert.Equal(normalized, DfsPath.Combine(DfsPath.GetParent(normalized), DfsPath.GetName(normalized)));
        Assert.True(DfsPath.IsAncestorOrSelf(filePath, normalized), "a file contains its streams");
        if (streamName is null)
        {
            Assert.Equal(normalized, DfsPath.Normalize(normalized + "::$DATA"));
        }
        else
        {
            Assert.False(DfsPath.IsAncestorOrSelf(normalized, filePath), "a stream does not contain its file");
            Assert.Equal(streamName, DfsStreamPath.NormalizeStreamName(streamName));
            Assert.Equal(normalized, DfsPath.Normalize(normalized + ":$DATA"));
            // Oracle: the platform's NFC (ICU) agrees with the normalised stream name.
            Assert.Equal(streamName.Normalize(NormalizationForm.FormC), streamName);
        }
    }

    public static TheoryData<string, string> RejectedStreamPaths => new()
    {
        { "/a:s/f", "colon in a non-final component" },
        { "/a::$DATA/f", "stream type in a non-final component" },
        { "/a/b:c/d", "colon in a non-final component" },
        { "/a/:s", "empty file name" },
        { "/a/::$DATA", "empty file name with the default stream" },
        { "/a/f:", "empty stream name" },
        { "/a/f:s:", "empty stream type" },
        { "/a/f:s:t", "unsupported stream type" },
        { "/a/f::$INDEX_ALLOCATION", "unsupported stream type" },
        { "/a/f:s:$BITMAP", "unsupported stream type" },
        { "/a/f:s:$data", "stream type is case-sensitive" },
        { "/a/f:s:$DATA:x", "more than one stream separator" },
        { "/a/f:s::$DATA", "more than one stream separator" },
        { "/a/f:::$DATA", "more than one stream separator" },
        { "/a/f:$I30", "stream names cannot start with '$'" },
        { "/:s", "the root has no streams" },
        { "/::$DATA", "the root has no streams" },
        { "/:s:$DATA", "the root has no streams" },
        { "/a/f:.", "stream names follow component rules" },
        { "/a/f:s/g", "a stream has no children" },
        { "/a/f:" + new string('s', 256), "stream name longer than 255 UTF-8 bytes" },
        { "/a/f:" + new string('\u00E9', 128), "stream name longer than 255 UTF-8 bytes (2-byte characters)" },
        { "/a/f:" + string.Concat(Enumerable.Repeat("e\u0301", 128)), "stream name longer than 255 UTF-8 bytes after NFC" },
        { "/a/" + new string('f', 256) + ":s", "file name longer than 255 UTF-8 bytes" },
        { "/a/f:..", "'..' as a stream name" },
        { "/a/..:s", "'..' as the file name" },
        { "/a/.:s", "'.' as the file name" },
        { "/a/f:a\\b", "backslash in a stream name" },
    };

    /// <summary>
    /// Stream names follow component rules for characters too: NUL and unpaired surrogates are rejected as invalid paths
    /// (kept out of theory data, which must round-trip through test-case serialization).
    /// </summary>
    [Fact]
    [Catalog("ADS-001", "U", "INV-07")]
    public void ADS_001_StreamPathSyntax_RejectedCharacters()
    {
        foreach (string streamName in new[] { "a\0b", "\uD800x", "x\uDC00", "a\\b", "a/b", "a:b" })
        {
            string input = "/a/f:" + streamName;
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsPath.Normalize(input));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
            Assert.False(DfsPath.TryNormalize(input, out _, out string? error));
            Assert.False(string.IsNullOrEmpty(error), $"TryNormalize must explain why stream name U+{string.Join(" U+", streamName.Select(c => ((int)c).ToString("X4", CultureInfo.InvariantCulture)))} is rejected");
            Assert.False(DfsStreamPath.TryNormalizeStreamName(streamName, out _, out string? nameError));
            Assert.False(string.IsNullOrEmpty(nameError));
            Assert.ThrowsAny<ArgumentException>(() => DfsStreamPath.NormalizeStreamName(streamName));
        }

        ArgumentException fileNul = Assert.Throws<ArgumentException>(() => DfsPath.Normalize("/a/f\0:s"));
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(fileNul));
    }

    /// <summary>
    /// Rejected stream syntax: a colon in a non-final component, empty file or stream names, more than one separator,
    /// other stream types and root streams are invalid paths (<see cref="ArgumentException"/>, ParamName "path",
    /// <see cref="DfsErrorCode.InvalidPath"/>), also for the non-throwing and stream-splitting helpers.
    /// </summary>
    [Theory]
    [MemberData(nameof(RejectedStreamPaths))]
    [Catalog("ADS-001", "U", "INV-07")]
    public void ADS_001_StreamPathSyntax_Rejected(string input, string rule)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsPath.Normalize(input));
        Assert.Equal("path", ex.ParamName);
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        Assert.False(DfsPath.TryNormalize(input, out string normalized, out string? error), rule);
        Assert.Equal(string.Empty, normalized);
        Assert.False(string.IsNullOrEmpty(error), $"TryNormalize must explain the rejection ({rule})");
        Assert.False(DfsPath.IsNormalized(input), rule);
        Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(Assert.Throws<ArgumentException>(() => DfsStreamPath.Split(input))));
    }

    /// <summary>
    /// <see cref="DfsStreamPath.Combine"/> builds only valid stream paths: no stream on the root, no stream of a stream,
    /// no type-like or separator-containing stream names; a null/empty name addresses the default stream.
    /// </summary>
    [Fact]
    [Catalog("ADS-001", "U", "INV-07")]
    public void ADS_001_StreamPathSyntax_Combine()
    {
        Assert.Equal("/a/f", DfsStreamPath.Combine("/a/f", null));
        Assert.Equal("/a/f", DfsStreamPath.Combine("/a/f", DfsStreamPath.DefaultStreamName));
        Assert.Equal("/a/f:\u00E9", DfsStreamPath.Combine("/a/f", "e\u0301"));
        Assert.Equal("/", DfsStreamPath.Combine("/", null));
        foreach ((string file, string stream) in new[]
        {
            ("/", "s"), ("/a/f:s", "t"), ("/a/f", "a:b"), ("/a/f", "$DATA"), ("/a/f", "$x"), ("/a/f", ".."), ("/a/f", "x/y"),
            ("/a/f::$DATA", "s"), ("/a/f", new string('s', 256)),
        })
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() => DfsStreamPath.Combine(file, stream));
            Assert.Equal(DfsErrorCode.InvalidPath, DfsErrors.ToCode(ex));
        }

        Assert.Equal(1024, DfsStreamPath.MaxStreamsPerInode);
        Assert.Equal(':', DfsPath.StreamSeparator);
    }

    private static readonly string[] RejectedSdkPaths =
    [
        "/a:s/f", "/a::$DATA/f", "/a/:s", "/a/f:", "/a/f:s:t", "/a/f:s:$DATA:x", "/a/f:s::$DATA", "/a/f::$INDEX_ALLOCATION",
        "/a/f:s:$data", "/a/f:$I30", "/:s", "/::$DATA",
    ];

    public static TheoryData<PathApi> AllPathApis()
    {
        var data = new TheoryData<PathApi>();
        foreach (PathApi api in Enum.GetValues<PathApi>())
            data.Add(api);
        return data;
    }

    /// <summary>A three-zone cluster whose volume defines the named policy "p1" (so SetStoragePolicy can only fail on the path).</summary>
    private static TestClusterOptions WithNamedPolicy => Core with
    {
        Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["p1"] = DfsStoragePolicy.Default } }],
    };

    /// <summary>
    /// Every path-taking <see cref="DfsFileClient"/> entry point (async and sync, source and destination of copy/move)
    /// rejects every invalid stream spelling as an invalid path, and nothing in the namespace (entries, revisions,
    /// named streams) changes.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllPathApis))]
    [Catalog("ADS-001", "U", "INV-07")]
    public async Task ADS_001_StreamPathSyntax_SdkRejects(PathApi api)
    {
        await ClusterTest.RunAsync(WithNamedPolicy, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/a", Ct);
            await v.Files.WriteAllBytesAsync("/a/f", NonZero(1, 100), Ct);
            await v.Files.WriteAllBytesAsync("/a/f:s", NonZero(2, 50), Ct);
            string before = await DescribeNamespaceAsync(v);

            foreach (string bad in RejectedSdkPaths)
                AssertInvalidPath(await CatchAsync(InvokePathApi(v.Files, api, bad)), $"{api}(\"{bad}\")");

            if (api == PathApi.ListStreams)
            {
                // ListStreams takes a file or directory path: a (valid) stream path is not one.
                AssertInvalidPath(await CatchAsync(() => v.Files.ListStreamsAsync("/a/f:s", Ct)), "ListStreams(\"/a/f:s\")");
                AssertInvalidPath(await CatchAsync(Sync(() => v.Files.ListStreams("/a/f:s"))), "ListStreams sync (\"/a/f:s\")");
            }

            Assert.Equal(before, await DescribeNamespaceAsync(v));
            Assert.False(await v.Files.ExistsAsync("/ok", Ct), "the valid counterpart of a rejected copy/move must not have been created");
        }, Ct);
    }

    /// <summary>
    /// Every path-taking <see cref="DfsDirectoryClient"/> entry point (async and sync, both positions of a move, recursive and
    /// non-recursive delete, enumeration and paging) rejects every invalid stream spelling as an invalid path, and nothing in
    /// the namespace changes.
    /// </summary>
    [Fact]
    [Catalog("ADS-001", "U", "INV-07")]
    public async Task ADS_001_StreamPathSyntax_DirectoryApisReject()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            DfsDirectoryClient d = v.Directories;
            await d.CreateDirectoryAsync("/a/sub", Ct);
            await v.Files.WriteAllBytesAsync("/a/f", NonZero(3, 100), Ct);
            await v.Files.WriteAllBytesAsync("/a:s", NonZero(4, 40), Ct);
            string before = await DescribeNamespaceAsync(v);

            (string Name, Func<string, Func<Task>> Call)[] apis =
            [
                ("CreateDirectoryAsync", p => () => d.CreateDirectoryAsync(p, Ct)),
                ("CreateDirectoryAsync(options)", p => () => d.CreateDirectoryAsync(p, new DfsMutationOptions(), Ct)),
                ("CreateDirectory(sync)", p => Sync(() => d.CreateDirectory(p))),
                ("ExistsAsync", p => () => d.ExistsAsync(p, Ct)),
                ("GetInfoAsync", p => () => d.GetInfoAsync(p, Ct)),
                ("DeleteAsync", p => () => d.DeleteAsync(p, false, Ct)),
                ("DeleteAsync(recursive)", p => () => d.DeleteAsync(p, true, Ct)),
                ("MoveAsync(source)", p => () => d.MoveAsync(p, "/ok", Ct)),
                ("MoveAsync(destination)", p => () => d.MoveAsync("/a/sub", p, Ct)),
                ("EnumerateAsync", p => async () =>
                {
                    await foreach (DfsEntryInfo entry in d.EnumerateAsync(p, null, Ct))
                        Assert.Fail($"EnumerateAsync yielded {entry.Path} for an invalid path");
                }),
                ("Enumerate(sync)", p => Sync(() => d.Enumerate(p).ToList())),
                ("ListPageAsync", p => () => d.ListPageAsync(p, null, null, Ct)),
            ];
            foreach (string bad in RejectedSdkPaths)
            {
                foreach ((string name, Func<string, Func<Task>> call) in apis)
                    AssertInvalidPath(await CatchAsync(call(bad)), $"Directories.{name}(\"{bad}\")");
            }

            Assert.Equal(before, await DescribeNamespaceAsync(v));
            Assert.False(await d.ExistsAsync("/ok", Ct), "the valid counterpart of a rejected move must not have been created");
        }, Ct);
    }

    private static Func<Task> InvokePathApi(DfsFileClient f, PathApi api, string path) => api switch
    {
        PathApi.OpenWithOptions => async () => { await using DfsStream s = await f.OpenAsync(path, Writer(FileMode.OpenOrCreate), Ct); },
        PathApi.OpenWithModes => async () => { await using DfsStream s = await f.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.Read, Ct); },
        PathApi.OpenRead => async () => { await using DfsStream s = await f.OpenReadAsync(path, Ct); },
        PathApi.OpenWrite => async () => { await using DfsStream s = await f.OpenWriteAsync(path, Ct); },
        PathApi.Create => async () => { await using DfsStream s = await f.CreateAsync(path, Ct); },
        PathApi.OpenSync => Sync(() => f.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read).Dispose()),
        PathApi.ReadAllBytes => () => f.ReadAllBytesAsync(path, Ct),
        PathApi.ReadAllBytesSync => Sync(() => f.ReadAllBytes(path)),
        PathApi.ReadAllText => () => f.ReadAllTextAsync(path, null, Ct),
        PathApi.ReadAllLines => () => f.ReadAllLinesAsync(path, null, Ct),
        PathApi.ReadLinesAsync => async () =>
        {
            await foreach (string line in f.ReadLinesAsync(path, null, Ct))
                Assert.Fail($"ReadLinesAsync yielded '{line}' for an invalid path");
        },
        PathApi.ReadLinesSync => Sync(() => f.ReadLines(path).ToList()),
        PathApi.WriteAllBytes => () => f.WriteAllBytesAsync(path, Payload, Ct),
        PathApi.WriteAllBytesWithOptions => () => f.WriteAllBytesAsync(path, Payload, new DfsMutationOptions(), Ct),
        PathApi.WriteAllBytesSync => Sync(() => f.WriteAllBytes(path, Payload)),
        PathApi.WriteAllText => () => f.WriteAllTextAsync(path, "text", null, Ct),
        PathApi.WriteAllLines => () => f.WriteAllLinesAsync(path, ["l1", "l2"], null, Ct),
        PathApi.AppendAllBytes => () => f.AppendAllBytesAsync(path, Payload, Ct),
        PathApi.AppendAllBytesWithOptions => () => f.AppendAllBytesAsync(path, Payload, new DfsMutationOptions(), Ct),
        PathApi.AppendAllText => () => f.AppendAllTextAsync(path, "text", null, Ct),
        PathApi.AppendAllLines => () => f.AppendAllLinesAsync(path, ["l1"], null, Ct),
        PathApi.CopySource => () => f.CopyAsync(path, "/ok", false, Ct),
        PathApi.CopyDestination => () => f.CopyAsync("/a/f", path, true, Ct),
        PathApi.CopyWithOptions => () => f.CopyAsync("/a/f", path, true, new DfsMutationOptions(), Ct),
        PathApi.MoveSource => () => f.MoveAsync(path, "/ok", false, Ct),
        PathApi.MoveDestination => () => f.MoveAsync("/a/f", path, true, Ct),
        PathApi.MoveWithOptions => () => f.MoveAsync("/a/f", path, true, new DfsMutationOptions(), Ct),
        PathApi.Delete => () => f.DeleteAsync(path, Ct),
        PathApi.DeleteWithOptions => () => f.DeleteAsync(path, new DfsMutationOptions(), Ct),
        PathApi.DeleteSync => Sync(() => f.Delete(path)),
        PathApi.Exists => () => f.ExistsAsync(path, Ct),
        PathApi.ExistsSync => Sync(() => f.Exists(path)),
        PathApi.GetInfo => () => f.GetInfoAsync(path, Ct),
        PathApi.GetInfoSync => Sync(() => f.GetInfo(path)),
        PathApi.ListStreams => () => f.ListStreamsAsync(path, Ct),
        PathApi.GetAccessControl => () => f.GetAccessControlAsync(path, Ct),
        PathApi.GetEffectiveRights => () => f.GetEffectiveRightsAsync(path, null, Ct),
        PathApi.SetStoragePolicy => () => f.SetStoragePolicyAsync(path, "p1", Ct),
        PathApi.SetAccess => () => f.SetAccessAsync(path, new DfsInodeAccess(), Ct),
        _ => throw new ArgumentOutOfRangeException(nameof(api), api, null),
    };

    /// <summary>
    /// Through the SDK the accepted aliases address the same streams (<c>f:s:$DATA</c> = <c>f:s</c>, <c>f::$DATA</c> = the
    /// default stream, NFC-equivalent spellings), and the server itself rejects every invalid stream spelling sent
    /// verbatim over the raw protocol (no reliance on SDK validation), with no namespace change.
    /// </summary>
    [Fact]
    [Catalog("ADS-001", "U", "INV-07", "INV-12")]
    public async Task ADS_001_StreamPathSyntax_SdkAliasesAndServerValidation()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            byte[] d1 = NonZero(11, 3000), s1 = NonZero(12, 700), d2 = NonZero(13, 5000), e1 = NonZero(14, 9);
            await v.Directories.CreateDirectoryAsync("/a", Ct);
            await v.Files.WriteAllBytesAsync("/a/f", d1, Ct);
            await v.Files.WriteAllBytesAsync("/a/f:s:$DATA", s1, Ct);

            BytesEqual(s1, await v.Files.ReadAllBytesAsync("/a/f:s", Ct), "written through f:s:$DATA, read through f:s");
            BytesEqual(s1, await v.Files.ReadAllBytesAsync("/a/f:s:$DATA", Ct), "read through f:s:$DATA");
            BytesEqual(d1, await v.Files.ReadAllBytesAsync("/a/f::$DATA", Ct), "f::$DATA addresses the default stream");
            DfsFileInfo stream = await v.Files.GetInfoAsync("/a/f:s:$DATA", Ct);
            Assert.Equal("/a/f:s", stream.Path);
            Assert.True(stream.IsStream);
            Assert.Equal("s", stream.StreamName);
            DfsFileInfo plain = await v.Files.GetInfoAsync("/a/f::$DATA", Ct);
            Assert.Equal("/a/f", plain.Path);
            Assert.False(plain.IsStream);
            Assert.Null(plain.StreamName);
            Assert.Equal(plain.InodeId, stream.InodeId);

            await v.Files.WriteAllBytesAsync("/a/f::$DATA", d2, Ct);
            await AssertStreamsAsync(v, "/a/f", new StreamModel(d2).With("s", s1), "f::$DATA replaced the default stream and created no stream");
            Assert.True(await v.Files.ExistsAsync("/a/f::$DATA", Ct));

            await v.Files.WriteAllBytesAsync("/a/e\u0301:e\u0301", e1, Ct);
            Assert.True(await v.Files.ExistsAsync("/a/\u00E9:\u00E9", Ct), "NFC-equivalent spellings address the same stream");
            BytesEqual(e1, await v.Files.ReadAllBytesAsync("/a/\u00E9:\u00E9", Ct), "decomposed write, precomposed read");
            Assert.Equal(new[] { "\u00E9" }, (await v.Files.ListStreamsAsync("/a/e\u0301", Ct)).Select(s => s.Name));
            List<DfsEntryInfo> entries = await EntriesAsync(v, "/a");
            Assert.Equal(new[] { "f", "\u00E9" }.Order(StringComparer.Ordinal), entries.Select(e => e.Name).Order(StringComparer.Ordinal));

            // The server validates stream syntax itself: raw requests bypassing the SDK are rejected as invalid paths.
            string before = await DescribeNamespaceAsync(v);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            DfsOperationKind[] kinds = [DfsOperationKind.CreateDirectory, DfsOperationKind.Delete, DfsOperationKind.Open, DfsOperationKind.GetInfo, DfsOperationKind.WriteAll];
            foreach (string bad in RejectedSdkPaths)
            {
                foreach (DfsOperationKind kind in kinds)
                    Assert.True(await raw.SendPathOperationAsync(Vol, kind, bad) == DfsErrorCode.InvalidPath, $"raw {kind}(\"{bad}\") must be rejected as InvalidPath by the server");
                Assert.True(await raw.SendPathOperationAsync(Vol, DfsOperationKind.Move, "/a/f", bad) == DfsErrorCode.InvalidPath, $"raw Move(\"/a/f\", \"{bad}\")");
                Assert.True(await raw.SendPathOperationAsync(Vol, DfsOperationKind.Move, bad, "/a/g") == DfsErrorCode.InvalidPath, $"raw Move(\"{bad}\", \"/a/g\")");
            }

            Assert.Equal(before, await DescribeNamespaceAsync(v));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-002 WriteReadNamedStream

    public static TheoryData<StreamWriteApi> AllStreamWriteApis()
    {
        var data = new TheoryData<StreamWriteApi>();
        foreach (StreamWriteApi api in Enum.GetValues<StreamWriteApi>())
            data.Add(api);
        return data;
    }

    private sealed record WriteExpectation(byte[]? Bytes, string? Text, string[]? Lines);

    /// <summary>
    /// Writing <c>/f:s</c> through every write entry point and boundary length (0, 1, inline ±1, chunk + 1, 2×chunk + 3)
    /// stores exactly the bytes the same call stores in a plain file's default stream (and the deterministic source
    /// bytes/text/lines); every read entry point returns them; the file's default stream keeps its bytes, length,
    /// revision, inode and extents, and the stream's objects are its own.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllStreamWriteApis))]
    [Catalog("ADS-002", "U", "INV-02", "INV-07")]
    public async Task ADS_002_WriteReadNamedStream(StreamWriteApi api)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "w");
            await using DfsClient readerClient = await ConnectAsync(cluster, "r");
            DfsVolume wv = writer.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await wv.Directories.CreateDirectoryAsync("/w", Ct);
            foreach (int length in new[] { 0, 1, Inline - 1, Inline, Inline + 1, Chunk + 1, (2 * Chunk) + 3 })
            {
                string file = $"/w/f{length}";
                string stream = file + ":s";
                string reference = $"/w/ref{length}";
                ulong seed = 0x2000UL + (ulong)length;
                byte[] defaultBytes = NonZero(seed ^ 0xFFFF, 10_000);
                await wv.Files.WriteAllBytesAsync(file, defaultBytes, Ct);
                DfsFileInfo before = await rv.Files.GetInfoAsync(file, Ct);
                HashSet<ObjectId> defaultObjects = Objects((await InspectFileAsync(cluster, file)).Extents);

                await WriteWithAsync(wv.Files, api, reference, length, seed);
                WriteExpectation expectation = await WriteWithAsync(wv.Files, api, stream, length, seed);
                string what = $"{api} of {length} units into {stream}";

                byte[] streamBytes = await rv.Files.ReadAllBytesAsync(stream, Ct);
                BytesEqual(await rv.Files.ReadAllBytesAsync(reference, Ct), streamBytes, $"{what}: a named stream holds exactly what the same call stores in a default stream");
                if (expectation.Bytes is not null)
                    BytesEqual(expectation.Bytes, streamBytes, $"{what}: source bytes");
                if (expectation.Text is not null)
                    Assert.Equal(expectation.Text, await rv.Files.ReadAllTextAsync(stream, Utf8NoBom, Ct));
                if (expectation.Lines is not null)
                {
                    Assert.Equal(expectation.Lines, await rv.Files.ReadAllLinesAsync(stream, Utf8NoBom, Ct));
                    Assert.Equal(expectation.Lines, rv.Files.ReadLines(stream, Utf8NoBom).ToArray());
                    var lazy = new List<string>();
                    await foreach (string line in rv.Files.ReadLinesAsync(stream, Utf8NoBom, Ct))
                        lazy.Add(line);
                    Assert.Equal(expectation.Lines, lazy);
                }

                // Every read entry point agrees on the stream.
                BytesEqual(streamBytes, rv.Files.ReadAllBytes(stream), $"{what}: ReadAllBytes (sync)");
                await using (DfsStream r = await rv.Files.OpenReadAsync(stream, Ct))
                {
                    Assert.Equal(streamBytes.Length, r.Length);
                    Assert.Equal(before.InodeId, r.InodeId);
                    BytesEqual(streamBytes, await ReadToEndAsync(r), $"{what}: OpenRead");
                }

                BytesEqual(streamBytes, await ReadCommittedAsync(rv, stream), $"{what}: version-pinned reader");
                DfsFileInfo info = await rv.Files.GetInfoAsync(stream, Ct);
                Assert.True(info.IsStream, what);
                Assert.Equal("s", info.StreamName);
                Assert.Equal(stream, info.Path);
                Assert.Equal(streamBytes.Length, info.Length);
                Assert.Equal(before.InodeId, info.InodeId);

                // The default stream is unaffected.
                BytesEqual(defaultBytes, await rv.Files.ReadAllBytesAsync(file, Ct), $"{what}: default stream of {file}");
                DfsFileInfo after = await rv.Files.GetInfoAsync(file, Ct);
                Assert.Equal(before.InodeId, after.InodeId);
                Assert.Equal(defaultBytes.Length, after.Length);
                Assert.Equal(before.Revision, after.Revision);
                Assert.False(after.IsStream);
                Assert.Equal(1, after.StreamCount);

                FileInspection inspected = await InspectFileAsync(cluster, file);
                Assert.Equal(defaultBytes.Length, inspected.Length);
                Assert.True(defaultObjects.SetEquals(Objects(inspected.Extents)), $"{what}: the default stream's extents must be untouched");
                StreamInspection named = Assert.Single(inspected.Streams);
                Assert.Equal("s", named.Name);
                Assert.Equal(streamBytes.Length, named.Length);
                Assert.Equal(info.Revision, named.Revision);
                Assert.False(Objects(named.Extents).Overlaps(defaultObjects), $"{what}: the stream must not reference the default stream's objects");
            }
        }, Ct);
    }

    private static async Task<WriteExpectation> WriteWithAsync(DfsFileClient files, StreamWriteApi api, string path, int length, ulong seed)
    {
        byte[] data = NonZero(seed, length);
        switch (api)
        {
            case StreamWriteApi.WriteAllBytes:
                await files.WriteAllBytesAsync(path, data, Ct);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.WriteAllBytesWithOptions:
                await files.WriteAllBytesAsync(path, data, new DfsMutationOptions(), Ct);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.WriteAllBytesSync:
                files.WriteAllBytes(path, data);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.WriteAllText:
            {
                string text = TestData.Utf8Text(seed, length);
                await files.WriteAllTextAsync(path, text, Utf8NoBom, Ct);
                return new WriteExpectation(Utf8NoBom.GetBytes(text), text, null);
            }

            case StreamWriteApi.WriteAllLines:
            {
                string[] lines = Lines(seed, length);
                await files.WriteAllLinesAsync(path, lines, Utf8NoBom, Ct);
                return new WriteExpectation(null, null, lines);
            }

            case StreamWriteApi.AppendAllBytes:
            {
                int half = length / 2;
                await files.AppendAllBytesAsync(path, data.AsMemory(0, half), Ct);
                await files.AppendAllBytesAsync(path, data.AsMemory(half), new DfsMutationOptions(), Ct);
                return new WriteExpectation(data, null, null);
            }

            case StreamWriteApi.AppendAllText:
            {
                string text = TestData.Utf8Text(seed, length);
                int half = SplitIndex(text);
                await files.AppendAllTextAsync(path, text[..half], Utf8NoBom, Ct);
                await files.AppendAllTextAsync(path, text[half..], Utf8NoBom, Ct);
                return new WriteExpectation(Utf8NoBom.GetBytes(text), text, null);
            }

            case StreamWriteApi.AppendAllLines:
            {
                string[] lines = Lines(seed, length);
                int half = lines.Length / 2;
                await files.AppendAllLinesAsync(path, lines[..half], Utf8NoBom, Ct);
                await files.AppendAllLinesAsync(path, lines[half..], Utf8NoBom, Ct);
                return new WriteExpectation(null, null, lines);
            }

            case StreamWriteApi.CreateStream:
                await using (DfsStream s = await files.CreateAsync(path, Ct))
                    await WritePiecesAsync(s, data);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.OpenCreateNew:
                await using (DfsStream s = await files.OpenAsync(path, Writer(FileMode.CreateNew), Ct))
                {
                    await WritePiecesAsync(s, data);
                    await s.FlushAsync(Ct);
                }

                return new WriteExpectation(data, null, null);
            case StreamWriteApi.OpenWrite:
                await using (DfsStream s = await files.OpenWriteAsync(path, Ct))
                    await WritePiecesAsync(s, data);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.OpenWriteThrough:
                await using (DfsStream s = await files.OpenAsync(path, Writer(FileMode.OpenOrCreate, writeMode: DfsWriteMode.WriteThrough), Ct))
                    await WritePiecesAsync(s, data);
                return new WriteExpectation(data, null, null);
            case StreamWriteApi.OpenSync:
                using (DfsStream s = files.Open(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None))
                {
                    s.Write(data);
                    s.Flush();
                }

                return new WriteExpectation(data, null, null);
            default:
                throw new ArgumentOutOfRangeException(nameof(api), api, null);
        }
    }

    /// <summary>Writes <paramref name="data"/> in irregular pieces (1 byte, odd sizes, more than a chunk).</summary>
    private static async Task WritePiecesAsync(Stream s, byte[] data)
    {
        int[] pieces = [7, 4093, 1, Chunk + 13, 500];
        int offset = 0;
        for (int i = 0; offset < data.Length; i++)
        {
            int n = Math.Min(pieces[i % pieces.Length], data.Length - offset);
            await s.WriteAsync(data.AsMemory(offset, n), Ct);
            offset += n;
        }
    }

    private static string[] Lines(ulong seed, int approxChars) => approxChars == 0
        ? []
        : Enumerable.Range(0, Math.Max(1, approxChars / 40)).Select(i => TestData.Utf8Text(seed + (ulong)i, 1 + (i * 7 % 39))).ToArray();

    /// <summary>A split index near the middle that does not separate a surrogate pair.</summary>
    private static int SplitIndex(string text)
    {
        int half = text.Length / 2;
        if (half > 0 && half < text.Length && char.IsLowSurrogate(text[half]))
            half++;
        return half;
    }

    // ------------------------------------------------------------------ ADS-003 CreatingStreamCreatesFile

    public static TheoryData<CreatingApi> AllCreatingApis()
    {
        var data = new TheoryData<CreatingApi>();
        foreach (CreatingApi api in Enum.GetValues<CreatingApi>())
            data.Add(api);
        return data;
    }

    private static bool IsOpenBased(CreatingApi api) => api is CreatingApi.OpenCreateNew or CreatingApi.OpenCreate or CreatingApi.OpenOrCreate
        or CreatingApi.OpenAppend or CreatingApi.CreateAsync or CreatingApi.OpenWriteAsync;

    private static Task<DfsStream> OpenCreatingAsync(DfsFileClient files, CreatingApi api, string path) => api switch
    {
        CreatingApi.OpenCreateNew => files.OpenAsync(path, Writer(FileMode.CreateNew), Ct),
        CreatingApi.OpenCreate => files.OpenAsync(path, Writer(FileMode.Create), Ct),
        CreatingApi.OpenOrCreate => files.OpenAsync(path, Writer(FileMode.OpenOrCreate), Ct),
        CreatingApi.OpenAppend => files.OpenAsync(path, Writer(FileMode.Append), Ct),
        CreatingApi.CreateAsync => files.CreateAsync(path, Ct),
        CreatingApi.OpenWriteAsync => files.OpenWriteAsync(path, Ct),
        _ => throw new ArgumentOutOfRangeException(nameof(api), api, null),
    };

    private static async Task<(CommitToken Token, byte[] Expected)> CreateWithHelperAsync(DfsFileClient files, CreatingApi api, string path, byte[] data)
    {
        switch (api)
        {
            case CreatingApi.WriteAllBytes:
                return (await files.WriteAllBytesAsync(path, data, Ct), data);
            case CreatingApi.WriteAllBytesWithOptions:
                return (await files.WriteAllBytesAsync(path, data, new DfsMutationOptions(), Ct), data);
            case CreatingApi.WriteAllBytesSync:
                return (files.WriteAllBytes(path, data), data);
            case CreatingApi.WriteAllText:
            {
                string text = TestData.Utf8Text(0x3333, 3000);
                return (await files.WriteAllTextAsync(path, text, Utf8NoBom, Ct), Utf8NoBom.GetBytes(text));
            }

            case CreatingApi.AppendAllBytes:
                return (await files.AppendAllBytesAsync(path, data, Ct), data);
            case CreatingApi.AppendAllText:
            {
                string text = TestData.Utf8Text(0x3334, 2500);
                return (await files.AppendAllTextAsync(path, text, Utf8NoBom, Ct), Utf8NoBom.GetBytes(text));
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(api), api, null);
        }
    }

    /// <summary>
    /// Polls <c>Exists(file)</c> then <c>Exists(file:stream)</c> from another client: since nothing is deleted, a file
    /// observed without its stream is an exposed non-atomic intermediate state.
    /// </summary>
    private sealed class CreationWatcher(DfsFileClient files, string file, string stream)
    {
        private int _observations;
        private volatile bool _stop;

        public string? Violation { get; private set; }

        public int Observations => Volatile.Read(ref _observations);

        public async Task RunAsync()
        {
            while (!_stop && Violation is null)
            {
                if (await files.ExistsAsync(file, Ct) && !await files.ExistsAsync(stream, Ct))
                    Violation = $"observation {Observations}: {file} existed while {stream} did not";
                Interlocked.Increment(ref _observations);
            }
        }

        public async Task StopAfterAsync(int more)
        {
            int target = Observations + more;
            await AssertEx.EventuallyAsync(() => Task.FromResult(Observations >= target || Violation is not null), Bounded,
                "the observer keeps polling after the creation");
            _stop = true;
        }
    }

    /// <summary>
    /// Creating <c>/c/f:s</c> while <c>/c/f</c> is absent creates the file with an empty default stream and the stream in
    /// one committed decision: another client never observes the file without the stream, both carry the same creation
    /// revision (the helper's commit token, or the creating open's revision), the directory lists exactly one empty file
    /// and the applied inode has no default-stream extents. A missing parent is never created as a side effect.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllCreatingApis))]
    [Catalog("ADS-003", "U", "INV-02", "INV-07")]
    public async Task ADS_003_CreatingStreamCreatesFile(CreatingApi api)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/c", Ct);
            const string file = "/c/f";
            const string stream = "/c/f:s";
            byte[] data = NonZero(0x3003, Chunk + 77);
            Assert.False(await obs.Files.ExistsAsync(file, Ct));

            var watcher = new CreationWatcher(obs.Files, file, stream);
            Task watching = watcher.RunAsync();
            CommitToken? token = null;
            byte[] expected = data;
            if (IsOpenBased(api))
            {
                await using DfsStream s = await OpenCreatingAsync(v.Files, api, stream);
                Assert.True(await obs.Files.ExistsAsync(file, Ct), $"{api}: the creating open committed the file");
                Assert.True(await obs.Files.ExistsAsync(stream, Ct), $"{api}: the creating open committed the stream");
                DfsFileInfo fileAtOpen = await obs.Files.GetInfoAsync(file, Ct);
                DfsFileInfo streamAtOpen = await obs.Files.GetInfoAsync(stream, Ct);
                Assert.Equal(0L, fileAtOpen.Length);
                Assert.Equal(0L, streamAtOpen.Length);
                Assert.True(fileAtOpen.Revision == streamAtOpen.Revision,
                    $"{api}: file (revision {fileAtOpen.Revision}) and stream (revision {streamAtOpen.Revision}) must be created by the same committed open");
                Assert.Equal(fileAtOpen.InodeId, s.InodeId);
                await s.WriteAsync(data, Ct);
            }
            else
            {
                (CommitToken t, byte[] bytes) = await CreateWithHelperAsync(v.Files, api, stream, data);
                token = t;
                expected = bytes;
            }

            await watcher.StopAfterAsync(3);
            await watching;
            Assert.True(watcher.Violation is null, $"{api}: {watcher.Violation}");

            Assert.True(await obs.Files.ExistsAsync(file, Ct));
            Assert.True(await obs.Files.ExistsAsync(stream, Ct));
            DfsFileInfo fileInfo = await obs.Files.GetInfoAsync(file, Ct);
            DfsFileInfo streamInfo = await obs.Files.GetInfoAsync(stream, Ct);
            Assert.Equal(0L, fileInfo.Length);
            Assert.False(fileInfo.IsStream);
            Assert.Equal(1, fileInfo.StreamCount);
            Assert.Empty(await obs.Files.ReadAllBytesAsync(file, Ct));
            BytesEqual(expected, await obs.Files.ReadAllBytesAsync(stream, Ct), $"{api}: stream contents");
            DfsStreamInfo listed = Assert.Single(await obs.Files.ListStreamsAsync(file, Ct));
            Assert.Equal("s", listed.Name);
            Assert.Equal(expected.Length, listed.Length);
            DfsEntryInfo entry = Assert.Single(await EntriesAsync(obs, "/c"));
            Assert.Equal("f", entry.Name);
            Assert.Equal(DfsEntryKind.File, entry.Kind);
            Assert.Equal(0L, entry.Length);
            Assert.Equal(fileInfo.InodeId, entry.InodeId);
            Assert.Equal(fileInfo.InodeId, streamInfo.InodeId);

            FileInspection inspected = await InspectFileAsync(cluster, file);
            Assert.Equal(0L, inspected.Length);
            Assert.Empty(inspected.Extents);
            StreamInspection named = Assert.Single(inspected.Streams);
            Assert.Equal("s", named.Name);
            Assert.Equal(expected.Length, named.Length);

            if (token is { } committed)
            {
                Assert.False(committed.IsNone, $"{api} must return the token of its durable publication");
                Assert.True(fileInfo.Revision == committed.Sequence && streamInfo.Revision == committed.Sequence,
                    $"{api}: file (revision {fileInfo.Revision}) and stream (revision {streamInfo.Revision}) must both be created at the helper's commit {committed.Sequence}");
            }

            // A missing parent directory is reported and never created as a side effect.
            Exception? missingParent = IsOpenBased(api)
                ? await CatchAsync(async () => { await using DfsStream s = await OpenCreatingAsync(v.Files, api, "/nodir/g:s"); })
                : await CatchAsync(() => CreateWithHelperAsync(v.Files, api, "/nodir/g:s", data));
            Assert.True(missingParent is DirectoryNotFoundException, $"{api} under a missing parent: expected DirectoryNotFoundException, got {Describe(missingParent)}");
            Assert.False(await obs.Directories.ExistsAsync("/nodir", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-004 ExistsDistinguishesStreams

    /// <summary>
    /// <c>Exists("/f:s")</c> is true exactly when the stream exists (for every alias spelling; case-sensitive; zero-length
    /// streams exist), independent of <c>Exists("/f")</c>; reading or inspecting an absent stream of an existing file is
    /// a missing-file error, never empty data. Answers are authoritative for a second client and the sync overload.
    /// </summary>
    [Fact]
    [Catalog("ADS-004", "U", "INV-07")]
    public async Task ADS_004_ExistsDistinguishesStreams()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsFileClient files = client.GetVolume(Vol).Files;
            DfsFileClient obs = observerClient.GetVolume(Vol).Files;

            Assert.False(await obs.ExistsAsync("/f:s", Ct), "absent file, absent stream");
            Assert.False(await obs.ExistsAsync("/f", Ct));

            await files.WriteAllBytesAsync("/f", NonZero(41, 2000), Ct);
            Assert.True(await obs.ExistsAsync("/f", Ct));
            Assert.False(await obs.ExistsAsync("/f:s", Ct), "existing file, absent stream");
            Assert.False(await obs.ExistsAsync("/f:t", Ct));
            Assert.False(obs.Exists("/f:s"), "sync Exists of an absent stream");
            AssertFileNotFound(await CatchAsync(() => obs.GetInfoAsync("/f:s", Ct)), "GetInfo of an absent stream");
            AssertFileNotFound(await CatchAsync(() => obs.ReadAllBytesAsync("/f:s", Ct)), "ReadAllBytes of an absent stream");
            AssertFileNotFound(await CatchAsync(async () => { await using DfsStream r = await obs.OpenReadAsync("/f:s", Ct); }), "OpenRead of an absent stream");
            Assert.False(await obs.ExistsAsync("/f:s", Ct), "probing an absent stream must not create it");

            byte[] s = NonZero(42, 300);
            await files.WriteAllBytesAsync("/f:s", s, Ct);
            Assert.True(await obs.ExistsAsync("/f:s", Ct));
            Assert.True(await obs.ExistsAsync("/f:s:$DATA", Ct));
            Assert.True(obs.Exists("/f:s"));
            Assert.False(await obs.ExistsAsync("/f:S", Ct), "stream names are case-sensitive");
            Assert.False(await obs.ExistsAsync("/f:t", Ct));
            Assert.True(await obs.ExistsAsync("/f", Ct), "the file still exists");
            Assert.True(await obs.ExistsAsync("/f::$DATA", Ct), "the default stream exists");

            await files.DeleteAsync("/f:s", Ct);
            Assert.False(await obs.ExistsAsync("/f:s", Ct), "deleted stream");
            Assert.True(await obs.ExistsAsync("/f", Ct), "deleting a stream keeps the file");

            await files.WriteAllBytesAsync("/f:empty", ReadOnlyMemory<byte>.Empty, Ct);
            Assert.True(await obs.ExistsAsync("/f:empty", Ct), "a zero-length stream exists");
            Assert.Equal(0L, (await obs.GetInfoAsync("/f:empty", Ct)).Length);

            await client.GetVolume(Vol).Directories.CreateDirectoryAsync("/d", Ct);
            Assert.False(await obs.ExistsAsync("/d:s", Ct), "a directory without the named stream");
            await files.WriteAllBytesAsync("/d:s", s, Ct);
            Assert.True(await obs.ExistsAsync("/d:s", Ct), "a directory's named stream");
            Assert.True(await observerClient.GetVolume(Vol).Directories.ExistsAsync("/d", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-005 ListStreams

    /// <summary>
    /// ListStreamsAsync returns exactly the named streams (never the default stream), ordinal-sorted by name, with exact
    /// lengths and revisions equal to the commit of each stream's last modification; changing one stream changes only
    /// its entry; the same list is seen by another client, the sync overload and the leader's applied state. A file
    /// without streams lists none; an absent file is a missing-file error; a stream path is an invalid path.
    /// </summary>
    [Fact]
    [Catalog("ADS-005", "U", "INV-07")]
    public async Task ADS_005_ListStreams()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsFileClient obs = observerClient.GetVolume(Vol).Files;
            string[] creationOrder = ["zeta", "Alpha", "alpha", "B", "a b", "\u00E9t\u00E9", "\U0001F600", "s$", "Zone.Identifier", "1", "\u00C5"];
            int[] lengths = [0, 1, Inline + 1, Chunk + 1, 17, 4095, 2, 300, 26, 1000, 64];
            await v.Files.WriteAllBytesAsync("/f", NonZero(50, 5000), Ct);
            var expected = new SortedDictionary<string, (long Length, ulong Revision)>(StringComparer.Ordinal);
            for (int i = 0; i < creationOrder.Length; i++)
            {
                CommitToken token = await v.Files.WriteAllBytesAsync("/f:" + creationOrder[i], NonZero(51 + (ulong)i, lengths[i]), Ct);
                expected[creationOrder[i]] = (lengths[i], token.Sequence);
            }

            await AssertListedAsync(obs, cluster, "/f", expected, "after creating eleven streams in non-sorted order");
            Assert.Equal((await obs.ListStreamsAsync("/f", Ct)).Select(s => (s.Name, s.Length, s.Revision)), obs.ListStreams("/f").Select(s => (s.Name, s.Length, s.Revision)));

            CommitToken appended = await v.Files.AppendAllBytesAsync("/f:B", NonZero(70, 123), Ct);
            Assert.True(appended.Sequence > expected["B"].Revision);
            expected["B"] = (expected["B"].Length + 123, appended.Sequence);
            await AssertListedAsync(obs, cluster, "/f", expected, "after appending to one stream (only its entry changes)");

            await v.Files.DeleteAsync("/f:zeta", Ct);
            expected.Remove("zeta");
            await AssertListedAsync(obs, cluster, "/f", expected, "after deleting one stream");

            await v.Files.WriteAllBytesAsync("/plain", NonZero(71, 10), Ct);
            Assert.Empty(await obs.ListStreamsAsync("/plain", Ct));
            AssertFileNotFound(await CatchAsync(() => obs.ListStreamsAsync("/absent", Ct)), "ListStreams of an absent file");
            AssertInvalidPath(await CatchAsync(() => obs.ListStreamsAsync("/f:B", Ct)), "ListStreams of a stream path");
        }, Ct);
    }

    private static async Task AssertListedAsync(DfsFileClient files, TestCluster cluster, string file,
        SortedDictionary<string, (long Length, ulong Revision)> expected, string because)
    {
        IReadOnlyList<DfsStreamInfo> listed = await files.ListStreamsAsync(file, Ct);
        Assert.True(expected.Keys.SequenceEqual(listed.Select(s => s.Name), StringComparer.Ordinal),
            $"{because}: listed [{string.Join(", ", listed.Select(s => s.Name))}], expected ordinal [{string.Join(", ", expected.Keys)}]");
        foreach (DfsStreamInfo info in listed)
        {
            Assert.True(info.Length == expected[info.Name].Length, $"{because}: {file}:{info.Name} length {info.Length}, expected {expected[info.Name].Length}");
            Assert.True(info.Revision == expected[info.Name].Revision, $"{because}: {file}:{info.Name} revision {info.Revision}, expected {expected[info.Name].Revision}");
        }

        Assert.DoesNotContain(listed, s => s.Name.Length == 0);
        Assert.Equal(expected.Count, (await files.GetInfoAsync(file, Ct)).StreamCount);
        FileInspection inspected = await InspectFileAsync(cluster, file);
        Assert.Equal(expected.Select(e => (Name: e.Key, e.Value.Length, e.Value.Revision)), inspected.Streams.Select(s => (s.Name, s.Length, s.Revision)));
    }

    // ------------------------------------------------------------------ ADS-006 EnumerationHidesStreams

    public static TheoryData<EnumerationApi> AllEnumerationApis()
    {
        var data = new TheoryData<EnumerationApi>();
        foreach (EnumerationApi api in Enum.GetValues<EnumerationApi>())
            data.Add(api);
        return data;
    }

    /// <summary>
    /// Directory listings (async, sync, one-entry pages, patterns, kind filters) list each file and directory once by its
    /// plain name — never a stream name, never a ':' — and report the default stream's length; a file created only through
    /// a stream appears once with length 0; the entry count and the parent's listing are unaffected by streams.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllEnumerationApis))]
    [Catalog("ADS-006", "U", "INV-07")]
    public async Task ADS_006_EnumerationHidesStreams(EnumerationApi api)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/d/sub", Ct);
            await WriteModelAsync(v, "/d/f", new StreamModel(NonZero(61, 3000)).With("a", NonZero(62, 10)).With("b", NonZero(63, Chunk + 1)).With("f", NonZero(64, 5)));
            await v.Files.WriteAllBytesAsync("/d/g", NonZero(65, 77), Ct);
            await v.Files.WriteAllBytesAsync("/d/h:only", NonZero(66, 500), Ct);
            await v.Files.WriteAllBytesAsync("/d/sub:x", NonZero(67, 50), Ct);
            await v.Files.WriteAllBytesAsync("/d:dirstream", NonZero(68, 40), Ct);

            var all = new SortedDictionary<string, (DfsEntryKind Kind, long Length)>(StringComparer.Ordinal)
            {
                ["f"] = (DfsEntryKind.File, 3000),
                ["g"] = (DfsEntryKind.File, 77),
                ["h"] = (DfsEntryKind.File, 0),
                ["sub"] = (DfsEntryKind.Directory, -1),
            };

            DfsEnumerationOptions? options = api switch
            {
                EnumerationApi.PatternStar => new DfsEnumerationOptions { Pattern = "*" },
                EnumerationApi.PatternPrefix => new DfsEnumerationOptions { Pattern = "f*" },
                EnumerationApi.PatternSingleChar => new DfsEnumerationOptions { Pattern = "?" },
                EnumerationApi.KindsFiles => new DfsEnumerationOptions { Kinds = DfsEntryKinds.Files },
                EnumerationApi.KindsDirectories => new DfsEnumerationOptions { Kinds = DfsEntryKinds.Directories },
                EnumerationApi.ListPageOneEntryPerPage => new DfsEnumerationOptions { PageSize = 1 },
                _ => null,
            };
            Func<string, bool> selects = api switch
            {
                EnumerationApi.PatternPrefix => n => n.StartsWith('f'),
                EnumerationApi.PatternSingleChar => n => n.Length == 1,
                EnumerationApi.KindsFiles => n => all[n].Kind == DfsEntryKind.File,
                EnumerationApi.KindsDirectories => n => all[n].Kind == DfsEntryKind.Directory,
                _ => _ => true,
            };

            List<DfsEntryInfo> entries = [];
            switch (api)
            {
                case EnumerationApi.EnumerateSync:
                    entries.AddRange(v.Directories.Enumerate("/d", options));
                    break;
                case EnumerationApi.ListPageOneEntryPerPage:
                {
                    string? token = null;
                    int pages = 0;
                    do
                    {
                        DfsDirectoryPage page = await v.Directories.ListPageAsync("/d", options, token, Ct);
                        Assert.True(page.Entries.Count <= 1, $"page {pages} holds {page.Entries.Count} entries with PageSize 1");
                        entries.AddRange(page.Entries);
                        token = page.ContinuationToken;
                        Assert.True(++pages <= 10, "paging must terminate");
                    }
                    while (token is not null);
                    break;
                }

                default:
                    entries.AddRange(await EntriesAsync(v, "/d", options));
                    break;
            }

            string[] expectedNames = all.Keys.Where(selects).ToArray();
            Assert.True(expectedNames.Order(StringComparer.Ordinal).SequenceEqual(entries.Select(e => e.Name).Order(StringComparer.Ordinal), StringComparer.Ordinal),
                $"{api}: listed [{string.Join(", ", entries.Select(e => e.Name))}], expected [{string.Join(", ", expectedNames)}]");
            foreach (DfsEntryInfo entry in entries)
            {
                Assert.False(entry.Name.Contains(':', StringComparison.Ordinal), $"{api}: entry name '{entry.Name}' names a stream");
                Assert.False(entry.Path.Contains(':', StringComparison.Ordinal), $"{api}: entry path '{entry.Path}' names a stream");
                Assert.Equal("/d/" + entry.Name, entry.Path);
                Assert.Equal(all[entry.Name].Kind, entry.Kind);
                if (entry.Kind == DfsEntryKind.File)
                    Assert.True(entry.Length == all[entry.Name].Length, $"{api}: {entry.Path} lists length {entry.Length}; streams must not count, expected {all[entry.Name].Length}");
            }

            Assert.Equal(4, (await v.Directories.GetInfoAsync("/d", Ct)).EntryCount);
            DfsEntryInfo d = Assert.Single(await EntriesAsync(v, "/"));
            Assert.Equal("d", d.Name);
            Assert.Empty(await EntriesAsync(v, "/d/sub"));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-007 DeleteStreamOnly

    public static TheoryData<StreamDeleteApi, StreamShape> StreamDeleteCases()
    {
        var data = new TheoryData<StreamDeleteApi, StreamShape>();
        foreach (StreamDeleteApi api in Enum.GetValues<StreamDeleteApi>())
        {
            foreach (StreamShape shape in Enum.GetValues<StreamShape>())
                data.Add(api, shape);
        }

        return data;
    }

    /// <summary>
    /// Deleting <c>/f:b</c> removes only that stream: the file, its default stream (bytes, revision, extents) and the other
    /// streams (bytes, revisions, extents) are unchanged; repeating the delete, or deleting a stream of an absent file,
    /// succeeds without any change.
    /// </summary>
    [Theory]
    [MemberData(nameof(StreamDeleteCases))]
    [Catalog("ADS-007", "U", "INV-02", "INV-07")]
    public async Task ADS_007_DeleteStreamOnly(StreamDeleteApi api, StreamShape shape)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            byte[] b = shape switch
            {
                StreamShape.Empty => [],
                StreamShape.Inline => NonZero(72, 900),
                _ => NonZero(72, (3 * Chunk) + 5),
            };
            StreamModel model = new StreamModel(NonZero(71, 9000)).With("a", NonZero(73, 100)).With("b", b).With("c", NonZero(74, Chunk + 9));
            await WriteModelAsync(v, "/f", model);
            DfsFileInfo fileBefore = await v.Files.GetInfoAsync("/f", Ct);
            IReadOnlyList<DfsStreamInfo> listBefore = await v.Files.ListStreamsAsync("/f", Ct);
            FileInspection inspectedBefore = await InspectFileAsync(cluster, "/f");

            CommitToken token = api switch
            {
                StreamDeleteApi.DeleteAsync => await v.Files.DeleteAsync("/f:b", Ct),
                StreamDeleteApi.DeleteWithOptions => await v.Files.DeleteAsync("/f:b", new DfsMutationOptions(), Ct),
                _ => v.Files.Delete("/f:b"),
            };
            Assert.False(token.IsNone, "deleting an existing stream is a committed mutation");
            Assert.False(await v.Files.ExistsAsync("/f:b", Ct));
            Assert.True(await v.Files.ExistsAsync("/f", Ct));
            model.NamedStreams.Remove("b");
            await AssertStreamsAsync(v, "/f", model, $"after {api} of /f:b ({shape})");

            DfsFileInfo fileAfter = await v.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(fileBefore.InodeId, fileAfter.InodeId);
            Assert.Equal(fileBefore.Revision, fileAfter.Revision);
            Assert.Equal(2, fileAfter.StreamCount);
            Assert.Equal(listBefore.Where(s => s.Name != "b").Select(s => (s.Name, s.Length, s.Revision)),
                (await v.Files.ListStreamsAsync("/f", Ct)).Select(s => (s.Name, s.Length, s.Revision)));

            FileInspection inspectedAfter = await InspectFileAsync(cluster, "/f");
            Assert.Equal(inspectedBefore.Extents, inspectedAfter.Extents);
            Assert.Equal(new[] { "a", "c" }, inspectedAfter.Streams.Select(s => s.Name));
            foreach (StreamInspection kept in inspectedAfter.Streams)
            {
                StreamInspection original = inspectedBefore.Streams.Single(s => s.Name == kept.Name);
                Assert.Equal(original.Revision, kept.Revision);
                Assert.Equal(original.Extents, kept.Extents);
            }

            string before = await DescribeNamespaceAsync(v);
            await v.Files.DeleteAsync("/f:b", Ct);
            await v.Files.DeleteAsync("/absent:s", Ct);
            Assert.Equal(before, await DescribeNamespaceAsync(v));
            Assert.False(await v.Files.ExistsAsync("/absent", Ct), "deleting a stream of an absent file must not create it");
        }, Ct);
    }

    /// <summary>Stream-set signature (ordinal by name, with lengths) of a model.</summary>
    private static string Signature(StreamModel model) => string.Join(",", model.NamedStreams.Select(s => $"{s.Key}={s.Value.Length}"));

    /// <summary>Stream-set signature (in listed order, with lengths) of a listing.</summary>
    private static string Signature(IEnumerable<DfsStreamInfo> listed) => string.Join(",", listed.Select(s => $"{s.Name}={s.Length}"));

    /// <summary>
    /// Polls ListStreams of one file from another client: every single observation (one call = one revision) must be the
    /// file's absence or exactly one of the allowed complete stream sets, never a partial set.
    /// </summary>
    private sealed class StreamSetWatcher(DfsFileClient files, string file, IReadOnlyCollection<string> allowed)
    {
        private int _observations;
        private volatile bool _stop;

        public string? Violation { get; private set; }

        public int Observations => Volatile.Read(ref _observations);

        public async Task RunAsync()
        {
            while (!_stop && Violation is null)
            {
                try
                {
                    string signature = Signature(await files.ListStreamsAsync(file, Ct));
                    if (!allowed.Contains(signature, StringComparer.Ordinal))
                        Violation = $"observation {Observations}: {file} listed [{signature}]; allowed: absent or [{string.Join("] | [", allowed)}]";
                }
                catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
                {
                    // Absent: allowed before a copy and after a delete (DirectoryNotFound once a recursive delete removed the parent).
                }

                Interlocked.Increment(ref _observations);
            }
        }

        public async Task StopAfterAsync(int more)
        {
            int target = Observations + more;
            await AssertEx.EventuallyAsync(() => Task.FromResult(Observations >= target || Violation is not null), Bounded,
                "the observer keeps polling after the operation");
            _stop = true;
        }
    }

    // ------------------------------------------------------------------ ADS-008 DeleteFileRemovesStreams

    public static TheoryData<FileRemoval> AllFileRemovals()
    {
        var data = new TheoryData<FileRemoval>();
        foreach (FileRemoval removal in Enum.GetValues<FileRemoval>())
            data.Add(removal);
        return data;
    }

    /// <summary>
    /// Removing a file (delete, delete with options, sync delete, rename-over, recursive delete of its directory) removes
    /// all its streams in the same decision: a concurrent observer sees the complete stream set or none; afterwards every
    /// stream path is absent (missing-file errors, no applied inode); recreating the name yields a new inode without any
    /// old stream, and a recreated stream holds only its new bytes.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllFileRemovals))]
    [Catalog("ADS-008", "U", "INV-02", "INV-07")]
    public async Task ADS_008_DeleteFileRemovesStreams(FileRemoval removal)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/p", Ct);
            StreamModel model = new StreamModel(NonZero(81, 6000)).With("s1", NonZero(82, 200)).With("s2", NonZero(83, (2 * Chunk) + 5)).With("s3", []);
            await WriteModelAsync(v, "/p/f", model);
            InodeId oldInode = (await v.Files.GetInfoAsync("/p/f", Ct)).InodeId;
            StreamModel? replacement = null;
            InodeId? replacementInode = null;
            if (removal == FileRemoval.RenameOver)
            {
                replacement = new StreamModel(NonZero(84, 1000)).With("z", NonZero(85, 70));
                await WriteModelAsync(v, "/p/g", replacement);
                replacementInode = (await v.Files.GetInfoAsync("/p/g", Ct)).InodeId;
            }

            List<string> allowed = [Signature(model)];
            if (replacement is not null)
                allowed.Add(Signature(replacement));
            var watcher = new StreamSetWatcher(obs.Files, "/p/f", allowed);
            Task watching = watcher.RunAsync();
            CommitToken token = removal switch
            {
                FileRemoval.DeleteAsync => await v.Files.DeleteAsync("/p/f", Ct),
                FileRemoval.DeleteWithOptions => await v.Files.DeleteAsync("/p/f", new DfsMutationOptions(), Ct),
                FileRemoval.DeleteSync => v.Files.Delete("/p/f"),
                FileRemoval.RenameOver => await v.Files.MoveAsync("/p/g", "/p/f", true, Ct),
                _ => await v.Directories.DeleteAsync("/p", true, Ct),
            };
            Assert.False(token.IsNone, $"{removal} of an existing file is a committed mutation");
            await watcher.StopAfterAsync(3);
            await watching;
            Assert.True(watcher.Violation is null, $"{removal}: {watcher.Violation}");

            // With the parent directory gone (recursive delete) a missing parent may be reported as DirectoryNotFound.
            Action<Exception?, string> assertAbsent = removal == FileRemoval.RecursiveParentDelete ? AssertFileOrParentNotFound : AssertFileNotFound;
            foreach (string name in model.NamedStreams.Keys)
            {
                string path = "/p/f:" + name;
                Assert.False(await obs.Files.ExistsAsync(path, Ct), $"{removal}: {path} must not survive its file");
                if (replacement is null)
                {
                    assertAbsent(await CatchAsync(() => obs.Files.GetInfoAsync(path, Ct)), $"{removal}: GetInfo({path})");
                    assertAbsent(await CatchAsync(() => obs.Files.ReadAllBytesAsync(path, Ct)), $"{removal}: ReadAllBytes({path})");
                }
            }

            if (replacement is null)
            {
                Assert.False(await obs.Files.ExistsAsync("/p/f", Ct));
                assertAbsent(await CatchAsync(() => obs.Files.ListStreamsAsync("/p/f", Ct)), $"{removal}: ListStreams of the removed file");
                Assert.Null(await cluster.Inspect.GetFileAsync(Vol, "/p/f"));
            }
            else
            {
                Assert.Equal(replacementInode, (await obs.Files.GetInfoAsync("/p/f", Ct)).InodeId);
                await AssertStreamsAsync(obs, "/p/f", replacement, "after renaming /p/g over /p/f: only the moved inode's streams");
                await v.Files.DeleteAsync("/p/f", Ct);
            }

            if (removal == FileRemoval.RecursiveParentDelete)
                await v.Directories.CreateDirectoryAsync("/p", Ct);

            // Recreating the name never resurrects a stream of a removed inode.
            byte[] fresh = NonZero(86, 300);
            await v.Files.WriteAllBytesAsync("/p/f", fresh, Ct);
            DfsFileInfo recreated = await obs.Files.GetInfoAsync("/p/f", Ct);
            Assert.NotEqual(oldInode, recreated.InodeId);
            Assert.Equal(0, recreated.StreamCount);
            await AssertStreamsAsync(obs, "/p/f", new StreamModel(fresh), $"{removal}: the recreated file has no streams");
            foreach (string name in model.NamedStreams.Keys)
                Assert.False(await obs.Files.ExistsAsync("/p/f:" + name, Ct), $"{removal}: /p/f:{name} resurrected");

            byte[] s2 = NonZero(87, 40);
            await v.Files.WriteAllBytesAsync("/p/f:s2", s2, Ct);
            await AssertStreamsAsync(obs, "/p/f", new StreamModel(fresh).With("s2", s2), $"{removal}: a recreated stream holds only its new bytes");
            FileInspection inspected = await InspectFileAsync(cluster, "/p/f");
            StreamInspection only = Assert.Single(inspected.Streams);
            Assert.Equal("s2", only.Name);
            Assert.Equal(s2.Length, only.Length);
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-009 MoveKeepsStreams

    public static TheoryData<MoveKind> AllMoveKinds()
    {
        var data = new TheoryData<MoveKind>();
        foreach (MoveKind kind in Enum.GetValues<MoveKind>())
            data.Add(kind);
        return data;
    }

    /// <summary>
    /// Renaming/moving a file (or its directory) keeps every stream under the new name: same inode id, same stream set,
    /// lengths and bytes, the same extent objects (metadata-only move); nothing remains at the old name; moving over an
    /// existing file discards the destination's own streams.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllMoveKinds))]
    [Catalog("ADS-009", "U", "INV-02", "INV-07")]
    public async Task ADS_009_MoveKeepsStreams(MoveKind kind)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/a", Ct);
            await v.Directories.CreateDirectoryAsync("/b", Ct);
            StreamModel model = new StreamModel(NonZero(91, 7000)).With("s1", NonZero(92, 50)).With("s2", NonZero(93, (3 * Chunk) + 11))
                .With("s3", []).With("é", NonZero(94, 300));
            await WriteModelAsync(v, "/a/f", model);
            FileInspection before = await InspectFileAsync(cluster, "/a/f");
            if (kind == MoveKind.MoveOverExisting)
                await WriteModelAsync(v, "/b/g", new StreamModel(NonZero(95, 10)).With("x", NonZero(96, 20)));

            string target = kind switch
            {
                MoveKind.RenameInPlace => "/a/renamed",
                MoveKind.ParentDirectoryMove => "/z/f",
                _ => "/b/g",
            };
            CommitToken token = kind switch
            {
                MoveKind.FilesMoveAsync => await v.Files.MoveAsync("/a/f", "/b/g", false, Ct),
                MoveKind.FilesMoveWithOptions => await v.Files.MoveAsync("/a/f", "/b/g", false, new DfsMutationOptions(), Ct),
                MoveKind.FilesMoveSync => v.Files.Move("/a/f", "/b/g"),
                MoveKind.RenameInPlace => await v.Files.MoveAsync("/a/f", "/a/renamed", false, Ct),
                MoveKind.ParentDirectoryMove => await v.Directories.MoveAsync("/a", "/z", Ct),
                _ => await v.Files.MoveAsync("/a/f", "/b/g", true, Ct),
            };
            Assert.False(token.IsNone);

            DfsFileInfo moved = await obs.Files.GetInfoAsync(target, Ct);
            Assert.Equal(before.Inode, moved.InodeId);
            Assert.Equal(model.NamedStreams.Count, moved.StreamCount);
            await AssertStreamsAsync(obs, target, model, $"{kind}: streams under {target}");
            foreach (string name in model.NamedStreams.Keys)
            {
                Assert.Equal(before.Inode, (await obs.Files.GetInfoAsync(target + ":" + name, Ct)).InodeId);
                Assert.False(await obs.Files.ExistsAsync("/a/f:" + name, Ct), $"{kind}: /a/f:{name} must not remain at the old name");
            }

            Assert.False(await obs.Files.ExistsAsync("/a/f", Ct));
            if (kind == MoveKind.MoveOverExisting)
                Assert.False(await obs.Files.ExistsAsync("/b/g:x", Ct), "the overwritten destination's streams died with it");

            FileInspection after = await InspectFileAsync(cluster, target);
            Assert.Equal(before.Inode, after.Inode);
            Assert.Equal(before.Extents, after.Extents);
            Assert.Equal(before.Streams.Select(s => (s.Name, s.Length)), after.Streams.Select(s => (s.Name, s.Length)));
            foreach (StreamInspection stream in after.Streams)
                Assert.Equal(before.Streams.Single(s => s.Name == stream.Name).Extents, stream.Extents);
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-010 CopyCopiesAllStreams

    public static TheoryData<CopyKind> AllCopyKinds()
    {
        var data = new TheoryData<CopyKind>();
        foreach (CopyKind kind in Enum.GetValues<CopyKind>())
            data.Add(kind);
        return data;
    }

    /// <summary>
    /// Copy creates a new inode holding the default stream and every named stream (inline, multi-extent, empty, multibyte
    /// name) in one decision — a concurrent observer sees no copy or the complete stream set, and every copied stream
    /// carries the copy's commit revision — and the copy is independent: later overwrites, appends, range overwrites,
    /// deletes and additions on the source streams do not change it, and changes to the copy do not reach the source.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllCopyKinds))]
    [Catalog("ADS-010", "U", "INV-02", "INV-07")]
    public async Task ADS_010_CopyCopiesAllStreams(CopyKind kind)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            StreamModel model = new StreamModel(NonZero(101, 9000)).With("inline", NonZero(102, 300)).With("big", NonZero(103, (3 * Chunk) + 17))
                .With("empty", []).With("é", NonZero(104, 5));
            await WriteModelAsync(v, "/src", model);
            await v.Directories.CreateDirectoryAsync("/other", Ct);
            string dst = kind == CopyKind.CopyToOtherDirectory ? "/other/dst" : "/dst";

            var watcher = new StreamSetWatcher(obs.Files, dst, [Signature(model)]);
            Task watching = watcher.RunAsync();
            CommitToken token = kind switch
            {
                CopyKind.CopyWithOptions => await v.Files.CopyAsync("/src", dst, false, new DfsMutationOptions(), Ct),
                CopyKind.CopySync => v.Files.Copy("/src", dst),
                _ => await v.Files.CopyAsync("/src", dst, false, Ct),
            };
            await watcher.StopAfterAsync(3);
            await watching;
            Assert.True(watcher.Violation is null, $"{kind}: {watcher.Violation}");
            Assert.False(token.IsNone);

            DfsFileInfo srcInfo = await obs.Files.GetInfoAsync("/src", Ct);
            DfsFileInfo dstInfo = await obs.Files.GetInfoAsync(dst, Ct);
            Assert.NotEqual(srcInfo.InodeId, dstInfo.InodeId);
            await AssertStreamsAsync(obs, dst, model, $"{kind}: the copy");
            Assert.Equal(token.Sequence, dstInfo.Revision);
            Assert.All(await obs.Files.ListStreamsAsync(dst, Ct), s => Assert.True(s.Revision == token.Sequence,
                $"{dst}:{s.Name} has revision {s.Revision}; every stream of the copy is published by the copy's decision {token.Sequence}"));

            // Later changes to the source do not affect the copy.
            StreamModel changed = model.Clone();
            changed.Default = NonZero(105, 4000);
            await v.Files.WriteAllBytesAsync("/src", changed.Default, Ct);
            changed.NamedStreams["inline"] = NonZero(106, 333);
            await v.Files.WriteAllBytesAsync("/src:inline", changed.NamedStreams["inline"], Ct);
            byte[] extra = NonZero(107, 1000);
            await v.Files.AppendAllBytesAsync("/src:big", extra, Ct);
            byte[] patch = NonZero(108, 100);
            await using (DfsStream s = await v.Files.OpenAsync("/src:big", Writer(FileMode.Open), Ct))
            {
                s.Seek(Chunk + 10, SeekOrigin.Begin);
                await s.WriteAsync(patch, Ct);
            }

            changed.NamedStreams["big"] = Overlay([.. model.NamedStreams["big"], .. extra], Chunk + 10, patch);
            await v.Files.DeleteAsync("/src:empty", Ct);
            changed.NamedStreams.Remove("empty");
            changed.With("new", NonZero(109, 64));
            await v.Files.WriteAllBytesAsync("/src:new", changed.NamedStreams["new"], Ct);
            await AssertStreamsAsync(obs, dst, model, $"{kind}: the copy after the source changed");
            await AssertStreamsAsync(obs, "/src", changed, $"{kind}: the changed source");

            // Changes to the copy do not reach the source.
            await v.Files.WriteAllBytesAsync(dst + ":é", NonZero(110, 50), Ct);
            await v.Files.DeleteAsync(dst + ":big", Ct);
            await AssertStreamsAsync(obs, "/src", changed, $"{kind}: the source after the copy changed");
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-011 WriteAllReplacesOnlyAddressedStream

    public static TheoryData<ReplaceApi, ReplaceTarget> ReplaceCases()
    {
        var data = new TheoryData<ReplaceApi, ReplaceTarget>();
        foreach (ReplaceApi api in Enum.GetValues<ReplaceApi>())
        {
            foreach (ReplaceTarget target in Enum.GetValues<ReplaceTarget>())
                data.Add(api, target);
        }

        return data;
    }

    /// <summary>
    /// WriteAll (bytes/text/lines, async/sync/with options) and Copy over an existing file replace only the addressed
    /// stream: on <c>/f</c> the default stream (named streams keep bytes, revisions and extents); on <c>/f:a</c> only that
    /// stream (the default stream keeps bytes, revision and extents, other streams theirs); on an absent <c>/f:c</c> a
    /// stream is added. The inode is preserved; a copy source's own named streams are not merged into the target.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReplaceCases))]
    [Catalog("ADS-011", "U", "INV-02", "INV-07")]
    public async Task ADS_011_WriteAllReplacesOnlyAddressedStream(ReplaceApi api, ReplaceTarget target)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            StreamModel expected = new StreamModel(NonZero(111, (2 * Chunk) + 1)).With("a", NonZero(112, 500)).With("b", NonZero(113, (2 * Chunk) + 3));
            await WriteModelAsync(v, "/f", expected);
            StreamModel source = new StreamModel(NonZero(114, Chunk + 7)).With("a", NonZero(115, 77)).With("z", NonZero(116, 88));
            if (api == ReplaceApi.CopyOverwrite)
                await WriteModelAsync(v, "/src", source);

            FileInspection before = await InspectFileAsync(cluster, "/f");
            DfsFileInfo infoBefore = await v.Files.GetInfoAsync("/f", Ct);
            Dictionary<string, ulong> revisionsBefore = (await v.Files.ListStreamsAsync("/f", Ct)).ToDictionary(s => s.Name, s => s.Revision, StringComparer.Ordinal);
            (string targetPath, string? targetStream) = target switch
            {
                ReplaceTarget.DefaultStream => ("/f", (string?)null),
                ReplaceTarget.ExistingNamedStream => ("/f:a", "a"),
                _ => ("/f:c", "c"),
            };

            byte[] stored;
            switch (api)
            {
                case ReplaceApi.WriteAllBytes:
                    stored = NonZero(117, 3000);
                    await v.Files.WriteAllBytesAsync(targetPath, stored, Ct);
                    break;
                case ReplaceApi.WriteAllBytesWithOptions:
                    stored = NonZero(117, 3000);
                    await v.Files.WriteAllBytesAsync(targetPath, stored, new DfsMutationOptions(), Ct);
                    break;
                case ReplaceApi.WriteAllBytesSync:
                    stored = NonZero(117, 3000);
                    v.Files.WriteAllBytes(targetPath, stored);
                    break;
                case ReplaceApi.WriteAllText:
                {
                    string text = TestData.Utf8Text(118, 2500);
                    stored = Utf8NoBom.GetBytes(text);
                    await v.Files.WriteAllTextAsync(targetPath, text, Utf8NoBom, Ct);
                    Assert.Equal(text, await v.Files.ReadAllTextAsync(targetPath, Utf8NoBom, Ct));
                    break;
                }

                case ReplaceApi.WriteAllLines:
                {
                    string[] lines = Lines(119, 2000);
                    await v.Files.WriteAllLinesAsync(targetPath, lines, Utf8NoBom, Ct);
                    await v.Files.WriteAllLinesAsync("/reference", lines, Utf8NoBom, Ct);
                    stored = await v.Files.ReadAllBytesAsync("/reference", Ct);
                    Assert.Equal(lines, await v.Files.ReadAllLinesAsync(targetPath, Utf8NoBom, Ct));
                    break;
                }

                default:
                    stored = source.Default;
                    if (target != ReplaceTarget.NewNamedStream)
                    {
                        Exception? refused = await CatchAsync(() => v.Files.CopyAsync("/src", targetPath, false, Ct));
                        Assert.True(refused is DfsAlreadyExistsException { ErrorCode: DfsErrorCode.AlreadyExists },
                            $"Copy without overwrite onto existing {targetPath}: expected AlreadyExists, got {Describe(refused)}");
                        await AssertStreamsAsync(v, "/f", expected, "a refused copy changes nothing");
                    }

                    await v.Files.CopyAsync("/src", targetPath, target != ReplaceTarget.NewNamedStream, Ct);
                    break;
            }

            if (targetStream is null)
                expected.Default = stored;
            else
                expected.NamedStreams[targetStream] = stored;
            string what = $"{api} on {targetPath}";
            await AssertStreamsAsync(v, "/f", expected, what);

            DfsFileInfo infoAfter = await v.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(infoBefore.InodeId, infoAfter.InodeId);
            FileInspection after = await InspectFileAsync(cluster, "/f");
            if (target != ReplaceTarget.DefaultStream)
            {
                Assert.True(infoBefore.Revision == infoAfter.Revision, $"{what}: the default stream's revision must not change");
                Assert.Equal(before.Extents, after.Extents);
            }

            Dictionary<string, ulong> revisionsAfter = (await v.Files.ListStreamsAsync("/f", Ct)).ToDictionary(s => s.Name, s => s.Revision, StringComparer.Ordinal);
            foreach ((string name, ulong revision) in revisionsBefore)
            {
                if (string.Equals(name, targetStream, StringComparison.Ordinal))
                    continue;
                Assert.True(revisionsAfter[name] == revision, $"{what}: untouched stream {name} changed revision {revision} -> {revisionsAfter[name]}");
                Assert.Equal(before.Streams.Single(s => s.Name == name).Extents, after.Streams.Single(s => s.Name == name).Extents);
            }

            if (api == ReplaceApi.CopyOverwrite)
                await AssertStreamsAsync(v, "/src", source, "the copy source is unchanged");
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-012 StreamOpenModes

    /// <summary>
    /// Each FileMode applied to a stream path follows FSD §6.1 for that stream alone: on an existing stream (CreateNew →
    /// AlreadyExists; Create/Truncate truncate at open; Open/OpenOrCreate keep bytes; Append writes at the stream's end,
    /// cannot seek or SetLength); on an absent stream of an existing file and on an absent file (creating modes create, Open
    /// and Truncate fail with FileNotFoundException and create nothing); access combinations the mode forbids are
    /// rejected before any change. The default stream and sibling streams never change.
    /// </summary>
    [Theory]
    [InlineData(FileMode.CreateNew)]
    [InlineData(FileMode.Create)]
    [InlineData(FileMode.Open)]
    [InlineData(FileMode.OpenOrCreate)]
    [InlineData(FileMode.Truncate)]
    [InlineData(FileMode.Append)]
    [Catalog("ADS-012", "U", "INV-02", "INV-07")]
    public async Task ADS_012_StreamOpenModes(FileMode mode)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume ov = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/m", Ct);
            byte[] d = NonZero(0x1201, 6000), s0 = NonZero(0x1202, 3000), t0 = NonZero(0x1203, Chunk + 3), x = NonZero(0x1204, 1000), y = NonZero(0x1205, 222);
            StreamModel f = new StreamModel(d).With("s", s0).With("t", t0);
            StreamModel g = new StreamModel(d).With("t", t0);
            await WriteModelAsync(v, "/m/f", f);
            await WriteModelAsync(v, "/m/g", g);
            bool creates = mode is FileMode.CreateNew or FileMode.Create or FileMode.OpenOrCreate or FileMode.Append;
            DfsOpenOptions options = Writer(mode, mode == FileMode.Open ? FileAccess.ReadWrite : FileAccess.Write);

            // 1. The addressed stream exists.
            if (mode == FileMode.CreateNew)
            {
                Exception? ex = await CatchAsync(async () => { await using DfsStream s = await v.Files.OpenAsync("/m/f:s", options, Ct); });
                Assert.True(ex is DfsAlreadyExistsException { ErrorCode: DfsErrorCode.AlreadyExists }, $"CreateNew on an existing stream: {Describe(ex)}");
            }
            else
            {
                await using (DfsStream s = await v.Files.OpenAsync("/m/f:s", options, Ct))
                {
                    long atOpen = mode is FileMode.Create or FileMode.Truncate ? 0 : s0.Length;
                    Assert.True((await ov.Files.GetInfoAsync("/m/f:s", Ct)).Length == atOpen, $"{mode}: committed stream length right after the open must be {atOpen}");
                    Assert.Equal(atOpen, await s.GetLengthAsync(Ct));
                    switch (mode)
                    {
                        case FileMode.Open:
                            BytesEqual(s0, await ReadToEndAsync(s), "Open preserves the stream's bytes");
                            s.Seek(0, SeekOrigin.Begin);
                            await s.WriteAsync(x, Ct);
                            f.NamedStreams["s"] = Overlay(s0, 0, x);
                            break;
                        case FileMode.OpenOrCreate:
                            Assert.Equal(0L, s.Position);
                            await s.WriteAsync(x, Ct);
                            f.NamedStreams["s"] = Overlay(s0, 0, x);
                            break;
                        case FileMode.Append:
                            Assert.True(s.IsAppend);
                            Assert.False(s.CanSeek);
                            Assert.Throws<NotSupportedException>(() => s.Seek(0, SeekOrigin.Begin));
                            Assert.Throws<NotSupportedException>(() => s.SetLength(1));
                            Assert.Throws<NotSupportedException>(() => s.Position = 0);
                            await s.WriteAsync(x, Ct);
                            await s.WriteAsync(y, Ct);
                            f.NamedStreams["s"] = [.. s0, .. x, .. y];
                            break;
                        default:
                            await s.WriteAsync(x, Ct);
                            f.NamedStreams["s"] = x;
                            break;
                    }
                }
            }

            await AssertStreamsAsync(ov, "/m/f", f, $"{mode} on an existing stream");

            // 2. Existing file, absent stream.
            if (creates)
            {
                await using (DfsStream s = await v.Files.OpenAsync("/m/g:s", options, Ct))
                {
                    Assert.True(await ov.Files.ExistsAsync("/m/g:s", Ct), $"{mode}: the creating open committed the stream");
                    Assert.Equal(0L, (await ov.Files.GetInfoAsync("/m/g:s", Ct)).Length);
                    await s.WriteAsync(x, Ct);
                }

                g.With("s", x);
            }
            else
            {
                AssertFileNotFound(await CatchAsync(async () => { await using DfsStream s = await v.Files.OpenAsync("/m/g:s", options, Ct); }), $"{mode} on an absent stream");
                Assert.False(await ov.Files.ExistsAsync("/m/g:s", Ct), $"{mode}: a failed open must not create the stream");
            }

            await AssertStreamsAsync(ov, "/m/g", g, $"{mode} on an absent stream of an existing file");

            // 3. Absent file.
            if (creates)
            {
                await using (DfsStream s = await v.Files.OpenAsync("/m/h:s", options, Ct))
                {
                    Assert.True(await ov.Files.ExistsAsync("/m/h", Ct), $"{mode}: the creating open committed the file");
                    await s.WriteAsync(x, Ct);
                }

                await AssertStreamsAsync(ov, "/m/h", new StreamModel([]).With("s", x), $"{mode} on an absent file");
            }
            else
            {
                AssertFileNotFound(await CatchAsync(async () => { await using DfsStream s = await v.Files.OpenAsync("/m/h:s", options, Ct); }), $"{mode} on an absent file");
                Assert.False(await ov.Files.ExistsAsync("/m/h", Ct), $"{mode}: a failed open must not create the file");
            }

            // 4. Access combinations the mode forbids are rejected before any change.
            FileAccess[] forbidden = mode switch
            {
                FileMode.CreateNew or FileMode.Create => [FileAccess.Read],
                FileMode.Truncate or FileMode.Append => [FileAccess.Read, FileAccess.ReadWrite],
                _ => [],
            };
            string before = await DescribeNamespaceAsync(v);
            foreach (FileAccess bad in forbidden)
            {
                foreach (string path in new[] { "/m/f:s", "/m/g:t", "/m/q:s" })
                {
                    Exception? ex = await CatchAsync(async () =>
                    {
                        await using DfsStream s = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = bad, Share = FileShare.Read }, Ct);
                    });
                    Assert.True(ex is ArgumentException, $"{mode}/{bad} on {path}: expected ArgumentException, got {Describe(ex)}");
                }
            }

            Assert.Equal(before, await DescribeNamespaceAsync(v));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-013 StreamSparseAndTruncate

    /// <summary>
    /// On a named stream: seeking beyond EOF does not extend it; a write beyond EOF leaves a zero-filled hole (no extent
    /// for a whole aligned hole; IsSparse); SetLength never moves Position, commits immediately with WriteThrough and at
    /// flush with OnFlush; shrinking through data then extending shows zeroes (no resurrected tail); a Truncate open then
    /// SetLength yields all zeroes. The default stream and the sibling stream are untouched throughout.
    /// </summary>
    [Theory]
    [InlineData(DfsWriteMode.OnFlush, false)]
    [InlineData(DfsWriteMode.OnFlush, true)]
    [InlineData(DfsWriteMode.WriteThrough, false)]
    [InlineData(DfsWriteMode.WriteThrough, true)]
    [Catalog("ADS-013", "U", "INV-07")]
    public async Task ADS_013_StreamSparseAndTruncate(DfsWriteMode writeMode, bool large)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume ov = observerClient.GetVolume(Vol);
            const string file = "/sp";
            const string stream = "/sp:s";
            byte[] d = NonZero(1301, 5000), t = NonZero(1302, 2000);
            int len0 = large ? (3 * Chunk) + 100 : 1000;
            byte[] s0 = NonZero(1303, len0);
            await WriteModelAsync(v, file, new StreamModel(d).With("s", s0).With("t", t));
            bool onFlush = writeMode == DfsWriteMode.OnFlush;

            // 1. Sparse write beyond EOF.
            long gapEnd = large ? len0 + (2L * Chunk) + 5 : len0 + 1500;
            byte[] tail = NonZero(1304, 500);
            await using (DfsStream s = await v.Files.OpenAsync(stream, Writer(FileMode.Open, FileAccess.ReadWrite, writeMode), Ct))
            {
                Assert.Equal(gapEnd, s.Seek(gapEnd, SeekOrigin.Begin));
                Assert.Equal(len0, await s.GetLengthAsync(Ct));
                Assert.Equal(len0, (await ov.Files.GetInfoAsync(stream, Ct)).Length);
                await s.WriteAsync(tail, Ct);
                if (onFlush)
                    await s.FlushAsync(Ct);
            }

            byte[] expected = Overlay(s0, gapEnd, tail);
            byte[] actual = await ReadCommittedAsync(ov, stream);
            BytesEqual(expected, actual, "sparse write on a stream");
            AssertAllZero(actual.AsSpan(len0, (int)(gapEnd - len0)), len0, "the hole left by a write beyond the stream's EOF");
            if (large)
            {
                Assert.True((await ov.Files.GetInfoAsync(stream, Ct)).IsSparse, "a stream with an unwritten 2-chunk gap is sparse");
                StreamInspection si = await InspectStreamAsync(cluster, file, "s");
                long holeStart = (len0 + Chunk - 1L) / Chunk * Chunk;
                long holeEnd = gapEnd / Chunk * Chunk;
                Assert.True(holeEnd > holeStart, "precondition: the gap covers a whole aligned chunk");
                Assert.Equal(expected.Length, si.Length);
                Assert.DoesNotContain(si.Extents, e => e.Offset < holeEnd && e.Offset + e.Length > holeStart);
            }

            // 2. Shrink through data, then extend again.
            long cut = (len0 / 2) + 3;
            await using (DfsStream s = await v.Files.OpenAsync(stream, Writer(FileMode.Open, FileAccess.ReadWrite, writeMode), Ct))
            {
                s.Position = gapEnd;
                await s.SetLengthAsync(cut, Ct);
                Assert.Equal(gapEnd, s.Position);
                Assert.Equal(cut, await s.GetLengthAsync(Ct));
                if (onFlush)
                {
                    Assert.True((await ov.Files.GetInfoAsync(stream, Ct)).Length == expected.Length, "OnFlush SetLength stays private until flush");
                    await s.FlushAsync(Ct);
                }

                Assert.Equal(cut, (await ov.Files.GetInfoAsync(stream, Ct)).Length);
                await s.SetLengthAsync(expected.Length, Ct);
                if (onFlush)
                    await s.FlushAsync(Ct);
            }

            expected = Resize(Resize(expected, cut), expected.Length);
            actual = await ReadCommittedAsync(ov, stream);
            BytesEqual(expected, actual, "shrink then extend on a stream");
            AssertAllZero(actual.AsSpan((int)cut), cut, "discarded stream bytes must never reappear after extension");

            // 3. Truncate at open, then extend.
            await using (DfsStream s = await v.Files.OpenAsync(stream, Writer(FileMode.Truncate, FileAccess.Write, writeMode), Ct))
            {
                Assert.Equal(0L, await s.GetLengthAsync(Ct));
                await s.SetLengthAsync(len0, Ct);
            }

            expected = new byte[len0];
            actual = await ReadCommittedAsync(ov, stream);
            BytesEqual(expected, actual, "truncated then extended stream is all zeroes");

            await AssertStreamsAsync(ov, file, new StreamModel(d).With("s", expected).With("t", t), "default and sibling streams are untouched");
        }, Ct);
    }

    /// <summary>Stages <paramref name="data"/> as the complete new contents of a seekable writable stream and commits it.</summary>
    private static async Task<CommitToken> ReplaceAndCommitAsync(DfsStream stream, byte[] data)
    {
        stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(data, Ct);
        await stream.SetLengthAsync(data.Length, Ct);
        return await stream.CommitAsync(Ct);
    }

    private static async Task<HandleInspection[]> ActiveHandlesAsync(TestCluster cluster, InodeId inode) =>
        (await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles.Where(h => h.Inode == inode && !h.Expired).ToArray();

    // ------------------------------------------------------------------ ADS-014 PerStreamSharing

    /// <summary>
    /// Writer exclusivity and share compatibility are per stream: with a writer on <c>/f:é</c>, writers on <c>/f:b</c>
    /// and on <c>/f</c> are admitted (three active writer handles on one inode), while a second writer on <c>/f:é</c> —
    /// through any spelling of that stream, including the implicit writers of WriteAll/AppendAll — fails with a typed
    /// sharing violation; a reader sharing ReadWrite coexists, one refusing to share Write conflicts; FileShare.None on an
    /// otherwise unused stream succeeds and excludes others only on that stream. All three writers publish independently,
    /// and once the first closes the conflicting opener is admitted with a newer generation.
    /// </summary>
    [Theory]
    [InlineData(StreamAlias.Plain)]
    [InlineData(StreamAlias.DataSuffix)]
    [InlineData(StreamAlias.Decomposed)]
    [Catalog("ADS-014", "U", "INV-02", "INV-07")]
    public async Task ADS_014_PerStreamSharing(StreamAlias alias)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            await using DfsClient c3 = await ConnectAsync(cluster, "c3");
            await using DfsClient c4 = await ConnectAsync(cluster, "c4");
            await using DfsClient c5 = await ConnectAsync(cluster, "c5");
            const string file = "/sh";
            const string a = "/sh:é";
            string aliasPath = alias switch
            {
                StreamAlias.Plain => a,
                StreamAlias.DataSuffix => a + ":$DATA",
                _ => file + ":é",
            };
            byte[] d0 = NonZero(1401, 5000), a0 = NonZero(1402, 3000), b0 = NonZero(1403, Chunk + 1), cBytes = NonZero(1404, 100), x = NonZero(1405, 64);
            await WriteModelAsync(c1.GetVolume(Vol), file, new StreamModel(d0).With("é", a0).With("b", b0).With("c", cBytes));

            DfsStream w1 = await c1.GetVolume(Vol).Files.OpenAsync(a, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
            DfsStream w2 = await c2.GetVolume(Vol).Files.OpenAsync("/sh:b", Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
            DfsStream w3 = await c3.GetVolume(Vol).Files.OpenAsync(file, Writer(FileMode.Open, FileAccess.ReadWrite), Ct);
            InodeId inode = w1.InodeId;
            Assert.Equal(inode, w2.InodeId);
            Assert.Equal(inode, w3.InodeId);
            Assert.True(w1.WriterGeneration > 0 && w2.WriterGeneration > 0 && w3.WriterGeneration > 0);
            HandleInspection[] writers = (await ActiveHandlesAsync(cluster, inode)).Where(h => h.IsWriter).ToArray();
            Assert.Equal(3, writers.Length);
            Assert.Equal(new[] { w1.WriterGeneration, w2.WriterGeneration, w3.WriterGeneration }.Order(), writers.Select(h => h.Generation).Order());

            DfsFileClient f4 = c4.GetVolume(Vol).Files;
            AssertSharingViolation(await CatchAsync(async () => { await using DfsStream s = await f4.OpenAsync(aliasPath, Writer(FileMode.Open), Ct); }),
                $"a second writer on {aliasPath}");
            AssertSharingViolation(await CatchAsync(() => f4.WriteAllBytesAsync(aliasPath, x, Ct)), $"WriteAllBytes({aliasPath}) needs the stream's writer reservation");
            AssertSharingViolation(await CatchAsync(() => f4.AppendAllBytesAsync(aliasPath, x, Ct)), $"AppendAllBytes({aliasPath}) needs the stream's writer reservation");
            Assert.Equal(3, (await ActiveHandlesAsync(cluster, inode)).Count(h => h.IsWriter));

            await using (DfsStream r = await f4.OpenAsync(aliasPath, Reader(), Ct))
                BytesEqual(a0, await ReadToEndAsync(r), "a reader sharing ReadWrite coexists with the stream's writer and sees committed bytes");
            AssertSharingViolation(await CatchAsync(async () =>
            {
                await using DfsStream r = await f4.OpenAsync(aliasPath, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read }, Ct);
            }), "a reader that refuses to share Write conflicts with the stream's writer");

            await using (DfsStream exclusive = await c5.GetVolume(Vol).Files.OpenAsync("/sh:c",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.None }, Ct))
            {
                AssertSharingViolation(await CatchAsync(async () => { await using DfsStream r = await f4.OpenAsync("/sh:c", Reader(), Ct); }),
                    "FileShare.None excludes other handles on that stream");
                await using DfsStream other = await f4.OpenAsync("/sh:b", Reader(), Ct);
                BytesEqual(b0, await ReadToEndAsync(other), "FileShare.None on /sh:c does not affect /sh:b");
            }

            byte[] a1 = NonZero(1406, 2000), b1 = NonZero(1407, (2 * Chunk) + 9), d1 = NonZero(1408, 7000);
            await Task.WhenAll(ReplaceAndCommitAsync(w1, a1), ReplaceAndCommitAsync(w2, b1), ReplaceAndCommitAsync(w3, d1));
            ulong g1 = w1.WriterGeneration;
            await w1.DisposeAsync();
            await w2.DisposeAsync();
            await w3.DisposeAsync();
            await AssertStreamsAsync(c5.GetVolume(Vol), file, new StreamModel(d1).With("é", a1).With("b", b1).With("c", cBytes),
                "three writers on three streams of one inode published independently");

            await using DfsStream w4 = await f4.OpenAsync(aliasPath, Writer(FileMode.Open), Ct);
            Assert.True(w4.WriterGeneration > g1, $"the successor writer on the stream must get a newer generation ({w4.WriterGeneration} > {g1})");
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-015 DeleteNeedsShareDeleteOnAllStreams

    private static DfsOpenOptions HandleOptions(StreamHandleKind kind, bool shareDelete)
    {
        FileShare delete = shareDelete ? FileShare.Delete : FileShare.None;
        return kind switch
        {
            StreamHandleKind.LiveReader => new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite | delete },
            StreamHandleKind.PinnedReader => new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite | delete, ReadView = DfsReadView.VersionPinned },
            _ => new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read | delete },
        };
    }

    /// <summary>
    /// While a handle on <c>/d15/f:s</c> lacks FileShare.Delete, deleting the file (plain, with options, through a recursive
    /// delete of its directory), renaming it, renaming another file over it and deleting the open stream all fail with a
    /// sharing violation and change nothing; deleting a different stream of the same file is allowed; the handle keeps
    /// working; after it closes, the file can be deleted.
    /// </summary>
    [Theory]
    [InlineData(StreamHandleKind.LiveReader)]
    [InlineData(StreamHandleKind.PinnedReader)]
    [InlineData(StreamHandleKind.Writer)]
    [Catalog("ADS-015", "U", "INV-02", "INV-07")]
    public async Task ADS_015_DeleteNeedsShareDeleteOnAllStreams(StreamHandleKind kind)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient holderClient = await ConnectAsync(cluster, "holder");
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/d15", Ct);
            byte[] d = NonZero(1501, 4000), s = NonZero(1502, Chunk + 50), t = NonZero(1503, 300);
            await WriteModelAsync(v, "/d15/f", new StreamModel(d).With("s", s).With("t", t));
            await v.Files.WriteAllBytesAsync("/d15/x", NonZero(1504, 10), Ct);

            await using DfsStream handle = await holderClient.GetVolume(Vol).Files.OpenAsync("/d15/f:s", HandleOptions(kind, shareDelete: false), Ct);
            string before = await DescribeNamespaceAsync(v);
            (string What, Func<Task> Action)[] refused =
            [
                ("Delete(/d15/f)", () => v.Files.DeleteAsync("/d15/f", Ct)),
                ("Delete(/d15/f) with options", () => v.Files.DeleteAsync("/d15/f", new DfsMutationOptions(), Ct)),
                ("Move(/d15/f -> /d15/g)", () => v.Files.MoveAsync("/d15/f", "/d15/g", false, Ct)),
                ("Move(/d15/x -> /d15/f, overwrite)", () => v.Files.MoveAsync("/d15/x", "/d15/f", true, Ct)),
                ("Delete(/d15/f:s) (the open stream)", () => v.Files.DeleteAsync("/d15/f:s", Ct)),
                ("recursive Delete(/d15)", () => v.Directories.DeleteAsync("/d15", true, Ct)),
            ];
            foreach ((string what, Func<Task> action) in refused)
            {
                AssertSharingViolation(await CatchAsync(action), $"{what} while a {kind} on /d15/f:s lacks FileShare.Delete");
                Assert.True(before == await DescribeNamespaceAsync(v), $"{what} was refused but changed the namespace");
            }

            await v.Files.DeleteAsync("/d15/f:t", Ct);
            Assert.False(await v.Files.ExistsAsync("/d15/f:t", Ct), "a stream without handles can be deleted");
            if (kind == StreamHandleKind.Writer)
            {
                byte[] patch = NonZero(1505, 30);
                handle.Seek(0, SeekOrigin.Begin);
                await handle.WriteAsync(patch, Ct);
                await handle.FlushAsync(Ct);
                s = Overlay(s, 0, patch);
            }
            else
            {
                BytesEqual(s, await ReadToEndAsync(handle), $"the {kind} still reads its stream");
            }

            await handle.DisposeAsync();
            await AssertStreamsAsync(v, "/d15/f", new StreamModel(d).With("s", s), "after the refused operations and the deletion of /d15/f:t");
            await v.Files.DeleteAsync("/d15/f", Ct);
            Assert.False(await v.Files.ExistsAsync("/d15/f:s", Ct));
        }, Ct);
    }

    /// <summary>
    /// With FileShare.Delete on the stream handle the file can be deleted: the file and every stream leave the namespace at
    /// once, the handle keeps reading (or writing and flushing) the detached stream without recreating any path, and a new
    /// file at the name has a new inode and no streams.
    /// </summary>
    [Theory]
    [InlineData(StreamHandleKind.LiveReader)]
    [InlineData(StreamHandleKind.PinnedReader)]
    [InlineData(StreamHandleKind.Writer)]
    [Catalog("ADS-015", "U", "INV-02", "INV-07")]
    public async Task ADS_015_DeleteNeedsShareDeleteOnAllStreams_ShareDeleteDetaches(StreamHandleKind kind)
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient holderClient = await ConnectAsync(cluster, "holder");
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            byte[] d = NonZero(1511, 4000), s = NonZero(1512, Chunk + 50), t = NonZero(1513, 300);
            await WriteModelAsync(v, "/f", new StreamModel(d).With("s", s).With("t", t));
            InodeId oldInode = (await v.Files.GetInfoAsync("/f", Ct)).InodeId;

            await using DfsStream handle = await holderClient.GetVolume(Vol).Files.OpenAsync("/f:s", HandleOptions(kind, shareDelete: true), Ct);
            await v.Files.DeleteAsync("/f", Ct);
            Assert.False(await v.Files.ExistsAsync("/f", Ct));
            Assert.False(await v.Files.ExistsAsync("/f:s", Ct));
            Assert.False(await v.Files.ExistsAsync("/f:t", Ct));
            AssertFileNotFound(await CatchAsync(() => v.Files.ListStreamsAsync("/f", Ct)), "ListStreams of the deleted file");

            if (kind == StreamHandleKind.Writer)
            {
                byte[] patch = NonZero(1514, 40);
                handle.Seek(0, SeekOrigin.Begin);
                await handle.WriteAsync(patch, Ct);
                await handle.FlushAsync(Ct);
                BytesEqual(Overlay(s, 0, patch), await ReadToEndAsync(handle), "the writer reads its own detached stream");
                Assert.False(await v.Files.ExistsAsync("/f", Ct), "writes through a detached stream never recreate the path");
                Assert.False(await v.Files.ExistsAsync("/f:s", Ct), "writes through a detached stream never recreate the stream path");
            }
            else
            {
                BytesEqual(s, await ReadToEndAsync(handle), $"the {kind} keeps reading the detached stream");
            }

            await handle.DisposeAsync();
            await v.Files.WriteAllBytesAsync("/f", NonZero(1515, 10), Ct);
            DfsFileInfo recreated = await v.Files.GetInfoAsync("/f", Ct);
            Assert.NotEqual(oldInode, recreated.InodeId);
            Assert.Empty(await v.Files.ListStreamsAsync("/f", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-017 StreamLimit

    private static string LimitStream(int i) => $"/lim:s{i:D4}";

    private static void AssertStreamLimitExceeded(Exception? ex, string because)
    {
        if (ex is not DfsLimitExceededException limit)
        {
            Assert.Fail($"{because}: expected DfsLimitExceededException but got {Describe(ex)}");
            return;
        }

        // Same typed surface as the plan's limits (TX/PUB suites): the limit is named after its setting, carries its value,
        // and the request exceeds it.
        Assert.Equal(DfsErrorCode.LimitExceeded, limit.ErrorCode);
        Assert.Equal(nameof(DfsStreamPath.MaxStreamsPerInode), limit.LimitName);
        Assert.Equal(DfsStreamPath.MaxStreamsPerInode, limit.Limit);
        Assert.True(limit.Requested > limit.Limit, $"{because}: Requested ({limit.Requested}) must exceed the limit ({limit.Limit})");
    }

    /// <summary>
    /// With MaxStreamsPerInode (1024) named streams, creating one more through any creating entry point (helpers, every
    /// creating open mode, Copy onto a new stream, a transaction) fails with LimitExceeded naming the limit and changes
    /// nothing; rewriting/appending existing streams still works; deleting one stream frees exactly one slot; copying a
    /// file at the limit is legal.
    /// </summary>
    [Fact]
    [Catalog("ADS-017", "U", "INV-02", "INV-07")]
    public async Task ADS_017_StreamLimit()
    {
        await ClusterTest.RunAsync(Core with { ScenarioTimeout = TimeSpan.FromMinutes(5) }, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            const int max = DfsStreamPath.MaxStreamsPerInode;
            await v.Files.WriteAllBytesAsync("/lim", NonZero(1700, 100), Ct);
            await v.Files.WriteAllBytesAsync("/other", NonZero(1701, 64), Ct);
            for (int first = 0; first < max; first += 64)
            {
                await Task.WhenAll(Enumerable.Range(first, Math.Min(64, max - first))
                    .Select(i => v.Files.WriteAllBytesAsync(LimitStream(i), NonZero(1702 + (ulong)i, 1 + (i % 7)), Ct)));
            }

            Assert.Equal(max, (await v.Files.ListStreamsAsync("/lim", Ct)).Count);
            Assert.Equal(max, (await v.Files.GetInfoAsync("/lim", Ct)).StreamCount);

            async Task AssertRefusedAsync(string what, string newStream, Func<Task> attempt)
            {
                string before = string.Join(",", (await v.Files.ListStreamsAsync("/lim", Ct)).Select(s => $"{s.Name}={s.Length}@{s.Revision}"));
                AssertStreamLimitExceeded(await CatchAsync(attempt), $"{what} creating stream #{max + 1}");
                string after = string.Join(",", (await v.Files.ListStreamsAsync("/lim", Ct)).Select(s => $"{s.Name}={s.Length}@{s.Revision}"));
                Assert.True(before == after, $"{what}: a refused creation changed the stream set");
                Assert.False(await v.Files.ExistsAsync(newStream, Ct), $"{what}: {newStream} must not exist");
            }

            const string extra = "/lim:extra";
            await AssertRefusedAsync("WriteAllBytes", extra, () => v.Files.WriteAllBytesAsync(extra, Payload, Ct));
            await AssertRefusedAsync("WriteAllText", extra, () => v.Files.WriteAllTextAsync(extra, "text", null, Ct));
            await AssertRefusedAsync("AppendAllBytes", extra, () => v.Files.AppendAllBytesAsync(extra, Payload, Ct));
            foreach (FileMode mode in new[] { FileMode.CreateNew, FileMode.Create, FileMode.OpenOrCreate, FileMode.Append })
                await AssertRefusedAsync($"Open({mode})", extra, async () => { await using DfsStream s = await v.Files.OpenAsync(extra, Writer(mode), Ct); });
            await AssertRefusedAsync("OpenWrite", extra, async () => { await using DfsStream s = await v.Files.OpenWriteAsync(extra, Ct); });
            await AssertRefusedAsync("Copy onto a new stream", extra, () => v.Files.CopyAsync("/other", extra, false, Ct));
            await AssertRefusedAsync("transaction", extra, async () =>
            {
                await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                await tx.Files.WriteAllBytesAsync(extra, Payload, Ct);
                await tx.CommitAsync(Ct);
            });

            // Existing streams can still be rewritten and appended (not creations).
            byte[] rewritten = NonZero(1790, 300);
            await v.Files.WriteAllBytesAsync(LimitStream(0), rewritten, Ct);
            BytesEqual(rewritten, await v.Files.ReadAllBytesAsync(LimitStream(0), Ct), "rewriting an existing stream at the limit");
            await v.Files.AppendAllBytesAsync(LimitStream(max - 1), Payload, Ct);

            // Deleting one stream frees exactly one slot.
            await v.Files.DeleteAsync(LimitStream(5), Ct);
            await v.Files.WriteAllBytesAsync(extra, Payload, Ct);
            Assert.Equal(max, (await v.Files.ListStreamsAsync("/lim", Ct)).Count);
            await AssertRefusedAsync("WriteAllBytes after refilling the freed slot", "/lim:extra2", () => v.Files.WriteAllBytesAsync("/lim:extra2", Payload, Ct));

            // A copy of a file at the limit is complete.
            await v.Files.CopyAsync("/lim", "/lim2", false, Ct);
            Assert.Equal((await v.Files.ListStreamsAsync("/lim", Ct)).Select(s => (s.Name, s.Length)), (await v.Files.ListStreamsAsync("/lim2", Ct)).Select(s => (s.Name, s.Length)));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-018 StreamNamesNormalised

    public static TheoryData<string, string, bool> StreamNamePairs => new()
    {
        { "Ångström", "Ångström", true },
        { "é", "é", true },
        { "가", "가", true },
        { "Å", "Å", true },
        { "ȩ́", "ȩ́", true },
        { "Stream", "stream", false },
        { "ﬁle", "file", false },
        { "i", "I", false },
        { "ı", "i", false },
        { "Å", "å", false },
    };

    /// <summary>
    /// NFC-equivalent stream names (composed/decomposed, singleton and reordered marks) address one stream — the same bytes,
    /// one ListStreams entry under the NFC name, replace and delete through either spelling — while case-different and
    /// compatibility-only-equivalent names are distinct streams (oracle: ICU NFC in the test process).
    /// </summary>
    [Theory]
    [MemberData(nameof(StreamNamePairs))]
    [Catalog("ADS-018", "U", "INV-07")]
    public async Task ADS_018_StreamNamesNormalised(string first, string second, bool sameStream)
    {
        string nfcFirst = first.Normalize(NormalizationForm.FormC);
        string nfcSecond = second.Normalize(NormalizationForm.FormC);
        Assert.Equal(sameStream, string.Equals(nfcFirst, nfcSecond, StringComparison.Ordinal));
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] a = NonZero(1801, 700), b = NonZero(1802, 900);
            await files.WriteAllBytesAsync("/n", NonZero(1800, 50), Ct);
            await files.WriteAllBytesAsync("/n:" + first, a, Ct);
            DfsFileInfo info = await files.GetInfoAsync("/n:" + first, Ct);
            Assert.Equal(nfcFirst, info.StreamName);
            Assert.Equal("/n:" + nfcFirst, info.Path);
            Assert.Equal(sameStream, await files.ExistsAsync("/n:" + second, Ct));
            if (sameStream)
            {
                BytesEqual(a, await files.ReadAllBytesAsync("/n:" + second, Ct), "the equivalent spelling reads the same stream");
                await files.WriteAllBytesAsync("/n:" + second, b, Ct);
                DfsStreamInfo only = Assert.Single(await files.ListStreamsAsync("/n", Ct));
                Assert.Equal(nfcFirst, only.Name);
                Assert.Equal(b.Length, only.Length);
                BytesEqual(b, await files.ReadAllBytesAsync("/n:" + first, Ct), "writing through the equivalent spelling replaced the same stream");
                await files.DeleteAsync("/n:" + second, Ct);
                Assert.Empty(await files.ListStreamsAsync("/n", Ct));
                Assert.False(await files.ExistsAsync("/n:" + first, Ct));
            }
            else
            {
                AssertFileNotFound(await CatchAsync(() => files.ReadAllBytesAsync("/n:" + second, Ct)), "a distinct name is a distinct (absent) stream");
                await files.WriteAllBytesAsync("/n:" + second, b, Ct);
                Assert.Equal(new[] { nfcFirst, nfcSecond }.Order(StringComparer.Ordinal), (await files.ListStreamsAsync("/n", Ct)).Select(s => s.Name));
                BytesEqual(a, await files.ReadAllBytesAsync("/n:" + first, Ct), "first stream");
                BytesEqual(b, await files.ReadAllBytesAsync("/n:" + second, Ct), "second stream");
                await files.DeleteAsync("/n:" + second, Ct);
                DfsStreamInfo remaining = Assert.Single(await files.ListStreamsAsync("/n", Ct));
                Assert.Equal(nfcFirst, remaining.Name);
                BytesEqual(a, await files.ReadAllBytesAsync("/n:" + first, Ct), "deleting the other name kept this stream");
            }
        }, Ct);
    }

    /// <summary>
    /// Stream and file names are compared ordinally and independently of the current culture (tr-TR: i/I/İ/ı are four
    /// distinct streams listed in ordinal order), and an NFC-equivalent file name addresses the same file's streams.
    /// </summary>
    [Fact]
    [Catalog("ADS-018", "U", "INV-07")]
    public async Task ADS_018_StreamNamesNormalised_CultureIndependent()
    {
        CultureInfo savedCulture = CultureInfo.CurrentCulture;
        CultureInfo savedUiCulture = CultureInfo.CurrentUICulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
        CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("tr-TR");
        try
        {
            await ClusterTest.RunAsync(Core, async cluster =>
            {
                await using DfsClient client = await ConnectAsync(cluster, "c0");
                DfsVolume v = client.GetVolume(Vol);
                string[] names = ["i", "I", "İ", "ı"];
                for (int i = 0; i < names.Length; i++)
                    await v.Files.WriteAllBytesAsync("/t:" + names[i], NonZero(1810 + (ulong)i, 10 + i), Ct);
                Assert.Equal(names.Order(StringComparer.Ordinal), (await v.Files.ListStreamsAsync("/t", Ct)).Select(s => s.Name));
                for (int i = 0; i < names.Length; i++)
                    BytesEqual(NonZero(1810 + (ulong)i, 10 + i), await v.Files.ReadAllBytesAsync("/t:" + names[i], Ct), $"stream '{names[i]}' under tr-TR");

                byte[] s = NonZero(1820, 77);
                await v.Files.WriteAllBytesAsync("/é:s", s, Ct);
                Assert.True(await v.Files.ExistsAsync("/é:s", Ct));
                BytesEqual(s, await v.Files.ReadAllBytesAsync("/é:s", Ct), "NFC-equivalent file name, same stream");
                Assert.Equal(new[] { "s" }, (await v.Files.ListStreamsAsync("/é", Ct)).Select(x => x.Name));
                Assert.Single(await EntriesAsync(v, "/"), e => e.Name == "é");
            }, Ct);
        }
        finally
        {
            CultureInfo.CurrentCulture = savedCulture;
            CultureInfo.CurrentUICulture = savedUiCulture;
        }
    }

    // ------------------------------------------------------------------ ADS-023 StreamPartialUpdate

    public static TheoryData<string, int> PartialUpdateCases() => new()
    {
        { "FirstByte", 1 },
        { "InsideChunk", 100 },
        { "InsideChunk", 4096 },
        { "AcrossChunkBoundary", 100 },
        { "LastBytes", 50 },
    };

    private sealed record UpdateCost(long Stored, long Uploaded, long Commit, int NewObjects);

    /// <summary>
    /// A small overwrite inside a 128-chunk named stream replicates only the touched logical update unit(s): stored and
    /// uploaded bytes are bounded by copies × touched chunks (plus a small allowance), the commit request is a small
    /// mapping update (below the size of the stream's whole mapping, as in LAY-010), untouched extents keep their objects,
    /// the default stream and a sibling stream keep their extents, and the bytes are exact. The same overwrite on a
    /// default stream (control) obeys the same bounds.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartialUpdateCases))]
    [Catalog("ADS-023", "U", "INV-02", "INV-07", "INV-09")]
    public async Task ADS_023_StreamPartialUpdate(string where, int length)
    {
        // 128 update units: a resend of the whole mapping (>= 32 bytes of object id + checksum per extent) alone reaches the
        // 4 KiB commit bound, so the bound discriminates a range update from a mapping rewrite.
        int size = 128 * Chunk;
        await ClusterTest.RunAsync(Core with { ScenarioTimeout = TimeSpan.FromMinutes(6) }, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            byte[] data = NonZero(2300, size);
            byte[] defaultBytes = NonZero(2301, 5000);
            byte[] sibling = NonZero(2302, 4 * Chunk);
            await v.Files.WriteAllBytesAsync("/big", defaultBytes, Ct);
            await SeedOneObjectPerChunkAsync(v, "/big:s", data);
            await v.Files.WriteAllBytesAsync("/big:t", sibling, Ct);
            await SeedOneObjectPerChunkAsync(v, "/ctl", data);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            int copies = (await v.GetInfoAsync(Ct)).DefaultPolicy.TargetCopies;

            long offset = where switch
            {
                "FirstByte" => 0,
                "InsideChunk" => (13L * Chunk) + 777,
                "AcrossChunkBoundary" => (20L * Chunk) - (length / 2),
                "LastBytes" => size - length,
                _ => throw new ArgumentOutOfRangeException(nameof(where), where, null),
            };
            byte[] patch = NonZero(2303, length);
            int unitsTouched = (int)(((offset + length - 1) / Chunk) - (offset / Chunk) + 1);
            long dataBound = (copies * (long)unitsTouched * Chunk) + (16 * 1024);
            const long commitBound = 4096;

            FileInspection fileBefore = await InspectFileAsync(cluster, "/big");
            StreamInspection streamBefore = fileBefore.Streams.Single(s => s.Name == "s");
            StreamInspection siblingBefore = fileBefore.Streams.Single(s => s.Name == "t");
            long wholeMappingFloor = 32L * streamBefore.Extents.Count;
            Assert.True(wholeMappingFloor >= commitBound,
                $"precondition: the stream's whole mapping (>= {wholeMappingFloor} B for {streamBefore.Extents.Count} extents) must not fit the {commitBound} B commit bound");
            UpdateCost streamCost = await OverwriteAndMeasureAsync(cluster, v, "/big:s", offset, patch,
                async () => Objects((await InspectStreamAsync(cluster, "/big", "s")).Extents), Objects(streamBefore.Extents));
            UpdateCost controlCost = await OverwriteAndMeasureAsync(cluster, v, "/ctl", offset, patch,
                async () => Objects((await InspectFileAsync(cluster, "/ctl")).Extents), Objects((await InspectFileAsync(cluster, "/ctl")).Extents));
            Log($"{where}/{length}: stream stored {streamCost.Stored} B, uploaded {streamCost.Uploaded} B, commit {streamCost.Commit} B, new objects {streamCost.NewObjects}; " +
                $"default-stream control stored {controlCost.Stored} B, uploaded {controlCost.Uploaded} B, commit {controlCost.Commit} B; bound {dataBound} B; whole stream {size} B");

            foreach ((string label, UpdateCost cost) in new[] { ("named stream", streamCost), ("default stream (control)", controlCost) })
            {
                Assert.True(cost.Stored <= dataBound, $"{label}: a {length}-byte overwrite stored {cost.Stored} bytes across replicas (bound {dataBound})");
                Assert.True(cost.Uploaded <= dataBound, $"{label}: a {length}-byte overwrite uploaded {cost.Uploaded} bytes (bound {dataBound})");
                Assert.True(cost.Commit > 0 && cost.Commit <= commitBound, $"{label}: the mapping update was {cost.Commit} bytes (bound {commitBound})");
                Assert.True(cost.NewObjects <= unitsTouched + 1, $"{label}: {cost.NewObjects} new objects for {unitsTouched} touched update units");
            }

            byte[] expected = Overlay(data, offset, patch);
            BytesEqual(expected, await ReadCommittedAsync(v, "/big:s"), $"{where}/{length}: stream contents");
            BytesEqual(expected, await ReadCommittedAsync(v, "/ctl"), $"{where}/{length}: control contents");

            FileInspection fileAfter = await InspectFileAsync(cluster, "/big");
            StreamInspection streamAfter = fileAfter.Streams.Single(s => s.Name == "s");
            Assert.Equal(fileBefore.Inode, fileAfter.Inode);
            Assert.Equal(size, streamAfter.Length);
            foreach (ExtentInspection e in streamBefore.Extents.Where(e => e.Offset + e.Length <= offset - Chunk || e.Offset >= offset + length + Chunk))
                Assert.Contains(e, streamAfter.Extents);
            Assert.Equal(fileBefore.Extents, fileAfter.Extents);
            Assert.Equal(siblingBefore.Extents, fileAfter.Streams.Single(s => s.Name == "t").Extents);
            await AssertStreamsAsync(v, "/big", new StreamModel(defaultBytes).With("s", expected).With("t", sibling), "default and sibling streams unchanged");
        }, Ct);
    }

    private static async Task<UpdateCost> OverwriteAndMeasureAsync(TestCluster cluster, DfsVolume v, string path, long offset, byte[] patch,
        Func<Task<HashSet<ObjectId>>> objectsAfter, HashSet<ObjectId> objectsBefore)
    {
        NodeCounters baseline = await cluster.Inspect.GetTotalCountersAsync();
        cluster.Network.Log.Reset();
        await using (DfsStream s = await v.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
        {
            s.Seek(offset, SeekOrigin.Begin);
            await s.WriteAsync(patch, Ct);
            await s.FlushAsync(Ct);
        }

        await cluster.WaitForRepairAsync(Vol, ct: Ct);
        NodeCounters done = await cluster.Inspect.GetTotalCountersAsync();
        long uploaded = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.PutObject, Direction: LinkDirection.Request));
        long commit = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.CommitFile, Direction: LinkDirection.Request));
        HashSet<ObjectId> after = await objectsAfter();
        after.ExceptWith(objectsBefore);
        return new UpdateCost(done.ObjectBytesReceived - baseline.ObjectBytesReceived, uploaded, commit, after.Count);
    }

    // ------------------------------------------------------------------ ADS-024 StreamInfo

    /// <summary>
    /// GetInfo on <c>/f:s</c> describes the addressed stream (path, IsStream, StreamName, length, revision = its last
    /// commit, inline/sparse flags, extent count matching the applied extent map) with the file's identity, owner, policy and
    /// times; GetInfo on <c>/f</c> describes the default stream and reports StreamCount; modifying any stream advances the
    /// file's modification time; stream handles and the sync overload agree; the security descriptor and effective rights
    /// of a stream are its file's.
    /// </summary>
    [Fact]
    [Catalog("ADS-024", "U", "INV-07")]
    public async Task ADS_024_StreamInfo()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsFileClient obs = observerClient.GetVolume(Vol).Files;
            byte[] d = NonZero(2401, (2 * Chunk) + 5), small = NonZero(2402, 100), large = NonZero(2403, (3 * Chunk) + 1), sparseTail = NonZero(2404, 10);
            CommitToken tDefault = await v.Files.WriteAllBytesAsync("/i", d, Ct);
            await v.Files.WriteAllBytesAsync("/i:small", small, Ct);
            CommitToken tLarge = await v.Files.WriteAllBytesAsync("/i:large", large, Ct);
            long sparseOffset = (3L * Chunk) + 3;
            await using (DfsStream s = await v.Files.OpenAsync("/i:sparse", Writer(FileMode.CreateNew), Ct))
            {
                s.Seek(sparseOffset, SeekOrigin.Begin);
                await s.WriteAsync(sparseTail, Ct);
            }

            DfsFileInfo fileBefore = await obs.GetInfoAsync("/i", Ct);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
            byte[] appended = NonZero(2405, 20);
            CommitToken tSmall = await v.Files.AppendAllBytesAsync("/i:small", appended, Ct);

            DfsFileInfo file = await obs.GetInfoAsync("/i", Ct);
            FileInspection inspected = await InspectFileAsync(cluster, "/i");
            Assert.Equal("/i", file.Path);
            Assert.False(file.IsStream);
            Assert.Null(file.StreamName);
            Assert.Equal(3, file.StreamCount);
            Assert.Equal(d.Length, file.Length);
            Assert.Equal(tDefault.Sequence, file.Revision);
            Assert.False(file.IsInline);
            Assert.Equal(inspected.Extents.Count, file.ExtentCount);
            Assert.True(file.ModifiedUtc > fileBefore.ModifiedUtc, $"modifying a stream must advance the file's modification time ({fileBefore.ModifiedUtc:O} -> {file.ModifiedUtc:O})");
            Assert.True(file.ObservedAt.IsAtOrAfter(tSmall));

            DfsFileInfo smallInfo = await obs.GetInfoAsync("/i:small", Ct);
            Assert.Equal("/i:small", smallInfo.Path);
            Assert.True(smallInfo.IsStream);
            Assert.Equal("small", smallInfo.StreamName);
            Assert.Equal(small.Length + appended.Length, smallInfo.Length);
            Assert.Equal(tSmall.Sequence, smallInfo.Revision);
            Assert.True(smallInfo.IsInline, "a 120-byte stream is stored inline");
            Assert.Equal(0, smallInfo.ExtentCount);
            Assert.False(smallInfo.IsSparse);
            Assert.True(smallInfo.ObservedAt.IsAtOrAfter(tSmall));

            DfsFileInfo largeInfo = await obs.GetInfoAsync("/i:large", Ct);
            StreamInspection largeInspected = inspected.Streams.Single(s => s.Name == "large");
            Assert.Equal(large.Length, largeInfo.Length);
            Assert.Equal(tLarge.Sequence, largeInfo.Revision);
            Assert.Equal(largeInspected.Revision, largeInfo.Revision);
            Assert.False(largeInfo.IsInline);
            Assert.False(largeInfo.IsSparse);
            Assert.Equal(largeInspected.Extents.Count, largeInfo.ExtentCount);
            Assert.True(largeInfo.ExtentCount > 0);

            DfsFileInfo sparseInfo = await obs.GetInfoAsync("/i:sparse", Ct);
            Assert.Equal(sparseOffset + sparseTail.Length, sparseInfo.Length);
            Assert.True(sparseInfo.IsSparse, "a stream written only beyond a 3-chunk gap is sparse");

            foreach (DfsFileInfo stream in new[] { smallInfo, largeInfo, sparseInfo })
            {
                Assert.Equal(file.InodeId, stream.InodeId);
                Assert.Equal(file.Owner, stream.Owner);
                Assert.Equal(file.StoragePolicy, stream.StoragePolicy);
                Assert.Equal(file.CreatedUtc, stream.CreatedUtc);
                Assert.Equal(file.ModifiedUtc, stream.ModifiedUtc);
                Assert.Equal(file.Access, stream.Access);
            }

            Assert.Equal(new[] { ("large", (long)large.Length, tLarge.Sequence), ("small", (long)(small.Length + appended.Length), tSmall.Sequence) },
                (await obs.ListStreamsAsync("/i", Ct)).Where(s => s.Name != "sparse").Select(s => (s.Name, s.Length, s.Revision)));

            await using (DfsStream h = await obs.OpenReadAsync("/i:large", Ct))
            {
                DfsFileInfo viaHandle = await h.GetInfoAsync(Ct);
                Assert.True(viaHandle.IsStream);
                Assert.Equal("large", viaHandle.StreamName);
                Assert.Equal(large.Length, viaHandle.Length);
                Assert.Equal(file.InodeId, h.InodeId);
                Assert.Equal(large.Length, await h.GetLengthAsync(Ct));
            }

            DfsFileInfo sync = obs.GetInfo("/i:large");
            Assert.Equal((largeInfo.Path, largeInfo.Length, largeInfo.Revision, largeInfo.StreamName), (sync.Path, sync.Length, sync.Revision, sync.StreamName));
            AssertFileNotFound(await CatchAsync(() => obs.GetInfoAsync("/i:none", Ct)), "GetInfo of an absent stream");

            Assert.Equal(await obs.GetAccessControlAsync("/i", Ct), await obs.GetAccessControlAsync("/i:large", Ct));
            Assert.Equal(await obs.GetEffectiveRightsAsync("/i", null, Ct), await obs.GetEffectiveRightsAsync("/i:large", null, Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-025 DirectoryStreams

    /// <summary>
    /// A directory carries named streams (<c>/d:s</c>) that are not directory entries, share the directory's inode, survive
    /// its moves and are deleted with it (plain and recursive delete; a recreated directory has none); deleting one stream
    /// keeps the directory. Stream syntax in directory operations, a directory's default stream as file data and streams
    /// on the root are rejected without any change.
    /// </summary>
    [Fact]
    [Catalog("ADS-025", "U", "INV-02", "INV-07")]
    public async Task ADS_025_DirectoryStreams()
    {
        await ClusterTest.RunAsync(Core, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/d/sub", Ct);
            byte[] x = NonZero(2501, 300), y = NonZero(2502, (2 * Chunk) + 1);
            await v.Files.WriteAllBytesAsync("/d:s", x, Ct);
            await v.Files.WriteAllBytesAsync("/d:t", y, Ct);
            DfsDirectoryInfo dirInfo = await v.Directories.GetInfoAsync("/d", Ct);

            BytesEqual(x, await v.Files.ReadAllBytesAsync("/d:s", Ct), "directory stream s");
            BytesEqual(y, await v.Files.ReadAllBytesAsync("/d:t", Ct), "directory stream t");
            Assert.Equal(new[] { ("s", (long)x.Length), ("t", (long)y.Length) }, (await v.Files.ListStreamsAsync("/d", Ct)).Select(s => (s.Name, s.Length)));
            Assert.True(await v.Files.ExistsAsync("/d:s", Ct));
            DfsFileInfo streamInfo = await v.Files.GetInfoAsync("/d:s", Ct);
            Assert.True(streamInfo.IsStream);
            Assert.Equal("s", streamInfo.StreamName);
            Assert.Equal(x.Length, streamInfo.Length);
            Assert.Equal(dirInfo.InodeId, streamInfo.InodeId);
            Assert.Equal(new[] { "sub" }, (await EntriesAsync(v, "/d")).Select(e => e.Name));
            Assert.Equal(1, dirInfo.EntryCount);
            DfsEntryInfo dEntry = Assert.Single(await EntriesAsync(v, "/"));
            Assert.Equal(("d", DfsEntryKind.Directory), (dEntry.Name, dEntry.Kind));

            // Stream syntax in directory operations, directory default-stream data and root streams are rejected.
            string before = await DescribeNamespaceAsync(v);
            (string What, Func<Task> Action)[] invalid =
            [
                ("CreateDirectory(/d:s)", () => v.Directories.CreateDirectoryAsync("/d:s", Ct)),
                ("CreateDirectory(/e:s)", () => v.Directories.CreateDirectoryAsync("/e:s", Ct)),
                ("CreateDirectory(/d/sub:x)", () => v.Directories.CreateDirectoryAsync("/d/sub:x", Ct)),
                ("CreateDirectory(/d/n:x) with options", () => v.Directories.CreateDirectoryAsync("/d/n:x", new DfsMutationOptions(), Ct)),
                ("Directories.Exists(/d:s)", () => v.Directories.ExistsAsync("/d:s", Ct)),
                ("Directories.GetInfo(/d:s)", () => v.Directories.GetInfoAsync("/d:s", Ct)),
                ("Directories.Delete(/d:s)", () => v.Directories.DeleteAsync("/d:s", false, Ct)),
                ("Directories.Move(/d:s, /e)", () => v.Directories.MoveAsync("/d:s", "/e", Ct)),
                ("Directories.Move(/d, /e:s)", () => v.Directories.MoveAsync("/d", "/e:s", Ct)),
                ("Directories.Enumerate(/d:s)", async () => { await foreach (DfsEntryInfo e in v.Directories.EnumerateAsync("/d:s", null, Ct)) Assert.Fail($"listed {e.Path}"); }),
                ("Directories.ListPage(/d:s)", () => v.Directories.ListPageAsync("/d:s", null, null, Ct)),
                ("Files.WriteAllBytes(/:s)", () => v.Files.WriteAllBytesAsync("/:s", x, Ct)),
            ];
            foreach ((string what, Func<Task> action) in invalid)
                AssertInvalidPath(await CatchAsync(action), what);
            // A directory has no default data stream: file data on its plain path is refused (IsADirectory; the .NET-style
            // access denial is accepted as in the PROP suite — the addendum does not pin the code).
            Exception? defaultStreamOfDirectory = await CatchAsync(() => v.Files.WriteAllBytesAsync("/d", x, Ct));
            Assert.True(defaultStreamOfDirectory is DfsException { ErrorCode: DfsErrorCode.IsADirectory } or UnauthorizedAccessException,
                $"writing a directory's default stream as file data: expected IsADirectory (or access denied), got {Describe(defaultStreamOfDirectory)}");
            Assert.Equal(before, await DescribeNamespaceAsync(v));
            Assert.False(await v.Directories.ExistsAsync("/e", Ct));

            // Deleting one stream keeps the directory and the other stream.
            await v.Files.DeleteAsync("/d:s", Ct);
            Assert.True(await v.Directories.ExistsAsync("/d", Ct));
            Assert.Equal(new[] { "t" }, (await v.Files.ListStreamsAsync("/d", Ct)).Select(s => s.Name));

            // Moving the directory keeps its streams.
            await v.Directories.MoveAsync("/d", "/e", Ct);
            BytesEqual(y, await v.Files.ReadAllBytesAsync("/e:t", Ct), "directory stream after the directory moved");
            Assert.Equal(dirInfo.InodeId, (await v.Files.GetInfoAsync("/e:t", Ct)).InodeId);
            Assert.False(await v.Files.ExistsAsync("/d:t", Ct));

            // Streams are deleted with their directory (a directory holding only streams is empty).
            await v.Directories.DeleteAsync("/e/sub", false, Ct);
            await v.Directories.DeleteAsync("/e", false, Ct);
            Assert.False(await v.Directories.ExistsAsync("/e", Ct));
            Assert.False(await v.Files.ExistsAsync("/e:t", Ct));
            await v.Directories.CreateDirectoryAsync("/e", Ct);
            Assert.Empty(await v.Files.ListStreamsAsync("/e", Ct));

            // Recursive delete removes nested directories' streams too.
            await v.Directories.CreateDirectoryAsync("/r/q", Ct);
            await v.Files.WriteAllBytesAsync("/r:y", x, Ct);
            await v.Files.WriteAllBytesAsync("/r/q:x", y, Ct);
            await v.Directories.DeleteAsync("/r", true, Ct);
            Assert.False(await v.Files.ExistsAsync("/r:y", Ct));
            Assert.False(await v.Files.ExistsAsync("/r/q:x", Ct));
            await v.Directories.CreateDirectoryAsync("/r/q", Ct);
            Assert.Empty(await v.Files.ListStreamsAsync("/r", Ct));
            Assert.Empty(await v.Files.ListStreamsAsync("/r/q", Ct));
        }, Ct);
    }
}
