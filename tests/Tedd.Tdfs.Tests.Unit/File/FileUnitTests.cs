using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.File.FileHelpers;

namespace Tedd.Tdfs.Tests.Unit.File;

/// <summary>
/// FILE suite, unit level: convenience file operations and encoding (test plan §7.7; FSD §§5.2–5.4, 6, 8).
/// Every scenario runs against an in-memory three-zone cluster (SDK contract level).
/// </summary>
public sealed class FileUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // FILE-001 WriteAllBytesAtomicReplacement
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(old length, new length, use sync API): inline/extent/chunk boundaries in both directions.</summary>
    public static TheoryData<int, int, bool> File001Lengths => new()
    {
        { 1, 0, false },
        { 0, Inline + 1, false },
        { Inline - 1, Inline, false },
        { Inline, Inline + 1, false },
        { Inline + 1, Inline - 1, true },
        { Chunk - 1, Chunk + 1, false },
        { Chunk + 1, 1, false },
        { 10, 2 * Chunk + 1, true },
        { 2 * Chunk + 1, 2 * Chunk, false },
    };

    [Theory]
    [MemberData(nameof(File001Lengths))]
    [Catalog("FILE-001", "U", "INV-02", "INV-07")]
    public async Task FILE_001_WriteAllBytesAtomicReplacement(int oldLength, int newLength, bool useSync)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            DfsVolume writer = writerClient.GetVolume(Vol);
            const string dir = "/file-001";
            const string path = dir + "/target.bin";
            await writer.Directories.CreateDirectoryAsync(dir, Ct);

            byte[] oldBytes = TestData.Bytes(0x1001, oldLength);
            byte[] newBytes = TestData.Bytes(0x1002, newLength);
            CommitToken created = await writer.Files.WriteAllBytesAsync(path, oldBytes, Ct);
            Assert.False(created.IsNone, "a standalone WriteAllBytes must return a real commit token");
            DfsFileInfo before = await writer.Files.GetInfoAsync(path, Ct);
            Assert.Equal(oldLength, before.Length);

            var readerClients = new List<DfsClient>();
            for (int i = 0; i < 3; i++)
                readerClients.Add(await cluster.ConnectAsync(Named($"reader{i}"), Ct));
            try
            {
                var writeDone = new TaskCompletionSource();

                // One read = one complete observation of a single revision: either the whole-file helper (which pins one
                // revision, FSD §5.2) or an explicit VersionPinned handle that shares ReadWrite|Delete (compatible with
                // any writer reservation, test plan §5). A live handle read in several calls is NOT one observation.
                async Task<byte[]> ObserveAsync(DfsVolume reader, bool viaPinnedHandle)
                {
                    if (!viaPinnedHandle)
                        return await reader.Files.ReadAllBytesAsync(path, Ct);
                    (byte[] bytes, InodeId inode) = await ReadSharedAsync(reader, path);
                    Assert.Equal(before.InodeId, inode); // the name never resolves to another inode
                    return bytes;
                }

                async Task ReadLoopAsync(DfsVolume reader, string process)
                {
                    int readsAfterWrite = 0;
                    for (int iteration = 0; iteration < 100_000 && readsAfterWrite < 3; iteration++)
                    {
                        bool invokedAfterWrite = writeDone.Task.IsCompleted;
                        bool viaPinnedHandle = iteration % 2 == 1;
                        byte[] observed;
                        try
                        {
                            observed = await cluster.History.RecordAsync(process, "read", path, null,
                                () => ObserveAsync(reader, viaPinnedHandle));
                        }
                        catch (FileNotFoundException ex)
                        {
                            Assert.Fail($"{process}: the destination was absent during atomic replacement ({ex.Message}).");
                            throw;
                        }
                        catch (DfsSharingViolationException)
                        {
                            // A reservation conflict with the writer is a retryable refusal (recorded as a failed read
                            // with no effect), not evidence about publication; back off and try again.
                            await Task.Delay(TimeSpan.FromMilliseconds(2), Ct);
                            continue;
                        }

                        bool isOld = observed.AsSpan().SequenceEqual(oldBytes);
                        bool isNew = observed.AsSpan().SequenceEqual(newBytes);
                        Assert.True(isOld || isNew,
                            $"{process} observed {observed.Length} bytes that are neither the old ({oldLength}) nor the complete new ({newLength}) contents.");
                        if (invokedAfterWrite)
                        {
                            readsAfterWrite++;
                            Assert.True(isNew, $"{process}: a read invoked after WriteAllBytes returned must observe the new contents.");
                        }

                        // Leave reservation-free windows so read helpers cannot starve the writer's implicit reservation.
                        if (!viaPinnedHandle)
                            await Task.Delay(TimeSpan.FromMilliseconds(1), Ct);
                    }

                    Assert.True(readsAfterWrite >= 3, $"{process} did not complete its post-write reads.");
                }

                Task[] readers = readerClients.Select((c, i) => ReadLoopAsync(c.GetVolume(Vol), $"reader{i}")).ToArray();

                long writeOp = cluster.History.Invoke("writer", "write", path, newBytes);
                CommitToken replaced = CommitToken.None;
                for (int attempt = 0; ; attempt++)
                {
                    try
                    {
                        replaced = useSync
                            ? writer.Files.WriteAllBytes(path, newBytes)
                            : await writer.Files.WriteAllBytesAsync(path, newBytes, Ct);
                        break;
                    }
                    catch (DfsSharingViolationException) when (attempt < 2_000)
                    {
                        // Retryable refusal while a reader's helper holds a reservation; a refused attempt has no effect.
                        await Task.Delay(TimeSpan.FromMilliseconds(5), Ct);
                    }
                }

                cluster.History.Complete(writeOp, OpOutcome.Ok, replaced);
                writeDone.SetResult();
                await Task.WhenAll(readers);

                // Readers saw old or complete new contents in a linearizable order (never new-then-old).
                LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(oldBytes)).AssertValid();

                // Inode identity preserved; revision and token advanced.
                DfsFileInfo after = await writer.Files.GetInfoAsync(path, Ct);
                Assert.Equal(before.InodeId, after.InodeId);
                Assert.True(after.Revision > before.Revision, $"revision must advance ({before.Revision} -> {after.Revision})");
                Assert.Equal(newLength, after.Length);
                Assert.False(replaced.IsNone);
                Assert.True(replaced.IsComparableWith(created));
                Assert.True(replaced.CompareTo(created) > 0, "the replacement commit token must order after the creation token");
                Assert.True(after.ObservedAt.IsAtOrAfter(replaced), "metadata read after the replacement must observe its commit");

                FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, path);
                Assert.NotNull(inspected);
                Assert.Equal(before.InodeId, inspected.Inode);
                Assert.Equal(newLength, inspected.Length);

                // No temporary or replaced entries are left in the parent directory.
                List<DfsEntryInfo> entries = await ToListAsync(writer.Directories.EnumerateAsync(dir, null, Ct));
                DfsEntryInfo only = Assert.Single(entries);
                Assert.Equal("target.bin", only.Name);
                Assert.Equal(before.InodeId, only.InodeId);

                // Every client (including a fresh one) now reads the complete new contents.
                await using DfsClient fresh = await cluster.ConnectAsync(Named("fresh"), Ct);
                await AssertFileAsync(fresh.GetVolume(Vol), path, newBytes, before.InodeId, "fresh client after replacement");
            }
            finally
            {
                foreach (DfsClient c in readerClients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-003 ReadAllAllocationLimit
    // ------------------------------------------------------------------------------------------------------------

    private const long MaterializeLimit = 256 * 1024;

    [Theory]
    [InlineData(-1L)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(MaterializeLimit)]
    [Catalog("FILE-003", "U", "INV-08", "INV-16")]
    public async Task FILE_003_ReadAllAllocationLimit(long deltaFromLimit)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            await using DfsClient limitedClient = await cluster.ConnectAsync(
                new TestClientOptions { Name = "limited", MaxMaterializeBytes = MaterializeLimit }, Ct);
            DfsVolume writer = writerClient.GetVolume(Vol);
            DfsVolume limited = limitedClient.GetVolume(Vol);

            int length = checked((int)(MaterializeLimit + deltaFromLimit));
            byte[] content = TestData.Bytes(0x3003, length);
            const string path = "/file-003.bin";
            await writer.Files.WriteAllBytesAsync(path, content, Ct);
            DfsFileInfo before = await limited.Files.GetInfoAsync(path, Ct);

            if (length <= MaterializeLimit)
            {
                AssertBytes(content, await limited.Files.ReadAllBytesAsync(path, Ct), "at or below the limit ReadAllBytesAsync materializes the file");
                AssertBytes(content, limited.Files.ReadAllBytes(path), "at or below the limit ReadAllBytes materializes the file");
            }
            else
            {
                DfsLimitExceededException ex = await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(
                    () => limited.Files.ReadAllBytesAsync(path, Ct), DfsErrorCode.LimitExceeded);
                Assert.Equal(MaterializeLimit, ex.Limit);
                Assert.Equal(length, ex.Requested);
                Assert.ThrowsAny<DfsLimitExceededException>(() => limited.Files.ReadAllBytes(path));
                await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(() => limited.Files.ReadAllTextAsync(path, null, Ct), DfsErrorCode.LimitExceeded);
                await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(() => limited.Files.ReadAllLinesAsync(path, null, Ct), DfsErrorCode.LimitExceeded);
            }

            // The same file remains readable as a bounded stream through the same (limited) client.
            await using (DfsStream stream = await limited.Files.OpenReadAsync(path, Ct))
            {
                Assert.Equal(before.InodeId, stream.InodeId);
                Assert.Equal(length, await stream.GetLengthAsync(Ct));
                var window = new byte[16 * 1024];
                int offset = 0;
                while (true)
                {
                    int read = await stream.ReadAsync(window.AsMemory(), Ct);
                    if (read == 0)
                        break;
                    Assert.True(read <= window.Length);
                    AssertBytes(content.AsSpan(offset, read), window.AsSpan(0, read), $"streamed bytes at offset {offset}");
                    offset += read;
                }

                Assert.Equal(length, offset);
            }

            // Rejection had no side effect on the file and leaked no pinned handle.
            DfsFileInfo after = await limited.Files.GetInfoAsync(path, Ct);
            Assert.Equal(before.InodeId, after.InodeId);
            Assert.Equal(before.Revision, after.Revision);
            await WaitForNoHandlesAsync(cluster, before.InodeId, "rejected or completed ReadAll helpers must release their pinned handle");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-004 Utf8WithoutBomDefault
    // ------------------------------------------------------------------------------------------------------------

    private static string File004Text(string kind) => kind switch
    {
        "ascii" => "Hello, world! 0123456789",
        "multibyte" => TestData.Utf8Text(0x4004, 2000),
        "supplementary" => "\U0001F600\U0001F30D\U00020000 mixed ascii \U0001F389",
        "empty" => string.Empty,
        "nfd-content" => "Café vs Café", // file contents are bytes: never normalised
        "long-multibyte" => TestData.Utf8Text(0x4005, 70_000),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
    };

    [Theory]
    [InlineData("ascii", false)]
    [InlineData("multibyte", false)]
    [InlineData("supplementary", true)]
    [InlineData("empty", false)]
    [InlineData("nfd-content", true)]
    [InlineData("long-multibyte", false)]
    [Catalog("FILE-004", "U", "INV-07")]
    public async Task FILE_004_Utf8WithoutBomDefault(string kind, bool useSync)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            await client.GetVolume(Vol).Directories.CreateDirectoryAsync("/file-004", Ct);
            string text = File004Text(kind);
            byte[] expected = Utf8NoBom.GetBytes(text);

            string path = $"/file-004/{kind}.txt";
            CommitToken token = useSync ? files.WriteAllText(path, text) : await files.WriteAllTextAsync(path, text, null, Ct);
            Assert.False(token.IsNone);
            byte[] raw = await files.ReadAllBytesAsync(path, Ct);
            Assert.False(raw.AsSpan().StartsWith(Utf8Bom), "the default text encoding must not emit a UTF-8 BOM");
            AssertBytes(expected, raw, "default WriteAllText stores exactly the BOM-less UTF-8 encoding");
            Assert.Equal(text, useSync ? files.ReadAllText(path) : await files.ReadAllTextAsync(path, null, Ct));

            // Appending default text never inserts a BOM.
            const string suffix = "→ appended ✓";
            await files.AppendAllTextAsync(path, suffix, null, Ct);
            AssertBytes(Concat(expected, Utf8NoBom.GetBytes(suffix)), await files.ReadAllBytesAsync(path, Ct),
                "AppendAllText appends BOM-less UTF-8");
            Assert.Equal(text + suffix, await files.ReadAllTextAsync(path, null, Ct));

            // AppendAllText creating an absent file also writes no BOM.
            string appendPath = $"/file-004/{kind}.append.txt";
            await files.AppendAllTextAsync(appendPath, text, null, Ct);
            AssertBytes(expected, await files.ReadAllBytesAsync(appendPath, Ct), "AppendAllText on an absent file writes BOM-less UTF-8");

            // Lines helpers: no BOM, valid strict UTF-8, round trip.
            string[] lines = [text, "second line ✓", string.Empty, "\U0001F680 last"];
            string linesPath = $"/file-004/{kind}.lines.txt";
            if (useSync)
                files.WriteAllLines(linesPath, lines);
            else
                await files.WriteAllLinesAsync(linesPath, lines, null, Ct);
            byte[] rawLines = await files.ReadAllBytesAsync(linesPath, Ct);
            Assert.False(rawLines.AsSpan().StartsWith(Utf8Bom), "WriteAllLines must not emit a BOM by default");
            _ = Utf8NoBom.GetString(rawLines); // strict decoder: throws on invalid UTF-8
            Assert.Equal(lines, OracleLines(rawLines));
            Assert.Equal(lines, await files.ReadAllLinesAsync(linesPath, null, Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-005 ExplicitEncodingAndBom
    // ------------------------------------------------------------------------------------------------------------

    private static Encoding File005Encoding(string name) => name switch
    {
        "utf-8-bom" => new UTF8Encoding(encoderShouldEmitUTF8Identifier: true),
        "utf-8-nobom" => new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
        "utf-16le" => new UnicodeEncoding(bigEndian: false, byteOrderMark: true),
        "utf-16be" => new UnicodeEncoding(bigEndian: true, byteOrderMark: true),
        "utf-32le" => new UTF32Encoding(bigEndian: false, byteOrderMark: true),
        "latin1" => Encoding.Latin1,
        "ascii" => Encoding.ASCII,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    private static string File005Text(string name) => name switch
    {
        "latin1" => "Grüße Ærø ¿qué?",
        "ascii" => "plain ascii text 0123",
        _ => "multi æøå € 中文 \U0001F600 end",
    };

    [Theory]
    [InlineData("utf-8-bom")]
    [InlineData("utf-8-nobom")]
    [InlineData("utf-16le")]
    [InlineData("utf-16be")]
    [InlineData("utf-32le")]
    [InlineData("latin1")]
    [InlineData("ascii")]
    [Catalog("FILE-005", "U", "INV-07")]
    public async Task FILE_005_ExplicitEncodingAndBom(string encodingName)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            await client.GetVolume(Vol).Directories.CreateDirectoryAsync("/file-005", Ct);
            Encoding encoding = File005Encoding(encodingName);
            string text = File005Text(encodingName);
            byte[] preamble = encoding.GetPreamble();
            byte[] payload = encoding.GetBytes(text);

            string path = $"/file-005/{encodingName}.txt";
            await files.WriteAllTextAsync(path, text, encoding, Ct);
            byte[] raw = await files.ReadAllBytesAsync(path, Ct);
            bool emitsPreamble = preamble.Length > 0 && raw.Length == preamble.Length + payload.Length && raw.AsSpan().StartsWith(preamble);
            AssertBytes(emitsPreamble ? Concat(preamble, payload) : payload, raw,
                $"WriteAllText with {encodingName} stores the encoding's bytes, optionally preceded by its preamble");
            if (preamble.Length == 0)
                Assert.False(emitsPreamble);
            Assert.Equal(text, await files.ReadAllTextAsync(path, encoding, Ct));

            // The preamble decision is consistent across text helpers.
            string[] lines = [text, "second", string.Empty];
            string linesPath = $"/file-005/{encodingName}.lines.txt";
            await files.WriteAllLinesAsync(linesPath, lines, encoding, Ct);
            byte[] rawLines = await files.ReadAllBytesAsync(linesPath, Ct);
            Assert.Equal(emitsPreamble, preamble.Length > 0 && rawLines.AsSpan().StartsWith(preamble));
            Assert.Equal(lines, await files.ReadAllLinesAsync(linesPath, encoding, Ct));

            // Appending to a nonempty file never inserts a second preamble mid-file.
            const string more = " +more";
            await files.AppendAllTextAsync(path, more, encoding, Ct);
            AssertBytes(Concat(raw, encoding.GetBytes(more)), await files.ReadAllBytesAsync(path, Ct),
                "AppendAllText must append only the encoded suffix");
            Assert.Equal(text + more, await files.ReadAllTextAsync(path, encoding, Ct));

            if (encodingName.StartsWith("utf-8", StringComparison.Ordinal))
            {
                // The default reader recognises a UTF-8 BOM and decodes BOM-less UTF-8 identically.
                Assert.Equal(text + more, await files.ReadAllTextAsync(path, null, Ct));
            }
        }, Ct);
    }

    [Theory]
    [InlineData("bom-text")]
    [InlineData("bom-only")]
    [InlineData("bom-lines-crlf")]
    [InlineData("inner-bom-is-content")]
    [Catalog("FILE-005", "U", "INV-07")]
    public async Task FILE_005_ExplicitEncodingAndBom_Utf8BomInputRecognised(string kind)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            (byte[] raw, string expectedText) = kind switch
            {
                "bom-text" => (Concat(Utf8Bom, Utf8NoBom.GetBytes("héllo \U0001F600")), "héllo \U0001F600"),
                "bom-only" => (Utf8Bom, string.Empty),
                "bom-lines-crlf" => (Concat(Utf8Bom, Utf8NoBom.GetBytes("first\r\nsecond\r\n")), "first\r\nsecond\r\n"),
                "inner-bom-is-content" => (Concat(Utf8NoBom.GetBytes("a"), Utf8Bom, Utf8NoBom.GetBytes("b")), "a﻿b"),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            string path = $"/file-005-{kind}.txt";
            await files.WriteAllBytesAsync(path, raw, Ct);
            AssertBytes(raw, await files.ReadAllBytesAsync(path, Ct), "WriteAllBytes stores the raw bytes verbatim");

            Assert.Equal(expectedText, await files.ReadAllTextAsync(path, null, Ct));
            Assert.Equal(expectedText, await files.ReadAllTextAsync(path, new UTF8Encoding(false), Ct));
            Assert.Equal(expectedText, files.ReadAllText(path));

            List<string> expectedLines = OracleLines(raw);
            Assert.Equal(expectedLines, await files.ReadAllLinesAsync(path, null, Ct));
            Assert.Equal(expectedLines, await ToListAsync(files.ReadLinesAsync(path, null, Ct)));
            Assert.Equal(expectedLines, files.ReadLines(path).ToList());
            if (expectedLines.Count > 0)
                Assert.False(expectedLines[0].StartsWith('﻿'), "a leading UTF-8 BOM must not leak into the first line");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-006 LineHelpersHandleBoundaries
    // ------------------------------------------------------------------------------------------------------------

    private const int LineBuffer = 4096;

    private static byte[] Pad(int count) => Encoding.ASCII.GetBytes(new string('x', count));

    private static byte[] U(string s) => Utf8NoBom.GetBytes(s);

    private static byte[] File006Bytes(string kind) => kind switch
    {
        "empty" => [],
        "single-lf" => U("\n"),
        "lf-terminated" => U("alpha\nbeta\n"),
        "unterminated-final-line" => U("alpha\nbeta"),
        "crlf" => U("alpha\r\nbeta\r\n"),
        "lone-cr-and-mixed" => U("a\r\nb\nc\rd\r"),
        "only-empty-lines" => U("\n\n\r\n\n"),
        "multibyte-lines" => U(string.Join('\n', Enumerable.Range(0, 50).Select(i => TestData.Utf8Text((ulong)(0x6000 + i), 1 + (i * 7 % 90))))),
        "3byte-split-at-buffer" => Concat(Pad(LineBuffer - 1), U("€\nnext")),
        "4byte-split-at-buffer" => Concat(Pad(LineBuffer - 2), U("\U0001F600\ntail")),
        "3byte-split-at-chunk" => Concat(Pad(Chunk - 2), U("€\r\nafter")),
        "4byte-split-at-chunk" => Concat(Pad(Chunk - 1), U("\U0001F600")),
        "crlf-split-at-buffer" => Concat(Pad(LineBuffer - 1), U("\r\ny")),
        "crlf-split-at-chunk" => Concat(Pad(Chunk - 1), U("\r\nz\n")),
        "bom-then-lines" => Concat(Utf8Bom, U("x\ny")),
        "long-line" => U(TestData.Utf8Text(0x6100, 150_000) + "\nshort"),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
    };

    [Theory]
    [InlineData("empty")]
    [InlineData("single-lf")]
    [InlineData("lf-terminated")]
    [InlineData("unterminated-final-line")]
    [InlineData("crlf")]
    [InlineData("lone-cr-and-mixed")]
    [InlineData("only-empty-lines")]
    [InlineData("multibyte-lines")]
    [InlineData("3byte-split-at-buffer")]
    [InlineData("4byte-split-at-buffer")]
    [InlineData("3byte-split-at-chunk")]
    [InlineData("4byte-split-at-chunk")]
    [InlineData("crlf-split-at-buffer")]
    [InlineData("crlf-split-at-chunk")]
    [InlineData("bom-then-lines")]
    [InlineData("long-line")]
    [Catalog("FILE-006", "U", "INV-07")]
    public async Task FILE_006_LineHelpersHandleBoundaries(string kind)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", BufferSize = LineBuffer }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            byte[] bytes = File006Bytes(kind);
            string path = $"/file-006-{kind}.txt";
            await files.WriteAllBytesAsync(path, bytes, Ct);

            List<string> expected = OracleLines(bytes);
            Assert.Equal(expected, await files.ReadAllLinesAsync(path, null, Ct));
            Assert.Equal(expected, files.ReadAllLines(path));
            Assert.Equal(expected, await ToListAsync(files.ReadLinesAsync(path, null, Ct)));
            Assert.Equal(expected, files.ReadLines(path).ToList());

            string expectedText;
            using (var reader = new StreamReader(new MemoryStream(bytes), new UTF8Encoding(false), detectEncodingFromByteOrderMarks: true))
                expectedText = await reader.ReadToEndAsync(Ct);
            Assert.Equal(expectedText, await files.ReadAllTextAsync(path, null, Ct));

            // Lines written by WriteAllLines read back identically (the terminator choice is the SDK's).
            string roundTripPath = $"/file-006-{kind}.roundtrip.txt";
            await files.WriteAllLinesAsync(roundTripPath, expected, null, Ct);
            Assert.Equal(expected, await files.ReadAllLinesAsync(roundTripPath, null, Ct));
            Assert.Equal(expected, await ToListAsync(files.ReadLinesAsync(roundTripPath, null, Ct)));

            // AppendAllLines continues the line sequence.
            await files.AppendAllLinesAsync(roundTripPath, ["appended æ", string.Empty], null, Ct);
            Assert.Equal(expected.Concat(new[] { "appended æ", string.Empty }).ToList(), await files.ReadAllLinesAsync(roundTripPath, null, Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-007 LazyReadLinesStaysBounded
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("full-enumeration", false)]
    [InlineData("full-enumeration-with-concurrent-replacement", false)]
    [InlineData("early-dispose", false)]
    [InlineData("early-dispose", true)]
    [Catalog("FILE-007", "U", "INV-16", "INV-07")]
    public async Task FILE_007_LazyReadLinesStaysBounded(string mode, bool useSync)
    {
        const int lineCount = 20_000;
        const long materializeLimit = 256 * 1024;
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            var readerOptions = new TestClientOptions { Name = "reader", MaxMaterializeBytes = materializeLimit };
            await using DfsClient readerClient = await cluster.ConnectAsync(readerOptions, Ct);
            DfsFileClient writer = writerClient.GetVolume(Vol).Files;
            DfsFileClient reader = readerClient.GetVolume(Vol).Files;

            string[] lines = Enumerable.Range(0, lineCount)
                .Select(i => $"line {i:D6} {TestData.Utf8Text((ulong)(0x7000 + i), 40)}")
                .ToArray();
            const string path = "/file-007.txt";
            await writer.WriteAllLinesAsync(path, lines, null, Ct);
            DfsFileInfo info = await reader.GetInfoAsync(path, Ct);
            Assert.True(info.Length > 4 * materializeLimit, "the file must be far larger than the materialization limit");

            // Materializing helpers refuse; the lazy enumeration does not.
            await AssertEx.ThrowsDfsAsync<DfsLimitExceededException>(() => reader.ReadAllLinesAsync(path, null, Ct), DfsErrorCode.LimitExceeded);
            await WaitForNoHandlesAsync(cluster, info.InodeId, "a rejected ReadAllLines must not leave a handle behind");

            cluster.Network.Log.Reset();
            var deliveredToReader = new MessageFilter(To: readerOptions.NetworkName);

            if (mode == "early-dispose")
            {
                int taken = 0;
                if (useSync)
                {
                    foreach (string line in reader.ReadLines(path))
                    {
                        Assert.Equal(lines[taken], line);
                        if (++taken == 10)
                            break;
                    }
                }
                else
                {
                    await using IAsyncEnumerator<string> e = reader.ReadLinesAsync(path, null, Ct).GetAsyncEnumerator(Ct);
                    while (taken < 10 && await e.MoveNextAsync())
                    {
                        Assert.Equal(lines[taken], e.Current);
                        taken++;
                    }

                    Assert.True(await CountHandlesAsync(cluster, info.InodeId) <= 1, "an active line enumeration holds at most one handle");
                }

                Assert.Equal(10, taken);
                long received = cluster.Network.Log.CountBytes(deliveredToReader);
                Assert.True(received < info.Length / 2,
                    $"an abandoned lazy enumeration must not transfer the whole file (received {received} of {info.Length} bytes)");
                await WaitForNoHandlesAsync(cluster, info.InodeId, "disposing the line enumerator early must release its handle/pin");
                return;
            }

            int index = 0;
            bool replaced = false;
            await foreach (string line in reader.ReadLinesAsync(path, null, Ct))
            {
                Assert.True(index < lineCount, "enumeration returned more lines than the pinned revision contains");
                Assert.Equal(lines[index], line);
                if (index == 0)
                {
                    long received = cluster.Network.Log.CountBytes(deliveredToReader);
                    Assert.True(received <= 4L * Chunk + LineBuffer * 4L,
                        $"the first line must be produced lazily (received {received} bytes before yielding it)");
                }

                if (index == 100)
                {
                    Assert.True(await CountHandlesAsync(cluster, info.InodeId) <= 1, "an active line enumeration holds at most one handle");
                    if (mode == "full-enumeration-with-concurrent-replacement")
                    {
                        // Either the replacement is allowed (the enumeration keeps its pinned revision) or the helper's
                        // read reservation legitimately refuses it; in both cases the enumeration stays on revision 1.
                        try
                        {
                            await writer.WriteAllBytesAsync(path, U("replaced\n"), Ct);
                            replaced = true;
                        }
                        catch (DfsSharingViolationException)
                        {
                            replaced = false;
                        }
                    }
                }

                index++;
            }

            Assert.Equal(lineCount, index);
            await WaitForNoHandlesAsync(cluster, info.InodeId, "a completed line enumeration must release its handle/pin");
            if (replaced)
                Assert.Equal(new[] { "replaced" }, await reader.ReadAllLinesAsync(path, null, Ct));
            else
                Assert.Equal(info.Revision, (await reader.GetInfoAsync(path, Ct)).Revision);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-011 CopySelfAndInvalidDestinations
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("same-path", false)]
    [InlineData("same-path", true)]
    [InlineData("normalization-equivalent-path", true)]
    [InlineData("destination-is-directory", false)]
    [InlineData("destination-is-directory", true)]
    [InlineData("destination-is-root", true)]
    [InlineData("source-is-directory", false)]
    [InlineData("destination-parent-missing", false)]
    [InlineData("destination-parent-is-file", true)]
    [InlineData("source-missing", false)]
    [InlineData("destination-dotdot", true)]
    [InlineData("destination-relative", false)]
    [InlineData("destination-trailing-slash", false)]
    [Catalog("FILE-011", "U", "INV-02", "INV-07", "INV-08")]
    public async Task FILE_011_CopySelfAndInvalidDestinations(string kind, bool overwrite)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/f11/dir/child", Ct);
            byte[] source = TestData.Bytes(0x1101, 3 * Chunk + 7);
            const string src = "/f11/café.bin";
            await v.Files.WriteAllBytesAsync(src, source, Ct);
            await v.Files.WriteAllBytesAsync("/f11/dir/child/keep.bin", TestData.Bytes(0x1102, 100), Ct);
            DfsFileInfo srcBefore = await v.Files.GetInfoAsync(src, Ct);
            List<ObjectId> srcExtentsBefore = await ExtentObjectsAsync(cluster, src);
            Assert.NotEmpty(srcExtentsBefore);
            DfsDirectoryInfo dirBefore = await v.Directories.GetInfoAsync("/f11/dir", Ct);

            (string from, string to) = kind switch
            {
                "same-path" => (src, src),
                "normalization-equivalent-path" => (src, "/f11/café.bin"),
                "destination-is-directory" => (src, "/f11/dir"),
                "destination-is-root" => (src, "/"),
                "source-is-directory" => ("/f11/dir", "/f11/copy-of-dir"),
                "destination-parent-missing" => (src, "/f11/missing/copy.bin"),
                "destination-parent-is-file" => (src, src + "/copy.bin"),
                "source-missing" => ("/f11/absent.bin", "/f11/copy.bin"),
                "destination-dotdot" => (src, "/f11/dir/../copy.bin"),
                "destination-relative" => (src, "f11/copy.bin"),
                "destination-trailing-slash" => (src, "/f11/copy.bin/"),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };

            Exception? failure = null;
            try
            {
                await v.Files.CopyAsync(from, to, overwrite, Ct);
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            Assert.True(failure is not null, $"Copy({from} -> {to}, overwrite={overwrite}) must be rejected");
            switch (kind)
            {
                case "same-path":
                case "normalization-equivalent-path":
                    Assert.True(failure is IOException or ArgumentException, $"copy onto itself: unexpected {failure}");
                    Assert.False(failure is FileNotFoundException or DirectoryNotFoundException, "copy onto itself is not an absence");
                    break;
                case "destination-is-directory":
                    Assert.True(failure is DfsException { ErrorCode: DfsErrorCode.IsADirectory or DfsErrorCode.AlreadyExists },
                        $"copy onto a directory must be rejected as IsADirectory/AlreadyExists, got {failure}");
                    break;
                case "destination-is-root":
                    Assert.True(failure is ArgumentException or DfsException { ErrorCode: DfsErrorCode.IsADirectory or DfsErrorCode.AlreadyExists or DfsErrorCode.InvalidPath },
                        $"copy onto the root must be rejected, got {failure}");
                    break;
                case "source-is-directory":
                    Assert.True(failure is DfsException { ErrorCode: DfsErrorCode.IsADirectory }, $"copying a directory with the file helper must be IsADirectory, got {failure}");
                    break;
                case "destination-parent-missing":
                    Assert.IsType<DirectoryNotFoundException>(failure, exactMatch: false);
                    break;
                case "destination-parent-is-file":
                    Assert.True(failure is DirectoryNotFoundException or DfsException { ErrorCode: DfsErrorCode.NotADirectory },
                        $"a file used as a directory must be NotADirectory/DirectoryNotFound, got {failure}");
                    break;
                case "source-missing":
                    Assert.IsType<FileNotFoundException>(failure, exactMatch: false);
                    break;
                default:
                    Assert.IsType<ArgumentException>(failure, exactMatch: false);
                    break;
            }

            // The source is never truncated or replaced; directories and their children are untouched.
            await AssertFileAsync(v, src, source, srcBefore.InodeId, "source after rejected copy");
            Assert.Equal(srcExtentsBefore, await ExtentObjectsAsync(cluster, src));
            if (from != to && kind != "normalization-equivalent-path")
            {
                // When the source is not also the destination nothing may even touch its inode record. (A self-copy may
                // legitimately take and release a destination writer reservation before rejecting, which can advance the
                // persisted writer generation; bytes, length, identity and extent map above prove it was not replaced.)
                Assert.Equal(srcBefore.Revision, (await v.Files.GetInfoAsync(src, Ct)).Revision);
            }

            DfsDirectoryInfo dirAfter = await v.Directories.GetInfoAsync("/f11/dir", Ct);
            Assert.Equal(dirBefore.InodeId, dirAfter.InodeId);
            Assert.Equal(dirBefore.Revision, dirAfter.Revision);
            Assert.True(await v.Files.ExistsAsync("/f11/dir/child/keep.bin", Ct));
            Assert.False(await v.Directories.ExistsAsync("/f11/missing", Ct), "a rejected copy must not create missing parents");
            Assert.False(await v.Files.ExistsAsync("/f11/copy.bin", Ct));
            Assert.False(await v.Directories.ExistsAsync("/f11/copy-of-dir", Ct));
            List<DfsEntryInfo> entries = await ToListAsync(v.Directories.EnumerateAsync("/f11", null, Ct));
            Assert.Equal(new[] { "café.bin", "dir" }, entries.Select(e => e.Name).Order(StringComparer.Ordinal).ToArray());
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-012 CopyOverwriteOptions
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [Catalog("FILE-012", "U", "INV-02", "INV-07")]
    public async Task FILE_012_CopyOverwriteOptions(bool destinationExists, bool overwrite)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            byte[] source = TestData.Bytes(0x1201, 2 * Chunk + 3);
            byte[] oldDest = TestData.Bytes(0x1202, 1000);
            const string src = "/f12-src.bin";
            const string dst = "/f12-dst.bin";
            CommitToken srcToken = await v.Files.WriteAllBytesAsync(src, source, Ct);
            DfsFileInfo srcInfo = await v.Files.GetInfoAsync(src, Ct);
            DfsFileInfo? dstBefore = null;
            if (destinationExists)
            {
                await v.Files.WriteAllBytesAsync(dst, oldDest, Ct);
                dstBefore = await v.Files.GetInfoAsync(dst, Ct);
            }

            if (destinationExists && !overwrite)
            {
                await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(() => v.Files.CopyAsync(src, dst, false, Ct), DfsErrorCode.AlreadyExists);
                await AssertFileAsync(v, dst, oldDest, dstBefore!.InodeId, "destination after rejected non-overwriting copy");
                Assert.Equal(dstBefore.Revision, (await v.Files.GetInfoAsync(dst, Ct)).Revision);
            }
            else
            {
                CommitToken token = await v.Files.CopyAsync(src, dst, overwrite, Ct);
                Assert.False(token.IsNone);
                Assert.True(token.IsComparableWith(srcToken) && token.CompareTo(srcToken) > 0);
                DfsFileInfo dstAfter = await v.Files.GetInfoAsync(dst, Ct);
                Assert.True(dstAfter.ObservedAt.IsAtOrAfter(token));
                await AssertFileAsync(v, dst, source, null, "destination after copy");
                Assert.NotEqual(srcInfo.InodeId, dstAfter.InodeId);
                if (dstBefore is not null)
                {
                    Assert.Equal(dstBefore.InodeId, dstAfter.InodeId); // overwrite preserves destination identity
                    Assert.True(dstAfter.Revision > dstBefore.Revision);
                }
            }

            // The source is never modified by a copy.
            await AssertFileAsync(v, src, source, srcInfo.InodeId, "source after copy");
            Assert.Equal(srcInfo.Revision, (await v.Files.GetInfoAsync(src, Ct)).Revision);
        }, Ct);
    }

    [Theory]
    [InlineData(FileAccess.Read, FileShare.Read, false)]
    [InlineData(FileAccess.Read, FileShare.None, false)]
    [InlineData(FileAccess.Read, FileShare.Delete, false)]
    [InlineData(FileAccess.Read, FileShare.Write, true)]
    [InlineData(FileAccess.Read, FileShare.ReadWrite, true)]
    [InlineData(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, true)]
    [InlineData(FileAccess.Write, FileShare.Read, false)]
    [InlineData(FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, false)]
    [Catalog("FILE-012", "U", "INV-02", "INV-07")]
    public async Task FILE_012_CopyOverwriteOptions_ShareReservations(FileAccess access, FileShare share, bool copyAllowed)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient holderClient = await cluster.ConnectAsync(Named("holder"), Ct);
            await using DfsClient copierClient = await cluster.ConnectAsync(Named("copier"), Ct);
            DfsVolume holder = holderClient.GetVolume(Vol);
            DfsVolume copier = copierClient.GetVolume(Vol);
            byte[] source = TestData.Bytes(0x1211, Chunk + 9);
            byte[] oldDest = TestData.Bytes(0x1212, 5000);
            const string src = "/f12s-src.bin";
            const string dst = "/f12s-dst.bin";
            await copier.Files.WriteAllBytesAsync(src, source, Ct);
            await copier.Files.WriteAllBytesAsync(dst, oldDest, Ct);
            DfsFileInfo dstBefore = await copier.Files.GetInfoAsync(dst, Ct);

            await using DfsStream handle = await holder.Files.OpenAsync(dst, FileMode.Open, access, share, Ct);
            Assert.Equal(dstBefore.InodeId, handle.InodeId);

            if (!copyAllowed)
            {
                DfsSharingViolationException ex = await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(() => copier.Files.CopyAsync(src, dst, true, Ct));
                Assert.True(ex.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict);
                Assert.True(ex.IsRetryable);
                DfsFileInfo refused = await copier.Files.GetInfoAsync(dst, Ct);
                Assert.Equal(dstBefore.InodeId, refused.InodeId);
                Assert.Equal(dstBefore.Revision, refused.Revision);
                Assert.Equal(oldDest.Length, refused.Length);
                if (handle.CanRead)
                {
                    handle.Position = 0;
                    AssertBytes(oldDest, await ReadToEndAsync(handle, Ct), "the existing handle still reads the untouched destination");
                }

                await handle.DisposeAsync();
                await AssertFileAsync(copier, dst, oldDest, dstBefore.InodeId, "destination after copy refused by a share reservation");
            }
            else
            {
                CommitToken token = await copier.Files.CopyAsync(src, dst, true, Ct);
                Assert.False(token.IsNone);
                DfsFileInfo replaced = await copier.Files.GetInfoAsync(dst, Ct);
                Assert.Equal(dstBefore.InodeId, replaced.InodeId);
                Assert.True(replaced.Revision > dstBefore.Revision);
                Assert.Equal(source.Length, replaced.Length);
                // The live read handle remains bound to the same inode and now observes the new committed revision.
                if (handle.CanRead)
                {
                    handle.Position = 0;
                    AssertBytes(source, await ReadToEndAsync(handle, Ct), "a live handle reads the replacement committed before the read");
                }

                await handle.DisposeAsync();
                await AssertFileAsync(copier, dst, source, dstBefore.InodeId, "compatible reservations allow atomic replacement with preserved identity");
            }
        }, Ct);
    }

    [Theory]
    [InlineData("allowed")]
    [InlineData("read-only-principal")]
    [InlineData("destination-owner-only")]
    [InlineData("destination-read-only-for-others")]
    [InlineData("source-owner-only")]
    [Catalog("FILE-012", "U", "INV-02", "INV-12")]
    public async Task FILE_012_CopyOverwriteOptions_Permissions(string kind)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(Named("admin"), Ct);
            DfsVolume av = admin.GetVolume(Vol);
            DfsPermissions grant = kind == "read-only-principal" ? DfsPermissions.Read : DfsPermissions.Read | DfsPermissions.Write;
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice", Grant(grant));
            await using DfsClient aliceClient = await cluster.ConnectAsync(Named("alice", alice.Credentials), Ct);
            DfsVolume aliceVol = aliceClient.GetVolume(Vol);

            byte[] source = TestData.Bytes(0x1221, 3000);
            byte[] oldDest = TestData.Bytes(0x1222, 2000);
            const string src = "/f12p-src.bin";
            const string dst = "/f12p-dst.bin";
            await av.Files.WriteAllBytesAsync(src, source, Ct);
            if (kind is "destination-owner-only" or "destination-read-only-for-others")
                await av.Files.WriteAllBytesAsync(dst, oldDest, Ct);
            if (kind == "destination-owner-only")
                await av.Files.SetAccessAsync(dst, new DfsInodeAccess(OwnerOnly: true), Ct);
            if (kind == "destination-read-only-for-others")
                await av.Files.SetAccessAsync(dst, new DfsInodeAccess(ReadOnlyForOthers: true), Ct);
            if (kind == "source-owner-only")
                await av.Files.SetAccessAsync(src, new DfsInodeAccess(OwnerOnly: true), Ct);
            bool destinationExisted = await av.Files.ExistsAsync(dst, Ct);
            DfsFileInfo? dstBefore = destinationExisted ? await av.Files.GetInfoAsync(dst, Ct) : null;

            if (kind == "allowed")
            {
                CommitToken token = await aliceVol.Files.CopyAsync(src, dst, true, Ct);
                Assert.False(token.IsNone);
                await AssertFileAsync(av, dst, source);
                Assert.Equal("alice", (await av.Files.GetInfoAsync(dst, Ct)).Owner);
                return;
            }

            await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => aliceVol.Files.CopyAsync(src, dst, true, Ct));
            if (dstBefore is null)
            {
                Assert.False(await av.Files.ExistsAsync(dst, Ct), "an unauthorized copy must not create the destination");
            }
            else
            {
                await AssertFileAsync(av, dst, oldDest, dstBefore.InodeId, "destination after unauthorized copy");
                Assert.Equal(dstBefore.Revision, (await av.Files.GetInfoAsync(dst, Ct)).Revision);
            }

            await AssertFileAsync(av, src, source, null, "source after unauthorized copy");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-013 DeleteMissingIsIdempotent
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("/f13/report.tx", false)]
    [InlineData("/f13/report.txt.bak", false)]
    [InlineData("/f13/Report.txt", true)]
    [InlineData("/f13/cafe", false)]
    [InlineData("/f13/CAFÉ", false)]
    [InlineData("/f13/sub/report.txt", false)]
    [Catalog("FILE-013", "U", "INV-07")]
    public async Task FILE_013_DeleteMissingIsIdempotent(string absentPath, bool useSync)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/f13/sub", Ct);
            string[] siblings = ["/f13/report.txt", "/f13/report", "/f13/reportx.txt", "/f13/Café", "/f13/sub/report.tx2"];
            var before = new Dictionary<string, (DfsFileInfo Info, byte[] Bytes)>(StringComparer.Ordinal);
            CommitToken last = CommitToken.None;
            for (int i = 0; i < siblings.Length; i++)
            {
                byte[] bytes = TestData.Bytes((ulong)(0x1300 + i), 100 + i);
                last = await v.Files.WriteAllBytesAsync(siblings[i], bytes, Ct);
                before[siblings[i]] = (await v.Files.GetInfoAsync(siblings[i], Ct), bytes);
            }

            DfsDirectoryInfo parentBefore = await v.Directories.GetInfoAsync("/f13", Ct);
            DfsDirectoryInfo subBefore = await v.Directories.GetInfoAsync("/f13/sub", Ct);
            DfsOperationId explicitId = await v.NewOperationIdAsync(Ct);

            for (int attempt = 0; attempt < 3; attempt++)
            {
                CommitToken token = useSync ? v.Files.Delete(absentPath) : await v.Files.DeleteAsync(absentPath, Ct);
                if (!token.IsNone)
                    Assert.True(token.IsComparableWith(last), "a no-op delete token must belong to this volume/epoch");
                CommitToken withId = await v.Files.DeleteAsync(absentPath, new DfsMutationOptions { OperationId = explicitId }, Ct);
                if (!withId.IsNone)
                    Assert.True(withId.IsComparableWith(last));
                Assert.False(await v.Files.ExistsAsync(absentPath, Ct));
            }

            // Neither parent nor similarly named entries were affected.
            foreach ((string path, (DfsFileInfo info, byte[] bytes)) in before)
            {
                await AssertFileAsync(v, path, bytes, info.InodeId, $"sibling {path} after deleting absent {absentPath}");
                Assert.Equal(info.Revision, (await v.Files.GetInfoAsync(path, Ct)).Revision);
            }

            DfsDirectoryInfo parentAfter = await v.Directories.GetInfoAsync("/f13", Ct);
            Assert.Equal(parentBefore.Revision, parentAfter.Revision);
            Assert.Equal(parentBefore.EntryCount, parentAfter.EntryCount);
            DfsDirectoryInfo subAfter = await v.Directories.GetInfoAsync("/f13/sub", Ct);
            Assert.Equal(subBefore.Revision, subAfter.Revision);
            Assert.Equal(subBefore.EntryCount, subAfter.EntryCount);

            // Deleting a real file, then deleting it again, is also idempotent.
            const string doomed = "/f13/doomed.bin";
            await v.Files.WriteAllBytesAsync(doomed, TestData.Bytes(0x13FF, 10), Ct);
            CommitToken deleted = await v.Files.DeleteAsync(doomed, Ct);
            Assert.False(deleted.IsNone, "deleting an existing file is a real commit");
            CommitToken again = await v.Files.DeleteAsync(doomed, Ct);
            if (!again.IsNone)
                Assert.True(again.IsComparableWith(deleted));
            Assert.False(await v.Files.ExistsAsync(doomed, Ct));
            Assert.Equal(parentBefore.EntryCount, (await v.Directories.GetInfoAsync("/f13", Ct)).EntryCount);
        }, Ct);
    }

    [Fact]
    [Catalog("FILE-013", "U", "INV-07", "INV-08")]
    public async Task FILE_013_DeleteMissingIsIdempotent_DirectoryNameIsNotAbsence()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            DfsDirectoryInfo dir = await v.Directories.CreateDirectoryAsync("/f13d/name", Ct);
            await v.Files.WriteAllBytesAsync("/f13d/name/inner.bin", TestData.Bytes(0x13D0, 10), Ct);

            // A directory at the path is not an absent file: the file delete must not succeed silently nor remove it.
            Exception? failure = await Record.ExceptionAsync(() => v.Files.DeleteAsync("/f13d/name", Ct));
            Assert.True(failure is DfsException { ErrorCode: DfsErrorCode.IsADirectory } or UnauthorizedAccessException,
                $"Files.Delete on a directory must be rejected (IsADirectory), got {failure?.ToString() ?? "success"}");
            DfsDirectoryInfo after = await v.Directories.GetInfoAsync("/f13d/name", Ct);
            Assert.Equal(dir.InodeId, after.InodeId);
            Assert.True(await v.Files.ExistsAsync("/f13d/name/inner.bin", Ct));

            // Absent file below an absent directory: success (absent) or DirectoryNotFound, never creation.
            Exception? deep = await Record.ExceptionAsync(() => v.Files.DeleteAsync("/f13d/missing/x.bin", Ct));
            Assert.True(deep is null or DirectoryNotFoundException, $"unexpected {deep}");
            Assert.False(await v.Directories.ExistsAsync("/f13d/missing", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-015 HelpersInsideTransactionDoNotCommit
    // ------------------------------------------------------------------------------------------------------------

    private sealed record NamespaceView(bool New, bool Copy, bool M1, bool M2, bool Del, bool NewDir);

    [Theory]
    [InlineData("commit")]
    [InlineData("abort")]
    [InlineData("dispose")]
    [Catalog("FILE-015", "U", "INV-02", "INV-04", "INV-07")]
    public async Task FILE_015_HelpersInsideTransactionDoNotCommit(string ending)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient owner = await cluster.ConnectAsync(Named("owner"), Ct);
            await using DfsClient observerClient = await cluster.ConnectAsync(Named("observer"), Ct);
            DfsVolume v = owner.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);

            await v.Directories.CreateDirectoryAsync("/t", Ct);
            byte[] srcBytes = TestData.Bytes(0x1501, Chunk + 1);
            byte[] m1Bytes = TestData.Bytes(0x1502, 700);
            byte[] delBytes = TestData.Bytes(0x1503, 800);
            byte[] appBytes = TestData.Bytes(0x1504, 900);
            byte[] newBytes = TestData.Bytes(0x1505, 2 * Chunk + 5);
            byte[] suffix = TestData.Bytes(0x1506, 300);
            await v.Files.WriteAllBytesAsync("/t/src.bin", srcBytes, Ct);
            await v.Files.WriteAllBytesAsync("/t/m1.bin", m1Bytes, Ct);
            await v.Files.WriteAllBytesAsync("/t/del.bin", delBytes, Ct);
            await v.Files.WriteAllBytesAsync("/t/app.bin", appBytes, Ct);
            await v.Files.WriteAllTextAsync("/t/text.txt", "original", null, Ct);
            var inodes = new Dictionary<string, InodeId>(StringComparer.Ordinal);
            foreach (string p in new[] { "/t/src.bin", "/t/m1.bin", "/t/del.bin", "/t/app.bin", "/t/text.txt" })
                inodes[p] = (await v.Files.GetInfoAsync(p, Ct)).InodeId;
            DfsDirectoryInfo dirBefore = await v.Directories.GetInfoAsync("/t", Ct);

            async Task AssertObserverSeesInitialStateAsync(string step)
            {
                Assert.False(await observer.Files.ExistsAsync("/t/new.bin", Ct), $"after {step}: tx WriteAllBytes leaked");
                Assert.False(await observer.Files.ExistsAsync("/t/copy.bin", Ct), $"after {step}: tx Copy leaked");
                Assert.True(await observer.Files.ExistsAsync("/t/m1.bin", Ct), $"after {step}: tx Move leaked (source gone)");
                Assert.False(await observer.Files.ExistsAsync("/t/m2.bin", Ct), $"after {step}: tx Move leaked (destination visible)");
                Assert.True(await observer.Files.ExistsAsync("/t/del.bin", Ct), $"after {step}: tx Delete leaked");
                Assert.False(await observer.Files.ExistsAsync("/t/lines.txt", Ct), $"after {step}: tx WriteAllLines leaked");
                Assert.False(await observer.Directories.ExistsAsync("/t/newdir", Ct), $"after {step}: tx CreateDirectory leaked");
                // Files the transaction reserved still show their committed contents. (Revision equality is not used for
                // reserved inodes: taking a writer reservation may advance the inode's persisted writer generation
                // without publishing anything.)
                await AssertCommittedContentAsync("/t/app.bin", appBytes, step);
                await AssertCommittedContentAsync("/t/text.txt", Encoding.UTF8.GetBytes("original"), step);
                await AssertCommittedContentAsync("/t/m1.bin", m1Bytes, step);
                await AssertCommittedContentAsync("/t/del.bin", delBytes, step);
                Assert.Equal(dirBefore.Revision, (await observer.Directories.GetInfoAsync("/t", Ct)).Revision);
                Assert.Equal(dirBefore.EntryCount, (await observer.Directories.GetInfoAsync("/t", Ct)).EntryCount);
            }

            async Task AssertCommittedContentAsync(string p, byte[] expected, string step)
            {
                // Metadata reads take no reservation, so they never conflict with the transaction's writer reservations.
                DfsFileInfo info = await observer.Files.GetInfoAsync(p, Ct);
                Assert.Equal(inodes[p], info.InodeId);
                Assert.Equal(expected.Length, info.Length);
                try
                {
                    (byte[] bytes, InodeId inode) = await ReadSharedAsync(observer, p);
                    AssertBytes(expected, bytes, $"after {step}: {p} must still hold its committed contents");
                    Assert.Equal(inodes[p], inode);
                }
                catch (DfsSharingViolationException)
                {
                    // The transaction's writer reservation may refuse even a ReadWrite|Delete-sharing reader; length and
                    // identity above plus the post-transaction content checks still apply.
                }
            }

            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            try
            {
                Assert.True(tx.Files.IsTransactionBound);
                Assert.True(tx.Directories.IsTransactionBound);
                Assert.Equal(DfsTransactionState.Active, tx.State);

                Assert.True((await tx.Files.WriteAllBytesAsync("/t/new.bin", newBytes, Ct)).IsNone, "tx-bound WriteAllBytes returns CommitToken.None");
                await AssertObserverSeesInitialStateAsync("WriteAllBytes");
                Assert.True((await tx.Files.CopyAsync("/t/src.bin", "/t/copy.bin", false, Ct)).IsNone);
                await AssertObserverSeesInitialStateAsync("Copy");
                Assert.True((await tx.Files.MoveAsync("/t/m1.bin", "/t/m2.bin", false, Ct)).IsNone);
                await AssertObserverSeesInitialStateAsync("Move");
                Assert.True((await tx.Files.DeleteAsync("/t/del.bin", Ct)).IsNone);
                await AssertObserverSeesInitialStateAsync("Delete");
                Assert.True((await tx.Files.AppendAllBytesAsync("/t/app.bin", suffix, Ct)).IsNone);
                await AssertObserverSeesInitialStateAsync("AppendAllBytes");
                Assert.True((await tx.Files.WriteAllTextAsync("/t/text.txt", "replaced", null, Ct)).IsNone);
                Assert.True((await tx.Files.AppendAllTextAsync("/t/text.txt", "+more", null, Ct)).IsNone);
                await AssertObserverSeesInitialStateAsync("WriteAllText/AppendAllText");
                Assert.True((await tx.Files.WriteAllLinesAsync("/t/lines.txt", ["one", "two"], null, Ct)).IsNone);
                Assert.True((await tx.Files.AppendAllLinesAsync("/t/lines.txt", ["three"], null, Ct)).IsNone);
                await tx.Directories.CreateDirectoryAsync("/t/newdir", Ct);
                await AssertObserverSeesInitialStateAsync("WriteAllLines/AppendAllLines/CreateDirectory");

                // The transaction reads its own writes.
                AssertBytes(newBytes, await tx.Files.ReadAllBytesAsync("/t/new.bin", Ct), "tx reads its own WriteAllBytes");
                AssertBytes(srcBytes, await tx.Files.ReadAllBytesAsync("/t/copy.bin", Ct), "tx reads its own Copy");
                Assert.False(await tx.Files.ExistsAsync("/t/m1.bin", Ct));
                AssertBytes(m1Bytes, await tx.Files.ReadAllBytesAsync("/t/m2.bin", Ct), "tx reads its own Move");
                Assert.False(await tx.Files.ExistsAsync("/t/del.bin", Ct));
                AssertBytes(Concat(appBytes, suffix), await tx.Files.ReadAllBytesAsync("/t/app.bin", Ct), "tx reads its own append");
                Assert.Equal("replaced+more", await tx.Files.ReadAllTextAsync("/t/text.txt", null, Ct));
                Assert.Equal(new[] { "one", "two", "three" }, await tx.Files.ReadAllLinesAsync("/t/lines.txt", null, Ct));
                Assert.True(await tx.Directories.ExistsAsync("/t/newdir", Ct));

                switch (ending)
                {
                    case "commit":
                    {
                        // Concurrent observer transactions see all-old or all-new, never a mix.
                        var commitDone = new TaskCompletionSource();
                        var views = new List<NamespaceView>();

                        async Task ObserveAsync()
                        {
                            for (int i = 0; i < 1000 && (!commitDone.Task.IsCompleted || views.Count < 2); i++)
                            {
                                await using DfsTransaction view = await observer.Transactions.BeginAsync(null, Ct);
                                views.Add(new NamespaceView(
                                    await view.Files.ExistsAsync("/t/new.bin", Ct),
                                    await view.Files.ExistsAsync("/t/copy.bin", Ct),
                                    await view.Files.ExistsAsync("/t/m1.bin", Ct),
                                    await view.Files.ExistsAsync("/t/m2.bin", Ct),
                                    await view.Files.ExistsAsync("/t/del.bin", Ct),
                                    await view.Directories.ExistsAsync("/t/newdir", Ct)));
                            }
                        }

                        Task observing = ObserveAsync();
                        CommitToken committed = await tx.CommitAsync(Ct);
                        commitDone.SetResult();
                        await observing;
                        Assert.False(committed.IsNone);
                        Assert.Equal(DfsTransactionState.Committed, tx.State);
                        var allOld = new NamespaceView(false, false, true, false, true, false);
                        var allNew = new NamespaceView(true, true, false, true, false, true);
                        Assert.All(views, view => Assert.True(view == allOld || view == allNew, $"observer transaction saw a partial commit: {view}"));

                        await AssertFileAsync(observer, "/t/new.bin", newBytes);
                        await AssertFileAsync(observer, "/t/copy.bin", srcBytes);
                        await AssertFileAsync(observer, "/t/m2.bin", m1Bytes, inodes["/t/m1.bin"], "moved inode keeps its identity");
                        Assert.False(await observer.Files.ExistsAsync("/t/m1.bin", Ct));
                        Assert.False(await observer.Files.ExistsAsync("/t/del.bin", Ct));
                        await AssertFileAsync(observer, "/t/app.bin", Concat(appBytes, suffix), inodes["/t/app.bin"], "appended file keeps its identity");
                        Assert.Equal("replaced+more", await observer.Files.ReadAllTextAsync("/t/text.txt", null, Ct));
                        Assert.Equal(inodes["/t/text.txt"], (await observer.Files.GetInfoAsync("/t/text.txt", Ct)).InodeId);
                        Assert.Equal(new[] { "one", "two", "three" }, await observer.Files.ReadAllLinesAsync("/t/lines.txt", null, Ct));
                        Assert.True(await observer.Directories.ExistsAsync("/t/newdir", Ct));
                        DfsFileInfo newInfo = await observer.Files.GetInfoAsync("/t/new.bin", Ct);
                        Assert.True(newInfo.ObservedAt.IsAtOrAfter(committed));
                        Assert.Equal(newInfo.Revision, (await observer.Files.GetInfoAsync("/t/copy.bin", Ct)).Revision);
                        break;
                    }

                    case "abort":
                        await tx.AbortAsync(Ct);
                        Assert.Equal(DfsTransactionState.Aborted, tx.State);
                        break;
                    case "dispose":
                        await tx.DisposeAsync();
                        break;
                }
            }
            finally
            {
                await tx.DisposeAsync();
            }

            if (ending != "commit")
            {
                await AssertObserverSeesInitialStateAsync($"transaction {ending}");
                await AssertFileAsync(observer, "/t/app.bin", appBytes, inodes["/t/app.bin"], $"append after transaction {ending}");
                Assert.Equal("original", await observer.Files.ReadAllTextAsync("/t/text.txt", null, Ct));
                // After abort/dispose the bound facade is unusable (FSD §8.2): an explicit rejection, never a silent
                // standalone write and never an unrelated crash.
                Exception? late = await Record.ExceptionAsync(() => tx.Files.WriteAllBytesAsync("/t/late.bin", newBytes, Ct));
                Assert.True(late is InvalidOperationException or DfsException,
                    $"a helper on an ended transaction must be rejected (InvalidOperation/ObjectDisposed or a transaction error), got {late?.ToString() ?? "success"}");
                Assert.False(await observer.Files.ExistsAsync("/t/late.bin", Ct));
                await WaitForNoOpenStagingAsync(cluster, "an ended transaction must close its staging session");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-016 HelperCancellationCleansResources
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Where the caller's token is cancelled.</summary>
    public enum CancelPoint
    {
        /// <summary>F01: after validation/op-id allocation, before anything is transmitted.</summary>
        BeforeTransmission,

        /// <summary>F03: while uploading the second data chunk (nothing published yet).</summary>
        DuringUpload,

        /// <summary>F10: on the leader just before the metadata proposal (commit request already sent).</summary>
        BeforePropose,

        /// <summary>F12: after authoritative apply, before the reply (the operation is committed).</summary>
        AfterApply,

        /// <summary>F14: while a read reply is being received.</summary>
        DuringRead,
    }

    [Theory]
    [InlineData("WriteAllBytes", CancelPoint.BeforeTransmission, true)]
    [InlineData("WriteAllBytes", CancelPoint.DuringUpload, true)]
    [InlineData("WriteAllBytes", CancelPoint.BeforePropose, true)]
    [InlineData("WriteAllBytes", CancelPoint.AfterApply, true)]
    [InlineData("WriteAllBytes", CancelPoint.AfterApply, false)]
    [InlineData("AppendAllBytes", CancelPoint.BeforeTransmission, true)]
    [InlineData("AppendAllBytes", CancelPoint.BeforePropose, true)]
    [InlineData("AppendAllBytes", CancelPoint.AfterApply, true)]
    [InlineData("Copy", CancelPoint.BeforeTransmission, true)]
    [InlineData("Copy", CancelPoint.AfterApply, true)]
    [InlineData("ReadAllBytes", CancelPoint.DuringRead, false)]
    [InlineData("ReadLines", CancelPoint.DuringRead, false)]
    [Catalog("FILE-016", "U", "INV-02", "INV-16", "INV-08")]
    public async Task FILE_016_HelperCancellationCleansResources(string helper, CancelPoint point, bool explicitOperationId)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            await using DfsClient observerClient = await cluster.ConnectAsync(Named("observer"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);

            const string target = "/f16-target.bin";
            const string source = "/f16-source.bin";
            byte[] oldBytes = TestData.Bytes(0x1601, 2 * Chunk + 11);
            byte[] payload = TestData.Bytes(0x1602, 3 * Chunk + 5);
            await v.Files.WriteAllBytesAsync(target, oldBytes, Ct);
            await v.Files.WriteAllBytesAsync(source, payload, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            DfsFileInfo before = await v.Files.GetInfoAsync(target, Ct);
            byte[] committedBytes = helper switch
            {
                "WriteAllBytes" => payload,
                "AppendAllBytes" => Concat(oldBytes, payload),
                "Copy" => payload,
                _ => oldBytes,
            };

            DfsOperationKind kind = helper switch
            {
                "WriteAllBytes" => DfsOperationKind.WriteAll,
                "AppendAllBytes" => DfsOperationKind.Append,
                "Copy" => DfsOperationKind.Copy,
                _ => DfsOperationKind.Read,
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            FaultAction cancelFromServer = FaultAction.Custom(ctx =>
            {
                _ = Task.Run(cts.Cancel, CancellationToken.None);
                return Task.CompletedTask;
            });
            FaultArm arm = point switch
            {
                CancelPoint.BeforeTransmission => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientValidated, Operation: kind), FaultAction.CancelCaller()),
                CancelPoint.DuringUpload => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 2), FaultAction.CancelCaller()),
                CancelPoint.BeforePropose => cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Operation: kind), cancelFromServer),
                CancelPoint.AfterApply => cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Operation: kind), cancelFromServer),
                CancelPoint.DuringRead => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientReplyReceived, Operation: DfsOperationKind.Read), FaultAction.CancelCaller()),
                _ => throw new ArgumentOutOfRangeException(nameof(point)),
            };

            DfsOperationId? opId = explicitOperationId ? await v.NewOperationIdAsync(Ct) : null;
            var options = new DfsMutationOptions { OperationId = opId };
            Exception? failure = null;
            CommitToken token = CommitToken.None;
            try
            {
                switch (helper)
                {
                    case "WriteAllBytes":
                        token = await v.Files.WriteAllBytesAsync(target, payload, options, cts.Token);
                        break;
                    case "AppendAllBytes":
                        token = await v.Files.AppendAllBytesAsync(target, payload, options, cts.Token);
                        break;
                    case "Copy":
                        token = await v.Files.CopyAsync(source, target, true, options, cts.Token);
                        break;
                    case "ReadAllBytes":
                        AssertBytes(oldBytes, await v.Files.ReadAllBytesAsync(target, cts.Token), "a read that completes despite cancellation returns the whole pinned revision");
                        break;
                    case "ReadLines":
                        await foreach (string _ in v.Files.ReadLinesAsync(target, null, cts.Token))
                        {
                        }

                        break;
                }
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            cluster.Faults.AssertReached(arm);
            cluster.Faults.DisarmAll();

            // Classify the legal outcome and check each branch's own invariant.
            bool committed;
            if (failure is null)
            {
                committed = helper is "WriteAllBytes" or "AppendAllBytes" or "Copy";
                if (committed)
                    Assert.False(token.IsNone);
            }
            else if (failure is DfsCommitOutcomeUnknownException unknown)
            {
                Assert.True(helper is "WriteAllBytes" or "AppendAllBytes" or "Copy", "only mutations can have an unknown outcome");
                Assert.False(unknown.OperationId.IsEmpty, "an unknown outcome must retain its operation id");
                if (opId is { } id)
                    Assert.Equal(id, unknown.OperationId);
                DfsOperationOutcome resolved = await v.QueryOperationAsync(unknown.OperationId, resolve: true, Ct);
                Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve must be definite, was {resolved.Status}");
                committed = resolved.Status == DfsOutcomeStatus.Committed;
            }
            else if (failure is OperationCanceledException)
            {
                // Cancellation is only legal when the SDK established that nothing was (or will be) published.
                committed = false;
                if (opId is { } id)
                    Assert.Equal(DfsOutcomeStatus.Aborted, (await v.QueryOperationAsync(id, resolve: true, Ct)).Status);
                Assert.True(point != CancelPoint.AfterApply,
                    "after authoritative apply the operation is committed; plain cancellation would misreport it as rollback");
            }
            else
            {
                Assert.Fail($"cancellation surfaced as an unexpected exception: {failure}");
                return;
            }

            if (point is CancelPoint.BeforeTransmission or CancelPoint.DuringUpload)
                Assert.False(committed, "nothing was published before the commit request, so the operation cannot have committed");

            (byte[] seen, InodeId seenInode) = await ReadSharedAsync(observer, target);
            AssertBytes(committed ? committedBytes : oldBytes, seen,
                committed ? "committed outcome must be complete" : "cancelled helper must leave the target unchanged");
            Assert.Equal(before.InodeId, seenInode);

            // Internal streams closed, reservations and staging released.
            await WaitForNoHandlesAsync(cluster, before.InodeId, "a cancelled helper must close its internal handle and release its reservation");
            await WaitForNoOpenStagingAsync(cluster, "a cancelled helper must close its staging session");
            await AssertFileAsync(observer, target, committed ? committedBytes : oldBytes, before.InodeId, "after resources were released");
            byte[] after = TestData.Bytes(0x1603, 100);
            await observer.Files.WriteAllBytesAsync(target, after, Ct);
            await AssertFileAsync(v, target, after, before.InodeId, "the target accepts a new writer after cancellation");
        }, Ct);
    }
}
