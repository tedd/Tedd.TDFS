using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Integration.Str.StrHelpers;

namespace Tedd.Tdfs.Tests.Integration.Str;

/// <summary>How the StreamWriter of STR-021 hands its data to the DfsStream.</summary>
public enum WriterHandOff { OuterFlush, DisposeWriterClosesStream, LeaveOpenThenCommit }

/// <summary>Standard compression stream of STR-023.</summary>
public enum CompressionKind { GZip, Deflate, ZLib, Brotli }

/// <summary>Seek-requiring consumer scenario of STR-024.</summary>
public enum ArchiveScenario { CreateOnSeekableStream, UpdateInPlace, CreateOnAppendStreaming }

/// <summary>
/// STR — stream interoperability with standard .NET consumers: the I-level rows (FSD §§5.3, 6, 6.3; AT-01; test plan
/// §7.5). Every row runs a three-zone cluster over the real QUIC transport with on-disk storage and uses no local files.
/// </summary>
public sealed class StrIntegrationTests
{
    // ------------------------------------------------------------------ STR-021

    public static TheoryData<int, WriterHandOff, bool, bool> ReaderWriterCases() => new()
    {
        { 1, WriterHandOff.OuterFlush, false, false },
        { 3, WriterHandOff.DisposeWriterClosesStream, true, false },
        { 7, WriterHandOff.LeaveOpenThenCommit, false, true },
        { 1, WriterHandOff.DisposeWriterClosesStream, false, true },
        { 16, WriterHandOff.OuterFlush, true, true },
        { 5, WriterHandOff.LeaveOpenThenCommit, true, false },
    };

    /// <summary>
    /// UTF-8 text with 2-, 3- and 4-byte characters is written through a StreamWriter with tiny buffers (char by char, so
    /// multibyte sequences and surrogate pairs are split across calls and encoder flushes, then odd fragments). Nothing is
    /// published until the outer layer flushes/disposes; then the committed bytes are exactly the encoding of the text
    /// (BOM only when requested), leaveOpen is respected, and StreamReader with a minimal buffer reads the same text and lines.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReaderWriterCases))]
    [Catalog("STR-021", "I", "INV-02", "INV-07")]
    public async Task STR_021_StreamReaderWriterInterop(int writerBufferChars, WriterHandOff handOff, bool useAsync, bool emitBom)
    {
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str021.txt";
            string text = TestData.Utf8Text(0x5021, 120_000, multibyte: true) + "\nsecond line\r\nthird line ÆØÅ \U0001F600 中文\n\nlast";
            var encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: emitBom);
            byte[] expected = [.. encoding.GetPreamble(), .. encoding.GetBytes(text)];
            Assert.True(expected.Length > 2 * 64 * 1024, "precondition: the encoded text spans several logical update units");

            DfsStream s = await vol.Files.CreateAsync(path, Ct);
            // StreamWriter clamps tiny buffers to its 128-char minimum: encoded flushes into the DfsStream happen every
            // ~128 chars, so multibyte sequences and surrogate pairs regularly straddle DfsStream writes.
            var writer = new StreamWriter(s, encoding, writerBufferChars, leaveOpen: handOff == WriterHandOff.LeaveOpenThenCommit);
            int i = 0;
            for (; i < 3000; i++)
            {
                if (useAsync)
                    await writer.WriteAsync(text[i]);
                else
                    writer.Write(text[i]);
            }

            int[] fragments = [7, 13, 1000, 1, 4097];
            for (int f = 0; i < text.Length; f++)
            {
                int n = Math.Min(fragments[f % fragments.Length], text.Length - i);
                if (useAsync)
                    await writer.WriteAsync(text.AsMemory(i, n), Ct);
                else
                    writer.Write(text.AsSpan(i, n));
                i += n;
            }

            Assert.True(await CommittedLengthAsync(ov, path) == 0,
                "Neither the writer's buffer nor the DfsStream's staging is published before the outer layer is flushed.");

            switch (handOff)
            {
                case WriterHandOff.OuterFlush:
                    if (useAsync)
                        await writer.FlushAsync(Ct);
                    else
                        writer.Flush();
                    BytesEqual(expected, await ReadCommittedAsync(ov, path), "the outer Flush hands every byte to the DfsStream and flushes it");
                    Assert.True(s.CanWrite, "an outer Flush does not close the DfsStream");
                    if (useAsync)
                        await writer.DisposeAsync();
                    else
                        writer.Dispose();
                    Assert.False(s.CanWrite, "disposing the writer disposes the DfsStream (leaveOpen: false)");
                    break;

                case WriterHandOff.DisposeWriterClosesStream:
                    if (useAsync)
                        await writer.DisposeAsync();
                    else
                        writer.Dispose();
                    Assert.False(s.CanWrite, "disposing the writer disposes the DfsStream (leaveOpen: false)");
                    break;

                case WriterHandOff.LeaveOpenThenCommit:
                {
                    if (useAsync)
                        await writer.DisposeAsync();
                    else
                        writer.Dispose();
                    Assert.True(s.CanWrite, "leaveOpen: true keeps the DfsStream open after the writer is disposed");
                    Assert.Equal(expected.Length, s.Position);
                    const string trailerText = "\n-- trailer written directly to the DfsStream --";
                    byte[] trailer = Encoding.UTF8.GetBytes(trailerText);
                    await s.WriteAsync(trailer, Ct);
                    Assert.False((await s.CommitAsync(Ct)).IsNone, "CommitAsync publishes and returns a commit token");
                    await s.DisposeAsync();
                    expected = [.. expected, .. trailer];
                    text += trailerText;
                    break;
                }
            }

            BytesEqual(expected, await ReadCommittedAsync(ov, path), "exact UTF-8 bytes; no split character was corrupted");

            await using (DfsStream r = await ov.Files.OpenReadAsync(path, Ct))
            {
                // A 1-char request is clamped to StreamReader's 128-byte minimum: multibyte characters straddle DfsStream reads.
                using (var reader = new StreamReader(r, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: true, bufferSize: 1, leaveOpen: true))
                {
                    string back = useAsync ? await reader.ReadToEndAsync(Ct) : reader.ReadToEnd();
                    Assert.True(string.Equals(text, back, StringComparison.Ordinal),
                        $"StreamReader text differs from the written text (lengths {text.Length} vs {back.Length}).");
                }

                Assert.True(r.CanRead, "leaveOpen: true keeps the DfsStream open after the reader is disposed");
                r.Seek(0, SeekOrigin.Begin);
                using var lines = new StreamReader(r, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: true, bufferSize: 1, leaveOpen: true);
                using var oracle = new StringReader(text);
                for (int line = 0; ; line++)
                {
                    string? expectedLine = oracle.ReadLine();
                    string? actualLine = useAsync ? await lines.ReadLineAsync(Ct) : lines.ReadLine();
                    Assert.True(string.Equals(expectedLine, actualLine, StringComparison.Ordinal), $"line {line} differs");
                    if (expectedLine is null)
                        break;
                }
            }

            Assert.True(string.Equals(text, await ov.Files.ReadAllTextAsync(path, null, Ct), StringComparison.Ordinal),
                "ReadAllTextAsync agrees with the stream (and recognizes the BOM when present)");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-022

    /// <summary>
    /// A BufferedStream over the DfsStream: while bytes sit only in the outer buffer nothing reaches or is published by the
    /// DfsStream; when the outer buffer spills, the committed state equals exactly what the DfsStream held at its last
    /// forwarded flush (never more); a proper outer Flush publishes everything. Mixed write/seek/read through the same
    /// BufferedStream sees the writer's own staging, disposal publishes, and buffered reads/seeks return exact bytes.
    /// </summary>
    [Theory]
    [InlineData(512, false)]
    [InlineData(4096, true)]
    [InlineData(100_000, false)]
    [InlineData(100_000, true)]
    [Catalog("STR-022", "I", "INV-02", "INV-07")]
    public async Task STR_022_BufferedStreamInterop(int bufferSize, bool useAsync)
    {
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str022.bin";
            byte[] data = TestData.Bytes(0x5022, 300_000);

            DfsStream s = await vol.Files.CreateAsync(path, Ct);
            var observing = new FlushObservingStream(s);
            var buffered = new BufferedStream(observing, bufferSize);

            // Phase 1: less than one outer buffer — nothing reaches the DfsStream.
            int written = 0;
            int phase1 = Math.Min(bufferSize - 1, 300);
            for (; written < 10; written++)
                buffered.WriteByte(data[written]);
            buffered.Write(data, written, phase1 - written);
            written = phase1;
            Assert.Equal(0, s.Position);
            Assert.Equal(0, s.PendingBytes);
            Assert.Equal(0, await CommittedLengthAsync(ov, path));

            // Phase 2: the rest in mixed fragments; outer buffers spill into the DfsStream.
            int[] fragments = [1, 7, 100, 3000, 70_000, 5];
            for (int f = 0; written < data.Length; f++)
            {
                int n = Math.Min(fragments[f % fragments.Length], data.Length - written);
                if (n == 1)
                    buffered.WriteByte(data[written]);
                else if (useAsync)
                    await buffered.WriteAsync(data.AsMemory(written, n), Ct);
                else
                    buffered.Write(data.AsSpan(written, n));
                written += n;

                Assert.True(s.Position <= written, "the DfsStream never holds more than was written to the outer layer");
                long committed = await CommittedLengthAsync(ov, path);
                long publishedAtFlush = observing.Flushes == 0 ? 0 : observing.LengthAtLastFlush;
                Assert.True(committed == publishedAtFlush,
                    $"after {written} bytes: committed {committed}, but the DfsStream was last flushed at {publishedAtFlush} ({observing.Flushes} forwarded flushes) — buffered bytes must not be published.");
            }

            if (useAsync)
                await buffered.FlushAsync(Ct);
            else
                buffered.Flush();
            Assert.True(observing.Flushes >= 1, "the outer Flush forwards a flush to the DfsStream");
            BytesEqual(data, await ReadCommittedAsync(ov, path), "a proper outer Flush publishes every byte");
            Assert.Equal(0, s.PendingBytes);

            // Mixed write / seek / read through the same BufferedStream on the read-write DfsStream.
            var model = new ReferenceFileModel(data);
            buffered.Seek(1000, SeekOrigin.Begin);
            byte[] patch = TestData.Bytes(0x5022_0001, 500);
            buffered.Write(patch);
            model.Write(1000, patch);
            int flushesBeforeRead = observing.Flushes;
            buffered.Seek(900, SeekOrigin.Begin);
            var got = new byte[700];
            if (useAsync)
                await buffered.ReadExactlyAsync(got, Ct);
            else
                buffered.ReadExactly(got);
            BytesEqual(model.Read(900, 700, staged: true), got, "reads after an outer write see the writer's own staging");
            buffered.Seek(-1, SeekOrigin.End);
            Assert.Equal(data[^1], buffered.ReadByte());
            Assert.Equal(-1, buffered.ReadByte());
            byte[] committedNow = await ReadCommittedAsync(ov, path);
            byte[] expectedNow = observing.Flushes > flushesBeforeRead ? model.Staged : model.Committed;
            BytesEqual(expectedNow, committedNow, "the committed state matches exactly the flushes the outer layer forwarded");

            if (useAsync)
                await buffered.DisposeAsync();
            else
                buffered.Dispose();
            Assert.False(s.CanWrite, "disposing the BufferedStream disposes the DfsStream");
            model.Flush();
            BytesEqual(model.Committed, await ReadCommittedAsync(ov, path), "disposal published the remaining changes");

            // Read side: BufferedStream over a read-only DfsStream.
            byte[] expected = model.Committed;
            DfsStream r = await ov.Files.OpenReadAsync(path, Ct);
            var br = new BufferedStream(r, bufferSize);
            using (var all = new MemoryStream())
            {
                int[] readSizes = [1, 333, 65_536, 7, 100_001];
                var buffer = new byte[100_001];
                for (int k = 0; ; k++)
                {
                    int want = readSizes[k % readSizes.Length];
                    int n;
                    if (want == 1)
                    {
                        int b = br.ReadByte();
                        n = b < 0 ? 0 : 1;
                        if (n == 1)
                            buffer[0] = (byte)b;
                    }
                    else
                    {
                        n = useAsync ? await br.ReadAsync(buffer.AsMemory(0, want), Ct) : br.Read(buffer.AsSpan(0, want));
                    }

                    if (n == 0)
                        break;
                    all.Write(buffer, 0, n);
                }

                BytesEqual(expected, all.ToArray(), "sequential buffered reads");
            }

            foreach (long p in new long[] { 0, 4095, 65_536, 299_000, 150_001, 900 })
            {
                br.Seek(p, SeekOrigin.Begin);
                var window = new byte[64];
                br.ReadExactly(window);
                BytesEqual(expected.AsMemory((int)p, 64), window, $"buffered random read at {p}");
                Assert.Equal(p + 64, br.Position);
            }

            await br.DisposeAsync();
            Assert.False(r.CanRead, "disposing the BufferedStream disposes the read-only DfsStream");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-023

    public static TheoryData<CompressionKind, bool> CompressionCases()
    {
        var data = new TheoryData<CompressionKind, bool>();
        foreach (CompressionKind kind in Enum.GetValues<CompressionKind>())
        {
            data.Add(kind, false);
            data.Add(kind, true);
        }

        return data;
    }

    private static Stream Compressor(CompressionKind kind, Stream target, bool leaveOpen) => kind switch
    {
        CompressionKind.GZip => new GZipStream(target, CompressionLevel.Optimal, leaveOpen),
        CompressionKind.Deflate => new DeflateStream(target, CompressionLevel.Optimal, leaveOpen),
        CompressionKind.ZLib => new ZLibStream(target, CompressionLevel.Optimal, leaveOpen),
        CompressionKind.Brotli => new BrotliStream(target, CompressionLevel.Optimal, leaveOpen),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
    };

    private static Stream Decompressor(CompressionKind kind, Stream source, bool leaveOpen) => kind switch
    {
        CompressionKind.GZip => new GZipStream(source, CompressionMode.Decompress, leaveOpen),
        CompressionKind.Deflate => new DeflateStream(source, CompressionMode.Decompress, leaveOpen),
        CompressionKind.ZLib => new ZLibStream(source, CompressionMode.Decompress, leaveOpen),
        CompressionKind.Brotli => new BrotliStream(source, CompressionMode.Decompress, leaveOpen),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
    };

    private static async Task WriteFragmentsAsync(Stream target, byte[] payload, int[] fragments, bool useAsync)
    {
        int written = 0;
        for (int f = 0; written < payload.Length; f++)
        {
            int n = Math.Min(fragments[f % fragments.Length], payload.Length - written);
            if (useAsync)
                await target.WriteAsync(payload.AsMemory(written, n), Ct);
            else
                target.Write(payload, written, n);
            written += n;
        }
    }

    /// <summary>
    /// GZip/Deflate/ZLib/Brotli round trip through DfsStream: nothing is published before the compressor is finalized;
    /// disposal writes the finalization bytes and the committed file equals the in-memory reference compression exactly;
    /// leaveOpen: true keeps the DfsStream usable (append, commit, truncate) while leaveOpen: false disposes it; decompression
    /// through a DfsStream returns the original payload and honours leaveOpen on the source.
    /// </summary>
    [Theory]
    [MemberData(nameof(CompressionCases))]
    [Catalog("STR-023", "I", "INV-02", "INV-07")]
    public async Task STR_023_CompressionStreamInterop(CompressionKind kind, bool leaveOpen)
    {
        bool useAsync = kind is CompressionKind.Deflate or CompressionKind.Brotli;
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            string path = $"/str023-{kind}.bin";
            byte[] payload = [.. TestData.Compressible(200_000), .. TestData.Bytes(0x5023, 300_000), .. Encoding.UTF8.GetBytes(TestData.Utf8Text(0x5023, 20_000))];
            int[] fragments = [1, 4096, 77_777, 13, 65_536];

            byte[] reference;
            using (var ms = new MemoryStream())
            {
                Stream c = Compressor(kind, ms, leaveOpen: true);
                await WriteFragmentsAsync(c, payload, fragments, useAsync);
                if (useAsync)
                    await c.DisposeAsync();
                else
                    c.Dispose();
                reference = ms.ToArray();
            }

            DfsStream s = await vol.Files.CreateAsync(path, Ct);
            Stream compressor = Compressor(kind, s, leaveOpen);
            await WriteFragmentsAsync(compressor, payload, fragments, useAsync);
            Assert.True(await CommittedLengthAsync(ov, path) == 0, "nothing is published before the compressor is finalized and the stream flushed");
            if (useAsync)
                await compressor.DisposeAsync(); // writes the finalization bytes
            else
                compressor.Dispose();

            if (leaveOpen)
            {
                Assert.True(s.CanWrite, "leaveOpen: true keeps the DfsStream open after the compressor is disposed");
                Assert.Equal(reference.Length, s.Position);
                byte[] trailer = Encoding.ASCII.GetBytes("TRAILER");
                await s.WriteAsync(trailer, Ct);
                Assert.False((await s.CommitAsync(Ct)).IsNone);
                BytesEqual(reference.Concat(trailer).ToArray(), await ReadCommittedAsync(ov, path), "the open DfsStream accepted and published more data");
                await s.SetLengthAsync(reference.Length, Ct);
                await s.DisposeAsync();
            }
            else
            {
                Assert.False(s.CanWrite || s.CanRead, "leaveOpen: false disposes (and thereby publishes) the DfsStream with the compressor");
            }

            byte[] committed = await ReadCommittedAsync(ov, path);
            BytesEqual(reference, committed, $"{kind}: committed bytes equal the reference compression, including finalization bytes");
            if (kind == CompressionKind.GZip)
            {
                Assert.True(committed[0] == 0x1F && committed[1] == 0x8B, "GZip header");
                Assert.Equal(payload.Length, BinaryPrimitives.ReadInt32LittleEndian(committed.AsSpan(committed.Length - 4)));
            }

            await using (DfsStream r = await ov.Files.OpenReadAsync(path, Ct))
            {
                using var output = new MemoryStream();
                Stream decompressor = Decompressor(kind, r, leaveOpen: true);
                if (useAsync)
                    await decompressor.CopyToAsync(output, Ct);
                else
                    decompressor.CopyTo(output);
                decompressor.Dispose();
                Assert.True(r.CanRead, "leaveOpen: true keeps the source DfsStream open after decompression");
                BytesEqual(payload, output.ToArray(), $"{kind}: round trip");
            }

            DfsStream r2 = await ov.Files.OpenReadAsync(path, Ct);
            using (Stream d2 = Decompressor(kind, r2, leaveOpen: false))
                d2.CopyTo(Stream.Null);
            Assert.False(r2.CanRead, "leaveOpen: false disposes the source DfsStream with the decompressor");
        }, Ct);
    }

    // ------------------------------------------------------------------ STR-024

    private static readonly DateTimeOffset Stamp = new(2026, 1, 2, 3, 4, 6, TimeSpan.Zero);

    private static (string Name, byte[] Data, CompressionLevel Level)[] ArchiveEntries() =>
    [
        ("empty.txt", [], CompressionLevel.Optimal),
        ("small.bin", TestData.Bytes(0x5024_0001, 100), CompressionLevel.Optimal),
        ("text/readme.md", Encoding.UTF8.GetBytes(TestData.Utf8Text(0x5024_0002, 5000)), CompressionLevel.Optimal),
        ("big.bin", TestData.Bytes(0x5024_0003, 300_000), CompressionLevel.NoCompression),
        ("zeros.bin", TestData.Compressible(200_000, 0), CompressionLevel.SmallestSize),
    ];

    private static readonly byte[] UpdatedSmall = TestData.Bytes(0x5024_0010, 777);
    private static readonly byte[] AddedEntry = TestData.Bytes(0x5024_0011, 70_000);

    private static void FillArchive(ZipArchive zip)
    {
        foreach ((string name, byte[] data, CompressionLevel level) in ArchiveEntries())
        {
            ZipArchiveEntry entry = zip.CreateEntry(name, level);
            entry.LastWriteTime = Stamp;
            using Stream es = entry.Open();
            es.Write(data);
        }
    }

    private static void UpdateArchive(ZipArchive zip)
    {
        zip.GetEntry("big.bin")!.Delete();
        ZipArchiveEntry small = zip.GetEntry("small.bin")!;
        using (Stream es = small.Open())
        {
            es.SetLength(0);
            es.Write(UpdatedSmall);
        }

        small.LastWriteTime = Stamp;
        ZipArchiveEntry added = zip.CreateEntry("added/new.bin", CompressionLevel.Optimal);
        using (Stream es = added.Open())
            es.Write(AddedEntry);
        added.LastWriteTime = Stamp;
    }

    private static (string Name, byte[] Data)[] UpdatedEntries() =>
        ArchiveEntries()
            .Where(e => e.Name != "big.bin")
            .Select(e => (e.Name, e.Name == "small.bin" ? UpdatedSmall : e.Data))
            .Append(("added/new.bin", AddedEntry))
            .ToArray();

    private static async Task VerifyArchiveAsync(DfsVolume volume, string path, (string Name, byte[] Data)[] expected, string because)
    {
        await using DfsStream r = await volume.Files.OpenReadAsync(path, Ct);
        using var zip = new ZipArchive(r, ZipArchiveMode.Read, leaveOpen: true); // reads the central directory at the end
        Assert.Equal(expected.Select(e => e.Name).ToArray(), zip.Entries.Select(e => e.FullName).ToArray());
        foreach ((string name, byte[] data) in expected.Reverse()) // backward seeks to earlier local headers
        {
            ZipArchiveEntry entry = zip.GetEntry(name)!;
            Assert.Equal(data.Length, entry.Length);
            using Stream es = entry.Open();
            using var ms = new MemoryStream();
            es.CopyTo(ms); // the entry stream validates the stored CRC-32 at its end
            BytesEqual(data, ms.ToArray(), $"{because}: entry {name}");
        }
    }

    /// <summary>
    /// ZipArchive as the seek-requiring consumer: Create on a seekable DfsStream (backward seeks patch local headers, the
    /// central directory is written last), Update in place (reads the directory, rewrites, shrinks the stream via
    /// SetLength) and Create in streaming mode on a non-seekable append stream. The committed bytes equal a reference
    /// archive built in memory with the same calls, and reading back through a DfsStream returns every entry exactly.
    /// </summary>
    [Theory]
    [InlineData(ArchiveScenario.CreateOnSeekableStream)]
    [InlineData(ArchiveScenario.UpdateInPlace)]
    [InlineData(ArchiveScenario.CreateOnAppendStreaming)]
    [Catalog("STR-024", "I", "INV-02", "INV-07")]
    public async Task STR_024_SeekRequiringConsumerInterop(ArchiveScenario scenario)
    {
        await ClusterTest.RunAsync(RealBoundaries(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observer = await ConnectAsync(cluster, "o");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume ov = observer.GetVolume(Vol);
            const string path = "/str024.zip";
            (string Name, byte[] Data)[] created = ArchiveEntries().Select(e => (e.Name, e.Data)).ToArray();

            byte[] seekableReference;
            using (var ms = new MemoryStream())
            {
                using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
                    FillArchive(zip);
                seekableReference = ms.ToArray();
            }

            if (scenario is ArchiveScenario.CreateOnSeekableStream or ArchiveScenario.UpdateInPlace)
            {
                DfsStream s = await vol.Files.CreateAsync(path, Ct);
                Assert.True(s.CanSeek && s.CanRead && s.CanWrite, "Create returns a seekable read/write stream");
                using (var zip = new ZipArchive(s, ZipArchiveMode.Create, leaveOpen: false))
                    FillArchive(zip);
                Assert.False(s.CanWrite, "disposing the archive disposed (and published) the DfsStream");
                BytesEqual(seekableReference, await ReadCommittedAsync(ov, path), "the archive written through backward seeks equals the reference");
                await VerifyArchiveAsync(ov, path, created, "created archive");
            }

            if (scenario == ArchiveScenario.UpdateInPlace)
            {
                byte[] updatedReference;
                using (var ms = new MemoryStream())
                {
                    ms.Write(seekableReference);
                    ms.Position = 0;
                    using (var zip = new ZipArchive(ms, ZipArchiveMode.Update, leaveOpen: true))
                        UpdateArchive(zip);
                    updatedReference = ms.ToArray();
                }

                Assert.True(updatedReference.Length < seekableReference.Length, "precondition: the update shrinks the archive");
                DfsStream u = await vol.Files.OpenAsync(path,
                    new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.None }, Ct);
                using (var zip = new ZipArchive(u, ZipArchiveMode.Update, leaveOpen: false))
                    UpdateArchive(zip);
                Assert.False(u.CanWrite, "disposing the archive disposed (and published) the DfsStream");
                byte[] committed = await ReadCommittedAsync(ov, path);
                BytesEqual(updatedReference, committed, "the in-place update (rewrite + SetLength shrink) equals the reference");
                Assert.Equal(updatedReference.Length, (await ov.Files.GetInfoAsync(path, Ct)).Length);
                await VerifyArchiveAsync(ov, path, UpdatedEntries(), "updated archive");
            }

            if (scenario == ArchiveScenario.CreateOnAppendStreaming)
            {
                byte[] streamingReference;
                using (var ms = new MemoryStream())
                {
                    using (var zip = new ZipArchive(new NonSeekableStream(ms), ZipArchiveMode.Create, leaveOpen: true))
                        FillArchive(zip);
                    streamingReference = ms.ToArray();
                }

                DfsStream a = await vol.Files.OpenAsync(path,
                    new DfsOpenOptions { Mode = FileMode.Append, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
                Assert.False(a.CanSeek, "append streams are not seekable");
                Assert.Throws<ArgumentException>(() => new ZipArchive(a, ZipArchiveMode.Update, leaveOpen: true));
                Assert.True(a.CanWrite, "the rejected Update-mode consumer did not close the stream");
                using (var zip = new ZipArchive(a, ZipArchiveMode.Create, leaveOpen: false))
                    FillArchive(zip); // streaming mode: data descriptors instead of backward seeks
                Assert.False(a.CanWrite, "disposing the archive disposed (and published) the DfsStream");
                BytesEqual(streamingReference, await ReadCommittedAsync(ov, path), "the streaming-mode archive equals the reference");
                await VerifyArchiveAsync(ov, path, created, "streaming archive");
            }
        }, Ct);
    }
}
