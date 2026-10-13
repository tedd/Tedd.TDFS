using System.Text;

namespace Tedd.Tdfs.Client.Internal.Text;

/// <summary>
/// Text rules of the whole-file helpers (FSD §5.3, FILE-004..007): UTF-8 without a BOM by default; an explicit
/// <see cref="Encoding"/> writes its preamble at the start of a file (never in the middle when appending); readers
/// recognise byte order marks (UTF-8, UTF-16 LE/BE, UTF-32 LE/BE) like <see cref="StreamReader"/> with BOM detection and
/// otherwise decode with the given encoding (UTF-8 by default). Lines end at <c>\n</c>, <c>\r</c> or <c>\r\n</c>; the
/// SDK writes <c>\n</c>.
/// </summary>
internal static class TextCodec
{
    /// <summary>UTF-8 without BOM, replacing invalid input (the default decoder).</summary>
    public static readonly Encoding DefaultEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);

    /// <summary>The terminator written after each line by <c>WriteAllLines</c>/<c>AppendAllLines</c>.</summary>
    public const string LineTerminator = "\n";

    /// <summary>
    /// Encodes <paramref name="text"/>; with <paramref name="includePreamble"/> the encoding's preamble is emitted first
    /// (the default encoding has none). Encoder failures surface as <see cref="ArgumentException"/> without the payload.
    /// </summary>
    public static byte[] Encode(string text, Encoding? encoding, bool includePreamble)
    {
        Encoding e = encoding ?? DefaultEncoding;
        ReadOnlySpan<byte> preamble = includePreamble ? e.Preamble : default;
        try
        {
            int count = e.GetByteCount(text);
            var bytes = new byte[preamble.Length + count];
            preamble.CopyTo(bytes);
            e.GetBytes(text, bytes.AsSpan(preamble.Length));
            return bytes;
        }
        catch (EncoderFallbackException ex)
        {
            throw new ArgumentException(
                $"The text cannot be encoded with {e.WebName}: a character at index {ex.Index} has no representation in that encoding.", "contents");
        }
    }

    /// <summary>Joins <paramref name="lines"/>, each followed by <see cref="LineTerminator"/>.</summary>
    public static string JoinLines(IEnumerable<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);
        var builder = new StringBuilder();
        foreach (string? line in lines)
        {
            builder.Append(line);
            builder.Append(LineTerminator);
        }

        return builder.ToString();
    }

    /// <summary>Decodes a complete file's bytes (BOM detection, then <paramref name="encoding"/> or UTF-8).</summary>
    public static string Decode(ReadOnlySpan<byte> bytes, Encoding? encoding)
    {
        (Encoding e, int skip) = Detect(bytes, encoding);
        return e.GetString(bytes[skip..]);
    }

    /// <summary>Splits decoded text into lines like <see cref="StreamReader.ReadLine"/>.</summary>
    public static string[] SplitLines(string text)
    {
        var lines = new List<string>();
        var reader = new LineSplitter();
        reader.Append(text.AsSpan(), lines);
        reader.Complete(lines);
        return [.. lines];
    }

    /// <summary>
    /// The encoding indicated by a byte order mark at the start of <paramref name="bytes"/> (and the mark's length), or
    /// <paramref name="fallback"/> (UTF-8 by default) without skipping anything.
    /// </summary>
    public static (Encoding Encoding, int Skip) Detect(ReadOnlySpan<byte> bytes, Encoding? fallback)
    {
        if (bytes.Length >= 4 && bytes[0] == 0xFF && bytes[1] == 0xFE && bytes[2] == 0 && bytes[3] == 0)
            return (new UTF32Encoding(bigEndian: false, byteOrderMark: false), 4);
        if (bytes.Length >= 4 && bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0xFE && bytes[3] == 0xFF)
            return (new UTF32Encoding(bigEndian: true, byteOrderMark: false), 4);
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            return (DefaultEncoding, 3);
        if (bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE)
            return (new UnicodeEncoding(bigEndian: false, byteOrderMark: false), 2);
        if (bytes.Length >= 2 && bytes[0] == 0xFE && bytes[1] == 0xFF)
            return (new UnicodeEncoding(bigEndian: true, byteOrderMark: false), 2);
        return (fallback ?? DefaultEncoding, 0);
    }

    /// <summary>The longest byte order mark (detection needs this many leading bytes, or the whole file when shorter).</summary>
    public const int MaxPreambleLength = 4;

    /// <summary>
    /// Incremental line splitting across decoded chunks: a <c>\r</c> at the end of a chunk is held until the next chunk
    /// shows whether a <c>\n</c> follows.
    /// </summary>
    internal sealed class LineSplitter
    {
        private readonly StringBuilder _current = new();
        private bool _pendingCr;
        private bool _hasContent;

        /// <summary>Appends decoded characters; complete lines are added to <paramref name="lines"/>.</summary>
        public void Append(ReadOnlySpan<char> chars, List<string> lines)
        {
            int start = 0;
            for (int i = 0; i < chars.Length; i++)
            {
                char c = chars[i];
                if (_pendingCr)
                {
                    _pendingCr = false;
                    if (c == '\n')
                    {
                        start = i + 1;
                        continue;
                    }
                }

                if (c is '\r' or '\n')
                {
                    _current.Append(chars[start..i]);
                    lines.Add(_current.ToString());
                    _current.Clear();
                    _hasContent = false;
                    _pendingCr = c == '\r';
                    start = i + 1;
                }
            }

            if (start < chars.Length)
            {
                _current.Append(chars[start..]);
                _hasContent = true;
            }
        }

        /// <summary>Flushes the final unterminated line (if any).</summary>
        public void Complete(List<string> lines)
        {
            if (_hasContent || _current.Length > 0)
                lines.Add(_current.ToString());
            _current.Clear();
            _hasContent = false;
            _pendingCr = false;
        }
    }
}
