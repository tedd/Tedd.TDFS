using System.Text;

namespace Tedd.Tdfs.Protocol;

/// <summary>UTF-8 helpers for the wire format.</summary>
internal static class WireText
{
    /// <summary>Strict UTF-8 encoding: throws on unpaired surrogates instead of substituting U+FFFD.</summary>
    public static readonly UTF8Encoding Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Replaces unpaired surrogates with U+FFFD and truncates to at most <paramref name="maxUtf8Bytes"/> UTF-8 bytes on a character boundary.</summary>
    public static string Sanitize(string? value, int maxUtf8Bytes)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        string text = value;
        if (DfsUnicode.IndexOfUnpairedSurrogate(text) >= 0)
        {
            var sb = new StringBuilder(text.Length);
            for (int i = 0; i < text.Length; i++)
            {
                char c = text[i];
                if (char.IsHighSurrogate(c) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
                {
                    sb.Append(c).Append(text[++i]);
                }
                else if (char.IsSurrogate(c))
                {
                    sb.Append('\uFFFD');
                }
                else
                {
                    sb.Append(c);
                }
            }

            text = sb.ToString();
        }

        if (Encoding.UTF8.GetByteCount(text) <= maxUtf8Bytes)
            return text;

        int bytes = 0;
        int end = 0;
        while (end < text.Length)
        {
            int width = char.IsHighSurrogate(text[end]) && end + 1 < text.Length ? 2 : 1;
            int size = Encoding.UTF8.GetByteCount(text.AsSpan(end, width));
            if (bytes + size > maxUtf8Bytes)
                break;
            bytes += size;
            end += width;
        }

        return text[..end];
    }
}
