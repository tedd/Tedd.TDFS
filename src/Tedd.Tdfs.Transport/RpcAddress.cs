using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace Tedd.Tdfs.Transport;

/// <summary>
/// An RPC endpoint address: <c>quic://host:port</c> (IPv6 hosts in brackets, port 0..65535; 0 means "any" for a
/// listener) or <c>mem://name</c> (in-memory network). For <c>mem</c> addresses <see cref="Host"/> and
/// <see cref="Name"/> are both the endpoint name and <see cref="Port"/> is 0. <see cref="ToString"/> round-trips
/// through <see cref="Parse"/>.
/// </summary>
/// <param name="Scheme">Lowercase scheme: <see cref="QuicScheme"/> or <see cref="MemoryScheme"/>.</param>
/// <param name="Host">Host name or IP address (without brackets); the endpoint name for <c>mem</c>.</param>
/// <param name="Port">UDP port for <c>quic</c>; 0 for <c>mem</c>.</param>
/// <param name="Name">Endpoint name for <c>mem</c>; null for <c>quic</c>.</param>
public readonly record struct RpcAddress(string Scheme, string Host, int Port, string? Name)
{
    /// <summary>Scheme of QUIC endpoints.</summary>
    public const string QuicScheme = "quic";

    /// <summary>Scheme of in-memory endpoints.</summary>
    public const string MemoryScheme = "mem";

    private const string SchemeSeparator = "://";

    /// <summary>True for <c>quic://</c> addresses.</summary>
    public bool IsQuic => string.Equals(Scheme, QuicScheme, StringComparison.Ordinal);

    /// <summary>True for <c>mem://</c> addresses.</summary>
    public bool IsMemory => string.Equals(Scheme, MemoryScheme, StringComparison.Ordinal);

    /// <summary>Creates a QUIC address.</summary>
    public static RpcAddress Quic(string host, int port)
    {
        string text = FormatQuic(host, port);
        return TryParse(text, out RpcAddress address, out string? error) ? address : throw new ArgumentException(error, nameof(host));
    }

    /// <summary>Creates an in-memory address.</summary>
    public static RpcAddress Memory(string name)
    {
        string text = MemoryScheme + SchemeSeparator + name;
        return TryParse(text, out RpcAddress address, out string? error) ? address : throw new ArgumentException(error, nameof(name));
    }

    /// <summary>Parses <c>quic://host:port</c> or <c>mem://name</c>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="endpoint"/> is null.</exception>
    /// <exception cref="FormatException">The endpoint is malformed or uses an unsupported scheme.</exception>
    public static RpcAddress Parse(string endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return TryParse(endpoint, out RpcAddress address, out string? error) ? address : throw new FormatException(error);
    }

    /// <summary>Tries to parse an endpoint.</summary>
    public static bool TryParse(string? endpoint, out RpcAddress address) => TryParse(endpoint, out address, out _);

    /// <summary>Tries to parse an endpoint, reporting why it is invalid.</summary>
    public static bool TryParse(string? endpoint, out RpcAddress address, out string? error)
    {
        address = default;
        if (string.IsNullOrEmpty(endpoint))
        {
            error = "The endpoint is empty.";
            return false;
        }

        int sep = endpoint.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        if (sep <= 0)
        {
            error = $"The endpoint '{endpoint}' has no scheme; expected 'quic://host:port' or 'mem://name'.";
            return false;
        }

        string scheme = endpoint[..sep].ToLowerInvariant();
        string rest = endpoint[(sep + SchemeSeparator.Length)..];
        switch (scheme)
        {
            case MemoryScheme:
                if (!IsValidMemoryName(rest))
                {
                    error = $"The in-memory endpoint '{endpoint}' has an invalid name.";
                    return false;
                }

                address = new RpcAddress(MemoryScheme, rest, 0, rest);
                error = null;
                return true;

            case QuicScheme:
                if (!TryParseHostPort(rest, out string? host, out int port, out error))
                {
                    error = $"The QUIC endpoint '{endpoint}' is invalid: {error}";
                    return false;
                }

                address = new RpcAddress(QuicScheme, host!, port, null);
                return true;

            default:
                error = $"The endpoint scheme '{scheme}' is not supported; expected 'quic' or 'mem'.";
                return false;
        }
    }

    /// <summary>Returns <c>quic://host:port</c> (IPv6 hosts bracketed) or <c>mem://name</c>.</summary>
    public override string ToString()
    {
        if (IsMemory)
            return MemoryScheme + SchemeSeparator + (Name ?? Host);
        if (IsQuic)
            return FormatQuic(Host, Port);
        return string.Concat(Scheme, SchemeSeparator, Host, ":", Port.ToString(CultureInfo.InvariantCulture));
    }

    private static string FormatQuic(string host, int port)
    {
        string h = host is not null && host.Contains(':', StringComparison.Ordinal) ? "[" + host + "]" : host ?? string.Empty;
        return string.Concat(QuicScheme, SchemeSeparator, h, ":", port.ToString(CultureInfo.InvariantCulture));
    }

    private static bool IsValidMemoryName(string name)
    {
        if (name.Length == 0)
            return false;
        foreach (char c in name)
        {
            if (char.IsControl(c) || char.IsWhiteSpace(c) || c is '/' or '\\' or '?' or '#')
                return false;
        }

        return true;
    }

    private static bool TryParseHostPort(string text, out string? host, out int port, out string? error)
    {
        host = null;
        port = 0;
        string hostPart;
        string portPart;
        if (text.StartsWith('['))
        {
            int close = text.IndexOf(']', StringComparison.Ordinal);
            if (close < 0 || close + 1 >= text.Length || text[close + 1] != ':')
            {
                error = "expected '[ipv6]:port'.";
                return false;
            }

            hostPart = text[1..close];
            portPart = text[(close + 2)..];
            if (!IPAddress.TryParse(hostPart, out IPAddress? ip) || ip.AddressFamily != AddressFamily.InterNetworkV6)
            {
                error = $"'{hostPart}' is not an IPv6 address.";
                return false;
            }
        }
        else
        {
            int colon = text.LastIndexOf(':');
            if (colon < 0)
            {
                error = "a port is required.";
                return false;
            }

            hostPart = text[..colon];
            portPart = text[(colon + 1)..];
            if (!IsValidHostName(hostPart))
            {
                error = $"'{hostPart}' is not a valid host name or IPv4 address (IPv6 addresses must be bracketed).";
                return false;
            }
        }

        if (portPart.Length is 0 or > 5 || !portPart.All(char.IsAsciiDigit) ||
            !int.TryParse(portPart, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port > 65535)
        {
            error = $"'{portPart}' is not a valid port (0-65535).";
            port = 0;
            return false;
        }

        host = hostPart;
        error = null;
        return true;
    }

    private static bool IsValidHostName(string host)
    {
        if (host.Length is 0 or > 253)
            return false;
        foreach (char c in host)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '-' or '.' or '_'))
                return false;
        }

        return true;
    }
}
