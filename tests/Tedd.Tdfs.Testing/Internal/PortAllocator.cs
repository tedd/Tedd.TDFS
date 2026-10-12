using System.Net;
using System.Net.Sockets;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Hands out loopback UDP ports for QUIC endpoints that must be known before a node starts (static bootstrap descriptions
/// list every member's endpoint). A port is probed by binding it, and never handed out twice by this process.
/// </summary>
internal static class PortAllocator
{
    private static readonly object Gate = new();
    private static readonly HashSet<int> Issued = [];

    /// <summary>Returns a currently free loopback UDP port that this process has not handed out before.</summary>
    public static int Allocate()
    {
        for (int attempt = 0; attempt < 256; attempt++)
        {
            int port;
            using (var probe = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0)))
                port = ((IPEndPoint)probe.Client.LocalEndPoint!).Port;
            lock (Gate)
            {
                if (Issued.Add(port))
                    return port;
            }
        }

        throw new InvalidOperationException("No free loopback UDP port could be allocated.");
    }
}
