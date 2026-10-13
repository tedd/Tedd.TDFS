using System.Net;
using System.Net.Sockets;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Hands out loopback UDP ports for QUIC endpoints that must be known before a node starts (static bootstrap descriptions
/// list every member's endpoint, and a restarted node binds the same port again). A port is probed by binding it, and never
/// handed out twice by this process.
/// </summary>
/// <remarks>
/// Ports come from a fixed range below every common ephemeral range (Windows 49152-65535, Linux 32768-60999): a port the
/// operating system picks for a connect-only transport (<c>quic://127.0.0.1:0</c>, every client) can then never be the
/// planned, not yet bound port of a node, which would make that node's start or restart fail with "address in use". Each
/// process starts at its own offset in the range so concurrent test processes rarely probe the same ports.
/// </remarks>
internal static class PortAllocator
{
    private const int RangeStart = 20000;
    private const int RangeEnd = 32000;

    private static readonly object Gate = new();
    private static readonly HashSet<int> Issued = [];
    private static int s_next = RangeStart + (int)((uint)Environment.ProcessId * 7919u % (RangeEnd - RangeStart));

    /// <summary>Returns a currently free loopback UDP port outside the ephemeral range that this process has not handed out before.</summary>
    public static int Allocate()
    {
        lock (Gate)
        {
            for (int attempt = 0; attempt < RangeEnd - RangeStart; attempt++)
            {
                int port = s_next;
                s_next = port + 1 >= RangeEnd ? RangeStart : port + 1;
                if (Issued.Contains(port) || !IsFree(port))
                    continue;
                Issued.Add(port);
                return port;
            }
        }

        throw new InvalidOperationException($"No free loopback UDP port could be allocated in {RangeStart}..{RangeEnd - 1}.");
    }

    /// <summary>
    /// Waits (harness time, at most <paramref name="timeout"/>) until the UDP port of a QUIC <paramref name="endpoint"/> can be
    /// bound. An in-process node that crashed releases its listener asynchronously (the dead incarnation's transport is
    /// disposed in the background), unlike a killed process whose port the operating system frees at once; a restart on the
    /// same endpoint waits for that instead of failing with "address in use". Other endpoints return at once.
    /// </summary>
    public static async Task WaitUntilFreeAsync(string endpoint, TimeSpan timeout, ClusterRuntime runtime)
    {
        if (!Transport.RpcAddress.TryParse(endpoint, out Transport.RpcAddress address, out _) || !address.IsQuic || address.Port == 0)
            return;
        long start = runtime.Time.GetTimestamp();
        while (!IsFree(address.Port) && runtime.Time.GetElapsedTime(start) < timeout)
            await runtime.DelayAsync(TimeSpan.FromMilliseconds(20));
    }

    private static bool IsFree(int port)
    {
        try
        {
            using var probe = new UdpClient(new IPEndPoint(IPAddress.Loopback, port));
            return true;
        }
        catch (SocketException)
        {
            return false; // in use, or reserved by the operating system (an excluded port range)
        }
    }
}
