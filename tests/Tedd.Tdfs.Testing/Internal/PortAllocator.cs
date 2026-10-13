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

    /// <summary>
    /// Whether a QUIC listener can bind <paramref name="port"/>. The QUIC stack binds the port on the wildcard address (it
    /// filters by local address itself), so a port some other program holds on 0.0.0.0 or [::] is busy even though a plain
    /// non-exclusive bind of 127.0.0.1 succeeds next to it (Windows allows that, e.g. next to Steam's UDP 27036).
    /// </summary>
    private static bool IsFree(int port)
        => CanBind(IPAddress.Loopback, port, dualMode: false)
           && CanBind(IPAddress.Any, port, dualMode: false)
           && (!Socket.OSSupportsIPv6 || CanBind(IPAddress.IPv6Any, port, dualMode: true));

    private static bool CanBind(IPAddress address, int port, bool dualMode)
    {
        try
        {
            using var probe = new Socket(address.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            if (OperatingSystem.IsWindows())
                probe.ExclusiveAddressUse = true; // fail next to any other binding of the port instead of sharing it
            if (dualMode)
                probe.DualMode = true;
            probe.Bind(new IPEndPoint(address, port));
            return true;
        }
        catch (SocketException)
        {
            return false; // in use, or reserved by the operating system (an excluded port range)
        }
    }
}
