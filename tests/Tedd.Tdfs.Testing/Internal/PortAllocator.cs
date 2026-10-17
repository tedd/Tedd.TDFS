using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Tedd.Tdfs.Transport;
using Tedd.Tdfs.Transport.Internal;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Hands out loopback UDP ports for QUIC endpoints that must be known before a node starts (static bootstrap descriptions
/// list every member's endpoint, and a restarted node binds the same port again). A port is probed by binding it, never
/// handed out twice by this process, and reserved machine-wide for the lifetime of the process, so that no other test
/// process takes it while its node is down between two incarnations.
/// </summary>
/// <remarks>
/// <para>
/// Ports come from a fixed range below every common ephemeral range (Windows 49152-65535, Linux 32768-60999): a port the
/// operating system picks for a connect-only transport (<c>quic://127.0.0.1:0</c>, every client) can then never be the
/// planned, not yet bound port of a node, which would make that node's start or restart fail with "address in use". Each
/// process starts at its own offset in the range so concurrent test processes rarely probe the same ports.
/// </para>
/// <para>
/// A free-port probe alone cannot protect a restart: while a node is stopped its port is free, and another test process
/// (a parallel suite, another worktree) walking the same part of the range would allocate it and keep it for the length of
/// its own test, so the restart fails with <c>QUIC_STATUS_ADDRESS_IN_USE</c> however long it waits. Every issued port is
/// therefore also reserved by a named system object (<see cref="ReservationPrefix"/> + port) that this process keeps open
/// until it exits (the operating system releases it after a crash too). Allocators of other processes skip reserved ports
/// without probing them, so they never bind the port of a running node either, not even for the instant of a probe.
/// </para>
/// </remarks>
internal static class PortAllocator
{
    /// <summary>
    /// How long a node start waits for an earlier transport on the same endpoint to release its MsQuic resources. The
    /// in-process QUIC transport closes its connections gracefully (bounded grace periods) and its registration close waits
    /// until every connection finished closing (three probe timeouts when the peer no longer answers): up to about three
    /// seconds on an idle machine, longer under load.
    /// </summary>
    public static readonly TimeSpan ReleaseTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Name prefix of the machine-wide port reservations (shared by every process using this allocator).</summary>
    internal const string ReservationPrefix = @"Global\Tedd.Tdfs.TestPort.";

    private const int RangeStart = 20000;
    private const int RangeEnd = 32000;

    /// <summary>A port wait shorter than this is not worth a trace line.</summary>
    private static readonly TimeSpan NotableWait = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How long a port may stay bound after every earlier transport on it released its MsQuic resources. MsQuic creates the
    /// connection for an arriving Initial packet on the UDP binding under its own internal (stateless) registration and
    /// hands it to the listener's registration only once the listener accepts the ClientHello. A handshake that is still
    /// half-open when the listener stops (a peer or client reconnecting while the node crashes, and then dying itself) is
    /// therefore owned by no registration of the transport: its registration close does not wait for it, and it keeps
    /// the binding, and the port, until MsQuic's default handshake idle timeout (10 s; the transport's own 5 s setting
    /// applies only to connections it configured) plus the closing period expire. Measured under four parallel test
    /// processes: 10.5-13.2 s after the crash.
    /// </summary>
    private static readonly TimeSpan SocketCloseGrace = TimeSpan.FromSeconds(20);

    private static readonly object Gate = new();
    private static readonly HashSet<int> Issued = [];
    private static readonly List<Mutex> Reservations = [];
    private static readonly Dictionary<int, List<Task>> Listeners = [];
    private static int s_next = RangeStart + (int)((uint)Environment.ProcessId * 7919u % (RangeEnd - RangeStart));

    /// <summary>
    /// Returns a currently free loopback UDP port outside the ephemeral range that neither this process nor another process
    /// using this allocator has handed out before.
    /// </summary>
    public static int Allocate()
    {
        lock (Gate)
        {
            for (int attempt = 0; attempt < RangeEnd - RangeStart; attempt++)
            {
                int port = s_next;
                s_next = port + 1 >= RangeEnd ? RangeStart : port + 1;
                if (Issued.Contains(port) || !TryReserve(port, out Mutex? reservation))
                    continue;
                if (!IsFree(port))
                {
                    reservation?.Dispose(); // bound by a program that does not use this allocator
                    continue;
                }

                if (reservation is not null)
                    Reservations.Add(reservation);
                Issued.Add(port);
                return port;
            }
        }

        throw new InvalidOperationException($"No free loopback UDP port could be allocated in {RangeStart}..{RangeEnd - 1}.");
    }

    /// <summary>
    /// Records <paramref name="transport"/>, created to listen on <paramref name="endpoint"/>, so that a later start on the
    /// same endpoint waits until its listener is released (<see cref="WaitUntilFreeAsync"/>). Transports other than QUIC
    /// and endpoints without a fixed port are ignored.
    /// </summary>
    public static void TrackListener(string endpoint, IRpcTransport transport)
    {
        if (transport is not QuiclyRpcTransport quic || !TryGetQuicPort(endpoint, out int port))
            return;
        lock (Gate)
        {
            if (!Listeners.TryGetValue(port, out List<Task>? released))
                Listeners[port] = released = [];
            released.RemoveAll(static t => t.IsCompleted);
            released.Add(quic.NativeResourcesReleased);
        }
    }

    /// <summary>
    /// Waits (harness time) until a listener can bind the UDP port of a QUIC <paramref name="endpoint"/>: first, at most
    /// <paramref name="timeout"/>, until every earlier in-process transport on that endpoint released its MsQuic listener
    /// and connections (a stopped or crashed incarnation's transport closes them in the background, unlike a killed process
    /// whose port the operating system frees at once), then until a probe can bind the port, which can take until a
    /// half-open handshake MsQuic accepted on the binding times out (<see cref="SocketCloseGrace"/>). Other endpoints return
    /// at once.
    /// </summary>
    /// <returns>
    /// A line for the cluster trace when the wait was notable: it took a while, or the port is still bound when the wait
    /// gives up (naming the processes bound to it). Otherwise null. The
    /// caller starts the listener either way, so a port that stayed bound fails the start with the transport's own error.
    /// </returns>
    public static async Task<string?> WaitUntilFreeAsync(string endpoint, TimeSpan timeout, ClusterRuntime runtime)
    {
        if (!TryGetQuicPort(endpoint, out int port))
            return null;
        long start = runtime.Time.GetTimestamp();
        Task[] earlier;
        lock (Gate)
            earlier = Listeners.TryGetValue(port, out List<Task>? pending) ? [.. pending.Where(static t => !t.IsCompleted)] : [];

        if (earlier.Length > 0 && !await runtime.WaitAsync(Task.WhenAll(earlier), timeout))
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"port {port}: the earlier transport on {endpoint} has not released its listener and connections after {runtime.Time.GetElapsedTime(start).TotalSeconds:0.0} s");
        }

        TimeSpan released = runtime.Time.GetElapsedTime(start);
        TimeSpan budget = earlier.Length > 0 ? SocketCloseGrace : timeout;
        while (!IsFree(port))
        {
            TimeSpan bound = runtime.Time.GetElapsedTime(start) - released;
            if (bound >= budget)
            {
                string owners = UdpPortOwners.Describe(port);
                return earlier.Length > 0
                    ? string.Create(CultureInfo.InvariantCulture, $"port {port}: still bound {bound.TotalSeconds:0.0} s after the earlier transport on {endpoint} released it; bound by {owners}")
                    : string.Create(CultureInfo.InvariantCulture, $"port {port}: still bound after {bound.TotalSeconds:0.0} s and no earlier transport of this process listens on {endpoint}; bound by {owners}");
            }

            await runtime.DelayAsync(TimeSpan.FromMilliseconds(20));
        }

        TimeSpan waited = runtime.Time.GetElapsedTime(start);
        return waited < NotableWait
            ? null
            : string.Create(CultureInfo.InvariantCulture, $"port {port}: free after {waited.TotalSeconds:0.0} s (earlier transport released after {released.TotalSeconds:0.0} s)");
    }

    private static bool TryGetQuicPort(string endpoint, out int port)
    {
        port = 0;
        if (!RpcAddress.TryParse(endpoint, out RpcAddress address, out _) || !address.IsQuic || address.Port == 0)
            return false;
        port = address.Port;
        return true;
    }

    /// <summary>
    /// Reserves <paramref name="port"/> machine-wide; false when another process (or this one) already reserved it. Where
    /// named system objects are unavailable no reservation is made (<paramref name="reservation"/> is null) and only the
    /// probe protects the port.
    /// </summary>
    private static bool TryReserve(int port, out Mutex? reservation)
    {
        reservation = null;
        Mutex mutex;
        bool createdNew;
        try
        {
            mutex = new Mutex(initiallyOwned: false, ReservationPrefix + port.ToString(CultureInfo.InvariantCulture), out createdNew);
        }
        catch (UnauthorizedAccessException)
        {
            return false; // reserved by a test process of another user
        }
        catch (Exception ex) when (ex is IOException or WaitHandleCannotBeOpenedException or PlatformNotSupportedException)
        {
            return true;
        }

        if (!createdNew)
        {
            mutex.Dispose();
            return false;
        }

        reservation = mutex;
        return true;
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
