using System.Net;
using System.Net.Sockets;

namespace Tedd.Tdfs.Testing;

/// <summary>Direction of a datagram through a <see cref="DatagramRelay"/>.</summary>
internal enum RelayDirection
{
    /// <summary>From the connecting side (front) to the target endpoint (back).</summary>
    Forward,

    /// <summary>From the target endpoint back to the connecting side.</summary>
    Backward,
}

/// <summary>
/// A UDP relay between the sockets of one QUIC connection (<see cref="TestClusterOptions.QuicDatagramInterposer"/>): the
/// connecting transport talks to <see cref="FrontEndpoint"/>, the relay forwards every datagram to the target endpoint
/// from its own back socket and every answer back to the connecting socket, like a NAT. It sees only encrypted QUIC
/// packets; an armed tamper (<see cref="Arm"/>) alters the next data-bearing datagram of one direction <b>after</b>
/// encryption, so the receiver's QUIC packet protection (TLS 1.3 AEAD, packet-number duplicate detection) must reject it
/// (SEC-011). Harness code: runs on its own receive loops, never inside a simulation.
/// </summary>
internal sealed class DatagramRelay : IAsyncDisposable
{
    /// <summary>
    /// Smallest datagram treated as carrying application data: acknowledgement-only 1-RTT packets are smaller, so an armed
    /// tamper is not spent on them.
    /// </summary>
    internal const int MinDataDatagramBytes = 80;

    /// <summary>How long an armed tamper waits for a data-bearing datagram before it lapses (unconsumed).</summary>
    private static readonly TimeSpan ArmLifetime = TimeSpan.FromSeconds(10);

    private readonly object _gate = new();
    private readonly Socket _front;
    private readonly Socket _back;
    private readonly IPEndPoint _target;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<Armed> _armed = [];
    private readonly Action<string> _trace;
    private readonly Random _random;
    private readonly Task _forward;
    private readonly Task _backward;
    private EndPoint? _client;

    private sealed record Armed(RelayDirection Direction, MessageRule Rule, string Description, long ArmedAt);

    public DatagramRelay(IPEndPoint target, Action<string> trace, int seed)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(trace);
        _target = target;
        _trace = trace;
        _random = new Random(seed);
        _front = Bind(target.AddressFamily);
        _back = Bind(target.AddressFamily);
        var front = (IPEndPoint)_front.LocalEndPoint!;
        FrontEndpoint = "quic://" + (front.AddressFamily == AddressFamily.InterNetworkV6 ? "[" + front.Address + "]" : front.Address.ToString()) + ":"
            + front.Port.ToString(System.Globalization.CultureInfo.InvariantCulture);
        BackPort = ((IPEndPoint)_back.LocalEndPoint!).Port;
        _forward = Task.Run(() => PumpAsync(RelayDirection.Forward));
        _backward = Task.Run(() => PumpAsync(RelayDirection.Backward));
    }

    /// <summary>The endpoint the connecting transport dials instead of the target.</summary>
    public string FrontEndpoint { get; }

    /// <summary>Local port of the back socket: the address the target sees as its peer.</summary>
    public int BackPort { get; }

    /// <summary>
    /// Arms a tamper of the next data-bearing datagram (at least <see cref="MinDataDatagramBytes"/>) travelling in
    /// <paramref name="direction"/>: it consumes one application of <paramref name="rule"/> when it alters a datagram, so
    /// the rule's match count proves that an encrypted packet was really altered.
    /// </summary>
    public void Arm(RelayDirection direction, MessageRule rule, string description)
    {
        ArgumentNullException.ThrowIfNull(rule);
        lock (_gate)
            _armed.Add(new Armed(direction, rule, description, Environment.TickCount64));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_stop.IsCancellationRequested)
            return;
        await _stop.CancelAsync().ConfigureAwait(false);
        _front.Dispose();
        _back.Dispose();
        try
        {
            await Task.WhenAll(_forward, _backward).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
        {
            // Stopped.
        }

        _stop.Dispose();
    }

    private static Socket Bind(AddressFamily family)
    {
        var socket = new Socket(family, SocketType.Dgram, ProtocolType.Udp);
        if (OperatingSystem.IsWindows())
        {
            // An ICMP "port unreachable" (a peer that is not listening yet, or any more) must not fail the relay's receives.
            const int SioUdpConnReset = -1744830452;
            socket.IOControl(SioUdpConnReset, [0, 0, 0, 0], null);
        }

        socket.Bind(new IPEndPoint(family == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Loopback : IPAddress.Loopback, 0));
        return socket;
    }

    private async Task PumpAsync(RelayDirection direction)
    {
        Socket from = direction == RelayDirection.Forward ? _front : _back;
        byte[] buffer = new byte[65_536];
        EndPoint any = new IPEndPoint(_target.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
        CancellationToken ct = _stop.Token;
        while (!ct.IsCancellationRequested)
        {
            SocketReceiveFromResult received;
            try
            {
                received = await from.ReceiveFromAsync(buffer, SocketFlags.None, any, ct).ConfigureAwait(false);
            }
            catch (SocketException) when (!ct.IsCancellationRequested)
            {
                continue; // a transient receive error (e.g. a reset reported for an earlier send)
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                return;
            }

            EndPoint? destination;
            if (direction == RelayDirection.Forward)
            {
                Volatile.Write(ref _client, received.RemoteEndPoint);
                destination = _target;
            }
            else
            {
                destination = Volatile.Read(ref _client);
                if (destination is null)
                    continue;
            }

            byte[] datagram = buffer.AsSpan(0, received.ReceivedBytes).ToArray();
            Socket to = direction == RelayDirection.Forward ? _back : _front;
            foreach (byte[] packet in Tamper(direction, datagram))
            {
                try
                {
                    await to.SendToAsync(packet, SocketFlags.None, destination, ct).ConfigureAwait(false);
                }
                catch (SocketException) when (!ct.IsCancellationRequested)
                {
                    // The datagram is lost, as on a real network.
                }
                catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>The datagram(s) to send for <paramref name="datagram"/>: altered when an armed tamper takes it.</summary>
    private IEnumerable<byte[]> Tamper(RelayDirection direction, byte[] datagram)
    {
        Armed? taken = null;
        if (datagram.Length >= MinDataDatagramBytes)
        {
            lock (_gate)
            {
                long now = Environment.TickCount64;
                _armed.RemoveAll(a => !a.Rule.IsActive || now - a.ArmedAt > (long)ArmLifetime.TotalMilliseconds);
                foreach (Armed armed in _armed)
                {
                    if (armed.Direction == direction && armed.Rule.TryConsume())
                    {
                        taken = armed;
                        break;
                    }
                }

                if (taken is not null)
                    _armed.Remove(taken);
            }
        }

        if (taken is null)
            return [datagram];
        TransportTamper mode = taken.Rule.Tamper;
        _trace($"tampered an encrypted QUIC datagram ({datagram.Length} bytes, {direction}) carrying {taken.Description} ({mode}) below packet protection");
        switch (mode)
        {
            case TransportTamper.Replay:
                return [datagram, (byte[])datagram.Clone()];
            case TransportTamper.Truncate:
                return [datagram.AsSpan(0, Math.Max(1, datagram.Length / 2)).ToArray()];
            case TransportTamper.AppendGarbage:
            {
                byte[] longer = new byte[datagram.Length + 16];
                datagram.CopyTo(longer, 0);
                lock (_random)
                    _random.NextBytes(longer.AsSpan(datagram.Length));
                return [longer];
            }

            case TransportTamper.Scramble:
            {
                byte[] scrambled = (byte[])datagram.Clone();
                lock (_random)
                    _random.NextBytes(scrambled.AsSpan(1));
                return [scrambled];
            }

            default:
            {
                // One bit of the protected payload (past the short header's first bytes, inside the AEAD-covered region).
                byte[] flipped = (byte[])datagram.Clone();
                int index;
                lock (_random)
                    index = _random.Next(Math.Min(32, flipped.Length - 1), flipped.Length);
                flipped[index] ^= 0x10;
                return [flipped];
            }
        }
    }
}
