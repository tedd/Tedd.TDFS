using System.Threading.Channels;

namespace Tedd.Tdfs.Host;

/// <summary>
/// Process control of a running host: Ctrl+C requests a graceful stop; the lines <c>stop</c>, <c>pause</c> and
/// <c>resume</c> on standard input are commands (used by launchers such as the test harness). The end of standard input
/// stops the host only when the launcher asked for it (<c>--control stdin</c>), so an orphaned host never outlives its
/// launcher; a host run as a service with no (or a closed) standard input keeps running.
/// </summary>
internal sealed class HostControl : IDisposable
{
    private readonly CancellationTokenSource _stop = new();
    private readonly Channel<string> _commands = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Thread _reader;
    private readonly bool _stopAtEndOfInput;

    /// <summary>Starts listening for Ctrl+C and standard-input commands.</summary>
    /// <param name="stopAtEndOfInput">True when the end of standard input requests a graceful stop (launcher-controlled hosts).</param>
    public HostControl(bool stopAtEndOfInput)
    {
        _stopAtEndOfInput = stopAtEndOfInput;
        Console.CancelKeyPress += OnCancelKeyPress;
        _reader = new Thread(ReadCommands) { IsBackground = true, Name = "tdfs-host-stdin" };
        _reader.Start();
    }

    /// <summary>Cancelled once a stop was requested (also cancels a start in progress).</summary>
    public CancellationToken Stopping => _stop.Token;

    /// <summary>Executes commands until a stop is requested.</summary>
    public async Task RunAsync(Action pause, Action resume)
    {
        try
        {
            await foreach (string command in _commands.Reader.ReadAllAsync(_stop.Token))
            {
                switch (command)
                {
                    case "stop":
                        _stop.Cancel();
                        return;
                    case "pause":
                        pause();
                        Console.Out.WriteLine("TDFS-PAUSED");
                        Console.Out.Flush();
                        break;
                    case "resume":
                        resume();
                        Console.Out.WriteLine("TDFS-RESUMED");
                        Console.Out.Flush();
                        break;
                    default:
                        Console.Error.WriteLine($"Unknown command '{command}' (expected stop, pause or resume).");
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            // Ctrl+C or end of input.
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Console.CancelKeyPress -= OnCancelKeyPress;
        _stop.Dispose();
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true; // stop gracefully instead of terminating
        RequestStop();
    }

    private void ReadCommands()
    {
        try
        {
            while (Console.In.ReadLine() is { } line)
            {
                string command = line.Trim().ToLowerInvariant();
                if (command.Length > 0)
                    _commands.Writer.TryWrite(command);
                if (command == "stop")
                    return;
            }
        }
        catch (IOException)
        {
            // Input closed abnormally: treat like end of input.
        }

        if (_stopAtEndOfInput)
            RequestStop();
    }

    private void RequestStop()
    {
        _commands.Writer.TryComplete();
        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already shut down.
        }
    }
}
