using System.Diagnostics;
using System.Text.Json;
using Tedd.Tdfs.Host;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// A running <c>Tedd.Tdfs.Host</c> process (out-of-process clusters): started with a configuration file, ready once it
/// printed its <c>TDFS-READY</c> line, controlled through standard input (<c>stop</c>, <c>pause</c>, <c>resume</c>) and
/// killed for crashes (the operating system keeps the page cache, so a kill loses nothing that was written).
/// </summary>
internal sealed class HostProcess : IAsyncDisposable
{
    private const int OutputTailLines = 200;

    private readonly Process _process;
    private readonly Queue<string> _output = new();
    private readonly TaskCompletionSource<HostReadyMessage> _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Action<string> _trace;

    private HostProcess(Process process, string name, Action<string> trace)
    {
        _process = process;
        Name = name;
        _trace = trace;
    }

    /// <summary>Node name.</summary>
    public string Name { get; }

    /// <summary>True until the process exited.</summary>
    public bool IsRunning => !_exited.Task.IsCompleted;

    /// <summary>Completes when the process exited.</summary>
    public Task Exited => _exited.Task;

    /// <summary>Starts a host process and waits for it to serve.</summary>
    /// <exception cref="InvalidOperationException">The process failed to start or exited before it served (the message carries its output).</exception>
    /// <exception cref="DfsException">The host reported a start failure with an error code (for example RecoveryKeyMissing).</exception>
    public static async Task<(HostProcess Process, HostReadyMessage Ready)> StartAsync(string hostDll, string command, string configPath, string name,
        TimeSpan timeout, Action<string> trace, CancellationToken ct)
    {
        var info = new ProcessStartInfo("dotnet")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(configPath)!,
        };
        info.ArgumentList.Add("exec");
        info.ArgumentList.Add(hostDll);
        info.ArgumentList.Add(command);
        info.ArgumentList.Add("--config");
        info.ArgumentList.Add(configPath);
        info.ArgumentList.Add("--log");
        info.ArgumentList.Add("warn");

        var process = new Process { StartInfo = info, EnableRaisingEvents = true };
        var host = new HostProcess(process, name, trace);
        process.OutputDataReceived += (_, e) => host.OnOutput(e.Data, isError: false);
        process.ErrorDataReceived += (_, e) => host.OnOutput(e.Data, isError: true);
        process.Exited += (_, _) => host.OnExited();
        if (!process.Start())
            throw new InvalidOperationException($"The host process for {name} could not be started.");
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        Task first = await Task.WhenAny(host._ready.Task, host._exited.Task, Task.Delay(timeout, ct));
        if (first == host._ready.Task)
            return (host, await host._ready.Task);
        host.Kill();
        if (host._ready.Task.Exception?.InnerException is DfsException reported)
            throw reported; // the host reported why it could not start
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(first == host._exited.Task
            ? $"The host process for {name} exited (code {SafeExitCode(process)}) before serving:{Environment.NewLine}{host.OutputTail()}"
            : $"The host process for {name} did not serve within {timeout}:{Environment.NewLine}{host.OutputTail()}");
    }

    /// <summary>Graceful stop: sends <c>stop</c> and waits for the exit; kills the process after <paramref name="timeout"/>.</summary>
    public async Task StopAsync(TimeSpan timeout)
    {
        if (!IsRunning)
            return;
        Send("stop");
        Task first = await Task.WhenAny(_exited.Task, Task.Delay(timeout));
        if (first != _exited.Task)
        {
            _trace($"host {Name}: graceful stop did not finish within {timeout}; killed");
            Kill();
        }
    }

    /// <summary>Freezes the node loop inside the process.</summary>
    public Task PauseAsync()
    {
        Send("pause");
        return Task.CompletedTask;
    }

    /// <summary>Unfreezes the node loop inside the process.</summary>
    public Task ResumeAsync()
    {
        Send("resume");
        return Task.CompletedTask;
    }

    /// <summary>Kills the process tree immediately (crash).</summary>
    public void Kill()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // Already gone.
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // Exiting concurrently.
        }
    }

    /// <summary>The last lines the process wrote (standard output and error).</summary>
    public string OutputTail()
    {
        lock (_output)
            return string.Join(Environment.NewLine, _output);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Kill();
        await Task.WhenAny(_exited.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        _process.Dispose();
    }

    private void Send(string command)
    {
        try
        {
            _process.StandardInput.WriteLine(command);
            _process.StandardInput.Flush();
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException)
        {
            // The process is gone; nothing to control.
        }
    }

    private void OnOutput(string? line, bool isError)
    {
        if (line is null)
            return;
        lock (_output)
        {
            _output.Enqueue((isError ? "err: " : "out: ") + line);
            while (_output.Count > OutputTailLines)
                _output.Dequeue();
        }

        if (isError)
            return;
        if (line.StartsWith(HostConfigurationJson.ReadyPrefix, StringComparison.Ordinal))
        {
            try
            {
                HostReadyMessage? ready = JsonSerializer.Deserialize<HostReadyMessage>(line[HostConfigurationJson.ReadyPrefix.Length..], HostConfigurationJson.Options);
                if (ready is not null)
                    _ready.TrySetResult(ready);
            }
            catch (JsonException ex)
            {
                _ready.TrySetException(new InvalidOperationException($"The host process for {Name} printed an unreadable ready line.", ex));
            }
        }
        else if (line.StartsWith(HostConfigurationJson.FailedPrefix, StringComparison.Ordinal))
        {
            HostFailedMessage? failed = null;
            try
            {
                failed = JsonSerializer.Deserialize<HostFailedMessage>(line[HostConfigurationJson.FailedPrefix.Length..], HostConfigurationJson.Options);
            }
            catch (JsonException)
            {
                // Reported generically below.
            }

            DfsErrorCode code = failed is not null && Enum.TryParse(failed.Code, out DfsErrorCode parsed) ? parsed : DfsErrorCode.Internal;
            _ready.TrySetException(DfsErrors.ToException(code, $"Host {Name} failed to start: {failed?.Type}: {failed?.Message}"));
        }
    }

    private void OnExited()
    {
        _exited.TrySetResult();
        _ready.TrySetException(new InvalidOperationException($"The host process for {Name} exited."));
    }

    private static string SafeExitCode(Process process)
    {
        try
        {
            return process.ExitCode.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (InvalidOperationException)
        {
            return "?";
        }
    }
}
