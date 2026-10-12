using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>
/// Reference-counted request for 1 ms system timer resolution on Windows (<c>timeBeginPeriod(1)</c>) while QUIC
/// transports run, so the pump's millisecond waits are not rounded up to the default 15.6 ms tick. A no-op elsewhere.
/// </summary>
internal static partial class TimerResolution
{
    private const uint PeriodMilliseconds = 1;
    private static readonly object Gate = new();
    private static int s_users;
    private static bool s_active;

    /// <summary>Adds a user; the first one raises the resolution.</summary>
    public static void Acquire()
    {
        if (!OperatingSystem.IsWindows())
            return;
        lock (Gate)
        {
            if (s_users++ == 0)
                s_active = TryBegin();
        }
    }

    /// <summary>Removes a user; the last one restores the resolution.</summary>
    public static void Release()
    {
        if (!OperatingSystem.IsWindows())
            return;
        lock (Gate)
        {
            if (s_users == 0)
                return;
            if (--s_users == 0 && s_active)
            {
                s_active = false;
                TryEnd();
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static bool TryBegin()
    {
        try
        {
            return TimeBeginPeriod(PeriodMilliseconds) == 0;
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            return false;
        }
    }

    [SupportedOSPlatform("windows")]
    private static void TryEnd()
    {
        try
        {
            _ = TimeEndPeriod(PeriodMilliseconds);
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            // Nothing to restore.
        }
    }

    [SupportedOSPlatform("windows")]
    [LibraryImport("winmm.dll", EntryPoint = "timeBeginPeriod")]
    private static partial uint TimeBeginPeriod(uint period);

    [SupportedOSPlatform("windows")]
    [LibraryImport("winmm.dll", EntryPoint = "timeEndPeriod")]
    private static partial uint TimeEndPeriod(uint period);
}
