using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Names the processes bound to a local UDP port (Windows: the IP helper's owner-PID UDP tables), for the trace line of a
/// node start whose port stayed bound. Elsewhere, and on any failure, it reports that the owner is unknown.
/// </summary>
internal static partial class UdpPortOwners
{
    private const int AfInet = 2;
    private const int AfInet6 = 23;
    private const int UdpTableOwnerPid = 1;

    /// <summary>A short description of the owners of <paramref name="port"/>, e.g. <c>"pid 1234 (dotnet, this process)"</c>.</summary>
    public static string Describe(int port)
    {
        if (!OperatingSystem.IsWindows())
            return "owner unknown";
        try
        {
            var pids = new SortedSet<int>();
            Collect(AfInet, port, pids);
            Collect(AfInet6, port, pids);
            if (pids.Count == 0)
                return "no owner listed";
            return string.Join(", ", pids.Select(Name));
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or InvalidOperationException)
        {
            return "owner unknown (" + ex.GetType().Name + ")";
        }
    }

    private static string Name(int pid)
    {
        string self = pid == Environment.ProcessId ? ", this process" : "";
        try
        {
            using Process process = Process.GetProcessById(pid);
            return string.Create(CultureInfo.InvariantCulture, $"pid {pid} ({process.ProcessName}{self})");
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            return string.Create(CultureInfo.InvariantCulture, $"pid {pid} (exited{self})");
        }
    }

    /// <summary>Adds the owning process ids of every <paramref name="family"/> UDP endpoint bound to <paramref name="port"/>.</summary>
    private static void Collect(int family, int port, SortedSet<int> pids)
    {
        int size = 0;
        _ = GetExtendedUdpTable(0, ref size, false, family, UdpTableOwnerPid, 0);
        for (int attempt = 0; attempt < 4; attempt++)
        {
            nint buffer = Marshal.AllocHGlobal(size);
            try
            {
                int result = GetExtendedUdpTable(buffer, ref size, false, family, UdpTableOwnerPid, 0);
                if (result == 122) // ERROR_INSUFFICIENT_BUFFER: the table grew meanwhile
                    continue;
                if (result != 0)
                    throw new InvalidOperationException("GetExtendedUdpTable failed with " + result.ToString(CultureInfo.InvariantCulture));

                // MIB_UDPTABLE_OWNER_PID: DWORD count, then rows of { DWORD addr; DWORD port; DWORD pid } (IPv4) or
                // MIB_UDP6TABLE_OWNER_PID rows of { BYTE addr[16]; DWORD scope; DWORD port; DWORD pid } (IPv6).
                int count = Marshal.ReadInt32(buffer);
                int rowSize = family == AfInet ? 12 : 28;
                int portOffset = family == AfInet ? 4 : 20;
                for (int i = 0; i < count; i++)
                {
                    nint row = buffer + 4 + i * rowSize;
                    int rowPort = BinaryPrimitives.ReverseEndianness((ushort)Marshal.ReadInt32(row + portOffset));
                    if (rowPort == port)
                        pids.Add(Marshal.ReadInt32(row + portOffset + 4));
                }

                return;
            }
            finally
            {
                Marshal.FreeHGlobal(buffer);
            }
        }
    }

    [LibraryImport("iphlpapi.dll")]
    private static partial int GetExtendedUdpTable(nint table, ref int size, [MarshalAs(UnmanagedType.Bool)] bool order, int family, int tableClass, int reserved);
}
