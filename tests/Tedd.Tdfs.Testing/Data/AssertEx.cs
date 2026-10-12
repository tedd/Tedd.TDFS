using System.Globalization;
using System.Text;

namespace Tedd.Tdfs.Testing;

/// <summary>Assertions specific to the filesystem's contracts.</summary>
public static class AssertEx
{
    /// <summary>
    /// Asserts that <paramref name="action"/> throws <typeparamref name="T"/> (exactly or a derived type) and, when
    /// <paramref name="code"/> is given and the exception is a <see cref="DfsException"/>, that its
    /// <see cref="DfsException.ErrorCode"/> equals <paramref name="code"/>. Returns the exception.
    /// </summary>
    public static async Task<T> ThrowsDfsAsync<T>(Func<Task> action, DfsErrorCode? code = null)
        where T : Exception
    {
        ArgumentNullException.ThrowIfNull(action);
        Exception? caught = null;
        try
        {
            Task task = action() ?? throw new InvalidOperationException("The action returned a null task.");
            await task;
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        if (caught is null)
        {
            Assert.Fail($"Expected {typeof(T).Name}{CodeText(code)} but no exception was thrown.");
            throw new InvalidOperationException("unreachable");
        }

        if (caught is not T typed)
        {
            Assert.Fail($"Expected {typeof(T).Name}{CodeText(code)} but got {Describe(caught)}");
            throw new InvalidOperationException("unreachable");
        }

        if (code is not null && caught is DfsException dfs && dfs.ErrorCode != code.Value)
            Assert.Fail($"Expected {typeof(T).Name} with ErrorCode {code.Value} but got ErrorCode {dfs.ErrorCode}: {Describe(caught)}");
        return typed;
    }

    /// <summary>Asserts byte-for-byte equality, reporting the length mismatch and the first differing offset.</summary>
    public static void BytesEqual(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual, string? because = null)
    {
        if (expected.SequenceEqual(actual))
            return;

        int common = expected.CommonPrefixLength(actual);
        var sb = new StringBuilder("Byte sequences differ");
        if (!string.IsNullOrEmpty(because))
            sb.Append(" (").Append(because).Append(')');
        sb.Append(": ");
        if (expected.Length != actual.Length)
            sb.Append(CultureInfo.InvariantCulture, $"expected length {expected.Length}, actual length {actual.Length}; ");
        if (common < expected.Length && common < actual.Length)
        {
            sb.Append(CultureInfo.InvariantCulture, $"first difference at offset {common} (expected 0x{expected[common]:X2}, actual 0x{actual[common]:X2}). ");
        }
        else
        {
            sb.Append(CultureInfo.InvariantCulture, $"the shorter sequence is a prefix of the longer; first difference at offset {common}. ");
        }

        int from = Math.Max(0, common - 8);
        sb.Append(CultureInfo.InvariantCulture, $"Expected[{from}..]: {Window(expected, from)}; actual[{from}..]: {Window(actual, from)}");
        Assert.Fail(sb.ToString());
    }

    /// <summary>
    /// Polls <paramref name="condition"/> (with short real-time delays) until it returns true; fails the test with
    /// <paramref name="because"/> when <paramref name="timeout"/> elapses. Exceptions from the condition propagate.
    /// </summary>
    public static async Task EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout, string because)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        TimeProvider time = TimeProvider.System;
        long start = time.GetTimestamp();
        TimeSpan delay = TimeSpan.FromMilliseconds(1);
        int attempts = 0;
        while (true)
        {
            attempts++;
            if (await condition())
                return;
            TimeSpan elapsed = time.GetElapsedTime(start);
            if (elapsed >= timeout)
                break;
            TimeSpan wait = delay < timeout - elapsed ? delay : timeout - elapsed;
            await Task.Delay(wait, time);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromMilliseconds(50).Ticks));
        }

        Assert.Fail($"Condition not met within {timeout} after {attempts} attempts: {because}");
    }

    private static string CodeText(DfsErrorCode? code) => code is null ? string.Empty : $" (ErrorCode {code.Value})";

    private static string Describe(Exception ex) =>
        $"{ex.GetType().FullName}{(ex is DfsException d ? $" (ErrorCode {d.ErrorCode})" : string.Empty)}: {ex.Message}{Environment.NewLine}{ex}";

    private static string Window(ReadOnlySpan<byte> data, int from)
    {
        if (from >= data.Length)
            return "(end)";
        int length = Math.Min(24, data.Length - from);
        string hex = Convert.ToHexString(data.Slice(from, length));
        return from + length < data.Length ? hex + "…" : hex;
    }
}
