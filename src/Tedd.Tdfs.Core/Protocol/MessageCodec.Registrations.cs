using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Protocol;

public static partial class MessageCodec
{
    /// <summary>
    /// Registers every built-in message record. Add one <c>Register&lt;T&gt;()</c> line per record in
    /// <c>Protocol/Messages</c>; registration runs once, from the static constructor.
    /// </summary>
    private static void RegisterBuiltIns()
    {
        Register<ErrorReplyMessage>();
    }
}
