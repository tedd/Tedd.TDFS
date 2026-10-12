namespace Tedd.Tdfs.Transport;

/// <summary>Direction of a message on a link.</summary>
public enum LinkDirection
{
    /// <summary>A request expecting a reply.</summary>
    Request,
    /// <summary>A reply to a request.</summary>
    Reply,
    /// <summary>A one-way message.</summary>
    OneWay,
}
