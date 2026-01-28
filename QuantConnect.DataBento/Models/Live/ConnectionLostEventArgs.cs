namespace QuantConnect.Lean.DataSource.DataBento.Models.Live;

/// <summary>
/// Provides data for the event that is raised when a connection is lost.
/// </summary>
public class ConnectionLostEventArgs : EventArgs
{
    /// <summary>
    /// Gets the identifier of the data set or logical stream
    /// associated with the lost connection.
    /// </summary>
    public string DataSet { get; }

    /// <summary>
    /// Gets a human-readable description of the reason
    /// why the connection was lost.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectionLostEventArgs"/> class.
    /// </summary>
    /// <param name="message">
    /// The identifier of the data set or logical stream related to the connection.
    /// </param>
    /// <param name="reason">
    /// A human-readable explanation describing why the connection was lost.
    /// </param>
    public ConnectionLostEventArgs(string message, string reason)
    {
        DataSet = message;
        Reason = reason;
    }
}
