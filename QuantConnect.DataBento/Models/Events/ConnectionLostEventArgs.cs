/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2026 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

namespace QuantConnect.Lean.DataSource.DataBento.Models.Events;

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
