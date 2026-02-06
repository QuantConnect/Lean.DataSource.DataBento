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

namespace QuantConnect.Lean.DataSource.DataBento.Models.Enums;

/// <summary>
/// Bit field containing additional information about a market data message.
/// Multiple flags may be set simultaneously.
/// </summary>
[Flags]
public enum Flags : byte
{
    /// <summary>
    /// No flags set.
    /// </summary>
    None = 0,

    /// <summary>
    /// Reserved for internal use. Can safely be ignored.
    /// </summary>
    InternalReserved = 1 << 0, // 1

    /// <summary>
    /// Semantics depend on the publisher_id.
    /// </summary>
    PublisherSpecific = 1 << 1, // 2

    /// <summary>
    /// An unrecoverable gap was detected in the channel.
    /// </summary>
    MaybeBadBook = 1 << 2, // 4

    /// <summary>
    /// The ts_recv value is inaccurate due to clock issues or packet reordering.
    /// </summary>
    BadReceiveTimestamp = 1 << 3, // 8

    /// <summary>
    /// Aggregated price level message (MBP), not an individual order.
    /// </summary>
    MarketByPrice = 1 << 4, // 16

    /// <summary>
    /// Message sourced from a replay, such as a snapshot server.
    /// </summary>
    Snapshot = 1 << 5, // 32

    /// <summary>
    /// Top-of-book message, not an individual order.
    /// </summary>
    TopOfBook = 1 << 6, // 64

    /// <summary>
    /// Marks the last record in a single event for a given instrument_id.
    /// </summary>
    Last = 1 << 7 // 128
}
