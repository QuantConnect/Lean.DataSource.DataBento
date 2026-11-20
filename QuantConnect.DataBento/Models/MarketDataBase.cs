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

using Newtonsoft.Json;
using QuantConnect.Lean.DataSource.DataBento.Converters;

namespace QuantConnect.Lean.DataSource.DataBento.Models;

/// <summary>
/// Base class for all market data records containing a standard metadata header.
/// </summary>
[JsonConverter(typeof(DataConverter))]
public abstract class MarketDataBase
{
    /// <summary>
    /// Gets or sets the standard metadata header for this market data record.
    /// </summary>
    [JsonProperty("hd")]
    public Header Header { get; set; }

    /// <summary>
    /// The capture-server-received timestamp in nanoseconds since the UNIX epoch, when the schema carries one.
    /// </summary>
    public ulong? TsRecv { get; set; }

    /// <summary>
    /// The timestamp Databento sorts and filters historical requests on:
    /// <see cref="TsRecv"/> when the schema has it, otherwise <see cref="Header.TsEvent"/>.
    /// </summary>
    /// <remarks>See <see href="https://databento.com/docs/standards-and-conventions/common-fields-enums-types#timestamps"/></remarks>
    public ulong IndexTimestamp => TsRecv ?? Header.TsEvent;
}
