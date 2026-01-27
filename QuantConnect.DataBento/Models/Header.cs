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

using QuantConnect.Lean.DataSource.DataBento.Models.Enums;

namespace QuantConnect.Lean.DataSource.DataBento.Models;

/// <summary>
/// Metadata header for a historical market data record.
/// Contains event timing, record type, data source, and instrument identifiers.
/// </summary>
public sealed class Header
{
    /// <summary>
    /// The matching-engine-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
    /// </summary>
    public long TsEvent { get; set; }

    /// <summary>
    /// Record type identifier defining the data schema (e.g. trade, quote, bar).
    /// </summary>
    public RecordType Rtype { get; set; }

    /// <summary>
    /// DataBento publisher (exchange / data source) identifier.
    /// </summary>
    public int PublisherId { get; set; }

    /// <summary>
    /// Internal instrument identifier for the symbol.
    /// </summary>
    public uint InstrumentId { get; set; }

    /// <summary>
    /// Event time converted to UTC <see cref="DateTime"/>.
    /// </summary>
    public DateTime UtcTime => Time.UnixNanosecondTimeStampToDateTime(TsEvent);
}
