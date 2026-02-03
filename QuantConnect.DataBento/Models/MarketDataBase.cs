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
    /// The capture-server-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
    /// </summary>
    public ulong? TsRecv { get; set; }

    /// <summary>
    /// Gets or sets the standard metadata header for this market data record.
    /// </summary>
    [JsonProperty("hd")]
    public Header Header { get; set; }

    /// <summary>
    /// Tries to get a UTC <see cref="DateTime"/> from available nanosecond Unix timestamps.
    /// </summary>
    /// <remarks>
    /// Uses <see cref="Header.TsEvent"/> first; if it is undefined (<c>UINT64_MAX</c>),
    /// falls back to <see cref="TsRecv"/>.
    /// <para/>
    /// Databento timestamp conventions:
    /// https://databento.com/docs/standards-and-conventions/common-fields-enums-types#timestamps
    /// </remarks>
    /// <param name="dateTimeUtc">
    /// The resolved UTC time if successful; otherwise <see cref="DateTime.MinValue"/>.
    /// </param>
    /// <returns>
    /// <c>true</c> if a valid timestamp was converted; otherwise <c>false</c>.
    /// </returns>
    public bool TryGetDateTimeUtc(out DateTime dateTimeUtc)
    {
        if (TryConvertTimestamp(Header.TsEvent, out dateTimeUtc))
        {
            return true;
        }

        if (TsRecv.HasValue && TryConvertTimestamp(TsRecv.Value, out dateTimeUtc))
        {
            return true;
        }

        dateTimeUtc = default;
        return false;
    }

    private static bool TryConvertTimestamp(ulong timestamp, out DateTime dateTimeUtc)
    {
        // UINT64_MAX (18446744073709551615) denotes a null or undefined timestamp
        if (timestamp == ulong.MaxValue)
        {
            dateTimeUtc = default;
            return false;
        }

        dateTimeUtc = Time.UnixNanosecondTimeStampToDateTime(Convert.ToInt64(timestamp));
        return true;
    }
}
