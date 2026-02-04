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

namespace QuantConnect.Lean.DataSource.DataBento.Models;

public sealed class BestBidOfferInterval : LevelOneDataBase
{
    /// <summary>
    /// The capture-server-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
    /// </summary>
    public ulong? TsRecv { get; set; }

    /// <summary>
    /// Gets the event timestamp converted to a UTC <see cref="DateTime"/>,
    /// or <c>null</c> when the timestamp is undefined.
    /// </summary>
    /// <remarks>
    /// The value is derived from <see cref="TsRecv"/>, which represents the number of
    /// nanoseconds since the UNIX epoch.
    /// <para/>
    /// A value of <see cref="ulong.MaxValue"/> (UNDEF_TIMESTAMP = 18446744073709551615)
    /// indicates a null or undefined timestamp and results in <c>null</c>.
    /// <para/>
    /// See DataBento timestamp conventions:
    /// <see href="https://databento.com/docs/standards-and-conventions/common-fields-enums-types#timestamps"/>
    /// <para/>
    /// Use for <see cref="Api.HistoricalAPIClient.BBO1mSchema"/> and <seealso cref="Api.HistoricalAPIClient.BBO1sSchema"/> data only.
    /// </remarks>
    public DateTime? UtcDateTime
    {
        get => TsRecv == ulong.MaxValue
            ? null
            : Time.UnixNanosecondTimeStampToDateTime(Convert.ToInt64(TsRecv));
    }
}
