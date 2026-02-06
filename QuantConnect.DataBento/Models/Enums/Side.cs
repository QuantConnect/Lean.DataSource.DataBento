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

using System.Runtime.Serialization;

namespace QuantConnect.Lean.DataSource.DataBento.Models.Enums;

/// <summary>
/// Indicates the side associated with an order or trade event.
/// </summary>
public enum Side
{
    /// <summary>
    /// No side specified.
    /// </summary>
    /// <remarks>
    /// Used when the data source does not disseminate a side or when the side
    /// cannot be determined.
    ///
    /// <para>Common cases include:</para>
    /// <list type="bullet">
    ///   <item><description>Clear book actions (always <c>N</c>)</description></item>
    ///   <item><description>Trades during opening or closing auctions</description></item>
    ///   <item><description>Trades against non-displayed or implied orders</description></item>
    ///   <item><description>Off-exchange trades</description></item>
    /// </list>
    /// </remarks>
    [EnumMember(Value = "N")]
    None,

    /// <summary>
    /// Buy side.
    /// </summary>
    /// <remarks>
    /// Interpretation depends on <see cref="ActionType"/>:
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="ActionType.Trade"/> — The trade aggressor was a buyer
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="ActionType.Fill"/> — A resting buy order was filled
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="ActionType.Add"/>, <see cref="ActionType.Modify"/>,
    ///     <see cref="ActionType.Cancel"/> — A resting buy order updated the book
    ///   </description></item>
    /// </list>
    /// </remarks>
    [EnumMember(Value = "B")]
    Buy,

    /// <summary>
    /// Sell side.
    /// </summary>
    /// <remarks>
    /// Interpretation depends on <see cref="ActionType"/>:
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="ActionType.Trade"/> — The trade aggressor was a seller
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="ActionType.Fill"/> — A resting sell order was filled
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="ActionType.Add"/>, <see cref="ActionType.Modify"/>,
    ///     <see cref="ActionType.Cancel"/> — A resting sell order updated the book
    ///   </description></item>
    /// </list>
    /// </remarks>
    [EnumMember(Value = "A")]
    Sell,
}

