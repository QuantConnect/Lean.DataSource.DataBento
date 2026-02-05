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
/// The action field contains information about the type of order event contained in the message.
/// </summary>
public enum ActionType
{
    /// <summary>
    /// No action: does not affect the book, but may carry flags or other information.
    /// </summary>
    [EnumMember(Value = "N")]
    None,

    /// <summary>
    /// Insert a new order into the book.
    /// </summary>
    [EnumMember(Value = "A")]
    Add,

    /// <summary>
    /// Change an order's price and/or size.
    /// </summary>
    [EnumMember(Value = "M")]
    Modify,

    /// <summary>
    /// Fully or partially cancel an order from the book.
    /// </summary>
    [EnumMember(Value = "C")]
    Cancel,

    /// <summary>
    /// Remove all resting orders for the instrument.
    /// </summary>
    [EnumMember(Value = "R")]
    Clear,

    /// <summary>
    /// An aggressing order traded. Does not affect the book.
    /// </summary>
    [EnumMember(Value = "T")]
    Trade,
}
