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

namespace QuantConnect.Lean.DataSource.DataBento.Models.Live;

/// <summary>
/// Provides data for the symbol-mapping confirmation event.
/// </summary>
public sealed class SymbolMappingConfirmationEventArgs : EventArgs
{
    /// <summary>
    /// Gets the original symbol that was mapped.
    /// </summary>
    public string Symbol { get; }

    /// <summary>
    /// Gets the internal instrument identifier associated with the symbol.
    /// </summary>
    public uint InstrumentId { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SymbolMappingConfirmationEventArgs"/> class.
    /// </summary>
    /// <param name="symbol">The symbol that was mapped.</param>
    /// <param name="instrumentId">The internal instrument identifier.</param>
    public SymbolMappingConfirmationEventArgs(string symbol, uint instrumentId)
    {
        Symbol = symbol;
        InstrumentId = instrumentId;
    }
}
