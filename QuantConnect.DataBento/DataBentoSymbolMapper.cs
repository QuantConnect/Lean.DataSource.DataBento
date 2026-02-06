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

using QuantConnect.Brokerages;
using System.Collections.Frozen;
using QuantConnect.Lean.DataSource.DataBento.Models;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// Provides the mapping between Lean symbols and DataBento symbols.
/// </summary>
public class DataBentoSymbolMapper : ISymbolMapper
{
    /// <summary>
    /// Dataset for CME Globex futures
    /// https://databento.com/docs/venues-and-datasets has more information on datasets through DataBento
    /// </summary>
    public FrozenDictionary<string, DataSetSpecifications> DataBentoDataSetByLeanMarket = new Dictionary<string, DataSetSpecifications>
    {
        { Market.EUREX, PredefinedDataSets.XEUR_EOBI },

        { Market.CBOT, PredefinedDataSets.GLBX_MDP3 },
        { Market.CME, PredefinedDataSets.GLBX_MDP3 },
        { Market.COMEX, PredefinedDataSets.GLBX_MDP3 },
        { Market.NYMEX, PredefinedDataSets.GLBX_MDP3 },

        { Market.ICE, PredefinedDataSets.IFUS_IMPACT },
        { Market.NYSELIFFE, PredefinedDataSets.IFUS_IMPACT }
    }.ToFrozenDictionary();

    /// <summary>
    /// Converts a Lean symbol instance to a brokerage symbol
    /// </summary>
    /// <param name="symbol">A Lean symbol instance</param>
    /// <returns>The brokerage symbol</returns>
    public string GetBrokerageSymbol(Symbol symbol)
    {
        switch (symbol.SecurityType)
        {
            case SecurityType.Future:
                return SymbolRepresentation.GenerateFutureTicker(symbol.ID.Symbol, symbol.ID.Date, doubleDigitsYear: false, includeExpirationDate: false);
            default:
                throw new Exception($"The unsupported security type: {symbol.SecurityType}");
        }
    }

    /// <summary>
    /// Converts a brokerage symbol to a Lean symbol instance
    /// </summary>
    /// <param name="brokerageSymbol">The brokerage symbol</param>
    /// <param name="securityType">The security type</param>
    /// <param name="market">The market</param>
    /// <param name="expirationDate">Expiration date of the security(if applicable)</param>
    /// <returns>A new Lean Symbol instance</returns>
    public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market,
        DateTime expirationDate = new DateTime(), decimal strike = 0, OptionRight optionRight = 0)
    {
        throw new NotImplementedException("This method is not used in the current implementation.");
    }
}
