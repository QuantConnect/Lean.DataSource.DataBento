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

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Provides the mapping between Lean symbols and DataBento symbols.
    /// </summary>
    public class DataBentoSymbolMapper : ISymbolMapper
    {

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
            switch (securityType)
            {
                case SecurityType.Future:
                    return Symbol.CreateFuture(brokerageSymbol, market, expirationDate);
                default:
                    throw new Exception($"The unsupported security type: {securityType}");
            }
        }

        /// <summary>
        /// Converts a brokerage future symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The brokerage symbol</param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbolForFuture(string brokerageSymbol)
        {
            // ignore futures spreads
            if (brokerageSymbol.Contains("-"))
            {
                return null;
            }

            return SymbolRepresentation.ParseFutureSymbol(brokerageSymbol);
        }
    }
}
