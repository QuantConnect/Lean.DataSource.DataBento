/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
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

using QuantConnect;
using QuantConnect.Brokerages;
using System.Globalization;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Provides the mapping between Lean symbols and DataBento symbols.
    /// </summary>
    public class DataBentoSymbolMapper : ISymbolMapper
    {
        private readonly Dictionary<string, Symbol> _leanSymbolsCache = new();
        private readonly Dictionary<Symbol, string> _brokerageSymbolsCache = new();
        private readonly object _locker = new();

        /// <summary>
        /// Converts a Lean symbol instance to a brokerage symbol
        /// </summary>
        /// <param name="symbol">A Lean symbol instance</param>
        /// <returns>The brokerage symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
            {
                throw new ArgumentException($"Invalid symbol: {(symbol == null ? "null" : symbol.ToString())}");
            }

            return GetBrokerageSymbol(symbol, false);
        }

        /// <summary>
        /// Converts a Lean symbol instance to a brokerage symbol with updating of cached symbol collection
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="isUpdateCachedSymbol"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public string GetBrokerageSymbol(Symbol symbol, bool isUpdateCachedSymbol)
        {
            lock (_locker)
            {
                if (!_brokerageSymbolsCache.TryGetValue(symbol, out var brokerageSymbol) || isUpdateCachedSymbol)
                {
                    switch (symbol.SecurityType)
                    {
                        case SecurityType.Future:
                            brokerageSymbol = $"{symbol.ID.Symbol}.FUT";
                            break;
                        
                        case SecurityType.Equity:
                            brokerageSymbol = symbol.Value;
                            break;

                        default:
                            throw new Exception($"DataBentoSymbolMapper.GetBrokerageSymbol(): unsupported security type: {symbol.SecurityType}");
                    }

                    // Lean-to-DataBento symbol conversion is accurate, so we can cache it both ways
                    _brokerageSymbolsCache[symbol] = brokerageSymbol;
                    _leanSymbolsCache[brokerageSymbol] = symbol;
                }

                return brokerageSymbol;
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
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
            {
                throw new ArgumentException("Invalid symbol: " + brokerageSymbol);
            }

            lock (_locker)
            {
                if (!_leanSymbolsCache.TryGetValue(brokerageSymbol, out var leanSymbol))
                {
                    switch (securityType)
                    {
                        case SecurityType.Future:
                            leanSymbol = Symbol.CreateFuture(brokerageSymbol, market, expirationDate);
                            break;

                        default:
                            throw new Exception($"DataBentoSymbolMapper.GetLeanSymbol(): unsupported security type: {securityType}");
                    }

                    _leanSymbolsCache[brokerageSymbol] = leanSymbol;
                    _brokerageSymbolsCache[leanSymbol] = brokerageSymbol;
                }

                return leanSymbol;
            }
        }

        /// <summary>
        /// Gets the Lean symbol for the specified DataBento symbol
        /// </summary>
        /// <param name="databentoSymbol">The databento symbol</param>
        /// <returns>The corresponding Lean symbol</returns>
        public Symbol GetLeanSymbol(string databentoSymbol)
        {
            lock (_locker)
            {
                if (!_leanSymbolsCache.TryGetValue(databentoSymbol, out var symbol))
                {
                    symbol = GetLeanSymbol(databentoSymbol, SecurityType.Equity, Market.USA);
                }

                return symbol;
            }
        }

        /// <summary>
        /// Converts a brokerage future symbol to a Lean symbol instance
        /// </summary>
        /// <param name="brokerageSymbol">The brokerage symbol</param>
        /// <returns>A new Lean Symbol instance</returns>
        public Symbol GetLeanSymbolForFuture(string brokerageSymbol)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
            {
                throw new ArgumentException("Invalid symbol: " + brokerageSymbol);
            }

            // ignore futures spreads
            if (brokerageSymbol.Contains("-"))
            {
                return null;
            }

            lock (_locker)
            {
                if (!_leanSymbolsCache.TryGetValue(brokerageSymbol, out var leanSymbol))
                {
                    leanSymbol = SymbolRepresentation.ParseFutureSymbol(brokerageSymbol);

                    if (leanSymbol == null)
                    {
                        throw new ArgumentException("Invalid future symbol: " + brokerageSymbol);
                    }

                    _leanSymbolsCache[brokerageSymbol] = leanSymbol;
                }

                return leanSymbol;
            }
        }
    }
}
