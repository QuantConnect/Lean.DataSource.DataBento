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
 *
*/

using QuantConnect.Brokerages;

namespace QuantConnect.DateBento;

public class DataBentoSymbolMapper : ISymbolMapper
{
    private readonly Dictionary<string, Symbol> _leanSymbolsCache = new();
    private readonly Dictionary<Symbol, string> _brokerageSymbolsCache = new();
    private readonly object _locker = new();
        
    public string GetBrokerageSymbol(Symbol symbol)
    {
        if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
        {
            throw new ArgumentException($"Invalid symbol: {(symbol == null ? "null" : symbol.ToString())}");
        }

        lock (_locker)
        {
            if (!_brokerageSymbolsCache.TryGetValue(symbol, out var brokerageSymbol))
            {
                var ticker = symbol.Value.Replace(" ", "");
                switch (symbol.SecurityType)
                {
                    case SecurityType.Equity:
                        brokerageSymbol = ticker;
                        break;

                    //todo?
                    case SecurityType.Index:
                        brokerageSymbol = $"I:{ticker}";
                        break;

                    //todo?
                    case SecurityType.Option:
                    case SecurityType.IndexOption:
                        brokerageSymbol = $"O:{ticker}";
                        break;

                    default:
                        throw new Exception($"{nameof(DataBentoSymbolMapper)}.{nameof(GetBrokerageSymbol)}(): unsupported security type: {symbol.SecurityType}");
                }

                // todo
                // Lean-to-Polygon symbol conversion is accurate, so we can cache it both ways
                _brokerageSymbolsCache[symbol] = brokerageSymbol;
                _leanSymbolsCache[brokerageSymbol] = symbol;
            }

            return brokerageSymbol;
        }
    }

    public Symbol GetLeanSymbol(string brokerageSymbol,
        SecurityType securityType, string market,
        DateTime expirationDate = new DateTime(), decimal strike = 0, OptionRight optionRight = OptionRight.Call)
    {
        throw new NotImplementedException();
    }
}