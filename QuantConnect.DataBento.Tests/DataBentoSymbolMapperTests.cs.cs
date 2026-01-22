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
 *
*/

using System;
using NUnit.Framework;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoSymbolMapperTests
{
    /// <summary>
    /// Provides the mapping between Lean symbols and brokerage specific symbols.
    /// </summary>
    private DataBentoSymbolMapper _symbolMapper;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _symbolMapper = new DataBentoSymbolMapper();
    }
    private static IEnumerable<TestCaseData> LeanSymbolTestCases
    {
        get
        {
            // TSLA - Equity
            var es = Symbol.CreateFuture(Securities.Futures.Indices.SP500EMini, Market.CME, new DateTime(2026, 3, 20));
            yield return new TestCaseData(es, "ESH6");

        }
    }

    [Test, TestCaseSource(nameof(LeanSymbolTestCases))]
    public void ReturnsCorrectBrokerageSymbol(Symbol symbol, string expectedBrokerageSymbol)
    {
        var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

        Assert.IsNotNull(brokerageSymbol);
        Assert.IsNotEmpty(brokerageSymbol);
        Assert.AreEqual(expectedBrokerageSymbol, brokerageSymbol);
    }
}
