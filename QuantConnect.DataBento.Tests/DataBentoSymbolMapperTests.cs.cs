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
using QuantConnect.Lean.DataSource.DataBento.Models;

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

    [TestCase(Market.CME, "GLBX.MDP3", true)]
    [TestCase(Market.EUREX, "XEUR.EOBI", true)]
    [TestCase(Market.USA, null, false)]
    public void ReturnsCorrectDataBentoDataSet(string market, string expectedDataSet, bool expectedExist)
    {
        var actualExist = _symbolMapper.DataBentoDataSetByLeanMarket.TryGetValue(market, out var actualDataSet);
        Assert.AreEqual(expectedExist, actualExist);
        Assert.AreEqual(expectedDataSet, actualDataSet);
    }

    private static IEnumerable<TestCaseData> SymbolToParentGroupParameters
    {
        get
        {
            var es = Symbol.CreateFuture(Securities.Futures.Indices.SP500EMini, Market.CME, new DateTime(2026, 3, 20));
            yield return new TestCaseData(es, "ES.FUT", PredefinedDataSets.GLBX_MDP3.DataSetID);

            var gf = Symbol.CreateFuture(Securities.Futures.Meats.FeederCattle, Market.CME, new(2026, 03, 26));
            yield return new TestCaseData(gf, "GF.FUT", PredefinedDataSets.GLBX_MDP3.DataSetID);

            var zo = Symbol.CreateFuture(Securities.Futures.Grains.Oats, Market.CBOT, new(2026, 03, 13));
            yield return new TestCaseData(zo, "ZO.FUT", PredefinedDataSets.GLBX_MDP3.DataSetID);
        }
    }

    [TestCaseSource(nameof(SymbolToParentGroupParameters))]
    public void ReturnsCorrectSymbolParentGroup(Symbol symbol, string expectedParentGroup, string expectedDataSet)
    {
        var (actualParentGroup, actualDataset) = _symbolMapper.GetSymbolParentGroupAndDataset(symbol);

        Assert.AreEqual(expectedParentGroup, actualParentGroup);
        Assert.AreEqual(expectedDataSet, actualDataset);
    }

    private static IEnumerable<TestCaseData> BrokerageSymbolTestCases
    {
        get
        {
            var esMarch = Symbol.CreateFuture(Securities.Futures.Indices.SP500EMini, Market.CME, new DateTime(2026, 3, 20));
            yield return new TestCaseData("ESH6", esMarch);

            var esJune = Symbol.CreateFuture(Securities.Futures.Indices.SP500EMini, Market.CME, new DateTime(2027, 6, 18));
            yield return new TestCaseData("ESM7", esJune);

            var esSeptember = Symbol.CreateFuture(Securities.Futures.Indices.SP500EMini, Market.CME, new DateTime(2028, 9, 15));
            yield return new TestCaseData("ESU8", esSeptember);
        }
    }

    [TestCaseSource(nameof(BrokerageSymbolTestCases))]
    public void ReturnsCorrectSymbol(string brokerageSymbol, Symbol expectedSymbol)
    {
        var symbol = _symbolMapper.GetLeanSymbol(brokerageSymbol, expectedSymbol.SecurityType, market: null);
        Assert.AreEqual(expectedSymbol, symbol);
    }
}
