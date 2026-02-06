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
using System.Text;
using NUnit.Framework;
using System.Threading;
using QuantConnect.Util;
using QuantConnect.Data;
using QuantConnect.Logging;
using System.Threading.Tasks;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoDataQueueHandlerTests
{
    private DataBentoProvider _dataProvider;
    private CancellationTokenSource _cancellationTokenSource;

    [SetUp]
    public void SetUp()
    {
        _cancellationTokenSource = new();
        _dataProvider = new();
    }

    [TearDown]
    public void TearDown()
    {
        _cancellationTokenSource?.Cancel();
        _cancellationTokenSource.DisposeSafely();
        _dataProvider?.DisposeSafely();
    }

    private static IEnumerable<TestCaseData> TestParameters
    {
        get
        {
            var sp500EMiniMarch = Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new(2026, 03, 20));

            yield return new TestCaseData(new Symbol[] { sp500EMiniMarch }, Resolution.Second);
        }
    }

    [Test, TestCaseSource(nameof(TestParameters))]
    public void CanSubscribeAndUnsubscribeOnDifferentResolution(Symbol[] symbols, Resolution resolution)
    {

        var configs = new List<SubscriptionDataConfig>();

        var dataFromEnumerator = new Dictionary<Symbol, Dictionary<Type, int>>();

        foreach (var symbol in symbols)
        {
            dataFromEnumerator[symbol] = new Dictionary<Type, int>();
            foreach (var config in GetSubscriptionDataConfigs(symbol, resolution))
            {
                configs.Add(config);

                var tickType = config.TickType switch
                {
                    TickType.Quote => typeof(QuoteBar),
                    TickType.Trade => typeof(TradeBar),
                    _ => throw new NotImplementedException()
                };

                dataFromEnumerator[symbol][tickType] = 0;
            }
        }

        Assert.That(configs, Is.Not.Empty);

        Action<BaseData> callback = (dataPoint) =>
        {
            if (dataPoint == null)
            {
                return;
            }

            switch (dataPoint)
            {
                case TradeBar tb:
                    dataFromEnumerator[tb.Symbol][typeof(TradeBar)] += 1;
                    break;
                case QuoteBar qb:
                    Assert.GreaterOrEqual(qb.Ask.Open, qb.Bid.Open, $"QuoteBar validation failed for {qb.Symbol}: Ask.Open ({qb.Ask.Open}) <= Bid.Open ({qb.Bid.Open}). Full data: {DisplayBaseData(qb)}");
                    Assert.GreaterOrEqual(qb.Ask.High, qb.Bid.High, $"QuoteBar validation failed for {qb.Symbol}: Ask.High ({qb.Ask.High}) <= Bid.High ({qb.Bid.High}). Full data: {DisplayBaseData(qb)}");
                    Assert.GreaterOrEqual(qb.Ask.Low, qb.Bid.Low, $"QuoteBar validation failed for {qb.Symbol}: Ask.Low ({qb.Ask.Low}) <= Bid.Low ({qb.Bid.Low}). Full data: {DisplayBaseData(qb)}");
                    Assert.GreaterOrEqual(qb.Ask.Close, qb.Bid.Close, $"QuoteBar validation failed for {qb.Symbol}: Ask.Close ({qb.Ask.Close}) <= Bid.Close ({qb.Bid.Close}). Full data: {DisplayBaseData(qb)}");
                    dataFromEnumerator[qb.Symbol][typeof(QuoteBar)] += 1;
                    break;
            }
            ;
        };

        foreach (var config in configs)
        {
            ProcessFeed(_dataProvider.Subscribe(config, (sender, args) =>
            {
                var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
            }), _cancellationTokenSource.Token, callback: callback);
        }

        Thread.Sleep(TimeSpan.FromSeconds(120));

        Log.Trace("Unsubscribing symbols");
        foreach (var config in configs)
        {
            _dataProvider.Unsubscribe(config);
        }

        Thread.Sleep(TimeSpan.FromSeconds(5));

        _cancellationTokenSource.Cancel();

        var str = new StringBuilder();

        str.AppendLine($"{nameof(DataBentoDataQueueHandlerTests)}.{nameof(CanSubscribeAndUnsubscribeOnDifferentResolution)}: ***** Summary *****");

        foreach (var symbol in symbols)
        {
            str.AppendLine($"Input parameters: ticker:{symbol} | securityType:{symbol.SecurityType} | resolution:{resolution}");

            foreach (var tickType in dataFromEnumerator[symbol])
            {
                str.AppendLine($"[{tickType.Key}] = {tickType.Value}");

                if (symbol.SecurityType != SecurityType.Index)
                {
                    Assert.Greater(tickType.Value, 0);
                }
                // The ThetaData returns TradeBar seldom. Perhaps should find more relevant ticker.
                Assert.GreaterOrEqual(tickType.Value, 0);
            }
            str.AppendLine(new string('-', 30));
        }

        Log.Trace(str.ToString());
    }

    private static string DisplayBaseData(BaseData item)
    {
        switch (item)
        {
            case TradeBar tradeBar:
                return $"Data Type: {item.DataType} | " + tradeBar.ToString() + $" Time: {tradeBar.Time}, EndTime: {tradeBar.EndTime}";
            default:
                return $"DEFAULT: Data Type: {item.DataType} | Time: {item.Time} | End Time: {item.EndTime} | Symbol: {item.Symbol} | Price: {item.Price} | IsFillForward: {item.IsFillForward}";
        }
    }

    private static IEnumerable<SubscriptionDataConfig> GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
    {
        yield return GetSubscriptionDataConfig<TradeBar>(symbol, resolution);
        yield return GetSubscriptionDataConfig<QuoteBar>(symbol, resolution);
    }

    public static IEnumerable<SubscriptionDataConfig> GetSubscriptionTickDataConfigs(Symbol symbol)
    {
        yield return new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, Resolution.Tick), tickType: TickType.Trade);
        yield return new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, Resolution.Tick), tickType: TickType.Quote);
    }

    private static SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
    {
        return new SubscriptionDataConfig(
            typeof(T),
            symbol,
            resolution,
            TimeZones.Utc,
            TimeZones.Utc,
            true,
            extendedHours: false,
            false);
    }

    private Task ProcessFeed(
        IEnumerator<BaseData> enumerator,
        CancellationToken cancellationToken,
        int cancellationTokenDelayMilliseconds = 100,
        Action<BaseData> callback = null,
        Action throwExceptionCallback = null)
    {
        return Task.Factory.StartNew(() =>
        {
            try
            {
                while (enumerator.MoveNext() && !cancellationToken.IsCancellationRequested)
                {
                    BaseData tick = enumerator.Current;

                    if (tick != null)
                    {
                        callback?.Invoke(tick);
                    }

                    cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(cancellationTokenDelayMilliseconds));
                }
            }
            catch (Exception ex)
            {
                Log.Debug($"{nameof(DataBentoDataQueueHandlerTests)}.{nameof(ProcessFeed)}.Exception: {ex.Message}");
                throw;
            }
        }, cancellationToken).ContinueWith(task =>
        {
            if (throwExceptionCallback != null)
            {
                throwExceptionCallback();
            }
            Log.Debug("The throwExceptionCallback is null.");
        }, TaskContinuationOptions.OnlyOnFaulted);
    }
}
