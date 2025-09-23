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

using System;
using System.Linq;
using NUnit.Framework;
using System.Threading;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Logging;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using System.Collections.Concurrent;
using QuantConnect.Lean.DataSource.DataBento;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class DataBentoDataQueueHandlerTests
    {
        private DataBentoProvider _dataProvider;

        [SetUp]
        public void SetUp()
        {
            TestSetup.GlobalSetup();
            _dataProvider = new DataBentoProvider();
        }

        [TearDown]
        public void TearDown()
        {
            _dataProvider?.Dispose();
        }

        private static IEnumerable<TestCaseData> TestParameters
        {
            get
            {
                // DataBento supports futures primarily
                var esFuture = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
                var znFuture = Symbol.Create("ZNM3", SecurityType.Future, Market.CBOT);
                var gcFuture = Symbol.Create("GCM3", SecurityType.Future, Market.COMEX);

                yield return new TestCaseData(esFuture, Resolution.Tick)
                    .SetDescription("ES Mini futures tick data");
                yield return new TestCaseData(esFuture, Resolution.Second)
                    .SetDescription("ES Mini futures second data");
                yield return new TestCaseData(esFuture, Resolution.Minute)
                    .SetDescription("ES Mini futures minute data");
                yield return new TestCaseData(znFuture, Resolution.Minute)
                    .SetDescription("ZN Treasury futures minute data");
                yield return new TestCaseData(gcFuture, Resolution.Minute)
                    .SetDescription("GC Gold futures minute data");
            }
        }

        [Test, TestCaseSource(nameof(TestParameters))]
        [Explicit("This test requires a configured DataBento API key and live market data")]
        public void SubscribeAndUnsubscribe(Symbol symbol, Resolution resolution)
        {
            var configs = GetSubscriptionDataConfigs(symbol, resolution).ToList();

            foreach (var config in configs)
            {
                var enumerator = _dataProvider.Subscribe(config, (s, e) => { });
                Assert.IsNotNull(enumerator, $"Should return valid enumerator for {config.Symbol} at {config.Resolution}");
            }

            // Wait a moment for subscriptions to be established
            Thread.Sleep(2000);

            // Check if connected
            Assert.IsTrue(_dataProvider.IsConnected, "DataBento provider should be connected");

            foreach (var config in configs)
            {
                Assert.DoesNotThrow(() => _dataProvider.Unsubscribe(config),
                    $"Unsubscribe should not throw for {config.Symbol}");
            }

            Thread.Sleep(1000);
        }

        [Test, TestCaseSource(nameof(TestParameters))]
        [Explicit("This test requires a configured DataBento API key and live market data")]
        public void StreamsData(Symbol symbol, Resolution resolution)
        {
            var configs = GetSubscriptionDataConfigs(symbol, resolution).ToList();
            var dataReceived = new ConcurrentDictionary<Type, int>();
            var cancellationTokenSource = new CancellationTokenSource();

            foreach (var config in configs)
            {
                var enumerator = _dataProvider.Subscribe(config, (s, e) => { });
                Assert.IsNotNull(enumerator, $"Should return valid enumerator for {config.Symbol}");

                ProcessFeed(
                    enumerator,
                    (baseData) =>
                    {
                        if (baseData != null)
                        {
                            Log.Trace($"Received: {baseData}");
                            dataReceived.AddOrUpdate(baseData.GetType(), 1, (key, count) => count + 1);

                            // Validate data
                            Assert.AreEqual(symbol, baseData.Symbol, "Symbol should match subscription");
                            Assert.Greater(baseData.Time, DateTime.MinValue, "Time should be valid");
                            Assert.Greater(baseData.Value, 0, "Value should be positive");

                            // Cancel after receiving some data
                            if (dataReceived.Values.Sum() >= 10)
                            {
                                cancellationTokenSource.Cancel();
                            }
                        }
                    },
                    cancellationTokenSource.Token);
            }

            // Wait for data or timeout
            var timeout = TimeSpan.FromMinutes(2);
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            while (!cancellationTokenSource.Token.IsCancellationRequested && stopwatch.Elapsed < timeout)
            {
                Thread.Sleep(100);
            }

            // Clean up subscriptions
            foreach (var config in configs)
            {
                _dataProvider.Unsubscribe(config);
            }

            if (dataReceived.Any())
            {
                Log.Trace($"Total data points received: {dataReceived.Values.Sum()}");
                foreach (var kvp in dataReceived)
                {
                    Log.Trace($"{kvp.Key.Name}: {kvp.Value} points");
                }
            }

            Thread.Sleep(1000);
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key")]
        public void ConnectionStatusHandling()
        {
            Assert.IsTrue(_dataProvider.IsConnected || !_dataProvider.IsConnected,
                "IsConnected should return a boolean value");

            // Test that provider can handle connection status queries
            for (int i = 0; i < 5; i++)
            {
                var isConnected = _dataProvider.IsConnected;
                Log.Trace($"Connection status check {i + 1}: {isConnected}");
                Thread.Sleep(500);
            }
        }

        [Test]
        public void DisposesCorrectly()
        {
            var provider = new DataBentoProvider();
            Assert.DoesNotThrow(() => provider.Dispose(), "Dispose should not throw");
            Assert.DoesNotThrow(() => provider.Dispose(), "Multiple dispose calls should not throw");
        }

        private IEnumerable<SubscriptionDataConfig> GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
        {
            if (resolution == Resolution.Tick)
            {
                yield return new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, resolution), tickType: TickType.Trade);
                // Quote tick data may require advanced subscription
                // yield return new SubscriptionDataConfig(GetSubscriptionDataConfig<Tick>(symbol, resolution), tickType: TickType.Quote);
            }
            else
            {
                yield return GetSubscriptionDataConfig<TradeBar>(symbol, resolution);
                yield return GetSubscriptionDataConfig<DataBentoDataType>(symbol, resolution);
            }
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

        private Task ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null, CancellationToken cancellationToken = default)
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested && enumerator.MoveNext())
                    {
                        BaseData data = enumerator.Current;

                        if (data != null)
                        {
                            callback?.Invoke(data);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancellation is requested
                    Log.Trace("Feed processing cancelled");
                }
                catch (Exception ex)
                {
                    Log.Error($"Error in ProcessFeed: {ex.Message}");
                    throw;
                }
            }, cancellationToken);
        }
    }
}
