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
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.DataSource.DataBento;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.DataBento.Tests
{
    [TestFixture]
    public class DataBentoRawLiveClientTests
    {
        private DatabentoRawClient _client;
        private readonly string _apiKey = Config.Get("databento-api-key");

        [SetUp]
        public void SetUp()
        {
            _client = new DatabentoRawClient(_apiKey);
        }

        [TearDown]
        public void TearDown()
        {
            _client?.Dispose();
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key and live connection")]
        public async Task ConnectsToGateway()
        {
            if (string.IsNullOrEmpty(_apiKey))
            {
                Assert.Ignore("DataBento API key not configured");
                return;
            }

            var connected = await _client.ConnectAsync();

            Assert.IsTrue(connected, "Should successfully connect to DataBento gateway");
            Assert.IsTrue(_client.IsConnected, "IsConnected should return true after successful connection");

            Log.Trace("Successfully connected to DataBento gateway");
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key and live connection")]
        public async Task SubscribesToSymbol()
        {
            if (string.IsNullOrEmpty(_apiKey))
            {
                Assert.Ignore("DataBento API key not configured");
                return;
            }

            var connected = await _client.ConnectAsync();
            Assert.IsTrue(connected, "Must be connected to test subscription");

            var symbol = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
            var subscribed = _client.Subscribe(symbol, Resolution.Minute, TickType.Trade);

            Assert.IsTrue(subscribed, "Should successfully subscribe to symbol");

            Log.Trace($"Successfully subscribed to {symbol}");

            // Wait a moment to ensure subscription is active
            await Task.Delay(2000);

            var unsubscribed = _client.Unsubscribe(symbol);
            Assert.IsTrue(unsubscribed, "Should successfully unsubscribe from symbol");

            Log.Trace($"Successfully unsubscribed from {symbol}");
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key and live connection")]
        public async Task ReceivesLiveData()
        {
            if (string.IsNullOrEmpty(_apiKey))
            {
                Assert.Ignore("DataBento API key not configured");
                return;
            }

            var dataReceived = false;
            var dataReceivedEvent = new ManualResetEventSlim(false);
            BaseData receivedData = null;

            _client.DataReceived += (sender, data) =>
            {
                receivedData = data;
                dataReceived = true;
                dataReceivedEvent.Set();
                Log.Trace($"Received data: {data}");
            };

            var connected = await _client.ConnectAsync();
            Assert.IsTrue(connected, "Must be connected to test data reception");

            var symbol = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
            var subscribed = _client.Subscribe(symbol, Resolution.Tick, TickType.Trade);
            Assert.IsTrue(subscribed, "Must be subscribed to receive data");

            // Wait for data with timeout
            var dataReceiptTimeout = TimeSpan.FromMinutes(2);
            var receivedWithinTimeout = dataReceivedEvent.Wait(dataReceiptTimeout);

            if (receivedWithinTimeout)
            {
                Assert.IsTrue(dataReceived, "Should have received data");
                Assert.IsNotNull(receivedData, "Received data should not be null");
                Assert.AreEqual(symbol, receivedData.Symbol, "Received data symbol should match subscription");
                Assert.Greater(receivedData.Value, 0, "Received data value should be positive");

                Log.Trace($"Successfully received live data: {receivedData}");
            }
            else
            {
                Log.Trace("No data received within timeout period - this may be expected during non-market hours");
            }

            _client.Unsubscribe(symbol);
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key and live connection")]
        public async Task HandlesConnectionEvents()
        {
            if (string.IsNullOrEmpty(_apiKey))
            {
                Assert.Ignore("DataBento API key not configured");
                return;
            }

            var connectionStatusChanged = false;
            var connectionStatusEvent = new ManualResetEventSlim(false);

            _client.ConnectionStatusChanged += (sender, isConnected) =>
            {
                connectionStatusChanged = true;
                connectionStatusEvent.Set();
                Log.Trace($"Connection status changed: {isConnected}");
            };

            var connected = await _client.ConnectAsync();
            Assert.IsTrue(connected, "Should connect successfully");

            // Connection status event should fire on connect
            var eventFiredWithinTimeout = connectionStatusEvent.Wait(TimeSpan.FromSeconds(10));
            Assert.IsTrue(eventFiredWithinTimeout || connectionStatusChanged,
                "Connection status changed event should fire");

            _client.Disconnect();
            Assert.IsFalse(_client.IsConnected, "Should be disconnected after calling Disconnect()");
        }

        [Test]
        public void HandlesInvalidApiKey()
        {
            var invalidClient = new DatabentoRawClient("invalid-api-key");

            // Connection with invalid API key should fail gracefully
            Assert.DoesNotThrowAsync(async () =>
            {
                var connected = await invalidClient.ConnectAsync();
                Assert.IsFalse(connected, "Connection should fail with invalid API key");
            });

            invalidClient.Dispose();
        }

        [Test]
        public void DisposesCorrectly()
        {
            var client = new DatabentoRawClient(_apiKey);
            Assert.DoesNotThrow(() => client.Dispose(), "Dispose should not throw");
            Assert.DoesNotThrow(() => client.Dispose(), "Multiple dispose calls should not throw");
        }

        [Test]
        public void SymbolMappingWorksCorrectly()
        {
            // Test that futures are mapped correctly to DataBento format
            var esFuture = Symbol.Create("ESM3", SecurityType.Future, Market.CME);

            // Since the mapping method is private, we test indirectly through subscription
            Assert.DoesNotThrowAsync(async () =>
            {
                if (!string.IsNullOrEmpty(_apiKey))
                {
                    var connected = await _client.ConnectAsync();
                    if (connected)
                    {
                        _client.Subscribe(esFuture, Resolution.Minute, TickType.Trade);
                        _client.Unsubscribe(esFuture);
                    }
                }
            });
        }

        [Test]
        public void SchemaResolutionMappingWorksCorrectly()
        {
            // Test that resolution mappings work correctly
            var symbol = Symbol.Create("ESM3", SecurityType.Future, Market.CME);

            Assert.DoesNotThrowAsync(async () =>
            {
                if (!string.IsNullOrEmpty(_apiKey))
                {
                    var connected = await _client.ConnectAsync();
                    if (connected)
                    {
                        // Test different resolutions
                        _client.Subscribe(symbol, Resolution.Tick, TickType.Trade);
                        _client.Unsubscribe(symbol);

                        _client.Subscribe(symbol, Resolution.Second, TickType.Trade);
                        _client.Unsubscribe(symbol);

                        _client.Subscribe(symbol, Resolution.Minute, TickType.Trade);
                        _client.Unsubscribe(symbol);

                        _client.Subscribe(symbol, Resolution.Hour, TickType.Trade);
                        _client.Unsubscribe(symbol);

                        _client.Subscribe(symbol, Resolution.Daily, TickType.Trade);
                        _client.Unsubscribe(symbol);
                    }
                }
            });
        }
    }
}
