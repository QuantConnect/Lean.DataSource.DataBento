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
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Packets;
using System.Threading.Tasks;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Implementation of Custom Data Provider
    /// </summary>
    public class DataBentoProvider : IDataQueueHandler
    {
        /// <summary>
        /// <inheritdoc cref="IDataAggregator"/>
        /// </summary>
        private readonly IDataAggregator _dataAggregator;

        /// <summary>
        /// <inheritdoc cref="EventBasedDataQueueHandlerSubscriptionManager"/>
        /// </summary>
        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        /// <summary>
        /// <inheritdoc cref="DatabentoRawClient"/>
        /// </summary>
        private DatabentoRawClient _client;

        /// <summary>
        /// DataBento API key
        /// </summary>
        private readonly string _apiKey;

        /// <summary>
        /// DataBento historical data downloader
        /// </summary>
        private readonly DataBentoDataDownloader _dataDownloader;

        /// <summary>
        /// Returns true if we're currently connected to the Data Provider
        /// </summary>
        public bool IsConnected => _client?.IsConnected == true;

        /// <summary>
        /// Initializes a new instance of the DataBentoProvider
        /// </summary>
        public DataBentoProvider()
        {
            _apiKey = Config.Get("databento-api-key");
            if (string.IsNullOrEmpty(_apiKey))
            {
                throw new ArgumentException("DataBento API key is required. Set 'databento-api-key' in configuration.");
            }

            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the DataBentoProvider with custom API key
        /// </summary>
        /// <param name="apiKey">DataBento API key</param>
        public DataBentoProvider(string apiKey)
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            Initialize();
        }

        /// <summary>
        /// Common initialization logic
        /// </summary>
        private void Initialize()
        {
            _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>("DataAggregator");
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _dataDownloader = new DataBentoDataDownloader(_apiKey);

            // Subscribe to aggregator events
            _subscriptionManager.SubscribeImpl += (sender, args) => Subscribe(args);
            _subscriptionManager.UnsubscribeImpl += (sender, args) => Unsubscribe(args);

            // Initialize the live client
            _client = new DatabentoRawClient(_apiKey);
            _client.DataReceived += OnDataReceived;
            _client.ConnectionStatusChanged += OnConnectionStatusChanged;

            // Connect to live gateway
            Task.Run(async () =>
            {
                var connected = await _client.ConnectAsync();
                if (!connected)
                {
                    Log.Error("DataBentoProvider.Initialize(): Failed to connect to DataBento live gateway");
                }
            });
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            // Subscribe to live data if client is connected
            if (_client?.IsConnected == true)
            {
                Task.Run(() =>
                {
                    var success = _client.Subscribe(dataConfig.Symbol, dataConfig.Resolution);
                    if (!success)
                    {
                        Log.Error($"DataBentoProvider.Subscribe(): Failed to subscribe to live data for {dataConfig.Symbol}");
                    }
                });
            }

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _dataAggregator.Remove(dataConfig);

            // Unsubscribe from live data if client is connected
            if (_client?.IsConnected == true)
            {
                Task.Run(() =>
                {
                    _client.Unsubscribe(dataConfig.Symbol);
                });
            }
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        /// <exception cref="NotImplementedException"></exception>
        public void SetJob(Packets.LiveNodePacket job)
        {
            // Not sure what to do in here.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Dispose of unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _dataAggregator?.DisposeSafely();
            _subscriptionManager?.DisposeSafely();
            _client?.Dispose();
            _dataDownloader?.Dispose();
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        private IEnumerable<BaseData> GetHistory(HistoryRequest request)
        {
            if (!CanSubscribe(request.Symbol))
            {
                return null;
            }

            try
            {
                // Use the data downloader to get historical data
                var parameters = new DataDownloaderGetParameters(
                    request.Symbol,
                    request.Resolution,
                    request.StartTimeUtc,
                    request.EndTimeUtc,
                    request.TickType);

                return _dataDownloader.Get(parameters);
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.GetHistory(): Failed to get history for {request.Symbol}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Checks if this Data provider supports the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>returns true if Data Provider supports the specified symbol; otherwise false</returns>
        /// <summary>
        /// Determines whether or not the specified config can be subscribed to
        /// </summary>
        private bool CanSubscribe(SubscriptionDataConfig config)
        {
            return
                // Filter out universe symbols
                config.Symbol.Value.IndexOfInvariant("universe", true) == -1 &&
                // Filter out canonical options
                !config.Symbol.IsCanonical() &&
                IsSupported(config.SecurityType, config.Type, config.TickType, config.Resolution);
        }

        /// <summary>
        /// Checks if the security type is supported
        /// </summary>
        /// <param name="securityType">Security type to check</param>
        /// <returns>True if supported</returns>
        private bool IsSecurityTypeSupported(SecurityType securityType)
        {
            // DataBento primarily supports futures, but also has equity and option coverage
            switch (securityType)
            {
                case SecurityType.Future:
                    return true;
                case SecurityType.Equity:
                case SecurityType.Option:
                case SecurityType.Index:
                case SecurityType.Forex:
                case SecurityType.Crypto:
                case SecurityType.Cfd:
                case SecurityType.Commodity:
                default:
                    return false;
            }
        }

        /// <summary>
        /// Determines if the specified subscription is supported
        /// </summary>
        private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                if (!_unsupportedSecurityTypeMessageLogged)
                {
                    _unsupportedSecurityTypeMessageLogged = true;
                    Log.Trace($"DataBentoDataProvider.IsSupported(): Unsupported security type: {securityType}");
                }
                return false;
            }

            // Check supported data types
            if (!dataType.IsAssignableFrom(typeof(TradeBar)) &&
                !dataType.IsAssignableFrom(typeof(QuoteBar)) &&
                !dataType.IsAssignableFrom(typeof(OpenInterest)) &&
                !dataType.IsAssignableFrom(typeof(Tick)))
            {
                if (!_unsupportedDataTypeMessageLogged)
                {
                    _unsupportedDataTypeMessageLogged = true;
                    Log.Trace($"DataBentoDataProvider.IsSupported(): Unsupported data type: {dataType}");
                }
                return false;
            }

            // Warn about potential limitations for tick data
            // I'm mimicing polygon implementation with this
            if (resolution < Resolution.Second && !_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
                Log.Trace("DataBentoDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced DataBento subscription plan is required to stream tick data.");
            }

            // Warn about potential limitations for quote data
            if (tickType == TickType.Quote && !_potentialUnsupportedTickTypeMessageLogged)
            {
                _potentialUnsupportedTickTypeMessageLogged = true;
                Log.Trace("DataBentoDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced DataBento subscription plan is required to stream quote data.");
            }

            return true;
        }
        // <summary>
        /// Handles data received from the live client
        /// </summary>
        private void OnDataReceived(object sender, BaseData data)
        {
            try
            {
                _dataAggregator.Update(data);
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.OnDataReceived(): Error updating data aggregator: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles connection status changes from the live client
        /// </summary>
        private void OnConnectionStatusChanged(object sender, bool isConnected)
        {
            Log.Debug($"DataBentoProvider.OnConnectionStatusChanged(): Connection status changed to: {isConnected}");

            if (isConnected)
            {
                // Resubscribe to all active subscriptions
                Task.Run(() =>
                {
                    foreach (var config in _subscriptionManager.Subscriptions)
                    {
                        _client.Subscribe(config.Symbol, config.Resolution);
                    }
                });
            }
        }
    }
}
