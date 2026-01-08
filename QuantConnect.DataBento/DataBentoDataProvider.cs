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
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using System.Collections.Concurrent;
using System.ComponentModel.Composition;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Implementation of Custom Data Provider with Universe support for futures contract resolution
    /// </summary>
    [Export(typeof(IDataQueueHandler))]
    public class DataBentoProvider : IDataQueueHandler, IDataQueueUniverseProvider
    {
        /// <summary>
        /// Track resolved contracts per canonical symbol.
        /// When Databento sends symbol mappings (MYM.FUT -> MYMH6), we parse them
        /// into proper LEAN Symbols and store here so LookupSymbols() can return them.
        /// </summary>
        private readonly ConcurrentDictionary<Symbol, HashSet<Symbol>> _resolvedContracts = new();

        private readonly IDataAggregator _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager = null!;
        private readonly List<SubscriptionDataConfig> _activeSubscriptionConfigs = new();
        private readonly ConcurrentDictionary<Symbol, SubscriptionDataConfig> _subscriptionConfigs = new();
        private DatabentoRawClient _client = null!;
        private readonly string _apiKey;
        private readonly DataBentoDataDownloader _dataDownloader;
        private bool _potentialUnsupportedResolutionMessageLogged;
        private bool _sessionStarted = false;
        private readonly object _sessionLock = new object();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly ConcurrentDictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        /// <summary>
        /// Replay start time for intraday historical data.
        /// - DateTime.MinValue = full replay (start=0, up to 24 hours)
        /// - Specific DateTime = replay from that time
        /// - null = live only (no replay) - but we default to 15 min ago if not configured
        /// Configure via "databento-replay-start" in config.json: "0" for full, ISO datetime for specific, absent for 15-min default
        /// </summary>
        private readonly DateTime? _replayStart;

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

            // Parse replay start config: "0" for full replay, ISO datetime for specific, absent for 15-min default
            var replayStartConfig = Config.Get("databento-replay-start", "");
            _replayStart = ParseReplayStart(replayStartConfig);
            Log.Trace($"DataBentoProvider: Replay start configured as: {FormatReplayStart(_replayStart)}");

            _dataDownloader = new DataBentoDataDownloader(_apiKey);
            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the DataBentoProvider with custom API key
        /// </summary>
        /// <param name="apiKey">DataBento API key</param>
        public DataBentoProvider(string apiKey)
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _dataDownloader = new DataBentoDataDownloader(_apiKey);
            Initialize();
        }

        /// <summary>
        /// Common initialization logic
        /// </summary>
        private void Initialize()
        {
            Log.Trace("DataBentoProvider.Initialize(): Starting initialization");
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl = (symbols, tickType) =>
            {
                Log.Trace($"DataBentoProvider.SubscribeImpl(): Received subscription request for {symbols.Count()} symbols, TickType={tickType}");
                foreach (var symbol in symbols)
                {
                    Log.Trace($"DataBentoProvider.SubscribeImpl(): Processing symbol {symbol}");
                    if (!_subscriptionConfigs.TryGetValue(symbol, out var config))
                    {
                        Log.Error($"DataBentoProvider.SubscribeImpl(): No subscription config found for {symbol}");
                        return false;
                    }
                    if (_client?.IsConnected != true)
                    {
                        Log.Error($"DataBentoProvider.SubscribeImpl(): Client is not connected. Cannot subscribe to {symbol}");
                        return false;
                    }

                    // Use intraday replay to get historical data at startup
                    var replayStart = GetEffectiveReplayStart();
                    Log.Trace($"DataBentoProvider.SubscribeImpl(): Using intraday replay: {FormatReplayStart(replayStart)}");
                    if (!_client.Subscribe(config.Symbol, config.Resolution, config.TickType, replayStart))
                    {
                        Log.Error($"Failed to subscribe to {config.Symbol}");
                        return false;
                    }

                    lock (_sessionLock)
                    {
                        if (!_sessionStarted)
                            _sessionStarted = _client.StartSession();
                    }
                }

                return true;
            };

            _subscriptionManager.UnsubscribeImpl = (symbols, tickType) =>
            {
                foreach (var symbol in symbols)
                {
                    Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Processing symbol {symbol}");
                    if (_client?.IsConnected != true)
                    {
                        throw new InvalidOperationException($"DataBentoProvider.UnsubscribeImpl(): Client is not connected. Cannot unsubscribe from {symbol}");
                    }

                    _client.Unsubscribe(symbol);
                }

                return true;
            };

            // Initialize the live client
            Log.Trace("DataBentoProvider.Initialize(): Creating DatabentoRawClient");
            _client = new DatabentoRawClient(_apiKey);
            _client.DataReceived += OnDataReceived;
            _client.ConnectionStatusChanged += OnConnectionStatusChanged;
            _client.SymbolMappingReceived += OnSymbolMappingReceived;

            // Connect to live gateway
            Log.Trace("DataBentoProvider.Initialize(): Attempting connection to DataBento live gateway");
            Task.Run(() =>
            {
                var connected = _client.Connect();
                Log.Trace($"DataBentoProvider.Initialize(): Connect() returned {connected}");

                if (connected)
                {
                    Log.Trace("DataBentoProvider.Initialize(): Successfully connected to DataBento live gateway");
                }
                else
                {
                    Log.Error("DataBentoProvider.Initialize(): Failed to connect to DataBento live gateway");
                }
            });


            Log.Trace("DataBentoProvider.Initialize(): Initialization complete");
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData>? Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            Log.Trace($"DataBentoProvider.Subscribe(): Received subscription request for {dataConfig.Symbol}, Resolution={dataConfig.Resolution}, TickType={dataConfig.TickType}");
            if (!CanSubscribe(dataConfig))
            {
                Log.Error($"DataBentoProvider.Subscribe(): Cannot subscribe to {dataConfig.Symbol} with Resolution={dataConfig.Resolution}, TickType={dataConfig.TickType}");
                return null;
            }

            _subscriptionConfigs[dataConfig.Symbol] = dataConfig;
            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);
            _activeSubscriptionConfigs.Add(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            Log.Trace($"DataBentoProvider.Unsubscribe(): Received unsubscription request for {dataConfig.Symbol}, Resolution={dataConfig.Resolution}, TickType={dataConfig.TickType}");
            _subscriptionConfigs.TryRemove(dataConfig.Symbol, out _);
            _subscriptionManager.Unsubscribe(dataConfig);
            var toRemove = _activeSubscriptionConfigs.FirstOrDefault(c => c.Symbol == dataConfig.Symbol && c.TickType == dataConfig.TickType);
            if (toRemove != null)
            {
                Log.Trace($"DataBentoProvider.Unsubscribe(): Removing active subscription for {dataConfig.Symbol}, Resolution={dataConfig.Resolution}, TickType={dataConfig.TickType}");
                _activeSubscriptionConfigs.Remove(toRemove);
            }
            _dataAggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            // No action required for DataBento since the job details are not used in the subscription process.
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
        public IEnumerable<BaseData>? GetHistory(Data.HistoryRequest request)
        {
            Log.Trace($"DataBentoProvider.GetHistory(): Received history request for {request.Symbol}, Resolution={request.Resolution}, TickType={request.TickType}");
            if (!CanSubscribe(request.Symbol))
            {
                Log.Error($"DataBentoProvider.GetHistory(): Cannot provide history for {request.Symbol} with Resolution={request.Resolution}, TickType={request.TickType}");
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
        /// Parses the databento-replay-start config value.
        /// </summary>
        /// <param name="configValue">Config value: "0" for full, ISO datetime for specific, empty for default</param>
        /// <returns>DateTime.MinValue for full replay, specific DateTime, or null for default (15-min lookback)</returns>
        private static DateTime? ParseReplayStart(string configValue)
        {
            if (string.IsNullOrWhiteSpace(configValue))
            {
                // Empty/absent = use default (will be calculated as 15 min ago at subscribe time)
                return null;
            }

            if (configValue == "0")
            {
                // "0" = full intraday replay (sentinel value)
                return DateTime.MinValue;
            }

            // Try to parse as ISO datetime
            if (DateTime.TryParse(configValue, out var parsed))
            {
                return parsed.ToUniversalTime();
            }

            Log.Error($"DataBentoProvider: Invalid databento-replay-start value '{configValue}'. Use '0' for full replay, ISO datetime, or omit for 15-min default.");
            return null;
        }

        /// <summary>
        /// Formats replay start for logging.
        /// </summary>
        private static string FormatReplayStart(DateTime? replayStart)
        {
            if (!replayStart.HasValue)
                return "15-minute lookback (default)";
            if (replayStart.Value == DateTime.MinValue)
                return "FULL intraday replay (start=0)";
            return $"from {replayStart.Value:yyyy-MM-ddTHH:mm:ss} UTC";
        }

        /// <summary>
        /// Gets the effective replay start time for subscription.
        /// If _replayStart is null (default), calculates 15 minutes ago.
        /// </summary>
        private DateTime? GetEffectiveReplayStart()
        {
            if (_replayStart.HasValue)
                return _replayStart.Value;
            // Default: 15 minutes ago
            return DateTime.UtcNow.AddMinutes(-15);
        }

        /// <summary>
        /// Checks if this Data provider supports the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>returns true if Data Provider supports the specified symbol; otherwise false</returns>
        private bool CanSubscribe(Symbol symbol)
        {
            // Reject universe symbols but allow canonical (continuous) futures
            return !symbol.Value.Contains("universe", StringComparison.InvariantCultureIgnoreCase) &&
                   IsSecurityTypeSupported(symbol.SecurityType);
        }

        /// <summary>
        /// Determines whether or not the specified config can be subscribed to
        /// </summary>
        private bool CanSubscribe(SubscriptionDataConfig config)
        {
            return CanSubscribe(config.Symbol) &&
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
            return securityType == SecurityType.Future;
        }

        /// <summary>
        /// Determines if the specified subscription is supported
        /// </summary>
        private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                throw new NotSupportedException($"Unsupported security type: {securityType}");
            }

            // Check supported data types
            if (dataType != typeof(TradeBar) &&
                dataType != typeof(QuoteBar) &&
                dataType != typeof(Tick) &&
                dataType != typeof(OpenInterest))
            {
                throw new NotSupportedException($"Unsupported data type: {dataType}");
            }

            // Warn about potential limitations for tick data
            // I'm mimicing polygon implementation with this
            if (!_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
                Log.Trace("DataBentoDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced DataBento subscription plan is required to stream tick data.");
            }

            return true;
        }

        /// <summary>
        /// Converts the given UTC time into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, DateTime utcTime)
        {
            var exchangeTimeZone = _symbolExchangeTimeZones.GetOrAdd(symbol, sym =>
            {
                if (_marketHoursDatabase.TryGetEntry(sym.ID.Market, sym, sym.SecurityType, out var entry))
                {
                    return entry.ExchangeHours.TimeZone;
                }
                // Futures default to Chicago
                return TimeZones.Chicago;
            });

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }

        // <summary>
        /// Handles data received from the live client
        /// </summary>
        private void OnDataReceived(object? sender, BaseData data)
        {
            try
            {
                if (data is Tick tick)
                {
                    tick.Time = GetTickTime(tick.Symbol, tick.Time);
                    _dataAggregator.Update(tick);
                    // Log.Trace removed - too spammy (millions of ticks)
                }
                else if (data is TradeBar tradeBar)
                {
                    tradeBar.Time = GetTickTime(tradeBar.Symbol, tradeBar.Time);
                    tradeBar.EndTime = GetTickTime(tradeBar.Symbol, tradeBar.EndTime);

                    // Classify as replay vs live data based on age
                    // Replay data (from start= parameter) is historical, live data is recent
                    var now = DateTime.UtcNow;
                    var dataAgeSeconds = (now - tradeBar.EndTime.ConvertToUtc(TimeZones.Chicago)).TotalSeconds;
                    var dataType = dataAgeSeconds > 60 ? "REPLAY" : "LIVE";

                    // Log first 10 bars and every 50th bar thereafter
                    _tradeBarCount++;
                    if (_tradeBarCount <= 10 || _tradeBarCount % 50 == 0)
                    {
                        Log.Trace($"DataBentoProvider.OnDataReceived(): [{dataType}] TradeBar #{_tradeBarCount} for {tradeBar.Symbol} at {tradeBar.Time} (age: {dataAgeSeconds:F0}s)");
                    }
                    _dataAggregator.Update(tradeBar);
                }
                else
                {
                    data.Time = GetTickTime(data.Symbol, data.Time);
                    _dataAggregator.Update(data);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.OnDataReceived(): Error updating data aggregator: {ex.Message}\n{ex.StackTrace}");
            }
        }

        private int _tradeBarCount = 0;

        /// <summary>
        /// Handles connection status changes from the live client
        /// </summary>
        private void OnConnectionStatusChanged(object? sender, bool isConnected)
        {
            Log.Trace($"DataBentoProvider.OnConnectionStatusChanged(): Connection status changed to: {isConnected}");

            if (isConnected)
            {
                // Reset session flag on reconnection
                lock (_sessionLock)
                {
                    _sessionStarted = false;
                }

                // Resubscribe to all active subscriptions
                foreach (var config in _activeSubscriptionConfigs)
                {
                    _client.Subscribe(config.Symbol, config.Resolution, config.TickType);
                }

                // Start session after resubscribing
                if (_activeSubscriptionConfigs.Any())
                {
                    lock (_sessionLock)
                    {
                        if (!_sessionStarted)
                        {
                            Log.Trace("DataBentoProvider.OnConnectionStatusChanged(): Starting session after reconnection");
                            _sessionStarted = _client.StartSession();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Handles symbol mapping events from Databento.
        /// When Databento resolves a continuous symbol (MYM.FUT) to a specific contract (MYMH6),
        /// we store the mapping so LEAN's universe selection can find the contract.
        /// </summary>
        private void OnSymbolMappingReceived(object? sender, SymbolMappingEventArgs e)
        {
            if (e.ContractSymbol == null)
            {
                Log.Trace($"DataBentoProvider.OnSymbolMappingReceived(): No contract symbol for {e.DatabentoSymbol} (canonical: {e.CanonicalSymbol})");
                return;
            }

            // Add the resolved contract to our tracking dictionary
            var contracts = _resolvedContracts.GetOrAdd(e.CanonicalSymbol, _ => new HashSet<Symbol>());
            lock (contracts)
            {
                if (contracts.Add(e.ContractSymbol))
                {
                    Log.Trace($"DataBentoProvider.OnSymbolMappingReceived(): Resolved {e.CanonicalSymbol} -> {e.ContractSymbol} (from {e.DatabentoSymbol})");
                }
            }
        }

        #region IDataQueueUniverseProvider Implementation

        /// <summary>
        /// Returns the symbols available for the specified canonical symbol.
        /// For futures, returns the resolved contracts (e.g., MYMH6 for canonical /MYM).
        /// </summary>
        /// <param name="symbol">The canonical symbol to lookup</param>
        /// <param name="includeExpired">Whether to include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency (not used)</param>
        /// <returns>Enumerable of resolved contract Symbols</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            Log.Trace($"DataBentoProvider.LookupSymbols(): Looking up symbols for {symbol}, includeExpired={includeExpired}");

            if (_resolvedContracts.TryGetValue(symbol, out var contracts))
            {
                lock (contracts)
                {
                    var contractList = contracts.ToList();
                    Log.Trace($"DataBentoProvider.LookupSymbols(): Found {contractList.Count} contracts for {symbol}");

                    // If not including expired, filter by expiry date
                    if (!includeExpired)
                    {
                        var now = DateTime.UtcNow.Date;
                        contractList = contractList.Where(c => c.ID.Date >= now).ToList();
                        Log.Trace($"DataBentoProvider.LookupSymbols(): After expiry filter: {contractList.Count} contracts");
                    }

                    return contractList;
                }
            }

            Log.Trace($"DataBentoProvider.LookupSymbols(): No contracts found for {symbol}");
            return Enumerable.Empty<Symbol>();
        }

        /// <summary>
        /// Returns whether selection can take place.
        /// Selection is allowed when we're connected and have resolved at least one contract.
        /// </summary>
        /// <returns>True if universe selection can proceed</returns>
        public bool CanPerformSelection()
        {
            var canPerform = IsConnected && _resolvedContracts.Any();
            Log.Trace($"DataBentoProvider.CanPerformSelection(): {canPerform} (IsConnected={IsConnected}, ResolvedContracts={_resolvedContracts.Count})");
            return canPerform;
        }

        #endregion
    }
}
