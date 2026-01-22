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

using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using System.Collections.Concurrent;
using QuantConnect.Lean.DataSource.DataBento.Api;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// A data Provider for DataBento that provides live market data and historical data.
/// Handles Subscribing, Unsubscribing, and fetching historical data from DataBento.
/// It will handle if a symbol is subscribable and will log errors if it is not.
/// </summary>
public partial class DataBentoProvider : IDataQueueHandler
{
    /// <summary>
    /// Resolves map files to correctly handle current and historical ticker symbols.
    /// </summary>
    private readonly IMapFileProvider _mapFileProvider = Composer.Instance.GetPart<IMapFileProvider>();

    private HistoricalAPIClient _historicalApiClient;

    private readonly DataBentoSymbolMapper _symbolMapper = new();

    private readonly IDataAggregator _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
        Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);
    private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
    private DataBentoRawLiveClient _client;
    private bool _potentialUnsupportedResolutionMessageLogged;
    private bool _sessionStarted = false;
    private readonly object _sessionLock = new();
    private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
    private readonly ConcurrentDictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();
    private bool _initialized;

    /// <summary>
    /// Returns true if we're currently connected to the Data Provider
    /// </summary>
    public bool IsConnected => _client?.IsConnected == true;

    /// <summary>
    /// Initializes a new instance of the DataBentoProvider
    /// </summary>
    public DataBentoProvider()
        : this(Config.Get("databento-api-key"))
    {
    }

    public DataBentoProvider(string apiKey)
    {
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            // If the API key is not provided, we can't do anything.
            // The handler might going to be initialized using a node packet job.
            return;
        }

        Initialize(apiKey);
    }

    /// <summary>
    /// Common initialization logic
    /// <param name="apiKey">DataBento API key from config file retrieved on constructor</param>
    /// </summary>
    private void Initialize(string apiKey)
    {
        Log.Debug("DataBentoProvider.Initialize(): Starting initialization");
        _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager()
        {
            SubscribeImpl = (symbols, tickType) =>
            {
                return SubscriptionLogic(symbols, tickType);
            },
            UnsubscribeImpl = (symbols, tickType) =>
            {
                return UnsubscribeLogic(symbols, tickType);
            }
        };

        // Initialize the live client
        _client = new DataBentoRawLiveClient(apiKey);
        _client.DataReceived += OnDataReceived;

        // Connect to live gateway
        Log.Debug("DataBentoProvider.Initialize(): Attempting connection to DataBento live gateway");
        var cancellationTokenSource = new CancellationTokenSource();
        Task.Factory.StartNew(() =>
        {
            try
            {
                var connected = _client.Connect();
                Log.Debug($"DataBentoProvider.Initialize(): Connect() returned {connected}");

                if (connected)
                {
                    Log.Debug("DataBentoProvider.Initialize(): Successfully connected to DataBento live gateway");
                }
                else
                {
                    Log.Error("DataBentoProvider.Initialize(): Failed to connect to DataBento live gateway");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.Initialize(): Exception during Connect(): {ex.Message}\n{ex.StackTrace}");
            }
        },
        cancellationTokenSource.Token,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default);

        _historicalApiClient = new(apiKey);
        _initialized = true;

        Log.Debug("DataBentoProvider.Initialize(): Initialization complete");
    }

    /// <summary>
    /// Logic to unsubscribe from the specified symbols
    /// </summary>
    public bool UnsubscribeLogic(IEnumerable<Symbol> symbols, TickType tickType)
    {
        foreach (var symbol in symbols)
        {
            Log.Debug($"DataBentoProvider.UnsubscribeImpl(): Processing symbol {symbol}");
            if (_client?.IsConnected != true)
            {
                throw new InvalidOperationException($"DataBentoProvider.UnsubscribeImpl(): Client is not connected. Cannot unsubscribe from {symbol}");
            }

            _client.Unsubscribe(symbol);
        }

        return true;
    }

    /// <summary>
    /// Logic to subscribe to the specified symbols
    /// </summary>
    public bool SubscriptionLogic(IEnumerable<Symbol> symbols, TickType tickType)
    {
        if (_client?.IsConnected != true)
        {
            Log.Error("DataBentoProvider.SubscriptionLogic(): Client is not connected. Cannot subscribe to symbols");
            return false;
        }

        foreach (var symbol in symbols)
        {
            if (!CanSubscribe(symbol))
            {
                Log.Error($"DataBentoProvider.SubscriptionLogic(): Unsupported subscription: {symbol}");
                return false;
            }

            _client.Subscribe(symbol, tickType);
        }

        return true;
    }

    /// <summary>
    /// Checks if this Data provider supports the specified symbol
    /// </summary>
    /// <param name="symbol">The symbol</param>
    /// <returns>returns true if Data Provider supports the specified symbol; otherwise false</returns>
    private static bool CanSubscribe(Symbol symbol)
    {
        return !symbol.Value.Contains("universe", StringComparison.InvariantCultureIgnoreCase) &&
               !symbol.IsCanonical() &&
                symbol.SecurityType == SecurityType.Future;
    }

    /// <summary>
    /// Subscribe to the specified configuration
    /// </summary>
    /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
    /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
    /// <returns>The new enumerator for this subscription request</returns>
    public IEnumerator<BaseData>? Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
    {
        if (!IsSupported(dataConfig.SecurityType, dataConfig.Type, dataConfig.TickType, dataConfig.Resolution))
        {
            return null;
        }

        lock (_sessionLock)
        {
            if (!_sessionStarted)
            {
                Log.Debug("DataBentoProvider.SubscriptionLogic(): Starting session");
                _sessionStarted = _client.StartSession();
            }
        }

        var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
        _subscriptionManager.Subscribe(dataConfig);

        return enumerator;
    }

    /// <summary>
    /// Removes the specified configuration
    /// </summary>
    /// <param name="dataConfig">Subscription config to be removed</param>
    public void Unsubscribe(SubscriptionDataConfig dataConfig)
    {
        Log.Debug($"DataBentoProvider.Unsubscribe(): Received unsubscription request for {dataConfig.Symbol}, Resolution={dataConfig.Resolution}, TickType={dataConfig.TickType}");
        _subscriptionManager.Unsubscribe(dataConfig);
        _dataAggregator.Remove(dataConfig);
    }

    /// <summary>
    /// Sets the job we're subscribing for
    /// </summary>
    /// <param name="job">Job we're subscribing for</param>
    public void SetJob(LiveNodePacket job)
    {
        if (_initialized)
        {
            return;
        }
    }

    /// <summary>
    /// Dispose of unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        _dataAggregator?.DisposeSafely();
        _subscriptionManager?.DisposeSafely();
        _client?.DisposeSafely();
    }

    /// <summary>
    /// Determines if the specified subscription is supported
    /// </summary>
    private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
    {
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
        DateTimeZone exchangeTimeZone;
        lock (_symbolExchangeTimeZones)
        {
            if (!_symbolExchangeTimeZones.TryGetValue(symbol, out exchangeTimeZone))
            {
                // read the exchange time zone from market-hours-database
                if (_marketHoursDatabase.TryGetEntry(symbol.ID.Market, symbol, symbol.SecurityType, out var entry))
                {
                    exchangeTimeZone = entry.ExchangeHours.TimeZone;
                }
                // If there is no entry for the given Symbol, default to New York
                else
                {
                    exchangeTimeZone = TimeZones.NewYork;
                }

                _symbolExchangeTimeZones[symbol] = exchangeTimeZone;
            }
        }

        return utcTime.ConvertFromUtc(exchangeTimeZone);
    }

    /// <summary>
    /// Handles data received from the live client
    /// </summary>
    private void OnDataReceived(object _, BaseData data)
    {
        try
        {
            switch (data)
            {
                case Tick tick:
                    tick.Time = GetTickTime(tick.Symbol, tick.Time);
                    lock (_dataAggregator)
                    {
                        _dataAggregator.Update(tick);
                    }
                    // Log.Trace($"DataBentoProvider.OnDataReceived(): Updated tick - Symbol: {tick.Symbol}, " +
                    //         $"TickType: {tick.TickType}, Price: {tick.Value}, Quantity: {tick.Quantity}");
                    break;

                case TradeBar tradeBar:
                    tradeBar.Time = GetTickTime(tradeBar.Symbol, tradeBar.Time);
                    tradeBar.EndTime = GetTickTime(tradeBar.Symbol, tradeBar.EndTime);
                    lock (_dataAggregator)
                    {
                        _dataAggregator.Update(tradeBar);
                    }
                    // Log.Trace($"DataBentoProvider.OnDataReceived(): Updated TradeBar - Symbol: {tradeBar.Symbol}, " +
                    //         $"O:{tradeBar.Open} H:{tradeBar.High} L:{tradeBar.Low} C:{tradeBar.Close} V:{tradeBar.Volume}");
                    break;

                default:
                    data.Time = GetTickTime(data.Symbol, data.Time);
                    lock (_dataAggregator)
                    {
                        _dataAggregator.Update(data);
                    }
                    break;
            }
        }
        catch (Exception ex)
        {
            Log.Error($"DataBentoProvider.OnDataReceived(): Error updating data aggregator: {ex.Message}\n{ex.StackTrace}");
        }
    }
}
