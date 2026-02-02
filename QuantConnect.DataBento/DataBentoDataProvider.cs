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

using System.Net;
using System.Text;
using Newtonsoft.Json;
using QuantConnect.Api;
using QuantConnect.Util;
using QuantConnect.Data;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Interfaces;
using QuantConnect.Configuration;
using System.Security.Cryptography;
using System.Net.NetworkInformation;
using System.Collections.Concurrent;
using QuantConnect.Brokerages.LevelOneOrderBook;
using QuantConnect.Lean.DataSource.DataBento.Api;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Events;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// A data Provider for DataBento that provides live market data and historical data.
/// Handles Subscribing, Unsubscribing, and fetching historical data from DataBento.
/// It will handle if a symbol is subscribable and will log errors if it is not.
/// </summary>
public partial class DataBentoProvider : IDataQueueHandler
{
    private HistoricalAPIClient _historicalApiClient;

    private readonly DataBentoSymbolMapper _symbolMapper = new();

    private readonly ConcurrentDictionary<string, Symbol> _pendingSubscriptions = [];

    private readonly Dictionary<uint, Symbol> _subscribedSymbolsByDataBentoInstrumentId = [];

    /// <summary>
    /// Manages Level 1 market data subscriptions and routing of updates to the shared <see cref="IDataAggregator"/>.
    /// Responsible for tracking and updating individual <see cref="LevelOneMarketData"/> instances per symbol.
    /// </summary>
    private LevelOneServiceManager _levelOneServiceManager;

    private IDataAggregator _aggregator;

    private LiveAPIClient _liveApiClient;

    private bool _initialized;

    /// <summary>
    /// Returns true if we're currently connected to the Data Provider
    /// </summary>
    public bool IsConnected => _liveApiClient.IsConnected;


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
        ValidateSubscription();

        _aggregator = Composer.Instance.GetPart<IDataAggregator>();
        if (_aggregator == null)
        {
            var aggregatorName = Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager");
            Log.Trace($"{nameof(DataBentoProvider)}.{nameof(Initialize)}: found no data aggregator instance, creating {aggregatorName}");
            _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(aggregatorName, forceTypeNameOnExisting: false);
        }

        _liveApiClient = new LiveAPIClient(apiKey, HandleLevelOneData);
        _liveApiClient.SymbolMappingConfirmation += OnSymbolMappingConfirmation;
        _liveApiClient.ConnectionLost += OnConnectionLost;

        _historicalApiClient = new(apiKey);

        _levelOneServiceManager = new LevelOneServiceManager(
            _aggregator,
            (symbols, _) => Subscribe(symbols),
            (symbols, _) => Unsubscribe(symbols));

        _initialized = true;
    }

    private void OnConnectionLost(object? _, ConnectionLostEventArgs cle)
    {
        Log.Trace($"{nameof(DataBentoProvider)}.{nameof(OnConnectionLost)}: The connection was lost. Starting ReSubscription process");

        var symbols = _levelOneServiceManager.GetSubscribedSymbols();

        Subscribe(symbols);

        Log.Trace($"{nameof(DataBentoProvider)}.{nameof(OnConnectionLost)}: Re-subscription completed successfully for {_levelOneServiceManager.Count} symbol(s).");
    }

    private void OnSymbolMappingConfirmation(object? _, SymbolMappingConfirmationEventArgs smce)
    {
        if (_pendingSubscriptions.TryRemove(smce.Symbol, out var symbol))
        {
            _subscribedSymbolsByDataBentoInstrumentId[smce.InstrumentId] = symbol;
        }
    }

    private void HandleLevelOneData(LevelOneData levelOneData)
    {
        if (_subscribedSymbolsByDataBentoInstrumentId.TryGetValue(levelOneData.Header.InstrumentId, out var symbol))
        {
            var time = levelOneData.Header.UtcTime;

            _levelOneServiceManager.HandleLastTrade(symbol, time, levelOneData.Size, levelOneData.Price);

            foreach (var l in levelOneData.Levels)
            {
                _levelOneServiceManager.HandleQuote(symbol, time, l.BidPx, l.BidSz, l.AskPx, l.AskSz);
            }
        }
    }

    /// <summary>
    /// Logic to subscribe to the specified symbols
    /// </summary>
    public bool Subscribe(IEnumerable<Symbol> symbols)
    {
        foreach (var symbol in symbols)
        {
            if (!_symbolMapper.DataBentoDataSetByLeanMarket.TryGetValue(symbol.ID.Market, out var dataSetSpecifications))
            {
                throw new ArgumentException($"No DataBento dataset mapping found for symbol {symbol} in market {symbol.ID.Market}. Cannot subscribe.");
            }

            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            _pendingSubscriptions[brokerageSymbol] = symbol;

            _liveApiClient.Subscribe(dataSetSpecifications.DataSetID, brokerageSymbol);
        }

        return true;
    }

    public bool Unsubscribe(IEnumerable<Symbol> symbols)
    {
        // Please note there is no unsubscribe method. Subscriptions end when the TCP connection closes.

        var symbolsToRemove = symbols.ToHashSet();

        foreach (var (instrumentId, symbol) in _subscribedSymbolsByDataBentoInstrumentId)
        {
            if (symbolsToRemove.Contains(symbol))
            {
                _subscribedSymbolsByDataBentoInstrumentId.Remove(instrumentId);
            }
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
        if (!CanSubscribe(dataConfig.Symbol))
        {
            return null;
        }

        var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
        _levelOneServiceManager.Subscribe(dataConfig);

        return enumerator;
    }

    /// <summary>
    /// Removes the specified configuration
    /// </summary>
    /// <param name="dataConfig">Subscription config to be removed</param>
    public void Unsubscribe(SubscriptionDataConfig dataConfig)
    {
        _levelOneServiceManager.Unsubscribe(dataConfig);
        _aggregator.Remove(dataConfig);
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

        if (!job.BrokerageData.TryGetValue("databento-api-key", out var apiKey) || string.IsNullOrWhiteSpace(apiKey))
        {
            throw new ArgumentException("The DataBento API key is missing from the brokerage data.");
        }

        Initialize(apiKey);
    }

    /// <summary>
    /// Dispose of unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        _levelOneServiceManager?.DisposeSafely();
        _aggregator?.DisposeSafely();
        _liveApiClient?.DisposeSafely();
        _historicalApiClient?.DisposeSafely();
    }

    private class ModulesReadLicenseRead : RestResponse
    {
        [JsonProperty(PropertyName = "license")]
        public string License;

        [JsonProperty(PropertyName = "organizationId")]
        public string OrganizationId;
    }

    /// <summary>
    /// Validate the user of this project has permission to be using it via our web API.
    /// </summary>
    private static void ValidateSubscription()
    {
        try
        {
            const int productId = 306;
            var userId = Globals.UserId;
            var token = Globals.UserToken;
            var organizationId = Globals.OrganizationID;
            // Verify we can authenticate with this user and token
            var api = new ApiConnection(userId, token);
            if (!api.Connected)
            {
                throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
            }
            // Compile the information we want to send when validating
            var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
            // IP and Mac Address Information
            try
            {
                var interfaceDictionary = new List<Dictionary<string, object>>();
                foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                {
                    var interfaceInformation = new Dictionary<string, object>();
                    // Get UnicastAddresses
                    var addresses = nic.GetIPProperties().UnicastAddresses
                        .Select(uniAddress => uniAddress.Address)
                        .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                    // If this interface has non-loopback addresses, we will include it
                    if (!addresses.IsNullOrEmpty())
                    {
                        interfaceInformation.Add("unicastAddresses", addresses);
                        // Get MAC address
                        interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                        // Add Interface name
                        interfaceInformation.Add("name", nic.Name);
                        // Add these to our dictionary
                        interfaceDictionary.Add(interfaceInformation);
                    }
                }
                information.Add("networkInterfaces", interfaceDictionary);
            }
            catch (Exception)
            {
                // NOP, not necessary to crash if fails to extract and add this information
            }
            // Include our OrganizationId if specified
            if (!string.IsNullOrEmpty(organizationId))
            {
                information.Add("organizationId", organizationId);
            }

            // Create HTTP request
            using var request = ApiUtils.CreateJsonPostRequest("modules/license/read", information);

            api.TryRequest(request, out ModulesReadLicenseRead result);
            if (!result.Success)
            {
                throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
            }

            var encryptedData = result.License;
            // Decrypt the data we received
            DateTime? expirationDate = null;
            long? stamp = null;
            bool? isValid = null;
            if (encryptedData != null)
            {
                // Fetch the org id from the response if it was not set, we need it to generate our validation key
                if (string.IsNullOrEmpty(organizationId))
                {
                    organizationId = result.OrganizationId;
                }
                // Create our combination key
                var password = $"{token}-{organizationId}";
                var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                // Split the data
                var info = encryptedData.Split("::");
                var buffer = Convert.FromBase64String(info[0]);
                var iv = Convert.FromBase64String(info[1]);
                // Decrypt our information
                using var aes = new AesManaged();
                var decryptor = aes.CreateDecryptor(key, iv);
                using var memoryStream = new MemoryStream(buffer);
                using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                using var streamReader = new StreamReader(cryptoStream);
                var decryptedData = streamReader.ReadToEnd();
                if (!decryptedData.IsNullOrEmpty())
                {
                    var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                    expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                    isValid = jsonInfo["isValid"]?.Value<bool>();
                    stamp = jsonInfo["stamped"]?.Value<int>();
                }
            }
            // Validate our conditions
            if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
            {
                throw new InvalidOperationException("Failed to validate subscription.");
            }

            var nowUtc = DateTime.UtcNow;
            var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
            if (timeSpan > TimeSpan.FromHours(12))
            {
                throw new InvalidOperationException("Invalid API response.");
            }
            if (!isValid.Value)
            {
                throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
            }
            if (expirationDate < nowUtc)
            {
                throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
            }
        }
        catch (Exception e)
        {
            Log.Error($"PolygonDataProvider.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
            throw;
        }
    }
}
