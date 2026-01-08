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
using System.IO;
using System.Text;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// DataBento Raw TCP client for live streaming data
    /// </summary>
    public class DatabentoRawClient : IDisposable
    {
        private readonly string _apiKey;
        private readonly string _gateway;
        private readonly string _dataset;
        private TcpClient? _tcpClient;
        private NetworkStream? _stream;
        private StreamReader? _reader;
        private StreamWriter? _writer;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Symbol, (Resolution, TickType)> _subscriptions;
        private readonly object _connectionLock = new object();
        private bool _isConnected;
        private bool _disposed;
        private const decimal PriceScaleFactor = 1e-9m;
        private readonly ConcurrentDictionary<long, Symbol> _instrumentIdToSymbol = new ConcurrentDictionary<long, Symbol>();
        private readonly ConcurrentDictionary<Symbol, Tick> _lastTicks = new ConcurrentDictionary<Symbol, Tick>();

        /// <summary>
        /// Event fired when new data is received
        /// </summary>
        public event EventHandler<BaseData>? DataReceived;

        /// <summary>
        /// Event fired when connection status changes
        /// </summary>
        public event EventHandler<bool>? ConnectionStatusChanged;

        /// <summary>
        /// Event fired when a symbol mapping is received from Databento.
        /// This maps canonical futures symbols to specific contract symbols.
        /// </summary>
        public event EventHandler<SymbolMappingEventArgs>? SymbolMappingReceived;

        /// <summary>
        /// Gets whether the client is currently connected
        /// </summary>
        public bool IsConnected => _isConnected && _tcpClient?.Connected == true;

        /// <summary>
        /// Initializes a new instance of the DatabentoRawClient
        /// </summary>
        public DatabentoRawClient(string apiKey, string gateway = "glbx-mdp3.lsg.databento.com:13000", string dataset = "GLBX.MDP3")
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
            _dataset = dataset;
            _subscriptions = new ConcurrentDictionary<Symbol, (Resolution, TickType)>();
            _cancellationTokenSource = new CancellationTokenSource();
        }

        /// <summary>
        /// Connects to the DataBento live gateway
        /// </summary>
        public bool Connect()
        {
            Log.Trace("DatabentoRawClient.Connect(): Connecting to DataBento live gateway");
            if (_isConnected || _disposed)
            {
                return _isConnected;
            }

            try
            {
                var parts = _gateway.Split(':');
                var host = parts[0];
                var port = parts.Length > 1 ? int.Parse(parts[1]) : 13000;

                _tcpClient = new TcpClient();
                _tcpClient.Connect(host, port);
                _stream = _tcpClient.GetStream();
                _reader = new StreamReader(_stream, Encoding.ASCII);
                _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };

                // Perform authentication handshake
                if (Authenticate())
                {
                    _isConnected = true;
                    ConnectionStatusChanged?.Invoke(this, true);

                    // Start message processing
                    ProcessMessages();

                    Log.Trace("DatabentoRawClient.Connect(): Connected and authenticated to DataBento live gateway");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.Connect(): Failed to connect: {ex.Message}");
                Disconnect();
            }

            return false;
        }

        /// <summary>
        /// Authenticates with the DataBento gateway using CRAM-SHA256
        /// </summary>
        private bool Authenticate()
        {
            if (_reader == null || _writer == null)
                return false;

            try
            {
                // Read greeting and challenge
                string? versionLine = _reader.ReadLine();
                string? cramLine = _reader.ReadLine();

                if (string.IsNullOrEmpty(versionLine) || string.IsNullOrEmpty(cramLine))
                {
                    Log.Error("DatabentoRawClient.Authenticate(): Failed to receive greeting or challenge");
                    return false;
                }

                Log.Trace($"DatabentoRawClient.Authenticate(): Version: {versionLine}");
                Log.Trace($"DatabentoRawClient.Authenticate(): Challenge: {cramLine}");

                // Parse challenge
                string[] cramParts = cramLine.Split('=');
                if (cramParts.Length != 2 || cramParts[0] != "cram")
                {
                    Log.Error("DatabentoRawClient.Authenticate(): Invalid challenge format");
                    return false;
                }
                string cram = cramParts[1].Trim();

                // Compute auth hash
                string concat = $"{cram}|{_apiKey}";
                string hashHex = ComputeSHA256(concat);
                string bucketId = _apiKey.Length >= 5 ? _apiKey.Substring(_apiKey.Length - 5) : _apiKey;
                string authString = $"{hashHex}-{bucketId}";

                // Send auth message
                string authMsg = $"auth={authString}|dataset={_dataset}|encoding=json|ts_out=0";
                Log.Trace($"DatabentoRawClient.Authenticate(): Sending auth");
                _writer.WriteLine(authMsg);

                // Read auth response
                string? authResp = _reader.ReadLine();
                if (string.IsNullOrEmpty(authResp))
                {
                    Log.Error("DatabentoRawClient.Authenticate(): No authentication response received");
                    return false;
                }

                Log.Trace($"DatabentoRawClient.Authenticate(): Auth response: {authResp}");

                if (!authResp.Contains("success=1"))
                {
                    Log.Error($"DatabentoRawClient.Authenticate(): Authentication failed: {authResp}");
                    return false;
                }

                Log.Trace("DatabentoRawClient.Authenticate(): Authentication successful");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.Authenticate(): Authentication failed: {ex.Message}");
                return false;
            }
        }
        private static string ComputeSHA256(string input)
        {
            using var sha = SHA256.Create();
            byte[] hash = sha.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sb = new StringBuilder();
            foreach (byte b in hash)
            {
                sb.Append(b.ToString("x2"));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Sentinel value for full intraday replay (up to 24 hours).
        /// Pass this as the start parameter to get all available intraday data.
        /// Maps to Databento's start=0 convention.
        /// </summary>
        public static readonly DateTime FullReplay = DateTime.MinValue;

        /// <summary>
        /// Subscribes to live data for a symbol with optional intraday replay
        /// </summary>
        /// <param name="symbol">The symbol to subscribe to</param>
        /// <param name="resolution">Data resolution</param>
        /// <param name="tickType">Type of tick data</param>
        /// <param name="start">Optional start time for intraday historical replay.
        /// Use DateTime.MinValue (or FullReplay constant) for full intraday replay (start=0).
        /// Use a specific DateTime for replay from that time.
        /// Use null for live-only (no replay).</param>
        public bool Subscribe(Symbol symbol, Resolution resolution, TickType tickType, DateTime? start = null)
        {
            if (!IsConnected || _writer == null)
            {
                Log.Error("DatabentoRawClient.Subscribe(): Not connected to gateway");
                return false;
            }

            try
            {
                // Get the databento symbol form LEAN symbol
                // Get schema from the resolution
                var databentoSymbol = MapSymbolToDataBento(symbol);
                var schema = GetSchema(resolution, tickType);

                // Build subscription message with optional start time for intraday replay
                var subscribeMessage = $"schema={schema}|stype_in=parent|symbols={databentoSymbol}";

                // Append start parameter based on value:
                // - DateTime.MinValue (sentinel) -> start=0 (full intraday replay)
                // - Specific DateTime -> start=<ISO timestamp>
                // - null -> no start parameter (live only)
                if (start.HasValue)
                {
                    if (start.Value == DateTime.MinValue)
                    {
                        // Sentinel: DateTime.MinValue -> start=0 (full intraday replay, up to 24 hours)
                        subscribeMessage += "|start=0";
                        Log.Trace($"DatabentoRawClient.Subscribe(): Using FULL intraday replay (start=0)");
                    }
                    else
                    {
                        // Specific timestamp for partial replay
                        var startStr = start.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss");
                        subscribeMessage += $"|start={startStr}";
                        Log.Trace($"DatabentoRawClient.Subscribe(): Using intraday replay from {startStr}");
                    }
                }
                Log.Trace($"DatabentoRawClient.Subscribe(): Subscribing with message: {subscribeMessage}");

                // Send subscribe message
                _writer.WriteLine(subscribeMessage);

                // Store subscription
                _subscriptions.TryAdd(symbol, (resolution, tickType));
                Log.Trace($"DatabentoRawClient.Subscribe(): Subscribed to {symbol} ({databentoSymbol}) at {resolution} resolution for {tickType}");

                // Also subscribe to trade data for OHLCV bars (for bar generation)
                // This is needed because Quote data (mbp-1) doesn't produce TradeBars
                if (tickType == TickType.Quote)
                {
                    var tradeSchema = GetSchema(resolution, TickType.Trade);
                    var tradeSubscribeMessage = $"schema={tradeSchema}|stype_in=parent|symbols={databentoSymbol}";
                    if (start.HasValue)
                    {
                        if (start.Value == DateTime.MinValue)
                        {
                            tradeSubscribeMessage += "|start=0";
                        }
                        else
                        {
                            var startStr = start.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss");
                            tradeSubscribeMessage += $"|start={startStr}";
                        }
                    }
                    Log.Trace($"DatabentoRawClient.Subscribe(): Also subscribing to trades with message: {tradeSubscribeMessage}");
                    _writer.WriteLine(tradeSubscribeMessage);
                }

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.Subscribe(): Failed to subscribe to {symbol}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Starts the session to begin receiving data
        /// </summary>
        public bool StartSession()
        {
            if (!IsConnected || _writer == null)
            {
                Log.Error("DatabentoRawClient.StartSession(): Not connected");
                return false;
            }

            try
            {
                Log.Trace("DatabentoRawClient.StartSession(): Starting session");
                _writer.WriteLine("start_session=1");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.StartSession(): Failed to start session: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Unsubscribes from live data for a symbol
        /// </summary>
        public bool Unsubscribe(Symbol symbol)
        {
            try
            {
                if (_subscriptions.TryRemove(symbol, out _))
                {
                    Log.Trace($"DatabentoRawClient.Unsubscribe(): Unsubscribed from {symbol}");
                }
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.Unsubscribe(): Failed to unsubscribe from {symbol}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Processes incoming messages from the DataBento gateway
        /// </summary>
        private void ProcessMessages()
        {
            Log.Trace("DatabentoRawClient.ProcessMessages(): Starting message processing");
            if (_reader == null)
            {
                Log.Error("DatabentoRawClient.ProcessMessages(): No reader available");
                return;
            }

            var messageCount = 0;

            try
            {
                while (!_cancellationTokenSource.IsCancellationRequested && IsConnected)
                {
                    var line = _reader.ReadLine();
                    if (line == null)
                    {
                        Log.Trace("DatabentoRawClient.ProcessMessages(): Connection closed by server");
                        break;
                    }

                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    messageCount++;
                    if (messageCount <= 50 || messageCount % 100 == 0)
                    {
                        Log.Trace($"DatabentoRawClient.ProcessMessages(): Message #{messageCount}: {line.Substring(0, Math.Min(150, line.Length))}...");
                    }

                    ProcessSingleMessage(line);
                }
            }
            catch (OperationCanceledException)
            {
                Log.Trace("DatabentoRawClient.ProcessMessages(): Message processing cancelled");
            }
            catch (IOException ex) when (ex.InnerException is SocketException)
            {
                Log.Trace($"DatabentoRawClient.ProcessMessages(): Socket exception: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ProcessMessages(): Error processing messages: {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                Log.Trace($"DatabentoRawClient.ProcessMessages(): Exiting. Total messages processed: {messageCount}");
                Disconnect();
            }
        }

        /// <summary>
        /// Processes a single message from DataBento
        /// </summary>
        private void ProcessSingleMessage(string message)
        {
            try
            {
                using var document = JsonDocument.Parse(message);
                var root = document.RootElement;

                // Check for error messages
                if (root.TryGetProperty("hd", out var headerElement))
                {
                    if (headerElement.TryGetProperty("rtype", out var rtypeElement))
                    {
                        var rtype = rtypeElement.GetInt32();

                        if (rtype == 23)
                        {
                            if (root.TryGetProperty("msg", out var msgElement))
                            {
                                Log.Trace($"DatabentoRawClient: System message: {msgElement.GetString()}");
                            }
                            return;
                        }
                        else if (rtype == 22)
                        {
                            // Symbol mapping message - maps continuous (MYM.FUT) to specific contract (MYMH6)
                            if (root.TryGetProperty("stype_in_symbol", out var inSymbol) &&
                                root.TryGetProperty("stype_out_symbol", out var outSymbol) &&
                                headerElement.TryGetProperty("instrument_id", out var instId))
                            {
                                var instrumentId = instId.GetInt64();
                                var outSymbolStr = outSymbol.GetString();
                                var inSymbolStr = inSymbol.GetString();

                                Log.Trace($"DatabentoRawClient: Symbol mapping: {inSymbolStr} -> {outSymbolStr} (instrument_id: {instrumentId})");

                                // Find the canonical subscription that matches this symbol
                                Symbol? canonicalSymbol = null;
                                foreach (var kvp in _subscriptions)
                                {
                                    canonicalSymbol = kvp.Key;
                                    break; // Take the first subscription as canonical
                                }

                                if (canonicalSymbol != null && outSymbolStr != null)
                                {
                                    // Parse the Databento symbol to get a proper LEAN contract Symbol
                                    var contractSymbol = ParseDatabentoContractSymbol(canonicalSymbol, outSymbolStr);

                                    // Store the contract symbol (if parsed) or canonical (as fallback)
                                    // CRITICAL: Route data to contract symbol, not canonical, so algorithm
                                    // receives data on tradable contracts (e.g., MYMH6 not just /MYM)
                                    var symbolToStore = contractSymbol ?? canonicalSymbol;
                                    _instrumentIdToSymbol[instrumentId] = symbolToStore;

                                    Log.Trace($"DatabentoRawClient: Mapped instrument_id {instrumentId} to {symbolToStore}" +
                                              (contractSymbol != null ? $" (parsed from {outSymbolStr})" : " (canonical fallback)"));

                                    // Raise event so DataBentoProvider can track resolved contracts
                                    SymbolMappingReceived?.Invoke(this, new SymbolMappingEventArgs(
                                        canonicalSymbol,
                                        outSymbolStr,
                                        instrumentId,
                                        contractSymbol));
                                }
                            }
                            return;
                        }
                        else if (rtype == 1)
                        {
                            // MBP-1 (Market By Price) - Quote ticks
                            HandleMBPMessage(root, headerElement);
                            return;
                        }
                        else if (rtype == 0)
                        {
                            // Trade messages - Trade ticks
                            HandleTradeTickMessage(root, headerElement);
                            return;
                        }
                        else if (rtype == 32 || rtype == 33 || rtype == 34 || rtype == 35)
                        {
                            // OHLCV bar messages
                            HandleOHLCVMessage(root, headerElement);
                            return;
                        }
                    }
                }

                // Handle other message types if needed
                if (root.TryGetProperty("error", out var errorElement))
                {
                    Log.Error($"DatabentoRawClient: Server error: {errorElement.GetString()}");
                }
            }
            catch (JsonException ex)
            {
                Log.Error($"DatabentoRawClient.ProcessSingleMessage(): JSON parse error: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ProcessSingleMessage(): Error: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Handles OHLCV messages and converts to LEAN TradeBar data
        /// </summary>
        private void HandleOHLCVMessage(JsonElement root, JsonElement header)
        {
            try
            {
                if (!header.TryGetProperty("ts_event", out var tsElement) ||
                    !header.TryGetProperty("instrument_id", out var instIdElement))
                {
                    return;
                }

                // Convert timestamp from nanoseconds to DateTime
                var timestampNs = long.Parse(tsElement.GetString()!);
                var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                var timestamp = unixEpoch.AddTicks(timestampNs / 100);

                var instrumentId = instIdElement.GetInt64();

                if (!_instrumentIdToSymbol.TryGetValue(instrumentId, out var matchedSymbol))
                {
                    Log.Trace($"DatabentoRawClient: No mapping for instrument_id {instrumentId} in OHLCV message.");
                    return;
                }

                // Get the resolution for this symbol
                if (!_subscriptions.TryGetValue(matchedSymbol, out var subscription))
                {
                    return;
                }

                var resolution = subscription.Item1;

                // Extract OHLCV data
                if (root.TryGetProperty("open", out var openElement) &&
                    root.TryGetProperty("high", out var highElement) &&
                    root.TryGetProperty("low", out var lowElement) &&
                    root.TryGetProperty("close", out var closeElement) &&
                    root.TryGetProperty("volume", out var volumeElement))
                {
                    // Parse prices and volume (all are strings in Databento JSON)
                    var openRaw = long.Parse(openElement.GetString()!);
                    var highRaw = long.Parse(highElement.GetString()!);
                    var lowRaw = long.Parse(lowElement.GetString()!);
                    var closeRaw = long.Parse(closeElement.GetString()!);
                    var volume = long.Parse(volumeElement.GetString()!);

                    var open = openRaw * PriceScaleFactor;
                    var high = highRaw * PriceScaleFactor;
                    var low = lowRaw * PriceScaleFactor;
                    var close = closeRaw * PriceScaleFactor;

                    // Determine the period based on resolution
                    TimeSpan period = resolution switch
                    {
                        Resolution.Second => TimeSpan.FromSeconds(1),
                        Resolution.Minute => TimeSpan.FromMinutes(1),
                        Resolution.Hour => TimeSpan.FromHours(1),
                        Resolution.Daily => TimeSpan.FromDays(1),
                        _ => TimeSpan.FromMinutes(1)
                    };

                    // Create TradeBar
                    var tradeBar = new TradeBar(
                        timestamp,
                        matchedSymbol,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        period
                    );

                    // OHLCV bars are now only used for historical replay, not live data
                    // Live data uses trades schema which gets consolidated by LEAN's aggregator
                    Log.Trace($"DatabentoRawClient: OHLCV bar (historical replay): {matchedSymbol} O={open} H={high} L={low} C={close} V={volume} at {timestamp}");
                    DataReceived?.Invoke(this, tradeBar);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.HandleOHLCVMessage(): Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles MBP messages for quote ticks
        /// </summary>
        private void HandleMBPMessage(JsonElement root, JsonElement header)
        {
            try
            {
                if (!header.TryGetProperty("ts_event", out var tsElement) ||
                    !header.TryGetProperty("instrument_id", out var instIdElement))
                {
                    return;
                }

                // Convert timestamp from nanoseconds to DateTime
                var timestampNs = long.Parse(tsElement.GetString()!);
                var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                var timestamp = unixEpoch.AddTicks(timestampNs / 100);

                var instrumentId = instIdElement.GetInt64();

                if (!_instrumentIdToSymbol.TryGetValue(instrumentId, out var matchedSymbol))
                {
                    Log.Trace($"DatabentoRawClient: No mapping for instrument_id {instrumentId} in MBP message.");
                    return;
                }

                // For MBP-1, bid/ask data is in the levels array at index 0
                if (root.TryGetProperty("levels", out var levelsElement) && 
                    levelsElement.GetArrayLength() > 0)
                {
                    var level0 = levelsElement[0];
                    
                    var quoteTick = new Tick
                    {
                        Symbol = matchedSymbol,
                        Time = timestamp,
                        TickType = TickType.Quote
                    };
                    
                    if (level0.TryGetProperty("ask_px", out var askPxElement) &&
                        level0.TryGetProperty("ask_sz", out var askSzElement))
                    {
                        var askPriceRaw = long.Parse(askPxElement.GetString()!);
                        quoteTick.AskPrice = askPriceRaw * PriceScaleFactor;
                        quoteTick.AskSize = askSzElement.GetInt32();
                    }
                    
                    if (level0.TryGetProperty("bid_px", out var bidPxElement) &&
                        level0.TryGetProperty("bid_sz", out var bidSzElement))
                    {
                        var bidPriceRaw = long.Parse(bidPxElement.GetString()!);
                        quoteTick.BidPrice = bidPriceRaw * PriceScaleFactor;
                        quoteTick.BidSize = bidSzElement.GetInt32();
                    }
                    
                    // Set the tick value to the mid price
                    quoteTick.Value = (quoteTick.BidPrice + quoteTick.AskPrice) / 2;
                    
                    // QuantConnect convention: Quote ticks should have zero Price and Quantity
                    quoteTick.Quantity = 0;
                    
                    // Log.Trace removed - too spammy (millions of ticks)
                    DataReceived?.Invoke(this, quoteTick);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.HandleMBPMessage(): Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles trade tick messages. Aggressor fills
        /// </summary>
        private void HandleTradeTickMessage(JsonElement root, JsonElement header)
        {
            try
            {
                if (!header.TryGetProperty("ts_event", out var tsElement) ||
                    !header.TryGetProperty("instrument_id", out var instIdElement))
                {
                    return;
                }

                // Convert timestamp from nanoseconds to DateTime
                var timestampNs = long.Parse(tsElement.GetString()!);
                var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                var timestamp = unixEpoch.AddTicks(timestampNs / 100);

                var instrumentId = instIdElement.GetInt64();

                if (!_instrumentIdToSymbol.TryGetValue(instrumentId, out var matchedSymbol))
                {
                    Log.Trace($"DatabentoRawClient: No mapping for instrument_id {instrumentId} in trade message.");
                    return;
                }

                if (root.TryGetProperty("price", out var priceElement) &&
                    root.TryGetProperty("size", out var sizeElement))
                {
                    var priceRaw = long.Parse(priceElement.GetString()!);
                    var size = sizeElement.GetInt32();
                    var price = priceRaw * PriceScaleFactor;

                    var tradeTick = new Tick
                    {
                        Symbol = matchedSymbol,
                        Time = timestamp,
                        Value = price,
                        Quantity = size,
                        TickType = TickType.Trade,
                        // Trade ticks should have zero bid/ask values
                        BidPrice = 0,
                        BidSize = 0,
                        AskPrice = 0,
                        AskSize = 0
                    };

                    // Log.Trace removed - too spammy (millions of ticks)
                    DataReceived?.Invoke(this, tradeTick);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.HandleTradeTickMessage(): Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Maps a LEAN symbol to DataBento symbol format
        /// </summary>
        private string MapSymbolToDataBento(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                string root;

                // Check if this is a canonical (continuous) symbol like "/YM"
                if (symbol.IsCanonical())
                {
                    // Canonical symbol - use ROOT.FUT format for parent subscription
                    // Databento will resolve to front month automatically
                    root = symbol.ID.Symbol; // e.g., "YM" from canonical "/YM"
                    Log.Trace($"DatabentoRawClient.MapSymbolToDataBento(): Canonical symbol {symbol} mapped to {root}.FUT");
                    return $"{root}.FUT";
                }

                // Specific contract symbol like "ES19Z25" - extract root
                var value = symbol.Value;
                root = new string(value.TakeWhile(c => !char.IsDigit(c)).ToArray());

                // Use parent subscription format for specific contracts too
                return $"{root}.FUT";
            }

            return symbol.Value;
        }

        /// <summary>
        /// CME month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
        /// </summary>
        private static readonly Dictionary<char, int> MonthCodeToMonth = new()
        {
            { 'F', 1 }, { 'G', 2 }, { 'H', 3 }, { 'J', 4 }, { 'K', 5 }, { 'M', 6 },
            { 'N', 7 }, { 'Q', 8 }, { 'U', 9 }, { 'V', 10 }, { 'X', 11 }, { 'Z', 12 }
        };

        /// <summary>
        /// CME quarterly month codes: H=Mar, M=Jun, U=Sep, Z=Dec
        /// </summary>
        private static readonly char[] QuarterlyMonthCodes = { 'H', 'M', 'U', 'Z' };
        private static readonly int[] QuarterlyMonths = { 3, 6, 9, 12 };

        /// <summary>
        /// Parses a Databento contract symbol (e.g., "MYMH6") into a LEAN Symbol.
        /// Format: ROOT + MONTH_CODE + YEAR (1 or 2 digits)
        /// Examples: MYMH6 = MYM March 2026, ESZ25 = ES December 2025
        /// </summary>
        /// <param name="canonicalSymbol">The canonical LEAN symbol for market reference</param>
        /// <param name="databentoSymbol">The Databento symbol (e.g., "MYMH6")</param>
        /// <returns>A LEAN Symbol for the specific contract, or null if parsing fails</returns>
        public Symbol? ParseDatabentoContractSymbol(Symbol canonicalSymbol, string databentoSymbol)
        {
            if (string.IsNullOrEmpty(databentoSymbol))
                return null;

            // Skip spread symbols (contain "-")
            if (databentoSymbol.Contains('-'))
            {
                Log.Trace($"DatabentoRawClient.ParseDatabentoContractSymbol(): Skipping spread symbol: {databentoSymbol}");
                return null;
            }

            try
            {
                // Parse format: ROOT + MONTH_CODE + YEAR (e.g., MYMH6, ESZ25)
                // The month code is a single letter from the set: F,G,H,J,K,M,N,Q,U,V,X,Z
                // We need to find the month code by searching from the end, since the root
                // symbol can contain letters that match month codes (e.g., MYM contains M)

                // Find where the digits start from the end
                int digitStartIndex = databentoSymbol.Length;
                while (digitStartIndex > 0 && char.IsDigit(databentoSymbol[digitStartIndex - 1]))
                {
                    digitStartIndex--;
                }

                if (digitStartIndex == databentoSymbol.Length || digitStartIndex < 2)
                {
                    Log.Trace($"DatabentoRawClient.ParseDatabentoContractSymbol(): Invalid format (no year digits or too short): {databentoSymbol}");
                    return null;
                }

                // Month code is the character just before the digits
                var monthCodeIndex = digitStartIndex - 1;
                var monthCode = char.ToUpper(databentoSymbol[monthCodeIndex]);
                if (!MonthCodeToMonth.TryGetValue(monthCode, out var month))
                {
                    Log.Error($"DatabentoRawClient.ParseDatabentoContractSymbol(): Unknown month code '{monthCode}' in {databentoSymbol}");
                    return null;
                }

                // Root is everything before the month code
                var root = databentoSymbol.Substring(0, monthCodeIndex);
                if (string.IsNullOrEmpty(root))
                {
                    Log.Trace($"DatabentoRawClient.ParseDatabentoContractSymbol(): Empty root in {databentoSymbol}");
                    return null;
                }

                // Year is the digits at the end
                var yearStr = databentoSymbol.Substring(digitStartIndex);
                if (!int.TryParse(yearStr, out var yearDigits))
                {
                    Log.Error($"DatabentoRawClient.ParseDatabentoContractSymbol(): Invalid year '{yearStr}' in {databentoSymbol}");
                    return null;
                }

                // Convert to full year (6 -> 2026, 25 -> 2025)
                int year;
                if (yearDigits < 100)
                {
                    // Assume 20xx for 2-digit years, handle century boundary
                    year = yearDigits < 50 ? 2000 + yearDigits : 1900 + yearDigits;
                    if (yearDigits < 10)
                    {
                        // Single digit like "6" means 2026
                        year = 2020 + yearDigits;
                    }
                }
                else
                {
                    year = yearDigits;
                }

                // Calculate expiry date (3rd Friday of contract month for index futures)
                var firstDay = new DateTime(year, month, 1);
                var firstFriday = firstDay.AddDays((DayOfWeek.Friday - firstDay.DayOfWeek + 7) % 7);
                var thirdFriday = firstFriday.AddDays(14);

                // Create the LEAN Symbol
                var market = canonicalSymbol.ID.Market;
                var contractSymbol = Symbol.CreateFuture(root, market, thirdFriday);

                Log.Trace($"DatabentoRawClient.ParseDatabentoContractSymbol(): Parsed {databentoSymbol} -> {contractSymbol} (expiry: {thirdFriday:yyyy-MM-dd})");
                return contractSymbol;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ParseDatabentoContractSymbol(): Error parsing {databentoSymbol}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Gets the front-month contract symbol for quarterly futures (ES, YM, NQ, etc.)
        /// CME index futures expire on 3rd Friday of contract month
        /// </summary>
        private string GetFrontMonthContract(string root, DateTime now)
        {
            // Find the next quarterly expiry that hasn't passed
            // Roll to next contract ~1 week before expiry for safety
            var rollBuffer = TimeSpan.FromDays(7);

            for (int yearOffset = 0; yearOffset <= 1; yearOffset++)
            {
                var year = now.Year + yearOffset;
                var yearCode = (year % 100).ToString("D2"); // "25" for 2025

                foreach (var i in Enumerable.Range(0, 4))
                {
                    var month = QuarterlyMonths[i];
                    var monthCode = QuarterlyMonthCodes[i];

                    // Skip months in previous year iterations
                    if (yearOffset == 0 && month < now.Month)
                        continue;

                    // Calculate 3rd Friday of the contract month (approximate expiry)
                    var firstDay = new DateTime(year, month, 1);
                    var firstFriday = firstDay.AddDays((DayOfWeek.Friday - firstDay.DayOfWeek + 7) % 7);
                    var thirdFriday = firstFriday.AddDays(14);
                    var rollDate = thirdFriday - rollBuffer;

                    // If we're past the roll date, skip to next contract
                    if (now > rollDate)
                        continue;

                    // Found the front month contract
                    // Format: YMH25 (root + month code + 2-digit year)
                    return $"{root}{monthCode}{yearCode}";
                }
            }

            // Fallback - shouldn't happen
            var fallbackYear = (now.Year % 100).ToString("D2");
            return $"{root}H{fallbackYear}";
        }

        /// <summary>
        /// Pick Databento schema from Lean resolution/ticktype
        /// For live streaming, we always use tick-level data (trades, mbp-1) because
        /// the LEAN aggregator expects to consolidate tick data into bars.
        /// OHLCV schemas are only appropriate for historical data requests.
        /// </summary>
        private string GetSchema(Resolution resolution, TickType tickType)
        {
            if (tickType == TickType.Trade)
            {
                // Always use trades schema for live data - the aggregator will consolidate
                // This ensures proper data flow through LEAN's consolidation pipeline
                return "trades";
            }
            else if (tickType == TickType.Quote)
            {
                // top of book - mbp-1 provides tick-level quote data
                return "mbp-1";
            }
            else if (tickType == TickType.OpenInterest)
            {
                return "statistics";
            }

            throw new NotSupportedException($"Unsupported resolution {resolution} / {tickType}");
        }

        /// <summary>
        /// Disconnects from the DataBento gateway
        /// </summary>
        public void Disconnect()
        {
            lock (_connectionLock)
            {
                if (!_isConnected)
                    return;

                _isConnected = false;
                _cancellationTokenSource?.Cancel();

                try
                {
                    _reader?.Dispose();
                    _writer?.Dispose();
                    _stream?.Close();
                    _tcpClient?.Close();
                }
                catch (Exception ex)
                {
                    Log.Trace($"DatabentoRawClient.Disconnect(): Error during disconnect: {ex.Message}");
                }

                ConnectionStatusChanged?.Invoke(this, false);
                Log.Trace("DatabentoRawClient.Disconnect(): Disconnected from DataBento gateway");
            }
        }

        /// <summary>
        /// Disposes of resources
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            Disconnect();

            _cancellationTokenSource?.Dispose();
            _reader?.Dispose();
            _writer?.Dispose();
            _stream?.Dispose();
            _tcpClient?.Dispose();
        }
    }

    /// <summary>
    /// Event args for symbol mapping messages from Databento.
    /// Contains the mapping from canonical symbol to resolved contract.
    /// </summary>
    public class SymbolMappingEventArgs : EventArgs
    {
        /// <summary>
        /// The canonical LEAN symbol that was subscribed (e.g., /MYM)
        /// </summary>
        public Symbol CanonicalSymbol { get; }

        /// <summary>
        /// The resolved contract symbol from Databento (e.g., MYMH6)
        /// </summary>
        public string DatabentoSymbol { get; }

        /// <summary>
        /// The Databento instrument ID for this contract
        /// </summary>
        public long InstrumentId { get; }

        /// <summary>
        /// The resolved LEAN contract Symbol with proper expiry
        /// </summary>
        public Symbol? ContractSymbol { get; }

        public SymbolMappingEventArgs(Symbol canonicalSymbol, string databentoSymbol, long instrumentId, Symbol? contractSymbol)
        {
            CanonicalSymbol = canonicalSymbol;
            DatabentoSymbol = databentoSymbol;
            InstrumentId = instrumentId;
            ContractSymbol = contractSymbol;
        }
    }
}
