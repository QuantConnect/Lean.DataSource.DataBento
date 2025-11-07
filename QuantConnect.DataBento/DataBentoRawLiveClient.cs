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
        /// Subscribes to live data for a symbol
        /// </summary>
        public bool Subscribe(Symbol symbol, Resolution resolution, TickType tickType)
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

                // subscribe
                var subscribeMessage = $"schema={schema}|stype_in=parent|symbols={databentoSymbol}";
                Log.Trace($"DatabentoRawClient.Subscribe(): Subscribing with message: {subscribeMessage}");

                // Send subscribe message
                _writer.WriteLine(subscribeMessage);

                // Store subscription
                _subscriptions.TryAdd(symbol, (resolution, tickType));
                Log.Trace($"DatabentoRawClient.Subscribe(): Subscribed to {symbol} ({databentoSymbol}) at {resolution} resolution for {tickType}");

                // If subscribing to quote ticks, also subscribe to trade ticks
                if (tickType == TickType.Quote && resolution == Resolution.Tick)
                {
                    var tradeSchema = GetSchema(resolution, TickType.Trade);
                    var tradeSubscribeMessage = $"schema={tradeSchema}|stype_in=parent|symbols={databentoSymbol}";
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
                            // Symbol mapping message
                            if (root.TryGetProperty("stype_in_symbol", out var inSymbol) &&
                                root.TryGetProperty("stype_out_symbol", out var outSymbol) &&
                                headerElement.TryGetProperty("instrument_id", out var instId))
                            {
                                var instrumentId = instId.GetInt64();
                                var outSymbolStr = outSymbol.GetString();

                                Log.Trace($"DatabentoRawClient: Symbol mapping: {inSymbol.GetString()} -> {outSymbolStr} (instrument_id: {instrumentId})");

                                // Find the subscription that matches this symbol
                                foreach (var kvp in _subscriptions)
                                {
                                    var leanSymbol = kvp.Key;
                                    if (outSymbolStr != null)
                                    {
                                        _instrumentIdToSymbol[instrumentId] = leanSymbol;
                                        Log.Trace($"DatabentoRawClient: Mapped instrument_id {instrumentId} to {leanSymbol}");
                                        break;
                                    }
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
                    // Parse prices
                    var openRaw = long.Parse(openElement.GetString()!);
                    var highRaw = long.Parse(highElement.GetString()!);
                    var lowRaw = long.Parse(lowElement.GetString()!);
                    var closeRaw = long.Parse(closeElement.GetString()!);
                    var volume = volumeElement.GetInt64();

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

                    Log.Trace($"DatabentoRawClient: OHLCV bar: {matchedSymbol} O={open} H={high} L={low} C={close} V={volume} at {timestamp}");
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
                    
                    Log.Trace($"DatabentoRawClient: Quote tick: {matchedSymbol} Bid={quoteTick.BidPrice}x{quoteTick.BidSize} Ask={quoteTick.AskPrice}x{quoteTick.AskSize}");
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

                    Log.Trace($"DatabentoRawClient: Trade tick: {matchedSymbol} Price={price} Quantity={size}");
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
                // For DataBento, use the root symbol with .FUT suffix for parent subscription
                // ES19Z25 -> ES.FUT
                var value = symbol.Value;
                
                // Extract root by removing digits and month codes
                var root = new string(value.TakeWhile(c => !char.IsDigit(c)).ToArray());
                
                return $"{root}.FUT";
            }

            return symbol.Value;
        }

        /// <summary>
        /// Pick Databento schema from Lean resolution/ticktype
        /// </summary>
        private string GetSchema(Resolution resolution, TickType tickType)
        {
            if (tickType == TickType.Trade)
            {
                if (resolution == Resolution.Tick)
                    return "trades";
                if (resolution == Resolution.Second)
                    return "ohlcv-1s";
                if (resolution == Resolution.Minute)
                    return "ohlcv-1m";
                if (resolution == Resolution.Hour)
                    return "ohlcv-1h";
                if (resolution == Resolution.Daily)
                    return "ohlcv-1d";
            }
            else if (tickType == TickType.Quote)
            {
                // top of book
                if (resolution == Resolution.Tick || resolution == Resolution.Second || resolution == Resolution.Minute || resolution == Resolution.Hour || resolution == Resolution.Daily)
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
}
