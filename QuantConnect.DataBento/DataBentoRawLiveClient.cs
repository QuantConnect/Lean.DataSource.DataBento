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

using System.Text;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Tasks;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// DataBento Raw TCP client for live streaming data
    /// </summary>
    public class DataBentoRawLiveClient : IDisposable
    {
        /// <summary>
        /// The DataBento API key for authentication
        /// </summary>
        private readonly string _apiKey;
        /// <summary>
        /// The DataBento live gateway address to receive data from
        /// </summary>
        private const string _gateway = "glbx-mdp3.lsg.databento.com:13000";
        /// <summary> 
        /// The dataset to subscribe to 
        /// </summary>
        private readonly string _dataset;
        private readonly TcpClient? _tcpClient;
        private readonly string _host;
        private readonly int _port;
        private NetworkStream? _stream;
        private StreamReader _reader;
        private StreamWriter _writer;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Symbol, (Resolution, TickType)> _subscriptions;
        private readonly object _connectionLock = new object();
        private bool _isConnected;
        private bool _disposed;
        private const decimal PriceScaleFactor = 1e-9m;
        private readonly ConcurrentDictionary<long, Symbol> _instrumentIdToSymbol = new ConcurrentDictionary<long, Symbol>();
        private readonly DataBentoSymbolMapper _symbolMapper;

        /// <summary>
        /// Event fired when new data is received
        /// </summary>
        public event EventHandler<BaseData> DataReceived;

        /// <summary>
        /// Event fired when connection status changes
        /// </summary>
        public event EventHandler<bool> ConnectionStatusChanged;

        /// <summary>
        /// Gets whether the client is currently connected
        /// </summary>
        public bool IsConnected => _isConnected && _tcpClient?.Connected == true;

        /// <summary>
        /// Initializes a new instance of the DataBentoRawLiveClient
        /// <param name="apiKey">The DataBento API key.</param>
        /// </summary>
        public DataBentoRawLiveClient(string apiKey, string dataset = "GLBX.MDP3")
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _dataset = dataset;
            _tcpClient = new TcpClient();
            _subscriptions = new ConcurrentDictionary<Symbol, (Resolution, TickType)>();
            _cancellationTokenSource = new CancellationTokenSource();
            _symbolMapper = new DataBentoSymbolMapper();

            var parts = _gateway.Split(':');
            _host = parts[0];
            _port = parts.Length > 1 ? int.Parse(parts[1]) : 13000;
        }

        /// <summary>
        /// Connects to the DataBento live gateway
        /// </summary>
        public bool Connect()
        {
            Log.Trace("DataBentoRawLiveClient.Connect(): Connecting to DataBento live gateway");
            if (_isConnected)
            {
                return _isConnected;
            }

            try
            {
                _tcpClient.Connect(_host, _port);
                _stream = _tcpClient.GetStream();
                _reader = new StreamReader(_stream, Encoding.ASCII);
                _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };

                // Perform authentication handshake
                if (Authenticate())
                {
                    _isConnected = true;
                    ConnectionStatusChanged?.Invoke(this, true);

                    // Start message processing
                    Task.Run(ProcessMessages, _cancellationTokenSource.Token);

                    Log.Trace("DataBentoRawLiveClient.Connect(): Connected and authenticated to DataBento live gateway");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.Connect(): Failed to connect: {ex.Message}");
                Disconnect();
            }

            return false;
        }

        /// <summary>
        /// Authenticates with the DataBento gateway using CRAM-SHA256
        /// </summary>
        private bool Authenticate()
        {
            try
            {
                // Read greeting and challenge
                var versionLine = _reader.ReadLine();
                var cramLine = _reader.ReadLine();

                if (string.IsNullOrEmpty(versionLine) || string.IsNullOrEmpty(cramLine))
                {
                    Log.Error("DataBentoRawLiveClient.Authenticate(): Failed to receive greeting or challenge");
                    return false;
                }

                // Parse challenge
                var cramParts = cramLine.Split('=');
                if (cramParts.Length != 2 || cramParts[0] != "cram")
                {
                    Log.Error("DataBentoRawLiveClient.Authenticate(): Invalid challenge format");
                    return false;
                }
                var cram = cramParts[1].Trim();

                // Auth
                _writer.WriteLine($"auth={GetAuthStringFromCram(cram)}|dataset={_dataset}|encoding=json|ts_out=0");
                var authResp = _reader.ReadLine();
                if (!authResp.Contains("success=1"))
                {
                    Log.Error($"DataBentoRawLiveClient.Authenticate(): Authentication failed: {authResp}");
                    return false;
                }

                Log.Trace("DataBentoRawLiveClient.Authenticate(): Authentication successful");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.Authenticate(): Authentication failed: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Handles the DataBento authentication string from a CRAM challenge
        /// </summary>
        /// <param name="cram">The CRAM challenge string</param>
        /// <returns>The auth string to send to the server</returns>
        private string GetAuthStringFromCram(string cram)
        {
            if (string.IsNullOrWhiteSpace(cram))
                throw new ArgumentException("CRAM challenge cannot be null or empty", nameof(cram));

            string concat = $"{cram}|{_apiKey}";
            string hashHex = ComputeSHA256(concat);
            string bucketId = _apiKey.Substring(_apiKey.Length - 5);

            return $"{hashHex}-{bucketId}";
        }

        /// <summary>
        /// Subscribes to live data for a symbol
        /// </summary>
        public bool Subscribe(Symbol symbol, TickType tickType)
        {
            if (!IsConnected)
            {
                Log.Error("DataBentoRawLiveClient.Subscribe(): Not connected to gateway");
                return false;
            }

            try
            {
                // Get the databento symbol form LEAN symbol
                var databentoSymbol = _symbolMapper.GetBrokerageSymbol(symbol);
                var schema = "mbp-1";
                var resolution = Resolution.Tick;

                // subscribe
                var subscribeMessage = $"schema={schema}|stype_in=parent|symbols={databentoSymbol}";
                Log.Debug($"DataBentoRawLiveClient.Subscribe(): Subscribing with message: {subscribeMessage}");

                // Send subscribe message
                _writer.WriteLine(subscribeMessage);

                // Store subscription
                _subscriptions.TryAdd(symbol, (resolution, tickType));
                Log.Debug($"DataBentoRawLiveClient.Subscribe(): Subscribed to {symbol} ({databentoSymbol}) at {resolution} resolution for {tickType}");

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.Subscribe(): Failed to subscribe to {symbol}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Starts the session to begin receiving data
        /// </summary>
        public bool StartSession()
        {
            if (!IsConnected)
            {
                Log.Error("DataBentoRawLiveClient.StartSession(): Not connected");
                return false;
            }

            try
            {
                Log.Trace("DataBentoRawLiveClient.StartSession(): Starting session");
                _writer.WriteLine("start_session=1");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.StartSession(): Failed to start session: {ex.Message}");
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
                    Log.Debug($"DataBentoRawLiveClient.Unsubscribe(): Unsubscribed from {symbol}");
                }
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.Unsubscribe(): Failed to unsubscribe from {symbol}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Processes incoming messages from the DataBento gateway
        /// </summary>
        private void ProcessMessages()
        {
            Log.Debug("DataBentoRawLiveClient.ProcessMessages(): Starting message processing");

            try
            {
                while (!_cancellationTokenSource.IsCancellationRequested && IsConnected)
                {
                    var line = _reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        Log.Trace("DataBentoRawLiveClient.ProcessMessages(): Line is null or empty. Issue receiving data.");
                        break;
                    }

                    ProcessSingleMessage(line);
                }
            }
            catch (OperationCanceledException)
            {
                Log.Trace("DataBentoRawLiveClient.ProcessMessages(): Message processing cancelled");
            }
            catch (IOException ex) when (ex.InnerException is SocketException)
            {
                Log.Trace($"DataBentoRawLiveClient.ProcessMessages(): Socket exception: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.ProcessMessages(): Error processing messages: {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
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

                        switch (rtype)
                        {
                            case 23:
                                // System message
                                if (root.TryGetProperty("msg", out var msgElement))
                                {
                                    Log.Debug($"DataBentoRawLiveClient: System message: {msgElement.GetString()}");
                                }
                                return;

                            case 22:
                                // Symbol mapping message
                                if (root.TryGetProperty("stype_in_symbol", out var inSymbol) &&
                                    root.TryGetProperty("stype_out_symbol", out var outSymbol) &&
                                    headerElement.TryGetProperty("instrument_id", out var instId))
                                {
                                    var instrumentId = instId.GetInt64();
                                    var outSymbolStr = outSymbol.GetString();

                                    Log.Debug($"DataBentoRawLiveClient: Symbol mapping: {inSymbol.GetString()} -> {outSymbolStr} (instrument_id: {instrumentId})");

                                    if (outSymbolStr != null)
                                    {
                                        // Let's find the subscribed symbol to get the market and security type
                                        var inSymbolStr = inSymbol.GetString();
                                        var subscription = _subscriptions.Keys.FirstOrDefault(s => _symbolMapper.GetBrokerageSymbol(s) == inSymbolStr);
                                        if (subscription != null)
                                        {
                                            if (subscription.SecurityType == SecurityType.Future)
                                            {
                                                var leanSymbol = _symbolMapper.GetLeanSymbolForFuture(outSymbolStr);
                                                if (leanSymbol == null)
                                                {
                                                    Log.Trace($"DataBentoRawLiveClient: Future spreads are not supported: {outSymbolStr}. Skipping mapping.");
                                                    return;
                                                }
                                                _instrumentIdToSymbol[instrumentId] = leanSymbol;
                                                Log.Debug($"DataBentoRawLiveClient: Mapped instrument_id {instrumentId} to {leanSymbol}");
                                            }
                                        }
                                    }
                                }
                                return;

                            case 1:
                                // MBP-1 (Market By Price)
                                HandleMBPMessage(root, headerElement);
                                return;

                            case 0:
                                // Trade messages
                                HandleTradeTickMessage(root, headerElement);
                                return;

                            case 32:
                            case 33:
                            case 34:
                            case 35:
                                // OHLCV bar messages
                                HandleOHLCVMessage(root, headerElement);
                                return;

                            default:
                                Log.Error($"DataBentoRawLiveClient: Unknown rtype {rtype} in message");
                                return;
                        }
                    }
                }

                // Handle other message types if needed
                if (root.TryGetProperty("error", out var errorElement))
                {
                    Log.Error($"DataBentoRawLiveClient: Server error: {errorElement.GetString()}");
                }
            }
            catch (JsonException ex)
            {
                Log.Error($"DataBentoRawLiveClient.ProcessSingleMessage(): JSON parse error: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.ProcessSingleMessage(): Error: {ex.Message}");
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
                    Log.Debug($"DataBentoRawLiveClient: No mapping for instrument_id {instrumentId} in OHLCV message.");
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

                    // Log.Trace($"DataBentoRawLiveClient: OHLCV bar: {matchedSymbol} O={open} H={high} L={low} C={close} V={volume} at {timestamp}");
                    DataReceived?.Invoke(this, tradeBar);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.HandleOHLCVMessage(): Error: {ex.Message}");
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
                    Log.Trace($"DataBentoRawLiveClient: No mapping for instrument_id {instrumentId} in MBP message.");
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
                    
                    // Log.Trace($"DataBentoRawLiveClient: Quote tick: {matchedSymbol} Bid={quoteTick.BidPrice}x{quoteTick.BidSize} Ask={quoteTick.AskPrice}x{quoteTick.AskSize}");
                    DataReceived?.Invoke(this, quoteTick);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.HandleMBPMessage(): Error: {ex.Message}");
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
                    Log.Trace($"DataBentoRawLiveClient: No mapping for instrument_id {instrumentId} in trade message.");
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

                    // Log.Trace($"DataBentoRawLiveClient: Trade tick: {matchedSymbol} Price={price} Quantity={size}");
                    DataReceived?.Invoke(this, tradeTick);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoRawLiveClient.HandleTradeTickMessage(): Error: {ex.Message}");
            }
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
                    Log.Trace($"DataBentoRawLiveClient.Disconnect(): Error during disconnect: {ex.Message}");
                }

                ConnectionStatusChanged?.Invoke(this, false);
                Log.Trace("DataBentoRawLiveClient.Disconnect(): Disconnected from DataBento gateway");
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

        /// <summary>
        /// Computes the SHA-256 hash of the input string
        /// </summary>
        private static string ComputeSHA256(string input)
        {
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sb = new StringBuilder();
            foreach (byte b in hash)
            {
                sb.Append(b.ToString("x2"));
            }
            return sb.ToString();
        }

    }
}
