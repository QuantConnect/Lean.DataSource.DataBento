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
using System.Net.Sockets;
using System.Text;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// DataBento Raw TCP client for live streaming data
    /// </summary>
    public class DatabentoRawClient : IDisposable
    {
        private readonly string _apiKey;
        private readonly string _gateway;
        private TcpClient? _tcpClient;
        private NetworkStream? _stream;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Symbol, (Resolution, TickType)> _subscriptions;
        private readonly object _connectionLock = new object();
        private bool _isConnected;
        private bool _disposed;
        private const decimal PriceScaleFactor = 1e-9m;

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
        /// <param name="apiKey">DataBento API key</param>
        /// <param name="gateway">Gateway address default for futures glbx-mdp3.lsg.databento.com:13000</param>
        public DatabentoRawClient(string apiKey, string gateway = "glbx-mdp3.lsg.databento.com:13000")
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
            _subscriptions = new ConcurrentDictionary<Symbol, (Resolution, TickType)>();
            _cancellationTokenSource = new CancellationTokenSource();
        }

        /// <summary>
        /// Connects to the DataBento live gateway
        /// </summary>
        public async Task<bool> ConnectAsync()
        {
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
                await _tcpClient.ConnectAsync(host, port).ConfigureAwait(false);
                _stream = _tcpClient.GetStream();

                // Send authentication
                if (await AuthenticateAsync().ConfigureAwait(false))
                {
                    _isConnected = true;
                    ConnectionStatusChanged?.Invoke(this, true);

                    // Start message processing task
                    _ = Task.Run(() => ProcessMessagesAsync(_cancellationTokenSource.Token));

                    Log.Debug("DatabentoRawClient.ConnectAsync(): Connected to DataBento live gateway");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ConnectAsync(): Failed to connect: {ex.Message}");
                Disconnect();
            }

            return false;
        }

        /// <summary>
        /// Authenticates with the DataBento gateway using CRAM
        /// </summary>
        private async Task<bool> AuthenticateAsync()
        {
            if (_stream == null)
                return false;

            try
            {
                // Read greeting and challenge from gateway
                var responseBuffer = new byte[1024];
                var messageBuffer = new StringBuilder();
                
                // Read until we have both the version and cram messages
                string? version = null;
                string? cram = null;
                
                while (version == null || cram == null)
                {
                    var authenticationBytesRead = await _stream.ReadAsync(responseBuffer, 0, responseBuffer.Length);
                    if (authenticationBytesRead == 0)
                    {
                        Log.Error("DatabentoRawClient.AuthenticateAsync(): Connection closed during authentication");
                        return false;
                    }
                    
                    var receivedData = Encoding.UTF8.GetString(responseBuffer, 0, authenticationBytesRead);
                    messageBuffer.Append(receivedData);
                    
                    var messages = messageBuffer.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    
                    foreach (var msg in messages)
                    {
                        if (msg.StartsWith("lsg_version="))
                        {
                            version = msg.Substring("lsg_version=".Length);
                            Log.Debug($"DatabentoRawClient.AuthenticateAsync(): Gateway version: {version}");
                        }
                        else if (msg.StartsWith("cram="))
                        {
                            cram = msg.Substring("cram=".Length);
                            Log.Debug($"DatabentoRawClient.AuthenticateAsync(): Received CRAM challenge");
                        }
                    }
                }

                if (string.IsNullOrEmpty(cram))
                {
                    Log.Error("DatabentoRawClient.AuthenticateAsync(): No CRAM challenge received");
                    return false;
                }
                
                // Generate authentication response
                // Concatenate cram and API key: $cram|$key
                var cramAndKey = $"{cram}|{_apiKey}";
                
                // Hash with SHA-256
                string authHash;
                using (var sha256 = SHA256.Create())
                {
                    var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(cramAndKey));
                    authHash = BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
                }
                
                // Get last 5 characters of API key (bucket_id)
                var bucketId = _apiKey.Substring(_apiKey.Length - 5);
                
                // Create auth response: $hash-$bucket_id
                var authResponse = $"{authHash}-{bucketId}";
                
                // Send authentication message
                // Format: auth=$authResponse|dataset=GLBX.MDP3|encoding=dbn|ts_out=0\n
                var authMessage = $"auth={authResponse}|dataset=GLBX.MDP3|encoding=dbn|ts_out=0\n";
                var authBytes = Encoding.UTF8.GetBytes(authMessage);
                
                await _stream.WriteAsync(authBytes, 0, authBytes.Length);
                await _stream.FlushAsync();
                
                Log.Debug("DatabentoRawClient.AuthenticateAsync(): Sent authentication message");
                
                // Read authentication response
                messageBuffer.Clear();
                var bytesRead = await _stream.ReadAsync(responseBuffer, 0, responseBuffer.Length);
                var response = Encoding.UTF8.GetString(responseBuffer, 0, bytesRead);
                
                Log.Debug($"DatabentoRawClient.AuthenticateAsync(): Auth response: {response}");
                
                // Check for success response: success=1|session_id=...
                if (response.Contains("success=1"))
                {
                    // Extract session ID if needed
                    if (response.Contains("session_id="))
                    {
                        var sessionIdStart = response.IndexOf("session_id=") + "session_id=".Length;
                        var sessionIdEnd = response.IndexOf('|', sessionIdStart);
                        if (sessionIdEnd == -1) sessionIdEnd = response.IndexOf('\n', sessionIdStart);
                        if (sessionIdEnd > sessionIdStart)
                        {
                            var sessionId = response.Substring(sessionIdStart, sessionIdEnd - sessionIdStart);
                            Log.Debug($"DatabentoRawClient.AuthenticateAsync(): Session ID: {sessionId}");
                        }
                    }
                    return true;
                }
                
                Log.Trace($"DatabentoRawClient.AuthenticateAsync(): Authentication failed: {response}");
                return false;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.AuthenticateAsync(): Authentication failed: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Subscribes to live data for a symbol
        /// </summary>
        /// <param name="symbol">Symbol to subscribe to</param>
        /// <param name="resolution">Data resolution</param>
        /// <param name="tickType">The tick type</param>
        public bool Subscribe(Symbol symbol, Resolution resolution, TickType tickType)
        {
            if (!IsConnected || _stream == null)
            {
                Log.Error("DatabentoRawClient.Subscribe(): Not connected to gateway");
                return false;
            }

            try
            {
                var databentoSymbol = MapSymbolToDataBento(symbol);
                var schema = GetSchema(resolution, tickType);

                var subscribeMessage = new
                {
                    msg_type = "subscribe",
                    dataset = "GLBX.MDP3", // hard coded for now. Later on can add equities and options with different mapping
                    schema = schema,
                    symbols = new[] { databentoSymbol },
                    stype_in = "continuous"
                };

                var messageJson = JsonSerializer.Serialize(subscribeMessage);
                var messageBytes = Encoding.UTF8.GetBytes(messageJson + "\n");

                _stream.Write(messageBytes, 0, messageBytes.Length);
                _stream.Flush();

                _subscriptions.TryAdd(symbol, (resolution, tickType));
                Log.Debug($"DatabentoRawClient.Subscribe(): Subscribed to {symbol} at {resolution} resolution");

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.Subscribe(): Failed to subscribe to {symbol}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Unsubscribes from live data for a symbol
        /// </summary>
        /// <param name="symbol">Symbol to unsubscribe from</param>
        public bool Unsubscribe(Symbol symbol)
        {
            if (!IsConnected || _stream == null)
                return false;

            try
            {
                var databentoSymbol = MapSymbolToDataBento(symbol);

                var unsubscribeMessage = new
                {
                    msg_type = "unsubscribe",
                    symbols = new[] { databentoSymbol }
                };

                var messageJson = JsonSerializer.Serialize(unsubscribeMessage);
                var messageBytes = Encoding.UTF8.GetBytes(messageJson + "\n");

                _stream.Write(messageBytes, 0, messageBytes.Length);
                _stream.Flush();

                _subscriptions.TryRemove(symbol, out _);
                Log.Debug($"DatabentoRawClient.Unsubscribe(): Unsubscribed from {symbol}");

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
        private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
        {
            if (_stream == null)
                return;

            var buffer = new byte[8192];
            var messageBuffer = new StringBuilder();

            try
            {
                while (!cancellationToken.IsCancellationRequested && IsConnected)
                {
                    if (_stream.DataAvailable)
                    {
                        var bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                        if (bytesRead == 0)
                        {
                            Log.Debug("DatabentoRawClient.ProcessMessagesAsync(): Connection closed by server");
                            break;
                        }

                        var receivedData = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        messageBuffer.Append(receivedData);

                        // Process complete messages
                        string messageContent = messageBuffer.ToString();
                        var messages = messageContent.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                        
                        for (int i = 0; i < messages.Length - 1; i++)
                        {
                            await ProcessSingleMessage(messages[i]);
                        }

                        // Keep the last incomplete message in the buffer
                        if (messageContent.EndsWith('\n'))
                        {
                            messageBuffer.Clear();
                        }
                        else
                        {
                            messageBuffer.Clear().Append(messages[messages.Length - 1]);
                        }
                    }
                    else
                    {
                        await Task.Delay(1, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log.Debug("DatabentoRawClient.ProcessMessagesAsync(): Message processing cancelled");
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ProcessMessagesAsync(): Error processing messages: {ex.Message}");
            }
            finally
            {
                Disconnect();
            }
        }

        /// <summary>
        /// Processes a single message from DataBento
        /// </summary>
        private async Task ProcessSingleMessage(string message)
        {
            try
            {
                using var document = JsonDocument.Parse(message);
                var root = document.RootElement;

                if (!root.TryGetProperty("msg_type", out var msgTypeElement))
                    return;

                var msgType = msgTypeElement.GetString();

                switch (msgType)
                {
                    case "heartbeat":
                        // Handle heartbeat 
                        break;
                        
                    case "error":
                        HandleErrorMessage(root);
                        break;
                        
                    case "subscription_response":
                        HandleSubscriptionResponse(root);
                        break;
                        
                    case "data":
                        await HandleDataMessage(root);
                        break;
                        
                    default:
                        Log.Debug($"DatabentoRawClient.ProcessSingleMessage(): Unknown message type: {msgType}");
                        break;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ProcessSingleMessage(): Error processing message: {ex.Message}");
            }
        }

        /// <summary>
        /// Handles error messages from DataBento
        /// </summary>
        private void HandleErrorMessage(JsonElement root)
        {
            if (root.TryGetProperty("error", out var errorElement))
            {
                var errorMsg = errorElement.GetString();
                Log.Error($"DatabentoRawClient.HandleErrorMessage(): DataBento error: {errorMsg}");
            }
        }

        /// <summary>
        /// Handles subscription response messages
        /// </summary>
        private void HandleSubscriptionResponse(JsonElement root)
        {
            if (root.TryGetProperty("success", out var successElement))
            {
                var success = successElement.GetBoolean();
                Log.Debug($"DatabentoRawClient.HandleSubscriptionResponse(): Subscription response: {success}");
            }
        }

        /// <summary>
        /// Handles incoming data messages and converts them to LEAN BaseData
        /// </summary>
        private async Task HandleDataMessage(JsonElement root)
        {
            await Task.CompletedTask;

            try
            {
                // Extract common fields
                if (!root.TryGetProperty("symbol", out var symbolElement) ||
                    !root.TryGetProperty("ts_event", out var timestampElement))
                {
                    return;
                }

                var databentoSymbol = symbolElement.GetString();
                var timestampNs = timestampElement.GetInt64();
                
                // Manual conversion from nanoseconds to DateTime
                // Unix epoch: January 1, 1970 00:00:00 UTC
                var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                var timestamp = unixEpoch.AddTicks(timestampNs / 100); // Convert nanoseconds to ticks (100ns per tick)

                // Find the corresponding LEAN symbol
                Symbol? leanSymbol = null;
                (Resolution resolution, TickType tickType) subscription = default;
                foreach (var kvp in _subscriptions)
                {
                    if (MapSymbolToDataBento(kvp.Key) == databentoSymbol)
                    {
                        leanSymbol = kvp.Key;
                        subscription = kvp.Value;
                        break;
                    }
                }

                if (leanSymbol == null)
                    return;

                BaseData? data = null;

                // Check if this is trade data or quote data based on available fields
                if (subscription.tickType == TickType.Trade)
                {
                    if (root.TryGetProperty("price", out var priceElement) && 
                        root.TryGetProperty("size", out var sizeElement))
                    {
                        // This is trade data
                        data = new Tick
                        {
                            Symbol = leanSymbol,
                            Time = timestamp,
                            Value = priceElement.GetDecimal(),
                            Quantity = sizeElement.GetDecimal(),
                            TickType = TickType.Trade
                        };
                    }
                    else if (root.TryGetProperty("open", out var openElement) &&
                             root.TryGetProperty("high", out var highElement) &&
                             root.TryGetProperty("low", out var lowElement) &&
                             root.TryGetProperty("close", out var closeElement) &&
                             root.TryGetProperty("volume", out var volumeElement))
                    {
                        // This is OHLCV bar data
                        data = new TradeBar
                        {
                            Symbol = leanSymbol,
                            Time = timestamp,
                            Open = openElement.GetDecimal(),
                            High = highElement.GetDecimal(),
                            Low = lowElement.GetDecimal(),
                            Close = closeElement.GetDecimal(),
                            Volume = volumeElement.GetDecimal()
                        };
                    }
                }
                else if (subscription.tickType == TickType.Quote)
                {
                    if (root.TryGetProperty("bid_px_00", out var bidPriceElement) &&
                        root.TryGetProperty("ask_px_00", out var askPriceElement) &&
                        root.TryGetProperty("bid_sz_00", out var bidSizeElement) &&
                        root.TryGetProperty("ask_sz_00", out var askSizeElement))
                    {
                        var bidPrice = bidPriceElement.GetInt64() * PriceScaleFactor;
                        var askPrice = askPriceElement.GetInt64() * PriceScaleFactor;

                        if (subscription.resolution == Resolution.Tick)
                        {
                            data = new Tick
                            {
                                Time = timestamp,
                                Symbol = leanSymbol,
                                AskPrice = askPrice,
                                BidPrice = bidPrice,
                                AskSize = askSizeElement.GetInt32(),
                                BidSize = bidSizeElement.GetInt32(),
                                TickType = TickType.Quote
                            };
                        }
                        else
                        {
                            var bidBar = new Bar(bidPrice, bidPrice, bidPrice, bidPrice);
                            var askBar = new Bar(askPrice, askPrice, askPrice, askPrice);
                            data = new QuoteBar(
                                timestamp,
                                leanSymbol,
                                bidBar,
                                bidSizeElement.GetInt32(),
                                askBar,
                                askSizeElement.GetInt32()
                            );
                        }
                    }
                }

                if (data != null)
                {
                    DataReceived?.Invoke(this, data);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.HandleDataMessage(): Error handling data message: {ex.Message}");
            }
        }

        /// <summary>
        /// Maps a LEAN symbol to DataBento symbol format
        /// </summary>
        private string MapSymbolToDataBento(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                return $"{symbol.ID.Symbol}.v.0"; // Continuous contract
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
                    _stream?.Close();
                    _tcpClient?.Close();
                }
                catch (Exception ex)
                {
                    Log.Debug($"DatabentoRawClient.Disconnect(): Error during disconnect: {ex.Message}");
                }

                ConnectionStatusChanged?.Invoke(this, false);
                Log.Debug("DatabentoRawClient.Disconnect(): Disconnected from DataBento gateway");
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
            _stream?.Dispose();
            _tcpClient?.Dispose();
        }
    }
}
