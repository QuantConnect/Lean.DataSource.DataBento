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
using System.Text;
using System.Text.Json;
using System.Security.Cryptography;
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
            Log.Trace("DatabentoRawClient.ConnectAsync(): Connecting to DataBento live gateway");
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

                    Log.Trace("DatabentoRawClient.ConnectAsync(): Connected to DataBento live gateway");
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
                // DataBento authentication message format
                var authMessage = new
                {
                    msg_type = "authenticate",
                    api_key = _apiKey,
                    version = "1.0"
                };

                var authJson = JsonSerializer.Serialize(authMessage);
                var authBytes = Encoding.UTF8.GetBytes(authJson + "\n");

                await _stream.WriteAsync(authBytes, 0, authBytes.Length);
                await _stream.FlushAsync();

                // Read authentication response
                messageBuffer.Clear();
                var bytesRead = await _stream.ReadAsync(responseBuffer, 0, responseBuffer.Length);
                var response = Encoding.UTF8.GetString(responseBuffer, 0, bytesRead);

                Log.Debug($"DatabentoRawClient.AuthenticateAsync(): Auth response: {response}");

                // Parse response to check if authentication was successful
                var responseDoc = JsonDocument.Parse(response.Trim());
                if (responseDoc.RootElement.TryGetProperty("success", out var successElement))
                {
                    return successElement.GetBoolean();
                }

                return response.Contains("success") || response.Contains("authenticated");
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

                var subscribeMessage = $"schema={schema}|stype_in=raw_symbol|symbols={databentoSymbol}\n";
                var messageBytes = Encoding.UTF8.GetBytes(subscribeMessage);

                _stream.Write(messageBytes, 0, messageBytes.Length);
                _stream.Flush();

                _subscriptions.TryAdd(symbol, (resolution, tickType));
                Log.Trace($"DatabentoRawClient.Subscribe(): Subscribed to {symbol} at {resolution} resolution");

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
        private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
        {
            Log.Trace("DatabentoRawClient.ProcessMessagesAsync(): Starting message processing");
            if (_stream == null)
            {
                Log.Trace("DatabentoRawClient.ProcessMessagesAsync(): No stream to process messages");
                return;
            }

            var buffer = new byte[8192];
            var messageBuffer = new StringBuilder();
            var messageCount = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested && IsConnected)
                {
                    var bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                    if (bytesRead == 0)
                    {
                        Log.Trace("DatabentoRawClient.ProcessMessagesAsync(): Connection closed by server");
                        break;
                    }

                    Log.Trace($"DatabentoRawClient.ProcessMessagesAsync(): Read {bytesRead} bytes");

                    var receivedData = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    messageBuffer.Append(receivedData);

                    // Process complete messages
                    var messageContent = messageBuffer.ToString();
                    var lastNewlineIndex = messageContent.LastIndexOf('\n');

                    if (lastNewlineIndex > -1)
                    {
                        var completeMessages = messageContent.Substring(0, lastNewlineIndex);
                        var messages = completeMessages.Split('\n', StringSplitOptions.RemoveEmptyEntries);

                        Log.Trace($"DatabentoRawClient.ProcessMessagesAsync(): Processing {messages.Length} complete messages");

                        foreach (var message in messages)
                        {
                            messageCount++;
                            Log.Trace($"DatabentoRawClient.ProcessMessagesAsync(): Message #{messageCount}: {message.Substring(0, Math.Min(200, message.Length))}...");
                            await ProcessSingleMessage(message);
                        }

                        messageBuffer.Clear();
                        if (lastNewlineIndex < messageContent.Length - 1)
                        {
                            messageBuffer.Append(messageContent.Substring(lastNewlineIndex + 1));
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log.Trace("DatabentoRawClient.ProcessMessagesAsync(): Message processing cancelled");
            }
            catch (IOException ex) when (ex.InnerException is SocketException)
            {
                Log.Trace($"DatabentoRawClient.ProcessMessagesAsync(): Socket exception: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log.Error($"DatabentoRawClient.ProcessMessagesAsync(): Error processing messages: {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                Log.Trace($"DatabentoRawClient.ProcessMessagesAsync(): Exiting. Total messages processed: {messageCount}");
                Disconnect();
            }
        }

        /// <summary>
        /// Processes a single message from DataBento
        /// </summary>
        private async Task ProcessSingleMessage(string message)
        {
            Log.Trace("DatabentoRawClient.ProcessSingleMessage(): Processing message");
            Log.Trace($"Msg {message}");
            try
            {
                // First, try to parse as JSON
                using (var document = JsonDocument.Parse(message))
                {
                    var root = document.RootElement;

                    if (root.TryGetProperty("hd", out var headerElement))
                    {
                        // This is a DBN message, let's check rtype
                        if (headerElement.TryGetProperty("rtype", out var rtypeElement))
                        {
                            var rtype = rtypeElement.GetByte();
                            if (rtype == 21) // Error
                            {
                                if (root.TryGetProperty("err", out var errElement))
                                {
                                    Log.Error($"Databento error: {errElement.GetString()}");
                                }
                                return;
                            }
                        }
                    }

                    if (root.TryGetProperty("msg_type", out var msgTypeElement))
                    {
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
                                Log.Trace($"DatabentoRawClient.ProcessSingleMessage(): Unknown message type: {msgType}");
                                break;
                        }
                    }
                }
            }
            catch (JsonException)
            {
                // If it's not a valid JSON
                if (message.Contains("error") || message.Contains("err"))
                {
                    Log.Error($"Databento error: {message}");
                }
                else if (message.Contains("success"))
                {
                    Log.Trace($"Databento success: {message}");
                }
                else
                {
                    Log.Trace($"Databento message: {message}");
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
                Log.Trace($"DatabentoRawClient.HandleSubscriptionResponse(): Subscription response: {success}");
            }
        }

        /// <summary>
        /// Handles incoming data messages and converts them to LEAN BaseData
        /// </summary>
        private async Task HandleDataMessage(JsonElement root)
        {
            Log.Trace($"DatabentoRawClient.HandleDataMessage(): Processing data message: {root}");
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
                {
                    Log.Trace($"DatabentoRawClient.HandleDataMessage(): Received data for unsubscribed symbol: {databentoSymbol}");
                    return;
                }

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
                // symbol.Value contains the full contract identifier (e.g., ES19Z25)
                var contractSymbol = symbol.Value;
                
                Log.Trace($"Original symbol: {contractSymbol}");
                
                // Convert LEAN futures format to Databento format
                // LEAN format: ES19Z25 (root + day + month code + year)
                // Databento format: ESZ25 (root + month code + year)
                
                var databentoSymbol = ConvertToDatabentoFormat(contractSymbol);
                
                Log.Trace($"Databento symbol: {databentoSymbol}");
                return databentoSymbol;
            }
            else
            {
                Log.Error($"DatabentoRawClient.MapSymbolToDataBento(): Unsupported symbol type: {symbol.SecurityType}");
                return "";
            }
        }

        /// <summary>
        /// Converts LEAN futures symbol format to Databento format
        /// </summary>
        /// <param name="leanSymbol">LEAN symbol (e.g., ES19Z25)</param>
        /// <returns>Databento symbol (e.g., ESZ5)</returns>
        private string ConvertToDatabentoFormat(string leanSymbol)
        {
            // Pattern: ROOT + DAY(1-2 digits) + MONTH_CODE(1 letter) + YEAR(2 digits)
            // Example: ES19Z25 -> ESZ5
            // Databento uses single digit year for the 2020s decade
            
            // Find the position of the month code (single letter: F,G,H,J,K,M,N,Q,U,V,X,Z)
            var monthCodes = new[] { 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z' };
            
            for (int i = 0; i < leanSymbol.Length; i++)
            {
                if (Array.IndexOf(monthCodes, leanSymbol[i]) >= 0)
                {
                    // Found the month code
                    // Extract root (everything before the digits before month code)
                    var root = leanSymbol.Substring(0, i);
                    
                    // Remove trailing digits from root (the day component)
                    while (root.Length > 0 && char.IsDigit(root[root.Length - 1]))
                    {
                        root = root.Substring(0, root.Length - 1);
                    }
                    
                    // Extract month code
                    var monthCode = leanSymbol[i];
                    
                    // Extract year (last 2 digits)
                    var yearStr = leanSymbol.Substring(i + 1);
                    
                    // Convert to single digit year for Databento format
                    // 25 -> 5, 24 -> 4, etc.
                    if (yearStr.Length == 2 && int.TryParse(yearStr, out int year))
                    {
                        var singleDigitYear = year % 10;
                        
                        // Databento format: ROOT + MONTH_CODE + SINGLE_DIGIT_YEAR
                        return $"{root}{monthCode}{singleDigitYear}";
                    }
                    else if (yearStr.Length == 1)
                    {
                        // Already single digit
                        return $"{root}{monthCode}{yearStr}";
                    }
                    
                    Log.Error($"ConvertToDatabentoFormat(): Invalid year format in {leanSymbol}");
                    return leanSymbol;
                }
            }
            
            // If no month code found, return as-is (might already be in correct format)
            Log.Trace($"ConvertToDatabentoFormat(): No month code found in {leanSymbol}, using as-is");
            return leanSymbol;
        }

        /// <summary>
        /// Pick Databento schema from Lean resolution/ticktype
        /// </summary>
        private string GetSchema(Resolution resolution, TickType tickType)
        {
            if (tickType == TickType.Trade || tickType == TickType.Quote)
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
            _stream?.Dispose();
            _tcpClient?.Dispose();
        }
    }
}
