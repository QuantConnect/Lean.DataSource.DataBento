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
        private TcpClient _tcpClient;
        private NetworkStream _stream;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Symbol, Resolution> _subscriptions;
        private readonly object _connectionLock = new object();
        private bool _isConnected;
        private bool _disposed;

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
        /// Initializes a new instance of the DatabentoRawClient
        /// </summary>
        /// <param name="apiKey">DataBento API key</param>
        /// <param name="gateway">Gateway address (default: live.databento.com:13000)</param>
        public DatabentoRawClient(string apiKey, string gateway = "live.databento.com:13000")
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
            _subscriptions = new ConcurrentDictionary<Symbol, Resolution>();
            _cancellationTokenSource = new CancellationTokenSource();
        }

        /// <summary>
        /// Connects to the DataBento live gateway
        /// </summary>
        public async Task<bool> ConnectAsync()
        {
            lock (_connectionLock)
            {
                if (_isConnected || _disposed)
                    return _isConnected;

                try
                {
                    var parts = _gateway.Split(':');
                    var host = parts[0];
                    var port = parts.Length > 1 ? int.Parse(parts[1]) : 13000;

                    _tcpClient = new TcpClient();
                    _tcpClient.Connect(host, port);
                    _stream = _tcpClient.GetStream();

                    // Send authentication
                    if (AuthenticateAsync().Result)
                    {
                        _isConnected = true;
                        ConnectionStatusChanged?.Invoke(this, true);

                        // Start message processing task
                        Task.Run(() => ProcessMessagesAsync(_cancellationTokenSource.Token));

                        Log.Debug("DatabentoRawClient.ConnectAsync(): Connected to DataBento live gateway");
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"DatabentoRawClient.ConnectAsync(): Failed to connect: {ex.Message}");
                    Disconnect();
                }
            }

            return false;
        }

        /// <summary>
        /// Authenticates with the DataBento gateway
        /// </summary>
        private async Task<bool> AuthenticateAsync()
        {
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
                var responseBuffer = new byte[1024];
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
        public async Task<bool> Subscribe(Symbol symbol, Resolution resolution)
        {
            if (!IsConnected)
            {
                Log.Error("DatabentoRawClient.Subscribe(): Not connected to gateway");
                return false;
            }

            try
            {
                var databentoSymbol = MapSymbolToDataBento(symbol);
                var schema = GetSchemaFromResolution(resolution);

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

                await _stream.WriteAsync(messageBytes, 0, messageBytes.Length);
                await _stream.FlushAsync();

                _subscriptions.TryAdd(symbol, resolution);
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
        public async Task<bool> Unsubscribe(Symbol symbol)
        {
            if (!IsConnected)
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

                await _stream.WriteAsync(messageBytes, 0, messageBytes.Length);
                await _stream.FlushAsync();

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

                        // Process complete messages (assuming newline-delimited JSON)
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
                        // Handle heartbeat - no action needed
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
                var timestamp = DateTimeOffset.FromUnixTimeNanoseconds(timestampNs).DateTime;

                // Find the corresponding LEAN symbol
                Symbol leanSymbol = null;
                foreach (var kvp in _subscriptions)
                {
                    if (MapSymbolToDataBento(kvp.Key) == databentoSymbol)
                    {
                        leanSymbol = kvp.Key;
                        break;
                    }
                }

                if (leanSymbol == null)
                    return;

                BaseData data = null;

                // Check if this is trade data or quote data based on available fields
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
                    data = new DataBentoDataType
                    {
                        Symbol = leanSymbol,
                        Time = timestamp,
                        Open = openElement.GetDecimal(),
                        High = highElement.GetDecimal(),
                        Low = lowElement.GetDecimal(),
                        Close = closeElement.GetDecimal(),
                        Volume = volumeElement.GetDecimal(),
                        Value = closeElement.GetDecimal()
                    };
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
        /// Gets the DataBento schema from LEAN resolution
        /// </summary>
        private string GetSchemaFromResolution(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Tick:
                    return "trades";
                case Resolution.Second:
                    return "ohlcv-1s";
                case Resolution.Minute:
                    return "ohlcv-1m";
                case Resolution.Hour:
                    return "ohlcv-1h";
                case Resolution.Daily:
                    return "ohlcv-1d";
                default:
                    throw new ArgumentException($"Unsupported resolution: {resolution}");
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