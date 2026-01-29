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

using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;
using QuantConnect.Lean.DataSource.DataBento.Models.Events;

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public sealed class LiveAPIClient : IDisposable
{
    private readonly string _apiKey;

    private readonly Dictionary<string, LiveDataTcpClientWrapper> _tcpClientByDataSet = [];

    private readonly Action<LevelOneData> _levelOneDataHandler;

    public event EventHandler<SymbolMappingConfirmationEventArgs>? SymbolMappingConfirmation;

    public event EventHandler<ConnectionLostEventArgs>? ConnectionLost;

    public bool IsConnected => _tcpClientByDataSet.Values.All(c => c.IsConnected);

    public LiveAPIClient(string apiKey, Action<LevelOneData> levelOneDataHandler)
    {
        _apiKey = apiKey;
        _levelOneDataHandler = levelOneDataHandler;
    }

    public void Dispose()
    {
        foreach (var tcpClient in _tcpClientByDataSet.Values)
        {
            tcpClient.DisposeSafely();
        }
        _tcpClientByDataSet.Clear();
    }

    private LiveDataTcpClientWrapper EnsureDatasetConnection(string dataSet)
    {
        if (_tcpClientByDataSet.TryGetValue(dataSet, out var liveDataTcpClient) && liveDataTcpClient.IsConnected)
        {
            return liveDataTcpClient;
        }

        LogTrace(nameof(EnsureDatasetConnection), "Starting connection to DataBento live API");

        if (liveDataTcpClient == null)
        {
            liveDataTcpClient = new LiveDataTcpClientWrapper(dataSet, _apiKey, MessageReceived);

            liveDataTcpClient.ConnectionLost += (sender, message) =>
            {
                LogError(nameof(EnsureDatasetConnection), $"Connection lost to DataBento live API (Dataset: {dataSet}). Reason: {message}");
                ConnectionLost?.Invoke(this, new ConnectionLostEventArgs(dataSet, message));
            };

            _tcpClientByDataSet[dataSet] = liveDataTcpClient;
        }

        liveDataTcpClient.Connect();

        if (!liveDataTcpClient.IsConnected)
        {
            var msg = $"Unable to establish a connection to the DataBento Live API (Dataset: {dataSet}).";
            LogError(nameof(EnsureDatasetConnection), msg);
            throw new Exception(msg);
        }

        LogTrace(nameof(EnsureDatasetConnection), $"Successfully connected to DataBento live API (Dataset: {dataSet})");

        return liveDataTcpClient;
    }

    public bool Subscribe(string dataSet, string symbol)
    {
        EnsureDatasetConnection(dataSet).SubscribeOnMarketBestPriceLevelOne(symbol);
        return true;
    }

    private void MessageReceived(string message)
    {
        var data = message.DeserializeObject<MarketDataBase>();

        if (data == null)
        {
            LogError(nameof(MessageReceived), $"Failed to deserialize live data message: {message}");
            return;
        }

        switch (data)
        {
            case SymbolMappingMessage smm:
                SymbolMappingConfirmation?.Invoke(this, new(smm.StypeInSymbol, smm.Header.InstrumentId));
                break;
            case LevelOneData lod:
                _levelOneDataHandler?.Invoke(lod);
                break;
            default:
                LogError(nameof(MessageReceived), $"Received unsupported record type: {data.Header.Rtype}. Message: {message}");
                break;
        }
    }

    private static void LogTrace(string method, string message)
    {
        Log.Trace($"LiveAPIClient.{method}: {message}");
    }

    private static void LogError(string method, string message)
    {
        Log.Error($"LiveAPIClient.{method}: {message}");
    }
}
