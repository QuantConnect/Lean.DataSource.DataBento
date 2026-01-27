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

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public sealed class LiveAPIClient : IDisposable
{
    private readonly string _apiKey;

    private readonly Dictionary<string, LiveDataTcpClientWrapper> _tcpClientByDataSet = [];

    public LiveAPIClient(string apiKey)
    {
        _apiKey = apiKey;
    }

    public void Dispose()
    {
        foreach (var tcpClient in _tcpClientByDataSet.Values)
        {
            tcpClient.DisposeSafely();
        }
        _tcpClientByDataSet.Clear();
    }

    public bool Start(string dataSet)
    {
        LogTrace(nameof(Start), "Starting connection to DataBento live API");

        if (_tcpClientByDataSet.TryGetValue(dataSet, out var existingClient) && existingClient.IsConnected)
        {
            LogTrace(nameof(Start), $"Already connected to DataBento live API (Dataset: {dataSet})");
            return true;
        }

        var liveDataTcpClient = new LiveDataTcpClientWrapper(dataSet, _apiKey);
        _tcpClientByDataSet[dataSet] = liveDataTcpClient;
        liveDataTcpClient.Connect();

        LogTrace(nameof(Start), $"Successfully connected to DataBento live API (Dataset: {dataSet})");

        return true;
    }

    public bool Subscribe(string dataSet, string symbol)
    {
        if (!_tcpClientByDataSet.TryGetValue(dataSet, out var tcpClient) || !tcpClient.IsConnected)
        {
            LogError(nameof(Subscribe), $"Not connected to DataBento live API (Dataset: {dataSet})");
            return false;
        }

        tcpClient.SubscribeOnMarketBestPriceLevelOne(symbol);

        return true;
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
