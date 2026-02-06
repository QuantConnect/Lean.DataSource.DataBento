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
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;
using QuantConnect.Lean.DataSource.DataBento.Exceptions;
using QuantConnect.Lean.DataSource.DataBento.Models.Events;
using QuantConnect.Lean.DataSource.DataBento.Models.Enums;

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public sealed class LiveAPIClient : IDisposable
{
    private readonly string _apiKey;

    private readonly Dictionary<string, LiveDataTcpClientWrapper> _tcpClientByDataSet = [];

    private readonly Action<LevelOneData> _levelOneDataHandler;

    /// <summary>
    /// A set of system messages that should be ignored by the message handler.
    /// </summary>
    private static readonly HashSet<string> IgnoredMessages = new(StringComparer.InvariantCultureIgnoreCase)
    {
        "Heartbeat",
        "Subscription request for mbp-1 data succeeded"
    };

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

        Log.Trace($"LiveAPIClient.{nameof(EnsureDatasetConnection)}: Starting connection to DataBento live API");

        if (liveDataTcpClient == null)
        {
            liveDataTcpClient = new LiveDataTcpClientWrapper(dataSet, _apiKey, MessageReceived);

            liveDataTcpClient.ConnectionLost += (sender, message) =>
            {
                Log.Error($"LiveAPIClient.{nameof(EnsureDatasetConnection)}: Connection lost to DataBento live API (Dataset: {dataSet}). Reason: {message}");
                ConnectionLost?.Invoke(this, new ConnectionLostEventArgs(dataSet, message));
            };

            _tcpClientByDataSet[dataSet] = liveDataTcpClient;
        }

        liveDataTcpClient.Connect();

        if (!liveDataTcpClient.IsConnected)
        {
            var msg = $"Unable to establish a connection to the DataBento Live API (Dataset: {dataSet}).";

            // TODO: remove after resolving https://github.com/QuantConnect/Lean/issues/9272
            var resultHandler = Composer.Instance.GetPart<IResultHandler>();
            if (resultHandler == null)
            {
                Log.Error($"LiveDataTcpClientWrapper[{dataSet}].{nameof(EnsureDatasetConnection)}: result handler is null");
            }
            else
            {
                resultHandler.RuntimeError(msg);
            }

            throw new Exception(msg);
        }

        Log.Trace($"LiveAPIClient.{nameof(EnsureDatasetConnection)}: Successfully connected to DataBento live API (Dataset: {dataSet})");

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
            Log.Error($"LiveAPIClient.{nameof(MessageReceived)}: Failed to deserialize live data message: {message}");
            return;
        }

        switch (data)
        {
            case SymbolMappingMessage smm:
                SymbolMappingConfirmation?.Invoke(this, new(smm.StypeInSymbol, smm.Header.InstrumentId));
                break;
            case LevelOneData lod:
                switch (lod.Action)
                {
                    case ActionType.Clear:
                        // TODO: should we pass this action?
                        // Clear('R') actions are when the order book is entirely cleared.
                        // At the beginning of the week, the order book is completely reset for an instrument,
                        // and you will see an Clear('R') record for it.
                        // You will very likely not see this during the week.
                        Log.Trace($"LiveAPIClient.{nameof(MessageReceived)}.Action.Clear: {message}");
                        break;
                    case ActionType.None:
                        Log.Trace($"LiveAPIClient.{nameof(MessageReceived)}.Action.None: {message}");
                        break;
                }
                _levelOneDataHandler?.Invoke(lod);
                break;
            case SystemMessage sm when IgnoredMessages.Contains(sm.Msg):
                break;
            case ErrorMessage error:
                // Terminate connection
                throw new LiveApiErrorException(error);
            default:
                Log.Error($"LiveAPIClient.{nameof(MessageReceived)}: Received unsupported record type: {data.Header.Rtype}. Message: {message}");
                break;
        }
    }
}
