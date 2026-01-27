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

using System.Text;
using QuantConnect.Util;
using System.Net.Sockets;
using QuantConnect.Logging;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public sealed class LiveDataTcpClientWrapper : IDisposable
{
    private const int DefaultPort = 13000;
    private const int ReceiveBufferSize = 8192;

    private readonly string _gateway;
    private readonly string _dataSet;
    private readonly string _apiKey;
    private readonly TimeSpan _heartBeatInterval = TimeSpan.FromSeconds(10);

    private readonly TcpClient _tcpClient = new();
    private readonly byte[] _receiveBuffer = new byte[ReceiveBufferSize];
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly char[] _newLine = Environment.NewLine.ToCharArray();

    private NetworkStream? _stream;
    private Task? _dataReceiverTask;
    private bool _isConnected;

    /// <summary>
    /// Is client connected
    /// </summary>
    public bool IsConnected => _isConnected;

    public LiveDataTcpClientWrapper(string dataSet, string apiKey)
    {
        _apiKey = apiKey;
        _dataSet = dataSet;
        _gateway = DetermineGateway(dataSet);
    }

    public void Connect()
    {
        _tcpClient.Connect(_gateway, DefaultPort);
        _stream = _tcpClient.GetStream();

        if (!Authenticate(_dataSet).SynchronouslyAwaitTask())
            throw new Exception("Authentication failed");

        _dataReceiverTask = new Task(async () => await DataReceiverAsync(_cancellationTokenSource.Token), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
        _dataReceiverTask.Start();

        _isConnected = true;
    }

    public void Dispose()
    {
        _isConnected = false;

        _stream?.Close();
        _stream?.DisposeSafely();

        _cancellationTokenSource?.Cancel();
        _cancellationTokenSource?.DisposeSafely();

        _dataReceiverTask?.DisposeSafely();
        _tcpClient?.Close();
        _tcpClient?.DisposeSafely();
    }

    public void SubscribeOnMarketBestPriceLevelOne(string symbol)
    {
        var request = $"schema=mbp-1|stype_in=raw_symbol|symbols={symbol}";
        WriteData(request);
    }

    private async Task DataReceiverAsync(CancellationToken ct)
    {
        var methodName = nameof(DataReceiverAsync);

        var readTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var readTimeout = _heartBeatInterval.Add(TimeSpan.FromSeconds(5));

        LogTrace(methodName, "Receiver started");

        try
        {
            while (!ct.IsCancellationRequested && IsConnected)
            {
                // Reset timeout
                readTimeoutCts.CancelAfter(readTimeout);

                var line = await ReadDataAsync(readTimeoutCts.Token);

                if (line == null)
                {
                    Log.Error("Remote closed connection");
                    break;
                }

                LogTrace(methodName, $"Received: {line}");
            }
        }
        catch (OperationCanceledException)
        {
            if (!_tcpClient.Connected)
            {
                LogError("DataReceiverAsync", "GG");
            }

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
            LogTrace(methodName, "Receiver stopped");
            readTimeoutCts.Dispose();
        }
    }

    private async Task<string?> ReadDataAsync(CancellationToken cancellationToken)
    {
        var numberOfBytesToRead = await _stream.ReadAsync(_receiveBuffer.AsMemory(0, _receiveBuffer.Length), cancellationToken).ConfigureAwait(false);

        if (numberOfBytesToRead == 0)
        {
            return null;
        }

        using var memoryStream = new MemoryStream();
        await memoryStream.WriteAsync(_receiveBuffer.AsMemory(0, numberOfBytesToRead), cancellationToken).ConfigureAwait(false);
        return Encoding.ASCII.GetString(memoryStream.ToArray(), 0, numberOfBytesToRead).TrimEnd(_newLine);
    }

    private void WriteData(string data)
    {
        if (!data.EndsWith('\n'))
        {
            data += '\n';
        }
        var bytes = Encoding.ASCII.GetBytes(data);
        _stream.Write(bytes, 0, bytes.Length);
    }

    private async Task<bool> Authenticate(string dataSet)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token);

        try
        {
            var versionLine = await ReadDataAsync(cts.Token);
            var cramLine = await ReadDataAsync(cts.Token);

            if (Log.DebuggingEnabled)
            {
                LogDebug(nameof(Authenticate), $"Received initial message: {versionLine}, {cramLine}");
            }

            var request = new AuthenticationMessageRequest(cramLine, _apiKey, dataSet, _heartBeatInterval);

            LogTrace("Authenticate", $"Sending CRAM reply: {request}");

            WriteData(request.ToString());

            var authResponse = await ReadDataAsync(cts.Token);

            var authenticationResponse = new AuthenticationMessageResponse(authResponse);

            if (!authenticationResponse.Success)
            {
                LogError(nameof(Authenticate), $"Authentication response: {authResponse}");
                return false;
            }

            LogTrace(nameof(Authenticate), $"Successfully authenticated with session ID: {authenticationResponse.SessionId}");

            WriteData(request.GetStartSessionMessage()); // after start_session -> we get heartbeats and data

            return true;
        }
        finally
        {
            cts.DisposeSafely();
        }
    }

    private static string DetermineGateway(string dataset)
    {
        dataset = dataset.Replace('.', '-').ToLowerInvariant();
        return dataset + ".lsg.databento.com";
    }

    private void LogTrace(string method, string message)
    {
        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{method}: {message}");
    }

    private void LogError(string method, string message)
    {
        Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{method}: {message}");
    }

    private void LogDebug(string method, string message)
    {
        Log.Debug($"LiveDataTcpClientWrapper[{_dataSet}].{method}: {message}");
    }
}
