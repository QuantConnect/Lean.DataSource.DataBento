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

    private readonly string _gateway;
    private readonly string _dataSet;
    private readonly string _apiKey;
    private readonly TimeSpan _heartBeatInterval = TimeSpan.FromSeconds(10);

    private TcpClient _tcpClient;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private NetworkStream? _stream;
    private StreamReader? _reader;
    private Task? _dataReceiverTask;
    private bool _isConnected;

    private readonly Action<string> MessageReceived;

    public event EventHandler<string>? ConnectionLost;

    /// <summary>
    /// Is client connected
    /// </summary>
    public bool IsConnected => _isConnected;

    public LiveDataTcpClientWrapper(string dataSet, string apiKey, Action<string> messageReceived)
    {
        _apiKey = apiKey;
        _dataSet = dataSet;
        _gateway = DetermineGateway(dataSet);
        MessageReceived = messageReceived;
    }

    public void Connect()
    {
        var attemptToConnect = 1;
        var error = default(string);
        do
        {
            try
            {
                _tcpClient = new(_gateway, DefaultPort);
                _stream = _tcpClient.GetStream();
                _reader = new StreamReader(_stream, Encoding.ASCII);

                if (!Authenticate(_dataSet).SynchronouslyAwaitTask())
                    throw new Exception("Authentication failed");

                _dataReceiverTask = new Task(async () => await DataReceiverAsync(_cancellationTokenSource.Token), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
                _dataReceiverTask.Start();

                _isConnected = true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            var retryDelayMs = attemptToConnect * 2 * 1000;
            LogError(nameof(Connect), $"Connection attempt #{attemptToConnect} failed. Retrying in {retryDelayMs} ms. Error: {error}");
            _cancellationTokenSource.Token.WaitHandle.WaitOne(attemptToConnect * 2 * 1000);

        } while (attemptToConnect++ < 5 && !_isConnected);
    }

    private void Close()
    {
        _isConnected = false;

        _reader?.Close();
        _reader?.DisposeSafely();

        _dataReceiverTask?.DisposeSafely();
        _tcpClient?.Close();
        _tcpClient?.DisposeSafely();
    }

    public void Dispose()
    {
        _isConnected = false;

        _reader?.Close();
        _reader?.DisposeSafely();

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

        LogTrace(methodName, "Task Receiver started");

        var errorMessage = string.Empty;

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

                MessageReceived.Invoke(line);
            }
        }
        catch (OperationCanceledException oce)
        {
            errorMessage = $"Read timeout exceeded: Outer CancellationToken: {ct.IsCancellationRequested}, Read Timeout: {readTimeoutCts.IsCancellationRequested}";
            LogTrace(methodName, errorMessage);
        }
        catch (Exception ex)
        {
            errorMessage += ex.Message;
            LogError(methodName, $"Error processing messages: {ex.Message}\n{ex.StackTrace}");
        }
        finally
        {
            LogTrace(methodName, "Task Receiver stopped");
            Close();
            readTimeoutCts.Dispose();
            ConnectionLost?.Invoke(this, new($"{errorMessage}. TcpConnected: {_tcpClient.Connected}"));
        }
    }

    private async Task<string?> ReadDataAsync(CancellationToken cancellationToken)
    {
        return await _reader.ReadLineAsync(cancellationToken);
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
