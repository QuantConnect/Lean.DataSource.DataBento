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
using System.Security.Authentication;
using QuantConnect.Lean.DataSource.DataBento.Exceptions;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public sealed class LiveDataTcpClientWrapper : IDisposable
{
    private const int DefaultPort = 13000;
    private const int MaxConnectionAttempts = 30;

    private readonly string _gateway;
    private readonly string _dataSet;
    private readonly string _apiKey;
    private readonly TimeSpan _heartBeatInterval = TimeSpan.FromSeconds(10);

    private TcpClient? _tcpClient;
    private readonly CancellationTokenSource _cancellationTokenSource;

    private NetworkStream? _stream;
    private StreamReader? _reader;
    private readonly Task? _dataReceiverTask;
    private readonly ManualResetEventSlim _connectionOpenResetEvent = new(false);

    private readonly Action<string> MessageReceived;

    public event EventHandler<string>? ConnectionLost;

    /// <summary>
    /// Is client connected
    /// </summary>
    public bool IsConnected => _connectionOpenResetEvent.IsSet;

    public LiveDataTcpClientWrapper(string dataSet, string apiKey, Action<string> messageReceived)
    {
        _apiKey = apiKey;
        _dataSet = dataSet;
        _gateway = DetermineGateway(dataSet);
        MessageReceived = messageReceived;

        _cancellationTokenSource = new();
        _dataReceiverTask = new Task(() => MonitorDataReceiverConnection(_cancellationTokenSource.Token), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
        _dataReceiverTask.Start();
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
                // Set read timeout slightly above the heartbeat interval to tolerate network latency.
                // A timeout helps detect stalled or lost connections in DataReceiverAsync.
                _stream.ReadTimeout = Convert.ToInt32(_heartBeatInterval.Add(TimeSpan.FromSeconds(5)).TotalMilliseconds);
                _reader = new StreamReader(_stream, Encoding.ASCII);

                if (!Authenticate(_reader, _dataSet))
                {
                    throw new AuthenticationException($"Authentication failed for [{_dataSet}]. Please check your API key.");
                }

                _connectionOpenResetEvent.Set();
                break;
            }
            catch (AuthenticationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                CleanupConnection();
                error = ex.Message;
            }

            var retryDelayMs = attemptToConnect * 2 * 1000;
            Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Connect)}: Connection attempt #{attemptToConnect} failed. Retrying in {retryDelayMs} ms. Error: {error}");
            _cancellationTokenSource.Token.WaitHandle.WaitOne(attemptToConnect * 2 * 1000);

        } while (attemptToConnect++ < MaxConnectionAttempts && !IsConnected);
        // 30 attempts => 15.5 min total retry delay (2s + 4s + 6s + ...).

        if (attemptToConnect >= MaxConnectionAttempts)
        {
            Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Connect)}: " +
                $"Maximum retry limit ({MaxConnectionAttempts}) reached. Connection not established.");
        }
    }

    /// <summary>
    /// Resets the connection and disposes the TCP client, stream, and reader.
    /// </summary>
    private void CleanupConnection()
    {
        if (_cancellationTokenSource.IsCancellationRequested)
        {
            // The Dispose() was called.
            return;
        }

        _connectionOpenResetEvent?.Reset();

        _reader?.Close();
        _reader?.DisposeSafely();

        _stream?.Close();
        _stream?.DisposeSafely();

        _tcpClient?.Close();
        _tcpClient?.DisposeSafely();
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Cancel();
        _cancellationTokenSource?.DisposeSafely();

        _connectionOpenResetEvent?.Reset();
        _connectionOpenResetEvent?.DisposeSafely();

        _reader?.Close();
        _reader?.DisposeSafely();

        _dataReceiverTask.SynchronouslyAwaitTask();
        _dataReceiverTask?.DisposeSafely();
        _tcpClient?.Close();
        _tcpClient?.DisposeSafely();
    }

    public void SubscribeOnMarketBestPriceLevelOne(string symbol)
    {
        var request = $"schema=mbp-1|stype_in=raw_symbol|symbols={symbol}";
        WriteData(request);
    }

    /// <summary>
    /// Runs a blocking loop that waits for the connection to open, receives manager state data,
    /// and raises a notification when the connection is lost.
    /// </summary>
    /// <param name="ct">
    /// Cancellation token used to stop the loop and interrupt waiting on the connection signal.
    /// </param>
    private void MonitorDataReceiverConnection(CancellationToken ct)
    {
        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(MonitorDataReceiverConnection)}: Starting connection monitor loop");
        while (!ct.IsCancellationRequested)
        {
            // Wait until the connection is opened (blocking)
            _connectionOpenResetEvent.Wait(ct);

            var errorMessage = default(string);

            try
            {
                errorMessage = DataReceiver(ct);
            }
            catch (LiveApiErrorException e)
            {
                Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(MonitorDataReceiverConnection)}: {e.Message}");
                break;
            }
            finally
            {
                if (!ct.IsCancellationRequested)
                {
                    _connectionOpenResetEvent.Reset();
                }
            }

            if (!ct.IsCancellationRequested)
            {
                try
                {
                    ConnectionLost?.Invoke(this, new($"{errorMessage}. TcpConnected: {_tcpClient?.Connected}"));
                }
                catch (Exception ex)
                {
                    Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(MonitorDataReceiverConnection)}.Event<ConnectionLost>.Raise.Exception: {ex.Message}");
                    break;
                }
            }
        }
        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(MonitorDataReceiverConnection)}: Stopping connection monitor loop");
    }

    /// <summary>
    /// Synchronously reads incoming messages from the TCP stream and dispatches them to subscribers.
    /// </summary>
    /// <remarks>
    /// The method runs until the connection is closed or cancellation is requested.
    /// Read timeouts and I/O errors are treated as connection failures and trigger cleanup.
    /// </remarks>
    private string DataReceiver(CancellationToken ct)
    {
        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}: started");

        var errorMessage = string.Empty;

        try
        {
            while (!ct.IsCancellationRequested && IsConnected)
            {
                var line = _reader?.ReadLine();

                if (line == null)
                {
                    errorMessage = "Remote closed the connection";
                    Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}: {errorMessage}");
                    break;
                }

                MessageReceived.Invoke(line);
            }
        }
        catch (OperationCanceledException)
        {
            errorMessage = $"Read timeout detected (CancellationRequested={ct.IsCancellationRequested})";
            Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}: " + errorMessage);
        }
        catch (IOException ioex)
        {
            errorMessage = ioex.Message;
            Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}.IOException: " + errorMessage);
        }
        catch (LiveApiErrorException)
        {
            throw;
        }
        catch (Exception ex)
        {
            errorMessage += ex.Message;
            Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}.Exception: Error processing messages: {ex.Message}\n{ex.StackTrace}");
        }
        finally
        {
            CleanupConnection();
        }
        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(DataReceiver)}: stopped");
        return errorMessage;
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

    private bool Authenticate(StreamReader reader, string dataSet)
    {
        var versionLine = default(string);
        var cramLine = default(string);

        try
        {
            versionLine = reader.ReadLine();
            cramLine = reader.ReadLine();
        }
        catch
        {
            return false;
        }

        if (Log.DebuggingEnabled)
        {
            Log.Debug($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Authenticate)}: Received initial message: {versionLine}, {cramLine}");
        }

        var request = new AuthenticationMessageRequest(cramLine, _apiKey, dataSet, _heartBeatInterval);

        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Authenticate)}: Sending CRAM reply: {request}");

        WriteData(request.ToString());

        var authResponse = default(string);
        try
        {
            authResponse = reader.ReadLine();
        }
        catch
        {
            return false;
        }

        var authenticationResponse = new AuthenticationMessageResponse(authResponse);

        if (!authenticationResponse.Success)
        {
            Log.Error($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Authenticate)}: Authentication response: {authResponse}");
            return false;
        }

        Log.Trace($"LiveDataTcpClientWrapper[{_dataSet}].{nameof(Authenticate)}: Successfully authenticated with session ID: {authenticationResponse.SessionId}");

        WriteData(request.GetStartSessionMessage()); // after start_session -> we get heartbeats and data

        return true;
    }

    private static string DetermineGateway(string dataset)
    {
        dataset = dataset.Replace('.', '-').ToLowerInvariant();
        return dataset + ".lsg.databento.com";
    }
}
