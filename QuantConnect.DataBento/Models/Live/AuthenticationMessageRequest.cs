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

namespace QuantConnect.Lean.DataSource.DataBento.Models.Live;

/// <summary>
/// Builds an authentication message for establishing a live Databento session
/// using a CRAM challenge, API key, and dataset.
/// </summary>
public readonly struct AuthenticationMessageRequest
{
    /// <summary>
    /// Length of the bucket identifier extracted from the API key.
    /// </summary>
    private const int BucketIdLength = 5;

    /// <summary>
    /// Dataset identifier to authenticate against.
    /// </summary>
    private readonly string _dataset;

    /// <summary>
    /// Computed authentication token in the format <c>$"{auth}-{bucket_id}"</c>.
    /// </summary>
    private readonly string _auth;

    /// <summary>
    /// Interval at which heartbeat messages are expected, used to keep the session alive.
    /// </summary>
    private readonly TimeSpan _heartBeatInterval;

    /// <summary>
    /// Initializes a new instance of the <see cref="AuthenticationMessageRequest"/> struct.
    /// </summary>
    /// <param name="cramLine">CRAM challenge line received from the server (e.g. <c>cram=...</c>).</param>
    /// <param name="apiKey">Databento API key used for authentication.</param>
    /// <param name="dataSet">Dataset to authenticate access for.</param>
    /// <param name="heartBeatInterval">Desired heartbeat interval for the live session.</param>
    public AuthenticationMessageRequest(string cramLine, string apiKey, string dataSet, TimeSpan heartBeatInterval)
    {
        _dataset = dataSet;
        _heartBeatInterval = heartBeatInterval;

        // Remove any trailing newline from the CRAM line
        var newLineIndex = cramLine.IndexOf('\n');
        if (newLineIndex >= 0)
        {
            cramLine = cramLine[..newLineIndex];
        }

        // Extract the challenge portion after "cram="
        cramLine = cramLine[(cramLine.IndexOf('=') + 1)..];

        var challengeKey = cramLine + '|' + apiKey;
        var bucketId = apiKey[^BucketIdLength..];

        _auth = $"{QuantConnect.Extensions.ToSHA256(challengeKey)}-{bucketId}";
    }

    /// <summary>
    /// Builds the authentication message sent to the server.
    /// </summary>
    /// <returns>
    /// A formatted authentication message including dataset, encoding,
    /// pretty price formatting, and heartbeat interval.
    /// </returns>
    public override string ToString()
    {
        return $"auth={_auth}|dataset={_dataset}|pretty_px=1|encoding=json|heartbeat_interval_s={_heartBeatInterval.TotalSeconds}";
    }

    /// <summary>
    /// Gets the message used to start a live session after successful authentication.
    /// </summary>
    /// <returns>
    /// The literal <c>start_session</c> command.
    /// </returns>
    public string GetStartSessionMessage()
    {
        return "start_session";
    }
}
