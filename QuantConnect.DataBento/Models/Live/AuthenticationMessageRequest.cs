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

public readonly struct AuthenticationMessageRequest
{
    private const int BucketIdLength = 5;

    private readonly string _dataset;

    private readonly string _auth;

    private readonly TimeSpan _heartBeatInterval;


    public AuthenticationMessageRequest(string cramLine, string apiKey, string dataSet, TimeSpan? heartBeatInterval = default)
    {
        _dataset = dataSet;

        var newLineIndex = cramLine.IndexOf('\n');
        if (newLineIndex >= 0)
        { 
            cramLine = cramLine[..newLineIndex];
        }

        cramLine = cramLine[(cramLine.IndexOf('=') + 1)..];

        var challengeKey = cramLine + '|' + apiKey;
        var bucketId = apiKey[^BucketIdLength..];

        _auth = $"{QuantConnect.Extensions.ToSHA256(challengeKey)}-{bucketId}";

        switch (heartBeatInterval)
        {
            case null:
                _heartBeatInterval = TimeSpan.FromSeconds(5);
                break;
            case { TotalSeconds: < 5 }:
                throw new ArgumentOutOfRangeException(nameof(heartBeatInterval), "The Heartbeat interval must be not les 5 seconds.");
            default:
                _heartBeatInterval = heartBeatInterval.Value;
                break;
        }
    }

    public override string ToString()
    {
        return $"auth={_auth}|dataset={_dataset}|pretty_px=1|encoding=json|heartbeat_interval_s={_heartBeatInterval.TotalSeconds}";
    }

    public string GetStartSessionMessage()
    {
        return "start_session";
    }
}
