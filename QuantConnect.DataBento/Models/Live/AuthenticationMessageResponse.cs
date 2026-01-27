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

using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.DataBento.Models.Live;

public readonly struct AuthenticationMessageResponse
{
    public bool Success { get; }
    public string? Error { get; }
    public int SessionId { get; }

    public AuthenticationMessageResponse(string input)
    {
        if (Log.DebuggingEnabled)
        {
            Log.Debug($"{nameof(AuthenticationMessageResponse)}.ctor: Authentication response: {input}");
        }

        var parts = input.Split('|', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var kv = part.Split('=', 2);

            switch (kv[0])
            {
                case "success":
                    Success = kv[1] == "1";
                    break;
                case "error":
                    Error = kv[1];
                    break;
                case "session_id":
                    SessionId = int.Parse(kv[1]);
                    break;
            }
        }
    }
}
