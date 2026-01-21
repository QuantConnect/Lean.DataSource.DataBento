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
*/

using Newtonsoft.Json.Serialization;

namespace QuantConnect.Lean.DataSource.DataBento.Serialization;

/// <summary>
/// A singleton <see cref="DefaultContractResolver"/> implementation that applies 
/// a <see cref="SnakeCaseNamingStrategy"/> to JSON property names.
/// </summary>
public sealed class SnakeCaseContractResolver : DefaultContractResolver
{
    /// <summary>
    /// Gets the singleton instance of the <see cref="SnakeCaseContractResolver"/>.
    /// </summary>
    public static readonly SnakeCaseContractResolver Instance = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="SnakeCaseContractResolver"/> class 
    /// with a <see cref="SnakeCaseNamingStrategy"/>.
    /// </summary>
    private SnakeCaseContractResolver()
    {
        NamingStrategy = new SnakeCaseNamingStrategy();
    }
}
