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

using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.DataBento.Serialization;

/// <summary>
/// Provides globally accessible instances of <see cref="JsonSerializerSettings"/> 
/// preconfigured with custom contract resolvers, such as snake-case formatting.
/// </summary>
public static class JsonSettings
{
    /// <summary>
    /// Gets a reusable instance of <see cref="JsonSerializerSettings"/> that uses
    /// <see cref="SnakeCaseContractResolver"/> for snake-case property name formatting.
    /// </summary>
    public static readonly JsonSerializerSettings SnakeCase = new()
    {
        ContractResolver = SnakeCaseContractResolver.Instance
    };

    /// <summary>
    /// Gets a reusable instance of <see cref="JsonSerializerSettings"/> that uses
    /// <see cref="SnakeCaseContractResolver"/> for snake-case property name formatting
    /// and custom <see cref="LiveDataSnakeCase"/> converter.
    /// </summary>
    public static readonly JsonSerializerSettings LiveDataSnakeCase = new()
    {
        ContractResolver = SnakeCaseContractResolver.Instance,
        Converters = { new Converters.LiveDataConverter() }
    };
}
