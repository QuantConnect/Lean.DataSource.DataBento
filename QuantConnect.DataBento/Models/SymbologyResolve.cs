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

namespace QuantConnect.Lean.DataSource.DataBento.Models;

/// <summary>
/// Symbology resolution result.
/// Only dictionary keys (resolved symbols) are used; values are ignored.
/// </summary>
public class SymbologyResolve
{
    /// <summary>
    /// A map of resolved symbol identifiers to API-defined payloads.
    /// </summary>>
    public Dictionary<string, object> Result { get; init; } = [];
}
