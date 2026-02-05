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

using QuantConnect.Lean.DataSource.DataBento.Models.Live;

namespace QuantConnect.Lean.DataSource.DataBento.Exceptions;

/// <summary>
/// Represents an exception that is thrown when the Live API
/// returns an error message.
/// </summary>
public sealed class LiveApiErrorException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="LiveApiErrorException"/> class
    /// using the specified Live API error message.
    /// </summary>
    /// <param name="error">
    /// The error message returned by the Live API.
    /// </param>
    public LiveApiErrorException(ErrorMessage error)
        : base(error.ToString())
    {
    }
}
