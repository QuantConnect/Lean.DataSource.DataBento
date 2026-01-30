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
/// Represents a structured error response returned by the Historical DataBento API.
/// </summary>
public sealed class ErrorResponse
{
    /// <summary>
    /// Detailed information about the error.
    /// </summary>
    public ErrorDetail? Detail { get; set; }
}

/// <summary>
/// Describes a DataBento API error.
/// </summary>
public sealed class ErrorDetail
{
    /// <summary>
    /// Machine-readable error identifier (e.g. data_end_after_available_end).
    /// </summary>
    public string? Case { get; set; }

    /// <summary>
    /// Human-readable explanation of the error.
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// HTTP status code associated with the error.
    /// </summary>
    public int StatusCode { get; set; }

    /// <summary>
    /// Link to the relevant DataBento API documentation.
    /// </summary>
    public string? Docs { get; set; }

    /// <summary>
    /// Request-specific data providing additional error context.
    /// </summary>
    public ErrorPayload? Payload { get; set; }
}

/// <summary>
/// Contains request parameters and dataset limits related to the error.
/// </summary>
public sealed class ErrorPayload
{
    /// <summary>
    /// Dataset identifier used in the request.
    /// </summary>
    public string Dataset { get; set; }

    /// <summary>
    /// Requested start timestamp.
    /// </summary>
    public DateTimeOffset Start { get; set; }

    /// <summary>
    /// Requested end timestamp.
    /// </summary>
    public DateTimeOffset End { get; set; }

    /// <summary>
    /// Earliest timestamp available for the dataset.
    /// </summary>
    public DateTimeOffset AvailableStart { get; set; }

    /// <summary>
    /// Latest timestamp available for the dataset.
    /// </summary>
    public DateTimeOffset AvailableEnd { get; set; }
}

/// <summary>
/// Contains known DataBento Historical API error case identifiers.
/// </summary>
public static class ErrorCases
{
    /// <summary>
    /// The requested end timestamp exceeds the dataset's available end.
    /// </summary>
    public const string DataEndAfterAvailableEnd = "data_end_after_available_end";

    /// <summary>
    /// The requested start timestamp exceeds the dataset's available start.
    /// </summary>
    public const string DataStartBeforeAvailableStart = "data_start_before_available_start";

    /// <summary>
    /// The requested start timestamp is greater than or equal to the requested end timestamp.
    /// </summary>
    public const string DataTimeRangeStartOnOrAfterEnd = "data_time_range_start_on_or_after_end";
}
