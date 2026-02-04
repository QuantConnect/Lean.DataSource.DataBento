namespace QuantConnect.Lean.DataSource.DataBento.Models;

public abstract class LevelOneDataBase : MarketDataBase
{
    /// <summary>
    /// Side of the book affected by the update.
    /// </summary>
    public char Side { get; set; }

    /// <summary>
    /// Price associated with the update.
    /// </summary>
    public decimal? Price { get; set; }

    /// <summary>
    /// The side that initiates the event.
    /// </summary>
    /// <remarks>
    /// Can be:
    /// - Ask for a sell order (or sell aggressor in a trade);
    /// - Bid for a buy order (or buy aggressor in a trade);
    /// - None where no side is specified by the original source.
    /// </remarks>
    public int Size { get; set; }

    /// <summary>
    /// A bit field indicating event end, message characteristics, and data quality.
    /// </summary>
    public int Flags { get; set; }

    /// <summary>
    /// Snapshot of level-one bid and ask data.
    /// </summary>
    public IReadOnlyList<LevelOneBookLevel> Levels { get; set; } = [];
}
