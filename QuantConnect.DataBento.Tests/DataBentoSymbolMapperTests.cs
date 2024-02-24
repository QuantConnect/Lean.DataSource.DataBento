using NUnit.Framework;
using QuantConnect.DateBento;
using QuantConnect.Tests;

namespace QuantConnect.DataBento.Tests;

[TestFixture]
public class DataBentoSymbolMapperTests
{
    
    [Test]
    public void ConvertsLeanEquitySymbolToPolygon()
    {
        var mapper = new DataBentoSymbolMapper();
        var symbol = Symbols.AAPL;
        var polygonSymbol = mapper.GetBrokerageSymbol(symbol);
        Assert.That(polygonSymbol, Is.EqualTo("AAPL"));
    }
}