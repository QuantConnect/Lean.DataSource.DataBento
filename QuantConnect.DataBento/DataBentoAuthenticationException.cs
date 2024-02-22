namespace QuantConnect.DateBento;

public class DataBentoAuthenticationException : Exception
{
    public DataBentoAuthenticationException(string? message) : base(message)
    {
    }
}