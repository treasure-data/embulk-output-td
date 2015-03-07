package com.treasuredata.api;

public class TDApiException
        extends RuntimeException
{
    public TDApiException(String message)
    {
        super(message);
    }

    public TDApiException(Throwable cause)
    {
        super(cause);
    }

    public TDApiException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
