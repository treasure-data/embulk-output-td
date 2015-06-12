package com.treasuredata.api;

public class TdApiException
        extends RuntimeException
{
    public TdApiException(String message)
    {
        super(message);
    }

    public TdApiException(Throwable cause)
    {
        super(cause);
    }

    public TdApiException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
