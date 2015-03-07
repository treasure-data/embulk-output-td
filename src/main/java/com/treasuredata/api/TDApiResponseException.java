package com.treasuredata.api;

public class TDApiResponseException
        extends TDApiException
{
    private final int status;
    private final byte[] body;

    public TDApiResponseException(int status, byte[] body)
    {
        super(String.format("TD API returned HTTP code %d", status));
        this.status = status;
        this.body = body;
    }

    public TDApiResponseException(int status, byte[] body, Throwable cause)
    {
        super(String.format("TD API returned HTTP code %d", status), cause);
        this.status = status;
        this.body = body;
    }

    public int getStatusCode()
    {
        return status;
    }

    public byte[] getBody()
    {
        return body;
    }
}
