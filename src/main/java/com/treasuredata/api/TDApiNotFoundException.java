package com.treasuredata.api;

public class TDApiNotFoundException
        extends TDApiResponseException
{
    public TDApiNotFoundException(int status, byte[] body)
    {
        super(status, body);
    }
}
