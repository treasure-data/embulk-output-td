package com.treasuredata.api;

public class TdApiNotFoundException
        extends TdApiResponseException
{
    public TdApiNotFoundException(int status, byte[] body)
    {
        super(status, body);
    }
}
