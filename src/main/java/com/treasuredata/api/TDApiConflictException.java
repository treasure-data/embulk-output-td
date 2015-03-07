package com.treasuredata.api;

public class TDApiConflictException
        extends TDApiResponseException
{
    public TDApiConflictException(int status, byte[] body)
    {
        super(status, body);
    }
}
