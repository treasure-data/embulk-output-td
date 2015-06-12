package com.treasuredata.api;

public class TdApiConflictException
        extends TdApiResponseException
{
    public TdApiConflictException(int status, byte[] body)
    {
        super(status, body);
    }
}
