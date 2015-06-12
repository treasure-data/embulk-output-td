package com.treasuredata.api;

import java.util.concurrent.TimeoutException;

public class TdApiExecutionTimeoutException
        extends TdApiExecutionException
{
    public TdApiExecutionTimeoutException(TimeoutException cause)
    {
        super(cause);
    }

    @Override
    public TimeoutException getCause() {
        return (TimeoutException) super.getCause();
    }
}
