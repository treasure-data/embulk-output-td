package com.treasuredata.api;

import java.util.concurrent.TimeoutException;

public class TDApiExecutionTimeoutException
        extends TDApiExecutionException
{
    public TDApiExecutionTimeoutException(TimeoutException cause)
    {
        super(cause);
    }

    @Override
    public TimeoutException getCause() {
        return (TimeoutException) super.getCause();
    }
}
