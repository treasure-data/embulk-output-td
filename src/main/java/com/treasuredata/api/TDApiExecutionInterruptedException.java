package com.treasuredata.api;

public class TDApiExecutionInterruptedException
        extends TDApiExecutionException
{
    public TDApiExecutionInterruptedException(InterruptedException cause)
    {
        super(cause);
    }

    @Override
    public InterruptedException getCause() {
        return (InterruptedException) super.getCause();
    }
}
