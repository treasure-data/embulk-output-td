package com.treasuredata.api;

public class TdApiExecutionInterruptedException
        extends TdApiExecutionException
{
    public TdApiExecutionInterruptedException(InterruptedException cause)
    {
        super(cause);
    }

    @Override
    public InterruptedException getCause()
    {
        return (InterruptedException) super.getCause();
    }
}
