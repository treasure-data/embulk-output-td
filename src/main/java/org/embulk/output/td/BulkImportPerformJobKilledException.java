package org.embulk.output.td;

public class BulkImportPerformJobKilledException extends RuntimeException
{
    public BulkImportPerformJobKilledException(String jobId)
    {
        super(String.format("The bulk import perform job with ID: '%s' was killed.", jobId));
    }
}
