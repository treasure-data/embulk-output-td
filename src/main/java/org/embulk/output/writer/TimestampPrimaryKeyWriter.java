package org.embulk.output.writer;

public class TimestampPrimaryKeyWriter
        extends TimestampLongFieldWriter
        implements PrimaryKeyWriter
{
    @Override
    public long getIndexKey() {
        return value;
    }
}