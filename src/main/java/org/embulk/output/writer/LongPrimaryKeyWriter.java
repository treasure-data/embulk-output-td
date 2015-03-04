package org.embulk.output.writer;

public class LongPrimaryKeyWriter
        extends LongFieldWriter
        implements PrimaryKeyWriter
{
    @Override
    public long getIndexKey() {
        return value;
    }
}