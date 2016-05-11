package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class TimestampLongFieldWriter
        extends FieldWriter
{
    private final long offset;

    public TimestampLongFieldWriter(String keyName, long offset)
    {
        super(keyName);
        this.offset = offset;
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeLong(reader.getTimestamp(column).getEpochSecond() + offset);
    }
}
