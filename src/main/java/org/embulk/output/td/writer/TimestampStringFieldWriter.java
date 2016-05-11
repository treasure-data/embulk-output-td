package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import java.io.IOException;

public class TimestampStringFieldWriter
        extends FieldWriter
{
    private final TimestampFormatter formatter;
    private final long offset;

    public TimestampStringFieldWriter(TimestampFormatter formatter, String keyName, long offset)
    {
        super(keyName);
        this.formatter = formatter;
        this.offset = offset;
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeString(formatter.format(reader.getTimestamp(column, offset)));
    }
}
