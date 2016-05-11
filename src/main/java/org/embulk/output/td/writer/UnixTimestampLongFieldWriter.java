package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class UnixTimestampLongFieldWriter
        extends FieldWriter
{
    private final int fractionUnit;
    private final long offset;

    UnixTimestampLongFieldWriter(String keyName, int fractionUnit, long offset)
    {
        super(keyName);
        this.fractionUnit = fractionUnit;
        this.offset = offset;
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeLong((reader.getLong(column) / fractionUnit) + offset);
    }
}
