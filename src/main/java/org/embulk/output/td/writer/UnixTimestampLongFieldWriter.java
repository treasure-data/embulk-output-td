package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class UnixTimestampLongFieldWriter
        extends LongFieldWriter
{
    private final int fractionUnit;

    public UnixTimestampLongFieldWriter(String keyName, int fractionUnit)
    {
        super(keyName);
        this.fractionUnit = fractionUnit;
    }

    @Override
    public void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeLong(reader.getLong(column) / fractionUnit);
    }
}
