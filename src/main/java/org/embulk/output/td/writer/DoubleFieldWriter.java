package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class DoubleFieldWriter
        extends FieldWriter
{
    public DoubleFieldWriter(String keyName)
    {
        super(keyName);
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeDouble(reader.getDouble(column));
    }
}
