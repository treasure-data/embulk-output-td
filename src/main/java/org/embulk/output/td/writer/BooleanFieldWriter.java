package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class BooleanFieldWriter
        extends FieldWriter
{
    public BooleanFieldWriter(String keyName)
    {
        super(keyName);
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeBoolean(reader.getBoolean(column));
    }
}
