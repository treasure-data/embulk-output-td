package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class ArrayFieldWriter
        extends JsonFieldWriter
{
    public ArrayFieldWriter(String keyName)
    {
        super(keyName);
    }

    @Override
    public void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeValue(reader.getJson(column));
    }
}
