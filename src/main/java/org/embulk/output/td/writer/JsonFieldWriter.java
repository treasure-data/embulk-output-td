package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class JsonFieldWriter
        extends FieldWriter
{
    public JsonFieldWriter(String keyName)
    {
        super(keyName);
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        builder.writeString(reader.getJson(column).toJson());
    }
}
