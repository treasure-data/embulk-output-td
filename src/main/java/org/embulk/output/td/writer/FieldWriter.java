package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public abstract class FieldWriter
{
    private final String keyName;

    protected FieldWriter(String keyName)
    {
        this.keyName = keyName;
    }

    public void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        writeKey(builder);
        if (reader.isNull(column)) {
            builder.writeNil();
        }
        else {
            writeValue(builder, reader, column);
        }
    }

    private void writeKey(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeString(keyName);
    }

    protected abstract void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;
}
