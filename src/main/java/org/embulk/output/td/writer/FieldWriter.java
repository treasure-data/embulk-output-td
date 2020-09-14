package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;

import java.io.IOException;

public abstract class FieldWriter
        implements IFieldWriter
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
            return;
        }

        if (column.getType() == Types.BOOLEAN) {
            writeBooleanValue(builder, reader, column);
        }
        else if (column.getType() == Types.LONG) {
            writeLongValue(builder, reader, column);
        }
        else if (column.getType() == Types.DOUBLE) {
            writeDoubleValue(builder, reader, column);
        }
        else if (column.getType() == Types.STRING) {
            writeStringValue(builder, reader, column);
        }
        else if (column.getType() == Types.TIMESTAMP) {
            writeTimestampValue(builder, reader, column);
        }
        else {
            writeJsonValue(builder, reader, column);
        }
    }

    private void writeKey(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeString(keyName);
    }

    protected abstract void writeBooleanValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;

    protected abstract void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;

    protected abstract void writeDoubleValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;

    protected abstract void writeStringValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;

    protected abstract void writeTimestampValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;

    protected abstract void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;
}
