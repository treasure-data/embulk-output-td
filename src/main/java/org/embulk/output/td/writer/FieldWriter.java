package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;

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

        if (column.getType() instanceof BooleanType) {
            writeBooleanValue(builder, reader, column);
        }
        else if (column.getType() instanceof LongType) {
            writeLongValue(builder, reader, column);
        }
        else if (column.getType() instanceof DoubleType) {
            writeDoubleValue(builder, reader, column);
        }
        else if (column.getType() instanceof StringType) {
            writeStringValue(builder, reader, column);
        }
        else if (column.getType() instanceof TimestampType) {
            writeTimestampValue(builder, reader, column);
        }
        else if (column.getType() instanceof JsonType){
            writeJsonValue(builder, reader, column);
        }
        else {
            // this state should not be reached because all supported types have been handled above.
            throw new IllegalArgumentException(String.format("Column: %s contains unsupported type: %s",
                    column.getName(), column.getType().getName()));
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
