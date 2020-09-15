package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class LongFieldWriter
        extends FieldWriter
{
    public LongFieldWriter(String keyName)
    {
        super(keyName);
    }

    @Override
    protected void writeBooleanValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeLong(reader.getBoolean(column) ? 1 : 0);
    }

    @Override
    protected void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeLong(reader.getLong(column));
    }

    @Override
    protected void writeDoubleValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeLong(Double.valueOf(reader.getDouble(column)).longValue());
    }

    @Override
    protected void writeStringValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeLong(Long.valueOf(reader.getString(column)));
    }

    @Override
    protected void writeTimestampValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeLong(reader.getTimestamp(column).getEpochSecond());
    }

    @Override
    protected void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from json to long.");
    }
}
