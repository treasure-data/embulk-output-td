package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
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
    protected void writeBooleanValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeBoolean(reader.getBoolean(column));
    }

    @Override
    protected void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeBoolean(reader.getLong(column) != 0);
    }

    @Override
    protected void writeDoubleValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeBoolean(Double.valueOf(reader.getDouble(column)).longValue() != 0);
    }

    @Override
    protected void writeStringValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeBoolean(reader.getString(column).length() > 0);
    }

    @Override
    protected void writeTimestampValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from timestamp to boolean.");
    }

    @Override
    protected void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from json to boolean.");
    }
}
