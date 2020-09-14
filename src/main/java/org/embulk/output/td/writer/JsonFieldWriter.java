package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
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
    protected void writeBooleanValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from boolean to json.");
    }

    @Override
    protected void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from long to json.");
    }

    @Override
    protected void writeDoubleValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from double to json.");
    }

    @Override
    protected void writeStringValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from string to json.");
    }

    @Override
    protected void writeTimestampValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        throw new DataException("It is not able to convert from timestamp to json.");
    }

    @Override
    protected void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(reader.getJson(column).toJson());
    }
}
