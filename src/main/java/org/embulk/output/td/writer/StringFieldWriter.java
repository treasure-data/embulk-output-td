package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import java.io.IOException;

public class StringFieldWriter
        extends FieldWriter
{
    private final TimestampFormatter formatter;

    public StringFieldWriter(String keyName, TimestampFormatter formatter)
    {
        super(keyName);
        this.formatter = formatter;
    }

    @Override
    protected void writeBooleanValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(reader.getBoolean(column) ? "true" : "false");
    }

    @Override
    protected void writeLongValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(String.valueOf(reader.getLong(column)));
    }

    @Override
    protected void writeDoubleValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(String.valueOf(reader.getDouble(column)));
    }

    @Override
    protected void writeStringValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(reader.getString(column));
    }

    @Override
    protected void writeTimestampValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(formatter.format(reader.getTimestamp(column)));
    }

    @Override
    protected void writeJsonValue(MsgpackGZFileBuilder builder, PageReader reader, Column column) throws IOException
    {
        builder.writeString(reader.getJson(column).toJson());
    }
}
