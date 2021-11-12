package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.io.IOException;
import java.time.Instant;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestStringFieldWriter
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private StringFieldWriter writer = new StringFieldWriter(KEY_NAME, null);

    @Test
    public void testWriteBooleanValue() throws IOException
    {
        // write 'true' if the boolean value is true
        {
            when(reader.getBoolean(column)).thenReturn(true);
            writer.writeBooleanValue(builder, reader, column);
            verify(builder).writeString("true");
        }
        // write 'false' if the boolean value is false
        {
            when(reader.getBoolean(column)).thenReturn(false);
            writer.writeBooleanValue(builder, reader, column);
            verify(builder).writeString("false");
        }
    }

    @Test
    public void testWriteLongValue() throws IOException
    {
        when(reader.getLong(column)).thenReturn(10L);
        writer.writeLongValue(builder, reader, column);
        verify(builder).writeString("10");
    }

    @Test
    public void testWriteDoubleValue() throws IOException
    {
        when(reader.getDouble(column)).thenReturn(50.5);
        writer.writeDoubleValue(builder, reader, column);
        verify(builder).writeString("50.5");
    }

    @Test
    public void testWriteStringValue() throws IOException
    {
        when(reader.getString(column)).thenReturn("a string");
        writer.writeStringValue(builder, reader, column);
        verify(builder).writeString("a string");
    }

    @Test
    public void testWriteTimestampValue() throws IOException
    {
        writer = new StringFieldWriter(KEY_NAME, TimestampFormatter.builderWithJava("yyyy-MM-dd").build());
        when(reader.getTimestampInstant(column)).thenReturn(Instant.ofEpochSecond(200));
        writer.writeTimestampValue(builder, reader, column);
        verify(builder).writeString("1970-01-01");
    }

    @Test
    public void testWriteJsonValue() throws IOException
    {
        when(reader.getJson(column)).thenReturn(new ImmutableStringValueImpl("json_value"));
        writer.writeJsonValue(builder, reader, column);
        verify(builder).writeString("json_value");
    }
}
