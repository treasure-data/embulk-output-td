package org.embulk.output.td.writer;

import org.apache.commons.lang3.StringUtils;
import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestBooleanFieldWriter
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private BooleanFieldWriter writer = new BooleanFieldWriter(KEY_NAME);

    @Test
    public void testWriteBooleanValue() throws IOException
    {
        when(reader.getBoolean(column)).thenReturn(true);
        writer.writeBooleanValue(builder, reader, column);
        verify(builder).writeBoolean(true);
    }

    @Test
    public void testWriteLongValue() throws IOException
    {
        // write true if the long value is not equal to 0
        {
            when(reader.getLong(column)).thenReturn(10L);
            writer.writeLongValue(builder, reader, column);
            verify(builder).writeBoolean(true);
        }

        // write false if long value is equal to 0
        {
            when(reader.getLong(column)).thenReturn(0L);
            writer.writeLongValue(builder, reader, column);
            verify(builder).writeBoolean(false);
        }
    }

    @Test
    public void testWriteDoubleValue() throws IOException
    {
        // write true if the truncation of double value is not equal to 0
        {
            when(reader.getDouble(column)).thenReturn(10.0);
            writer.writeDoubleValue(builder, reader, column);
            verify(builder).writeBoolean(true);
        }

        // write false if the round to nearest of double value is equal to 0
        {
            when(reader.getDouble(column)).thenReturn(0.5);
            writer.writeDoubleValue(builder, reader, column);
            verify(builder).writeBoolean(false);
        }
    }

    @Test
    public void testWriteStringValue() throws IOException
    {
        // write true if the length of string value is larger than 0
        {
            when(reader.getString(column)).thenReturn("larger_than_0");
            writer.writeStringValue(builder, reader, column);
            verify(builder).writeBoolean(true);
        }

        // write false if the length of string value is equal to 0
        {
            when(reader.getString(column)).thenReturn(StringUtils.EMPTY);
            writer.writeStringValue(builder, reader, column);
            verify(builder).writeBoolean(false);
        }
    }

    @Test(expected = DataException.class)
    public void testWriteTimestampValue()
    {
        writer.writeTimestampValue(builder, reader, column);
    }

    @Test(expected = DataException.class)
    public void testWriteJsonValue()
    {
        writer.writeJsonValue(builder, reader, column);
    }
}
