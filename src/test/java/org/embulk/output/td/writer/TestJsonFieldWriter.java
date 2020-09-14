package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJsonFieldWriter
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private JsonFieldWriter writer = new JsonFieldWriter(KEY_NAME);

    @Test(expected = DataException.class)
    public void testWriteBooleanValue()
    {
        writer.writeBooleanValue(builder, reader, column);
    }

    @Test(expected = DataException.class)
    public void testWriteLongValue()
    {
        writer.writeLongValue(builder, reader, column);
    }

    @Test(expected = DataException.class)
    public void testWriteDoubleValue()
    {
        writer.writeDoubleValue(builder, reader, column);
    }

    @Test(expected = DataException.class)
    public void testWriteStringValue()
    {
        writer.writeStringValue(builder, reader, column);
    }

    @Test(expected = DataException.class)
    public void testWriteTimestampValue()
    {
        writer.writeTimestampValue(builder, reader, column);
    }

    @Test
    public void testWriteJsonValue() throws IOException
    {
        when(reader.getJson(column)).thenReturn(new ImmutableStringValueImpl("json_value"));
        writer.writeJsonValue(builder, reader, column);
        verify(builder).writeString("\"json_value\"");
    }
}
