package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
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
public class TestArrayFieldWriter
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private ArrayFieldWriter writer = new ArrayFieldWriter(KEY_NAME);

    @Test
    public void testWriteJsonValue() throws IOException
    {
        ImmutableStringValueImpl value = new ImmutableStringValueImpl("json_value");
        when(reader.getJson(column)).thenReturn(value);
        writer.writeJsonValue(builder, reader, column);
        verify(builder).writeValue(value);
    }
}
