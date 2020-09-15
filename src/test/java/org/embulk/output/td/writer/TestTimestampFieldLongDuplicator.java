package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestTimestampFieldLongDuplicator
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private IFieldWriter nextWriter;

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private TimestampFieldLongDuplicator timestampFieldLongDuplicator;

    @Before
    public void setUp()
    {
        timestampFieldLongDuplicator = new TimestampFieldLongDuplicator(nextWriter, KEY_NAME);
    }

    @Test
    public void testWriteKeyValue() throws IOException
    {
        when(reader.getLong(column)).thenReturn(10L);
        when(column.getType()).thenReturn(Types.LONG);
        timestampFieldLongDuplicator.writeKeyValue(builder, reader, column);
        verify(nextWriter).writeKeyValue(builder, reader, column);
        verify(builder).writeLong(10);
    }
}
