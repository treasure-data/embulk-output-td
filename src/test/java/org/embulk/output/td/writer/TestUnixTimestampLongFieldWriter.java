package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.output.td.TdOutputPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestUnixTimestampLongFieldWriter
{
    private static final String KEY_NAME = "key_name";

    @Mock
    private MsgpackGZFileBuilder builder;

    @Mock
    private PageReader reader;

    @Mock
    private Column column;

    private UnixTimestampLongFieldWriter writer = new UnixTimestampLongFieldWriter(KEY_NAME, TdOutputPlugin.UnixTimestampUnit.MILLI.getFractionUnit());

    @Test
    public void testWriteLongValue() throws IOException
    {
        when(reader.getLong(column)).thenReturn(1000L);
        writer.writeLongValue(builder, reader, column);
        verify(builder).writeLong(1);
    }
}
