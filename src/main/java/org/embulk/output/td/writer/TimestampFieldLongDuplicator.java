package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import java.io.IOException;

public class TimestampFieldLongDuplicator
        extends TimestampStringFieldWriter
{
    private final TimestampLongFieldWriter timeFieldWriter;

    public TimestampFieldLongDuplicator(TimestampFormatter formatter, String keyName, String longDuplicateKeyName)
    {
        super(formatter, keyName);
        timeFieldWriter = new TimestampLongFieldWriter(longDuplicateKeyName);
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        super.writeValue(builder, reader, column);
        timeFieldWriter.writeKeyValue(builder, reader, column);
    }
}
