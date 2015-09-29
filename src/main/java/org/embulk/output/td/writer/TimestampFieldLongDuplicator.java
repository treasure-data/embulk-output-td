package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import java.io.IOException;

public class TimestampFieldLongDuplicator
        implements IFieldWriter
{
    private final IFieldWriter nextWriter;
    private final TimestampLongFieldWriter timeFieldWriter;

    public TimestampFieldLongDuplicator(IFieldWriter nextWriter, String duplicateKeyName)
    {
        this.nextWriter = nextWriter;
        timeFieldWriter = new TimestampLongFieldWriter(duplicateKeyName);
    }

    @Override
    public void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        nextWriter.writeKeyValue(builder, reader, column);
        timeFieldWriter.writeKeyValue(builder, reader, column);
    }
}
