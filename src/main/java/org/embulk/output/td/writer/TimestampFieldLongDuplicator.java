package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class TimestampFieldLongDuplicator
        implements IFieldWriter
{
    private final IFieldWriter nextWriter;
    private final LongFieldWriter timeFieldWriter;

    public TimestampFieldLongDuplicator(IFieldWriter nextWriter, String duplicateKeyName)
    {
        this.nextWriter = nextWriter;
        timeFieldWriter = new LongFieldWriter(duplicateKeyName);
    }

    public void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        nextWriter.writeKeyValue(builder, reader, column);
        timeFieldWriter.writeKeyValue(builder, reader, column);
    }
}
