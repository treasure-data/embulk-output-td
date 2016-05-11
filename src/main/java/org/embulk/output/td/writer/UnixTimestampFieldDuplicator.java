package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class UnixTimestampFieldDuplicator
        implements IFieldWriter
{
    private final IFieldWriter nextWriter;
    private final UnixTimestampLongFieldWriter timeFieldWriter;

    public UnixTimestampFieldDuplicator(IFieldWriter nextWriter, String duplicateKeyName, int fractionUnit, long offset)
    {
        this.nextWriter = nextWriter;
        timeFieldWriter = new UnixTimestampLongFieldWriter(duplicateKeyName, fractionUnit, offset);
    }

    public void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        nextWriter.writeKeyValue(builder, reader, column);
        timeFieldWriter.writeKeyValue(builder, reader, column);
    }
}
