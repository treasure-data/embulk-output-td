package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public class UnixTimestampFieldDuplicator
        extends LongFieldWriter
{
    private final UnixTimestampLongFieldWriter timeFieldWriter;

    public UnixTimestampFieldDuplicator(String keyName, String duplicateKeyName, int fractionUnit)
    {
        super(keyName);
        timeFieldWriter = new UnixTimestampLongFieldWriter(duplicateKeyName, fractionUnit);
    }

    @Override
    public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException
    {
        super.writeValue(builder, reader, column);
        timeFieldWriter.writeKeyValue(builder, reader, column);
    }
}
