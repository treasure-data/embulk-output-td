package org.embulk.output.writer;

import org.embulk.spi.time.Timestamp;
import org.msgpack.packer.Packer;

import java.io.IOException;

public class TimestampLongFieldWriter
        extends FieldWriter
        implements TimestampFieldWriter
{
    protected long value;

    @Override
    public boolean update(Timestamp value)
    {
        this.value = value.getEpochSecond();
        return true;
    }

    @Override
    public void writeTo(Packer packer)
            throws IOException
    {
        packer.write(value);
    }
}