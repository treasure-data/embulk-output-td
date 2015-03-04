package org.embulk.output.writer;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.packer.Packer;

import java.io.IOException;

public class TimestampStringFieldWriter
        extends FieldWriter
        implements TimestampFieldWriter
{
    protected String value;
    protected TimestampFormatter formatter;

    public TimestampStringFieldWriter(TimestampFormatter formatter)
    {
        this.formatter = formatter;
    }

    @Override
    public boolean update(Timestamp value)
    {
        this.value = formatter.format(value);
        return true;
    }

    public TimestampFormatter getTimestampFormatter()
    {
        return formatter;
    }

    @Override
    public void writeTo(Packer packer) throws IOException
    {
        packer.write(value);
    }
}