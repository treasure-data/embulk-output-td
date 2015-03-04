package org.embulk.output.writer;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;

public class TimestampPrimaryKeyDuplicater
        extends TimestampStringFieldWriter
        implements PrimaryKeyWriter
{
    private final LongFieldWriter timeColumnWriter;

    public TimestampPrimaryKeyDuplicater(TimestampFormatter formatter)
    {
        super(formatter);
        this.timeColumnWriter = new LongFieldWriter();
    }

    public FieldWriter getTimeColumnWriter()
    {
        return timeColumnWriter;
    }

    @Override
    public boolean update(Timestamp value)
    {
        timeColumnWriter.update(value.getEpochSecond());
        return super.update(value);
    }

    @Override
    public long getIndexKey() {
        return timeColumnWriter.value;
    }
}