package org.embulk.output.writer;

import org.embulk.spi.time.Timestamp;

public interface TimestampFieldWriter
{
    boolean update(Timestamp value);
}
