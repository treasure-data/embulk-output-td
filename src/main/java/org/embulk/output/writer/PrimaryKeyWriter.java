package org.embulk.output.writer;

import org.msgpack.type.Value;

public interface PrimaryKeyWriter
        extends Value
{
    public long getIndexKey();
}