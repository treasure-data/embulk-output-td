package org.embulk.output.writer;

import org.msgpack.packer.Packer;

import java.io.IOException;

public class LongFieldWriter
        extends FieldWriter
{
    protected long value;

    public boolean update(long value)
    {
        this.value = value;
        return true;
    }

    @Override
    public void writeTo(Packer packer)
            throws IOException
    {
        if (getIsNil()) {
            packer.writeNil();
            setIsNil(false);
        } else {
            packer.write(value);
        }
    }
}