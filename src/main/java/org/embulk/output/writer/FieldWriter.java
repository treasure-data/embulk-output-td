package org.embulk.output.writer;

import org.msgpack.packer.Packer;

import java.io.IOException;

public abstract class FieldWriter
        extends WriteOnlyValue
{
    protected boolean isNil = false;

    public void setIsNil(boolean b)
    {
        isNil = b;
    }

    public boolean getIsNil()
    {
        return isNil;
    }

    @Override
    public void writeTo(Packer packer)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
