package org.embulk.output.writer;

import org.msgpack.packer.Packer;

import java.io.IOException;

public class DoubleFieldWriter
        extends FieldWriter
{
    private double value;

    public boolean update(double value)
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