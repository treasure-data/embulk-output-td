package org.embulk.output.writer;

import org.msgpack.packer.Packer;

import java.io.IOException;

public class StringFieldWriter
        extends FieldWriter
{
    private String value;

    public boolean update(String value)
    {
        this.value = value;
        return true;
    }

    @Override
    public void writeTo(Packer packer)
            throws IOException
    {
        if (value == null || getIsNil()) {
            packer.writeNil();
            setIsNil(false);
        } else {
            packer.write(value);
        }
    }
}