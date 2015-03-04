package org.embulk.output.writer;

import org.msgpack.MessageTypeException;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.BooleanValue;
import org.msgpack.type.FloatValue;
import org.msgpack.type.IntegerValue;
import org.msgpack.type.MapValue;
import org.msgpack.type.NilValue;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueType;

public abstract class WriteOnlyValue
    implements Value
{

    public ValueType getType()
    {
        return null;
    }

    public StringBuilder toString(StringBuilder sb)
    {
        return sb.append(this);
    }

    public boolean isNilValue()
    {
        return false;
    }

    public boolean isBooleanValue()
    {
        return false;
    }

    public boolean isIntegerValue()
    {
        return false;
    }

    public boolean isFloatValue()
    {
        return false;
    }

    public boolean isArrayValue()
    {
        return false;
    }

    public boolean isMapValue()
    {
        return false;
    }

    public boolean isRawValue()
    {
        return false;
    }

    public NilValue asNilValue()
    {
        throw new MessageTypeException();
    }

    public BooleanValue asBooleanValue()
    {
        throw new MessageTypeException();
    }

    public IntegerValue asIntegerValue()
    {
        throw new MessageTypeException();
    }

    public FloatValue asFloatValue()
    {
        throw new MessageTypeException();
    }

    public ArrayValue asArrayValue()
    {
        throw new MessageTypeException();
    }

    public MapValue asMapValue()
    {
        throw new MessageTypeException();
    }

    public RawValue asRawValue()
    {
        throw new MessageTypeException();
    }
}
