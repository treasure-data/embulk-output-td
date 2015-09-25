package com.treasuredata.api.model;

import com.google.common.base.Objects;

public class TDMapColumnType
        implements TDColumnType
{
    private TDColumnType keyType;
    private TDColumnType valueType;

    public TDMapColumnType(TDColumnType keyType, TDColumnType valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public TDColumnType getKeyType()
    {
        return keyType;
    }

    public TDColumnType getValueType()
    {
        return valueType;
    }

    @Override
    public String toString()
    {
        return "map<" + keyType + "," + valueType + ">";
    }

    @Override
    public boolean isPrimitive()
    {
        return false;
    }

    @Override
    public boolean isArrayType()
    {
        return false;
    }

    @Override
    public boolean isMapType()
    {
        return true;
    }

    @Override
    public TDPrimitiveColumnType asPrimitiveType()
    {
        return null;
    }

    @Override
    public TDArrayColumnType asArrayType()
    {
        return null;
    }

    @Override
    public TDMapColumnType asMapType()
    {
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TDMapColumnType other = (TDMapColumnType) obj;
        return Objects.equal(this.keyType, other.keyType) &&
                Objects.equal(this.valueType, other.valueType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyType, valueType);
    }
}
