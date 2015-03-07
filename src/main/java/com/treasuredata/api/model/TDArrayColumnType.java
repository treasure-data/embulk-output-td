package com.treasuredata.api.model;

import com.google.common.base.Objects;

public class TDArrayColumnType
        implements TDColumnType
{
    private final TDColumnType elementType;

    public TDArrayColumnType(TDColumnType elementType)
    {
        this.elementType = elementType;
    }

    public TDColumnType getElementType()
    {
        return elementType;
    }

    @Override
    public String toString()
    {
        return "array<"+elementType+">";
    }

    @Override
    public boolean isPrimitive()
    {
        return false;
    }

    @Override
    public boolean isArrayType()
    {
        return true;
    }

    @Override
    public boolean isMapType()
    {
        return false;
    }

    @Override
    public TDPrimitiveColumnType asPrimitiveType()
    {
        return null;
    }

    @Override
    public TDArrayColumnType asArrayType()
    {
        return this;
    }

    @Override
    public TDMapColumnType asMapType()
    {
        return null;
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
        TDArrayColumnType other = (TDArrayColumnType) obj;
        return Objects.equal(this.elementType, other.elementType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(elementType);
    }
}
