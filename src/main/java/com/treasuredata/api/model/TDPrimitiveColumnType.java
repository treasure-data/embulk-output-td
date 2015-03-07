package com.treasuredata.api.model;

public enum TDPrimitiveColumnType
        implements TDColumnType
{
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    STRING("string");

    private String name;

    private TDPrimitiveColumnType(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public boolean isPrimitive()
    {
        return true;
    }

    @Override
    public boolean isArrayType()
    {
        return false;
    }

    @Override
    public boolean isMapType()
    {
        return false;
    }

    @Override
    public TDPrimitiveColumnType asPrimitiveType()
    {
        return this;
    }

    @Override
    public TDArrayColumnType asArrayType()
    {
        return null;
    }

    @Override
    public TDMapColumnType asMapType()
    {
        return null;
    }
}
