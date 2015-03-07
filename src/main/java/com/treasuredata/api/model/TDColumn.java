package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.common.base.Objects;

public class TDColumn
{
    private String name;
    private TDColumnType type;
    private byte[] key;

    public TDColumn(String name, TDColumnType type, byte[] key)
    {
        this.name = name;
        this.type = type;
        this.key = key;
    }

    public String getName()
    {
        return name;
    }

    public TDColumnType getType()
    {
        return type;
    }

    public byte[] getKey()
    {
        return key;
    }

    @JsonCreator
    public static TDColumn valueFromTuple(String[] tuple)
    {
        // TODO encode key in some ways
        if (tuple != null && tuple.length == 2) {
            return new TDColumn(
                    tuple[0],
                    TDColumnTypeDeserializer.parseColumnType(tuple[1]),
                    tuple[0].getBytes());

        } else if (tuple != null && tuple.length == 3) {
            return new TDColumn(
                    tuple[0],
                    TDColumnTypeDeserializer.parseColumnType(tuple[1]),
                    tuple[2].getBytes());

        } else {
            throw new RuntimeJsonMappingException("Unexpected string tuple to deserialize TDColumn");
        }
    }

    @JsonValue
    public String[] getTuple()
    {
        return new String[] { name, type.toString(), new String(key) };
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
        TDColumn other = (TDColumn) obj;
        return Objects.equal(this.name, other.name) &&
            Objects.equal(type, other.type) &&
            Objects.equal(key, other.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, key);
    }
}
