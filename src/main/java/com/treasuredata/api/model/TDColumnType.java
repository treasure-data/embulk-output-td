package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = TDColumnTypeDeserializer.class)
public interface TDColumnType
{
    public boolean isPrimitive();

    public boolean isArrayType();

    public boolean isMapType();

    public TDPrimitiveColumnType asPrimitiveType();

    public TDArrayColumnType asArrayType();

    public TDMapColumnType asMapType();

    @JsonValue
    public String toString();
}
