package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

public enum TDTableType
{
    LOG("log"),
    ITEM("item");

    private String name;

    private TDTableType(String name)
    {
        this.name = name;
    }

    @JsonCreator
    public static TDTableType fromName(String name)
    {
        if ("log".equals(name)) {
            return LOG;
        }
        else if ("item".equals(name)) {
            return ITEM;
        }
        throw new RuntimeJsonMappingException("Unexpected string tuple to deserialize TDTableType");
    }

    @JsonValue
    @Override
    public String toString()
    {
        return name;
    }
}
