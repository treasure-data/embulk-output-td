package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TDTableList
{
    private String name;
    private List<TDTable> tables;

    @JsonCreator
    public TDTableList(
            @JsonProperty("name") String name,
            @JsonProperty("tables") List<TDTable> tables)
    {
        this.name = name;
        this.tables = tables;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<TDTable> getTables()
    {
        return tables;
    }
}
