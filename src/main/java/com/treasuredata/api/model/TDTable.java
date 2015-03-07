package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

public class TDTable
{
    private String name;
    private TDTableType type;
    private List<TDColumn> columns;

    @JsonCreator
    public TDTable(
            @JsonProperty("name") String name,
            @JsonProperty("type") TDTableType type,
            @JsonProperty("columns") List<TDColumn> columns)
    {
        this.name = name;
        this.type = type;
        this.columns = columns;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TDTableType getType()
    {
        return type;
    }

    @JsonProperty
    public List<TDColumn> getColumns()
    {
        return columns;
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
        TDTable other = (TDTable) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, columns);
    }
}
