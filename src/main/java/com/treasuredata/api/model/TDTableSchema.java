package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

public class TDTableSchema
{
    private List<TDColumn> columns;

    @JsonCreator
    public TDTableSchema(
            @JsonProperty("columns") List<TDColumn> columns)
    {
        this.columns = columns;
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
        TDTableSchema other = (TDTableSchema) obj;
        return Objects.equal(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columns);
    }
}
