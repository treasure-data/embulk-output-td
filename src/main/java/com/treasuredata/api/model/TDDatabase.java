package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class TDDatabase
{
    private String name;
    // "permission" field is also available but not necessary yet

    @JsonCreator
    public TDDatabase(
            @JsonProperty("name") String name)
    {
        this.name = name;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public boolean isWritable()
    {
        // TODO not implemented yet
        return true;
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
        TDDatabase other = (TDDatabase) obj;
        return Objects.equal(this.name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }
}
