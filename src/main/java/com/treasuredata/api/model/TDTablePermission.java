package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class TDTablePermission
{
    private boolean importable;
    private boolean queryable;

    public TDTablePermission(
            @JsonProperty("importable") boolean importable,
            @JsonProperty("queryable") boolean queryable)
    {
        this.importable = importable;
        this.queryable = queryable;
    }

    @JsonProperty("importable")
    public boolean isImportable() {
        return importable;
    }

    @JsonProperty("queryable")
    public boolean isQueryable() {
        return queryable;
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
        TDTablePermission other = (TDTablePermission) obj;
        return Objects.equal(this.importable, other.importable) &&
                Objects.equal(this.queryable, other.queryable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(importable, queryable);
    }
}
