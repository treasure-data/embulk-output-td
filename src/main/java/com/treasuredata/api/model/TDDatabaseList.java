package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TDDatabaseList
{
    private List<TDDatabase> databases;

    @JsonCreator
    public TDDatabaseList(
            @JsonProperty("databases") List<TDDatabase> databases)
    {
        this.databases = databases;
    }

    @JsonProperty
    public List<TDDatabase> getDatabases()
    {
        return databases;
    }
}
