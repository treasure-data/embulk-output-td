package com.treasuredata.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TDBulkImportSession
{
    public static enum ImportStatus
    {
        UPLOADING("uploading"),
        PERFORMING("performing"),
        READY("ready"),
        COMMITTING("committing"),
        COMMITTED("committed"),
        UNKNOWN("unknown");

        private final String name;

        private ImportStatus(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static ImportStatus forName(String name)
        {
            return ImportStatus.valueOf(name.toUpperCase());
        }
    }

    private final String name;
    private final String databaseName;
    private final String tableName;

    private final ImportStatus status;

    private final boolean uploadFrozen;
    private final String jobID; // nullable
    private final long validRecords;
    private final long errorRecords;
    private final long validParts;
    private final long errorParts;

    @JsonCreator
    public TDBulkImportSession(
            @JsonProperty("name") String name,
            @JsonProperty("database") String databaseName,
            @JsonProperty("table") String tableName,
            @JsonProperty("status") ImportStatus status,
            @JsonProperty("upload_frozen") boolean uploadFrozen,
            @JsonProperty("job_id") String jobID,
            @JsonProperty("valid_records") long validRecords,
            @JsonProperty("error_records") long errorRecords,
            @JsonProperty("valid_parts") long validParts,
            @JsonProperty("error_parts") long errorParts)
    {
        this.name = name;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.status = status;
        this.uploadFrozen = uploadFrozen;
        this.jobID = jobID;
        this.validRecords = validRecords;
        this.errorRecords = errorRecords;
        this.validParts = validParts;
        this.errorParts = errorParts;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public ImportStatus getStatus()
    {
        return status;
    }

    @JsonProperty
    public boolean getUploadFrozen()
    {
        return uploadFrozen;
    }

    public boolean isUploading()
    {
        return status == ImportStatus.UPLOADING;
    }

    public boolean is(ImportStatus expecting)
    {
        return status == expecting;
    }

    public boolean isPeformError()
    {
        return validRecords == 0 || errorParts > 0 || errorRecords > 0;
    }

    public String getErrorMessage()
    {
        if (validRecords == 0)
            return "No record processed";
        if (errorRecords > 0)
            return String.format("%d invalid parts", errorParts);
        if (errorRecords > 0)
            return String.format("%d invalid records", errorRecords);

        return null;
    }
}
