package org.embulk.output.td.writer;

import java.io.IOException;
import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.output.td.TdOutputPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Timestamps;
import org.embulk.output.td.MsgpackGZFileBuilder;
import org.slf4j.Logger;

public class FieldWriterSet
{
    private enum ColumnWriterMode
    {
        PRIMARY_KEY,
        SIMPLE_VALUE,
        DUPLICATE_PRIMARY_KEY;
    }

    private final int fieldCount;
    private final IFieldWriter[] fieldWriters;
    private final Optional<Long> staticTimeValue;

    public FieldWriterSet(Logger log, TdOutputPlugin.PluginTask task, Schema schema)
    {
        Optional<String> userDefinedPrimaryKeySourceColumnName = task.getTimeColumn();
        TdOutputPlugin.ConvertTimestampType convertTimestamp = task.getConvertTimestampType();
        staticTimeValue = task.getTimeValue();
        if (staticTimeValue.isPresent() && userDefinedPrimaryKeySourceColumnName.isPresent()) {
            throw new ConfigException("Setting both time_column and time_value is invalid");
        }

        boolean hasPkWriter = false;
        int duplicatePrimaryKeySourceIndex = -1;
        int firstTimestampColumnIndex = -1;

        int fc = 0;
        fieldWriters = new IFieldWriter[schema.size()];
        TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());

        for (int i = 0; i < schema.size(); i++) {
            String columnName = schema.getColumnName(i);
            Type columnType = schema.getColumnType(i);

            // choose the mode
            final ColumnWriterMode mode;

            if (userDefinedPrimaryKeySourceColumnName.isPresent() &&
                    columnName.equals(userDefinedPrimaryKeySourceColumnName.get())) {
                // found time_column
                if ("time".equals(userDefinedPrimaryKeySourceColumnName.get())) {
                    mode = ColumnWriterMode.PRIMARY_KEY;
                }
                else {
                    mode = ColumnWriterMode.DUPLICATE_PRIMARY_KEY;
                }
            }
            else if ("time".equals(columnName)) {
                // the column name is same with the primary key name.
                if (userDefinedPrimaryKeySourceColumnName.isPresent()) {
                    columnName = newColumnUniqueName(columnName, schema);
                    mode = ColumnWriterMode.SIMPLE_VALUE;
                    log.warn("time_column '{}' is set but 'time' column also exists. The existent 'time' column is renamed to {}",
                            userDefinedPrimaryKeySourceColumnName.get(), columnName);
                }
                else if (staticTimeValue.isPresent()) {
                    columnName = newColumnUniqueName(columnName, schema);
                    mode = ColumnWriterMode.SIMPLE_VALUE;
                    log.warn("time_value is set but 'time' column also exists. The existent 'time' column is renamed to {}",
                            columnName);
                }
                else {
                    mode = ColumnWriterMode.PRIMARY_KEY;
                }
            }
            else {
                mode = ColumnWriterMode.SIMPLE_VALUE;
            }

            // create the fieldWriters writer depending on the mode
            final FieldWriter writer;

            switch (mode) {
                case PRIMARY_KEY:
                    log.info("Using {}:{} column as the data partitioning key", columnName, columnType);
                    if (columnType instanceof LongType) {
                        if (task.getUnixTimestampUnit() != TdOutputPlugin.UnixTimestampUnit.SEC) {
                            log.warn("time column is converted from {} to seconds", task.getUnixTimestampUnit());
                        }
                        writer = new UnixTimestampLongFieldWriter(columnName, task.getUnixTimestampUnit().getFractionUnit());
                        hasPkWriter = true;
                    }
                    else if (columnType instanceof TimestampType) {
                        writer = new TimestampLongFieldWriter(columnName);

                        hasPkWriter = true;
                    }
                    else {
                        throw new ConfigException(String.format("Type of '%s' column must be long or timestamp but got %s",
                                columnName, columnType));
                    }
                    break;

                case SIMPLE_VALUE:
                    if (columnType instanceof BooleanType) {
                        writer = new BooleanFieldWriter(columnName);
                    }
                    else if (columnType instanceof LongType) {
                        writer = new LongFieldWriter(columnName);
                    }
                    else if (columnType instanceof DoubleType) {
                        writer = new DoubleFieldWriter(columnName);
                    }
                    else if (columnType instanceof StringType) {
                        writer = new StringFieldWriter(columnName);
                    }
                    else if (columnType instanceof TimestampType) {
                        switch (convertTimestamp) {
                        case STRING:
                            writer = new TimestampStringFieldWriter(timestampFormatters[i], columnName);
                            break;
                        case SEC:
                            writer = new TimestampLongFieldWriter(columnName);
                            break;
                        default:
                            // Thread of control doesn't come here but, just in case, it throws ConfigException.
                            throw new ConfigException(String.format("Unknown option {} as convert_timestamp_type", convertTimestamp));
                        }
                        if (firstTimestampColumnIndex < 0) {
                            firstTimestampColumnIndex = i;
                        }
                    }
                    else {
                        throw new ConfigException("Unsupported type: " + columnType);
                    }
                    break;

                case DUPLICATE_PRIMARY_KEY:
                    duplicatePrimaryKeySourceIndex = i;
                    writer = null;  // handle later
                    break;

                default:
                    throw new AssertionError();
            }

            fieldWriters[i] = writer;
            fc += 1;
        }

        if (staticTimeValue.isPresent()) {
            // "time" column is written by RecordWriter
            fc += 1;
        }
        else if (!hasPkWriter) {
            // PRIMARY_KEY was not found.
            if (duplicatePrimaryKeySourceIndex < 0) {
                if (userDefinedPrimaryKeySourceColumnName.isPresent()) {
                    throw new ConfigException(String.format("time_column '%s' does not exist", userDefinedPrimaryKeySourceColumnName.get()));
                }
                else if (firstTimestampColumnIndex >= 0) {
                    // if time is not found, use the first timestamp column
                    duplicatePrimaryKeySourceIndex = firstTimestampColumnIndex;
                }
                else {
                    throw new ConfigException(String.format("TD output plugin requires at least one timestamp column, or a long column named 'time'"));
                }
            }

            String columnName = schema.getColumnName(duplicatePrimaryKeySourceIndex);
            Type columnType = schema.getColumnType(duplicatePrimaryKeySourceIndex);

            IFieldWriter writer;
            if (columnType instanceof LongType) {
                log.info("Duplicating {}:{} column (unix timestamp {}) to 'time' column as seconds for the data partitioning",
                        columnName, columnType, task.getUnixTimestampUnit());
                IFieldWriter fw = new LongFieldWriter(columnName);
                writer = new UnixTimestampFieldDuplicator(fw, "time", task.getUnixTimestampUnit().getFractionUnit());
            }
            else if (columnType instanceof TimestampType) {
                log.info("Duplicating {}:{} column to 'time' column as seconds for the data partitioning",
                        columnName, columnType);
                IFieldWriter fw;
                switch (convertTimestamp) {
                    case STRING:
                        fw = new TimestampStringFieldWriter(timestampFormatters[duplicatePrimaryKeySourceIndex], columnName);
                        break;
                    case SEC:
                        fw = new TimestampLongFieldWriter(columnName);
                        break;
                    default:
                        // Thread of control doesn't come here but, just in case, it throws ConfigException.
                        throw new ConfigException(String.format("Unknown option {} as convert_timestamp_type", convertTimestamp));
                }
                writer = new TimestampFieldLongDuplicator(fw, "time");
            }
            else {
                throw new ConfigException(String.format("Type of '%s' column must be long or timestamp but got %s",
                        columnName, columnType));
            }

            // replace existint writer
            fieldWriters[duplicatePrimaryKeySourceIndex] = writer;
            fc += 1;
        }

        fieldCount = fc;
    }

    private static String newColumnUniqueName(String originalName, Schema schema)
    {
        String name = originalName;
        do {
            name += "_";
        }
        while (containsColumnName(schema, name));
        return name;
    }

    private static boolean containsColumnName(Schema schema, String name)
    {
        for (Column c : schema.getColumns()) {
            if (c.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    public IFieldWriter getFieldWriter(int index)
    {
        return fieldWriters[index];
    }

    public void beginRecord(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeMapBegin(fieldCount);
        if (staticTimeValue.isPresent()) {
            builder.writeString("time");
            builder.writeLong(staticTimeValue.get());
        }
    }

    public void endRecord(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeMapEnd();
    }

    public int getFieldCount()
    {
        return fieldCount;
    }
}
