package org.embulk.output.td.writer;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.td.TdOutputPlugin;
import org.embulk.output.td.TdOutputPlugin.ConvertTimestampType;
import org.embulk.output.td.TimeValueConfig;
import org.embulk.output.td.TimeValueGenerator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageReader;
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
    private final Optional<TimeValueGenerator> staticTimeValue;

    public FieldWriterSet(Logger log, TdOutputPlugin.PluginTask task, Schema schema)
    {
        Optional<String> userDefinedPrimaryKeySourceColumnName = task.getTimeColumn();
        ConvertTimestampType convertTimestampType = task.getConvertTimestampType();
        Optional<TimeValueConfig> timeValueConfig = task.getTimeValue();
        if (timeValueConfig.isPresent() && userDefinedPrimaryKeySourceColumnName.isPresent()) {
            throw new ConfigException("Setting both time_column and time_value is invalid");
        }

        boolean foundPrimaryKey = false;
        int duplicatePrimaryKeySourceIndex = -1;

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
                else if (timeValueConfig.isPresent()) {
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
                        foundPrimaryKey = true;
                    }
                    else if (columnType instanceof TimestampType) {
                        writer = new TimestampLongFieldWriter(columnName);
                        foundPrimaryKey = true;
                    }
                    else {
                        throw new ConfigException(String.format("Type of '%s' column must be long or timestamp but got %s",
                                columnName, columnType));
                    }
                    break;

                case SIMPLE_VALUE:
                    writer = newSimpleFieldWriter(columnName, columnType, convertTimestampType, timestampFormatters[i]);
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

        if (foundPrimaryKey) {
            // appropriate 'time' column is found

            staticTimeValue = Optional.absent();
            fieldCount = fc;
            return;
        }

        if (timeValueConfig.isPresent()) {
            // 'time_value' option is specified

            staticTimeValue = Optional.of(TimeValueGenerator.newGenerator(timeValueConfig.get()));
            fieldCount = fc + 1;
            return;
        }

        if (!foundPrimaryKey && duplicatePrimaryKeySourceIndex >= 0) {
            // 'time_column' option is correctly specified

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
                IFieldWriter fw = newSimpleTimestampFieldWriter(columnName, columnType, convertTimestampType, timestampFormatters[duplicatePrimaryKeySourceIndex]);
                writer = new TimestampFieldLongDuplicator(fw, "time");
            }
            else {
                throw new ConfigException(String.format("Type of '%s' column must be long or timestamp but got %s",
                        columnName, columnType));
            }

            // replace existint writer
            fieldWriters[duplicatePrimaryKeySourceIndex] = writer;
            staticTimeValue = Optional.absent();
            fieldCount = fc + 1;
            return;
        }

        if (!foundPrimaryKey) {
            // primary key is not found yet

            if (userDefinedPrimaryKeySourceColumnName.isPresent()) {
                throw new ConfigException(String.format("A specified time_column '%s' does not exist", userDefinedPrimaryKeySourceColumnName.get()));
            }

            long uploadTime = System.currentTimeMillis() / 1000;
            log.info("'time' column is generated and is set to a unix time {}", uploadTime);
            TimeValueConfig newConfig = Exec.newConfigSource().set("mode", "fixed_time").set("value", uploadTime).loadConfig(TimeValueConfig.class);
            task.setTimeValue(Optional.of(newConfig));
            staticTimeValue = Optional.of(TimeValueGenerator.newGenerator(newConfig));
            fieldCount = fc + 1;
            return;
        }

        throw new AssertionError("Cannot select primary key");
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

    private static FieldWriter newSimpleFieldWriter(String columnName, Type columnType, ConvertTimestampType convertTimestampType, TimestampFormatter timestampFormatter)
    {
        if (columnType instanceof BooleanType) {
            return new BooleanFieldWriter(columnName);
        }
        else if (columnType instanceof LongType) {
            return new LongFieldWriter(columnName);
        }
        else if (columnType instanceof DoubleType) {
            return new DoubleFieldWriter(columnName);
        }
        else if (columnType instanceof StringType) {
            return new StringFieldWriter(columnName);
        }
        else if (columnType instanceof TimestampType) {
            return newSimpleTimestampFieldWriter(columnName, columnType, convertTimestampType, timestampFormatter);
        }
        else {
            throw new ConfigException("Unsupported type: " + columnType);
        }
    }

    private static FieldWriter newSimpleTimestampFieldWriter(String columnName, Type columnType, ConvertTimestampType convertTimestampType, TimestampFormatter timestampFormatter)
    {
        switch (convertTimestampType) {
        case STRING:
            return new TimestampStringFieldWriter(timestampFormatter, columnName);

        case SEC:
            return new TimestampLongFieldWriter(columnName);

        default:
            // Thread of control doesn't come here but, just in case, it throws ConfigException.
            throw new ConfigException(String.format("Unknown option {} as convert_timestamp_type", convertTimestampType));
        }
    }

    @VisibleForTesting
    public IFieldWriter getFieldWriter(int index)
    {
        return fieldWriters[index];
    }

    public void addRecord(final MsgpackGZFileBuilder builder, final PageReader reader)
            throws IOException
    {
        beginRecord(builder);

        reader.getSchema().visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column)
            {
                addColumn(builder, reader, column);
            }

            @Override
            public void longColumn(Column column)
            {
                addColumn(builder, reader, column);
            }

            @Override
            public void doubleColumn(Column column)
            {
                addColumn(builder, reader, column);
            }

            @Override
            public void stringColumn(Column column)
            {
                addColumn(builder, reader, column);
            }

            @Override
            public void timestampColumn(Column column)
            {
                addColumn(builder, reader, column);
            }

        });

        endRecord(builder);
    }

    private void beginRecord(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeMapBegin(fieldCount);
        if (staticTimeValue.isPresent()) {
            builder.writeString("time");
            builder.writeLong(staticTimeValue.get().next());
        }
    }

    private void endRecord(MsgpackGZFileBuilder builder)
            throws IOException
    {
        builder.writeMapEnd();
    }

    private void addColumn(MsgpackGZFileBuilder builder, PageReader reader, Column column)
    {
        try {
            fieldWriters[column.getIndex()].writeKeyValue(builder, reader, column);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
