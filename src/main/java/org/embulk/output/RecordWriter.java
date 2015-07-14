package org.embulk.output;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.treasuredata.api.TdApiClient;
import org.embulk.config.CommitReport;
import org.embulk.config.ConfigException;
import org.embulk.output.TdOutputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.msgpack.MessagePack;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import org.embulk.output.TdOutputPlugin.UnixTimestampUnit;

public class RecordWriter
        implements TransactionalPageOutput
{
    private final Logger log;
    private final TdApiClient client;
    private final String sessionName;

    private final MessagePack msgpack;
    private final FieldWriterSet fieldWriters;
    private final File tempDir;

    private int seqid = 0;
    private PageReader pageReader;
    private MsgpackGZFileBuilder builder;

    private final FinalizableExecutorService executor;
    private final int uploadConcurrency;
    private final long fileSplitSize; // unit: kb

    public RecordWriter(PluginTask task, TdApiClient client, FieldWriterSet fieldWriters)
    {
        this.log = Exec.getLogger(getClass());
        this.client = checkNotNull(client);
        this.sessionName = task.getSessionName();

        this.msgpack = new MessagePack();
        this.fieldWriters = fieldWriters;
        this.tempDir = new File(task.getTempDir());
        this.executor = new FinalizableExecutorService();
        this.uploadConcurrency = task.getUploadConcurrency();
        this.fileSplitSize = task.getFileSplitSize() * 1024;
    }

    public static void validateSchema(Logger log, PluginTask task, Schema schema)
    {
        new FieldWriterSet(log, task, schema);
    }

    void open(final Schema schema)
            throws IOException
    {
        this.pageReader = new PageReader(checkNotNull(schema));
        prepareNextBuilder();
    }

    private void prepareNextBuilder()
            throws IOException
    {
        String prefix = String.format("%s-%d-", sessionName, seqid);
        File tempFile = File.createTempFile(prefix, ".msgpack.gz", tempDir);
        this.builder = new MsgpackGZFileBuilder(msgpack, tempFile);
    }

    @Override
    public void add(final Page page)
    {
        pageReader.setPage(checkNotNull(page));

        try {
            while (pageReader.nextRecord()) {
                builder.writeMapBegin(fieldWriters.getFieldCount());

                pageReader.getSchema().visitColumns(new ColumnVisitor() {
                    @Override
                    public void booleanColumn(Column column)
                    {
                        write(column);
                    }

                    @Override
                    public void longColumn(Column column)
                    {
                        write(column);
                    }

                    @Override
                    public void doubleColumn(Column column)
                    {
                        write(column);
                    }

                    @Override
                    public void stringColumn(Column column)
                    {
                        write(column);
                    }

                    @Override
                    public void timestampColumn(Column column)
                    {
                        write(column);
                    }

                    private void write(Column column)
                    {
                        FieldWriter fieldWriter = fieldWriters.getFieldWriter(column.getIndex());
                        try {
                            fieldWriter.writeKeyValue(builder, pageReader, column);
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });

                builder.writeMapEnd();

                if (builder.getWrittenSize() > fileSplitSize) {
                    flush();
                    prepareNextBuilder();
                }
            }

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void flush() throws IOException
    {
        builder.finish();

        if (builder.getRecordCount() > 0) {
            log.info("{uploading: {rows: {}, size: {} bytes (compressed)}}",
                    builder.getRecordCount(),
                    NumberFormat.getNumberInstance().format(builder.getWrittenSize()));
            upload(builder);
            builder = null;
        }
    }

    private void upload(final MsgpackGZFileBuilder builder)
            throws IOException
    {
        executor.joinPartial(uploadConcurrency - 1);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                File file = builder.getFile();

                try {
                    log.debug("{uploading: {file: {}}}", file.getAbsolutePath());
                    Stopwatch stopwatch = Stopwatch.createStarted();

                    client.uploadBulkImport(sessionName, file);

                    stopwatch.stop();
                    stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    log.debug("{uploaded: {file: {}, time: {}}}", file.getAbsolutePath(), stopwatch);

                } finally {
                    log.debug("Remove file: {}", file.getAbsolutePath());
                    builder.removeFile();
                }

                return null;
            }
        }, builder);
        seqid++;
    }

    @Override
    public void finish()
    {
        try {
            flush();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            close();
        }
    }

    @Override
    public void close()
    {
        try {
            try {
                executor.joinAll();
                executor.shutdown(); // shutdown calls joinAll
            } finally {
                if (builder != null) {
                    builder.close();
                    builder = null;
                }

                if (client != null) {
                    client.close();
                }
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void abort()
    {
        //  do nothing
    }

    @Override
    public CommitReport commit()
    {
        CommitReport report = Exec.newCommitReport();
        //  TODO
        return report;
    }

    static class FieldWriterSet
    {
        private enum ColumnWriterMode
        {
            PRIMARY_KEY,
            SIMPLE_VALUE,
            DUPLICATE_PRIMARY_KEY;
        }

        private final int fieldCount;
        private final FieldWriter[] fieldWriters;

        public FieldWriterSet(Logger log, PluginTask task, Schema schema)
        {
            Optional<String> userDefinedPrimaryKeySourceColumnName = task.getTimeColumn();
            boolean hasPkWriter = false;
            int duplicatePrimaryKeySourceIndex = -1;
            int firstTimestampColumnIndex = -1;

            int fc = 0;
            fieldWriters = new FieldWriter[schema.size()];

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
                    } else {
                        mode = ColumnWriterMode.DUPLICATE_PRIMARY_KEY;
                    }

                } else if ("time".equals(columnName)) {
                    // the column name is same with the primary key name.
                    if (userDefinedPrimaryKeySourceColumnName.isPresent()) {
                        columnName = newColumnUniqueName(columnName, schema);
                        mode = ColumnWriterMode.SIMPLE_VALUE;
                        log.warn("time_column '{}' is set but 'time' column also exists. The existent 'time' column is renamed to {}",
                                userDefinedPrimaryKeySourceColumnName.get(), "time", "time", columnName);
                    } else {
                        mode = ColumnWriterMode.PRIMARY_KEY;
                    }

                } else {
                    mode = ColumnWriterMode.SIMPLE_VALUE;
                }

                // create the fieldWriters writer depending on the mode
                final FieldWriter writer;

                switch (mode) {
                    case PRIMARY_KEY:
                        log.info("Using {}:{} column as the data partitioning key", columnName, columnType);
                        if (columnType instanceof LongType) {
                            if (task.getUnixTimestampUnit() != UnixTimestampUnit.SEC) {
                                log.warn("time column is converted from {} to seconds", task.getUnixTimestampUnit());
                            }
                            writer = new UnixTimestampLongFieldWriter(columnName, task.getUnixTimestampUnit().getFractionUnit());
                            hasPkWriter = true;
                        } else if (columnType instanceof TimestampType) {
                            writer = new TimestampStringFieldWriter(task.getJRuby(), columnName);
                            hasPkWriter = true;
                        } else {
                            throw new ConfigException(String.format("Type of '%s' column must be long or timestamp but got %s",
                                    columnName, columnType));
                        }
                        break;

                    case SIMPLE_VALUE:
                        if (columnType instanceof BooleanType) {
                            writer = new BooleanFieldWriter(columnName);
                        } else if (columnType instanceof LongType) {
                            writer = new LongFieldWriter(columnName);
                        } else if (columnType instanceof DoubleType) {
                            writer = new DoubleFieldWriter(columnName);
                        } else if (columnType instanceof StringType) {
                            writer = new StringFieldWriter(columnName);
                        } else if (columnType instanceof TimestampType) {
                            writer = new TimestampStringFieldWriter(task.getJRuby(), columnName);
                            if (firstTimestampColumnIndex < 0) {
                                firstTimestampColumnIndex = i;
                            }
                        } else {
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

            if (!hasPkWriter) {
                // PRIMARY_KEY was not found.
                if (duplicatePrimaryKeySourceIndex < 0) {
                    if (userDefinedPrimaryKeySourceColumnName.isPresent()) {
                        throw new ConfigException(String.format("time_column '%s' does not exist", userDefinedPrimaryKeySourceColumnName.get()));
                    } else if (firstTimestampColumnIndex >= 0) {
                        // if time is not found, use the first timestamp column
                        duplicatePrimaryKeySourceIndex = firstTimestampColumnIndex;
                    } else {
                        throw new ConfigException(String.format("TD output plugin requires at least one timestamp column, or a long column named 'time'"));
                    }
                }

                String columnName = schema.getColumnName(duplicatePrimaryKeySourceIndex);
                Type columnType = schema.getColumnType(duplicatePrimaryKeySourceIndex);

                FieldWriter writer;
                if (columnType instanceof LongType) {
                    log.info("Duplicating {}:{} column (unix timestamp {}) to 'time' column as seconds for the data partitioning",
                            columnName, columnType, task.getUnixTimestampUnit());
                    writer = new UnixTimestampFieldDuplicator(columnName, "time", task.getUnixTimestampUnit().getFractionUnit());
                } else if (columnType instanceof TimestampType) {
                    log.info("Duplicating {}:{} column to 'time' column as seconds for the data partitioning",
                            columnName, columnType);
                    writer = new TimestampFieldLongDuplicator(task.getJRuby(), columnName, "time");
                } else {
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
            } while (containsColumnName(schema, name));
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

        public FieldWriter getFieldWriter(int index)
        {
            return fieldWriters[index];
        }

        public int getFieldCount()
        {
            return fieldCount;
        }
    }

    static abstract class FieldWriter
    {
        private final String keyName;

        protected FieldWriter(String keyName)
        {
            this.keyName = keyName;
        }

        public void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            writeKey(builder);
            if (reader.isNull(column)) {
                builder.writeNil();
            } else {
                writeValue(builder, reader, column);
            }
        }

        private void writeKey(MsgpackGZFileBuilder builder)
                throws IOException
        {
            builder.writeString(keyName);
        }

        protected abstract void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException;
    }

    static class DoubleFieldWriter
            extends FieldWriter
    {
        public DoubleFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeDouble(reader.getDouble(column));
        }
    }

    static class BooleanFieldWriter
            extends FieldWriter
    {
        public BooleanFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeBoolean(reader.getBoolean(column));
        }
    }

    static class LongFieldWriter
            extends FieldWriter
    {
        LongFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeLong(reader.getLong(column));
        }
    }

    static class UnixTimestampLongFieldWriter
            extends FieldWriter
    {
        private final int fractionUnit;

        UnixTimestampLongFieldWriter(String keyName, int fractionUnit)
        {
            super(keyName);
            this.fractionUnit = fractionUnit;
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeLong(reader.getLong(column) / fractionUnit);
        }
    }

    static class StringFieldWriter
            extends FieldWriter
    {
        public StringFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeString(reader.getString(column));
        }
    }

    static class TimestampStringFieldWriter
            extends FieldWriter
    {
        // to format timestamp values to string by "%Y-%m-%d %H:%M:%S.%3N"
        private final TimestampFormatter defaultFormatter;

        public TimestampStringFieldWriter(ScriptingContainer jruby, String keyName)
        {
            super(keyName);
            this.defaultFormatter = new TimestampFormatter(jruby, "%Y-%m-%d %H:%M:%S.%3N", DateTimeZone.UTC);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeString(defaultFormatter.format(reader.getTimestamp(column)));
        }
    }

    static class TimestampLongFieldWriter
            extends FieldWriter
    {
        public TimestampLongFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeLong(reader.getTimestamp(column).getEpochSecond());
        }
    }

    static class UnixTimestampFieldDuplicator
            extends LongFieldWriter
    {
        private final UnixTimestampLongFieldWriter timeFieldWriter;

        public UnixTimestampFieldDuplicator(String keyName, String duplicateKeyName, int fractionUnit)
        {
            super(keyName);
            timeFieldWriter = new UnixTimestampLongFieldWriter(duplicateKeyName, fractionUnit);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            super.writeValue(builder, reader, column);
            timeFieldWriter.writeKeyValue(builder, reader, column);
        }
    }

    static class TimestampFieldLongDuplicator
            extends TimestampStringFieldWriter
    {
        private final TimestampLongFieldWriter timeFieldWriter;

        public TimestampFieldLongDuplicator(ScriptingContainer jruby, String keyName, String longDuplicateKeyName)
        {
            super(jruby, keyName);
            timeFieldWriter = new TimestampLongFieldWriter(longDuplicateKeyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            super.writeValue(builder, reader, column);
            timeFieldWriter.writeKeyValue(builder, reader, column);
        }
    }
}
