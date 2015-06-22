package org.embulk.output;

import com.google.common.base.Optional;
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
import org.msgpack.MessagePack;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordWriter
        implements TransactionalPageOutput
{
    private final Logger log;
    private final TdApiClient client;
    private final String sessionName;

    private final MessagePack msgpack;
    private final FieldWriters fieldWriters;
    private final File tempDir;

    private int seqid = 0;
    private PageReader pageReader;
    private MsgpackGZFileBuilder builder;

    private final FinalizableExecutorService executor;
    private final int uploadConcurrency;
    private final long fileSplitSize; // unit: kb

    RecordWriter(PluginTask task, TdApiClient client, FieldWriters fieldWriters)
    {
        this.log = Exec.getLogger(getClass());
        this.client = checkNotNull(client);
        this.sessionName = task.getSessionName();

        this.msgpack = new MessagePack();
        this.fieldWriters = fieldWriters;
        this.tempDir = new File(task.getTempDir());
        this.executor = new FinalizableExecutorService();
        this.uploadConcurrency = checkUploadConcurrency(task.getUploadConcurrency(), 1, 8);
        this.fileSplitSize = task.getFileSplitSize() * 1024;
    }

    private static int checkUploadConcurrency(int v, int lower, int upper)
    {
        return v < lower ? lower : (upper < v ? upper : v);
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
                            fieldWriter.write(builder, pageReader, column);
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });

                builder.writeMapEnd();

                if (builder.getWrittenSize() > fileSplitSize) {
                    flush();
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

        prepareNextBuilder();
    }

    private void upload(final MsgpackGZFileBuilder builder)
            throws IOException
    {
        executor.joinPartial(uploadConcurrency - 1);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                client.uploadBulkImport(sessionName, builder.getFile());
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

    static class FieldWriters
    {
        private enum ColumnWriterMode
        {
            PRIMARY_KEY,
            SIMPLE_VALUE,
            DUPLICATE_PRIMARY_KEY;
        }

        // to format timestamp values to string by "%Y-%m-%d %H:%M:%S.%3N"
        private static TimestampFormatter defaultTimestampFormatter;

        private final int fieldCount;
        private final FieldWriter[] fieldWriters;

        public FieldWriters(Logger log, PluginTask task, Schema schema)
        {
            defaultTimestampFormatter = new TimestampFormatter(task.getJRuby(), "%Y-%m-%d %H:%M:%S.%3N", DateTimeZone.UTC);

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
                            writer = new LongFieldWriter(columnName);
                            hasPkWriter = true;
                        } else if (columnType instanceof TimestampType) {
                            writer = new TimestampStringFieldWriter(columnName);
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
                            writer = new TimestampStringFieldWriter(columnName);
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

                log.info("Duplicating {}:{} column to 'time' column for the data partitioning",
                        columnName, columnType);

                FieldWriter writer;
                if (columnType instanceof LongType) {
                    writer = new LongFieldDuplicator(columnName);
                } else if (columnType instanceof TimestampType) {
                    writer = new TimestampFieldDuplicator(columnName);
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
        protected String keyName;

        protected FieldWriter(String keyName)
        {
            this.keyName = keyName;
        }

        public void write(MsgpackGZFileBuilder builder, PageReader reader, Column column)
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
        public TimestampStringFieldWriter(String keyName)
        {
            super(keyName);
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            builder.writeString(FieldWriters.defaultTimestampFormatter.format(reader.getTimestamp(column)));
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

    static class LongFieldDuplicator
            extends LongFieldWriter
    {
        private FieldWriter timeFieldWriter;

        public LongFieldDuplicator(String keyName)
        {
            super(keyName);
            timeFieldWriter = new LongFieldWriter("time");
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            super.writeValue(builder, reader, column);
            timeFieldWriter.write(builder, reader, column);
        }
    }

    static class TimestampFieldDuplicator
            extends TimestampStringFieldWriter
    {
        private FieldWriter timeFieldWriter;

        public TimestampFieldDuplicator(String keyName)
        {
            super(keyName);
            timeFieldWriter = new TimestampLongFieldWriter("time");
        }

        @Override
        public void writeValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
                throws IOException
        {
            super.writeValue(builder, reader, column);
            timeFieldWriter.write(builder, reader, column);
        }
    }

}
