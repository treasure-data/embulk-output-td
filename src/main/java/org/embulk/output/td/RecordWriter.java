package org.embulk.output.td;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.treasuredata.api.TdApiClient;
import org.embulk.config.CommitReport;
import org.embulk.output.td.writer.FieldWriter;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.msgpack.MessagePack;
import org.slf4j.Logger;

import java.io.File;
import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;
import java.text.NumberFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordWriter
        implements TransactionalPageOutput
{
    private final Logger log;
    private final TdApiClient client;
    private final String sessionName;
    private final int taskIndex;

    private final MessagePack msgpack;
    private final FieldWriterSet fieldWriters;
    private final File tempDir;

    private int partSeqId = 0;
    private PageReader pageReader;
    private MsgpackGZFileBuilder builder;

    private final FinalizableExecutorService executor;
    private final int uploadConcurrency;
    private final long fileSplitSize; // unit: kb

    public RecordWriter(TdOutputPlugin.PluginTask task, int taskIndex, TdApiClient client, FieldWriterSet fieldWriters)
    {
        this.log = Exec.getLogger(getClass());
        this.client = checkNotNull(client);
        this.sessionName = task.getSessionName();
        this.taskIndex = taskIndex;

        this.msgpack = new MessagePack();
        this.fieldWriters = fieldWriters;
        this.tempDir = new File(task.getTempDir());
        this.executor = new FinalizableExecutorService();
        this.uploadConcurrency = task.getUploadConcurrency();
        this.fileSplitSize = task.getFileSplitSize() * 1024;
    }

    public static void validateSchema(Logger log, TdOutputPlugin.PluginTask task, Schema schema)
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
        String prefix = String.format("%s-", sessionName);
        File tempFile = File.createTempFile(prefix, ".msgpack.gz", tempDir);
        this.builder = new MsgpackGZFileBuilder(msgpack, tempFile);
    }

    @VisibleForTesting
    MsgpackGZFileBuilder getBuilder()
    {
        return builder;
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
                        }
                        catch (IOException e) {
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

        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void flush() throws IOException
    {
        if (builder.getRecordCount() > 0) {
            builder.finish();

            log.info("{uploading: {rows: {}, size: {} bytes (compressed)}}",
                    builder.getRecordCount(),
                    NumberFormat.getNumberInstance().format(builder.getWrittenSize()));
            upload(builder, String.format(Locale.ENGLISH, "task-%d_%d", taskIndex, partSeqId));
            partSeqId++;
            builder = null;
        }
    }

    private void upload(final MsgpackGZFileBuilder builder, final String uniquePartName)
            throws IOException
    {
        executor.joinPartial(uploadConcurrency - 1);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception
            {
                File file = builder.getFile();

                log.debug("{uploading: {file: {}}}", file.getAbsolutePath());
                Stopwatch stopwatch = Stopwatch.createStarted();

                client.uploadBulkImportPart(sessionName, uniquePartName, builder.getFile());

                stopwatch.stop();
                stopwatch.elapsed(TimeUnit.MILLISECONDS);
                log.debug("{uploaded: {file: {}, time: {}}}", file.getAbsolutePath(), stopwatch);
                return null;
            }
        },
        new Closeable() {
            public void close() throws IOException
            {
                builder.close();
                if (!builder.delete()) {
                    log.warn("Failed to delete local temporary file {}. Ignoring.", builder.getFile());
                }
            }
        });
    }

    @Override
    public void finish()
    {
        try {
            flush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
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
            }
            finally {
                if (builder != null) {
                    builder.close();
                    builder.delete();
                    builder = null;
                }

                if (client != null) {
                    client.close();
                }
            }
        }
        catch (IOException e) {
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
}
