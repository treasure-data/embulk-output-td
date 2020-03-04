package org.embulk.output.td;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.treasuredata.client.TDClient;
import org.embulk.config.TaskReport;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final TDClient client;
    private final String sessionName;
    private final int taskIndex;

    private final FieldWriterSet fieldWriters;
    private final File tempDir;

    private int partSeqId = 0;
    private PageReader pageReader;
    private MsgpackGZFileBuilder builder;

    private final FinalizableExecutorService executor;
    private final int uploadConcurrency;
    private final long fileSplitSize; // unit: kb

    public RecordWriter(TdOutputPlugin.PluginTask task, int taskIndex, TDClient client, FieldWriterSet fieldWriters)
    {
        this.log = LoggerFactory.getLogger(getClass());
        this.client = checkNotNull(client);
        this.sessionName = task.getSessionName();
        this.taskIndex = taskIndex;

        this.fieldWriters = fieldWriters;
        this.tempDir = new File(task.getTempDir().get());
        this.executor = new FinalizableExecutorService();
        this.uploadConcurrency = task.getUploadConcurrency();
        this.fileSplitSize = task.getFileSplitSize() * 1024;
    }

    @VisibleForTesting
    public void open(final Schema schema)
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
        this.builder = new MsgpackGZFileBuilder(tempFile);
    }

    @VisibleForTesting
    public MsgpackGZFileBuilder getBuilder()
    {
        return builder;
    }

    @Override
    public void add(final Page page)
    {
        pageReader.setPage(checkNotNull(page));

        try {
            while (pageReader.nextRecord()) {
                fieldWriters.addRecord(builder, pageReader);

                if (builder.getWrittenSize() > fileSplitSize) {
                    flush();
                    prepareNextBuilder();
                }
            }

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() throws IOException
    {
        if (builder != null && builder.getRecordCount() > 0) {
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
        //  do nothing
    }

    @Override
    public TaskReport commit()
    {
        TaskReport report = Exec.newTaskReport()
                .set(TdOutputPlugin.TASK_REPORT_UPLOADED_PART_NUMBER, partSeqId);
        return report;
    }
}
