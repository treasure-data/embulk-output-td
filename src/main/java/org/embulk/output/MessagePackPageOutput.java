package org.embulk.output;

import com.google.common.base.Throwables;
import com.treasuredata.api.TdApiClient;
import org.embulk.config.CommitReport;
import org.embulk.output.writer.BooleanFieldWriter;
import org.embulk.output.writer.DoubleFieldWriter;
import org.embulk.output.writer.LongFieldWriter;
import org.embulk.output.writer.StringFieldWriter;
import org.embulk.output.writer.TimestampFieldWriter;
import org.embulk.output.TdOutputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.msgpack.MessagePack;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessagePackPageOutput
        implements TransactionalPageOutput
{
    private static final long FILE_SPLIT_SIZE = 1024*1024*500;  //  TODO configurable split size

    private final Logger log;
    private final PluginTask task;
    private final TdApiClient client;

    private final MessagePack msgpack;
    private final MessagePackRecordOutput recordOutput;
    private final File tempDir;

    private PageReader pageReader;
    private MessagePackGZFileBuilder builder;

    MessagePackPageOutput(final PluginTask task, final TdApiClient client, final MessagePackRecordOutput recordOutput)
    {
        log = Exec.getLogger(getClass());
        this.task = checkNotNull(task);
        this.client = checkNotNull(client);

        msgpack = new MessagePack();
        this.recordOutput = checkNotNull(recordOutput);
        tempDir = new File(task.getTempDir());
    }

    void open(final Schema schema)
            throws IOException
    {
        this.pageReader = new PageReader(checkNotNull(schema));
        this.builder = newMessagePackFileBuilder();
    }

    private MessagePackGZFileBuilder newMessagePackFileBuilder()
            throws IOException
    {
        File tempFile = File.createTempFile("msgpack-gz-", ".tmp", tempDir); //  TODO
        return new MessagePackGZFileBuilder(msgpack, tempFile);
    }

    @Override
    public void add(final Page page)
    {
        pageReader.setPage(checkNotNull(page));
        while (pageReader.nextRecord()) {
            pageReader.getSchema().visitColumns(new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        recordOutput.getFieldWriter(column.getIndex()).setIsNil(true);
                    }
                    boolean value = pageReader.getBoolean(column);
                    ((BooleanFieldWriter)recordOutput.getFieldWriter(column.getIndex())).update(value);
                }

                @Override
                public void longColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        recordOutput.getFieldWriter(column.getIndex()).setIsNil(true);
                    }
                    long value = pageReader.getLong(column);
                    ((LongFieldWriter)recordOutput.getFieldWriter(column.getIndex())).update(value);
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        recordOutput.getFieldWriter(column.getIndex()).setIsNil(true);
                    }
                    double value = pageReader.getDouble(column);
                    ((DoubleFieldWriter)recordOutput.getFieldWriter(column.getIndex())).update(value);
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        recordOutput.getFieldWriter(column.getIndex()).setIsNil(true);
                    }
                    String value = pageReader.getString(column);
                    ((StringFieldWriter)recordOutput.getFieldWriter(column.getIndex())).update(value);
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (pageReader.isNull(column)) {
                        recordOutput.getFieldWriter(column.getIndex()).setIsNil(true);
                    }
                    Timestamp value = pageReader.getTimestamp(column);
                    ((TimestampFieldWriter)recordOutput.getFieldWriter(column.getIndex())).update(value);
                }
            });

            try {
                recordOutput.writeTo(builder);

                if (builder.getEstimatedFileSize() > FILE_SPLIT_SIZE) {
                    flush();
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

    }

    public void flush() throws IOException
    {
        builder.finish();

        //  TODO parallelize
        if (builder.getRecordCount() > 0) {
            upload(builder);
            builder.close();
            builder = null;

            builder = newMessagePackFileBuilder();
        }
    }

    private void upload(final MessagePackGZFileBuilder builder)
            throws IOException
    {
        checkNotNull(builder);
        client.uploadBulkImport(task.getSessionName(), builder.getFile());
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
        if (builder != null) {
            try {
                builder.close();
                builder = null;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        if (client != null) {
            client.close();
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
