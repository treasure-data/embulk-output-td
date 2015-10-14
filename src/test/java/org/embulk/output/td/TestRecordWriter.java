package org.embulk.output.td;

import com.treasuredata.api.TdApiClient;
import org.embulk.EmbulkTestRuntime;
import org.embulk.output.td.TdOutputPlugin.PluginTask;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.unpacker.Unpacker;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import static org.embulk.output.td.TestTdOutputPlugin.config;
import static org.embulk.output.td.TestTdOutputPlugin.fieldWriters;
import static org.embulk.output.td.TestTdOutputPlugin.plugin;
import static org.embulk.output.td.TestTdOutputPlugin.pluginTask;
import static org.embulk.output.td.TestTdOutputPlugin.schema;
import static org.embulk.output.td.TestTdOutputPlugin.recordWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.msgpack.type.ValueFactory.createRawValue;

public class TestRecordWriter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private MessagePack msgpack;
    private Logger log;
    private Schema schema;
    private TdOutputPlugin plugin; // mock
    private PluginTask task;
    private RecordWriter recordWriter; // mock

    @Before
    public void createResources()
    {
        msgpack = new MessagePack();
        log = runtime.getExec().getLogger(TestRecordWriter.class);

        schema = schema("time", Types.LONG, "_c0", Types.LONG, "_c1", Types.STRING,
                "_c2", Types.BOOLEAN, "_c3", Types.DOUBLE, "_c4", Types.TIMESTAMP);

        plugin = plugin();
        task = pluginTask(config().set("session_name", "my_session"));

        TdApiClient client = plugin.newTdApiClient(task);
        FieldWriterSet fieldWriters = fieldWriters(log, task, schema);
        recordWriter = recordWriter(task, client, fieldWriters);
    }

    @Test
    public void checkOpenAndClose()
            throws Exception
    {
        // confirm that no error happens
        try {
            recordWriter.open(schema);
        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void checkFlushAndFinish()
            throws Exception
    {
        TdApiClient client = spy(plugin.newTdApiClient(task));

        { // add no record
            RecordWriter recordWriter = recordWriter(task, client, fieldWriters(log, task, schema));
            try {
                recordWriter.open(schema);
            }
            finally {
                recordWriter.finish();
            }
        }

        { // add 1 record
            doNothing().when(client).uploadBulkImportPart(anyString(), anyString(), any(File.class));

            RecordWriter recordWriter = recordWriter(task, client, fieldWriters(log, task, schema));
            try {
                recordWriter.open(schema);

                // values are not null
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                        1442595600L, 0L, "v", true, 0.0, Timestamp.ofEpochSecond(1442595600L))) {
                    recordWriter.add(page);
                }
            }
            finally {
                recordWriter.finish();
            }
        }
    }

    @Test
    public void addNonNullValues()
            throws Exception
    {
        try {
            recordWriter.open(schema);

            // values are not null
            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                    1442595600L, 0L, "v", true, 0.0, Timestamp.ofEpochSecond(1442595600L))) {
                recordWriter.add(page);
            }

            MsgpackGZFileBuilder builder = recordWriter.getBuilder();
            builder.finish();

            // record count 1
            assertEquals(1, builder.getRecordCount());

            Unpacker u = msgpack.createUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            MapValue v = u.readValue().asMapValue();

            // compare actual values
            assertEquals(1442595600L, v.get(createRawValue("time")).asIntegerValue().getLong());
            assertEquals(0L, v.get(createRawValue("_c0")).asIntegerValue().getLong());
            assertEquals("v", v.get(createRawValue("_c1")).asRawValue().getString());
            assertEquals(true, v.get(createRawValue("_c2")).asBooleanValue().getBoolean());
            assertEquals(0.0, v.get(createRawValue("_c3")).asFloatValue().getDouble(), 0.000001);
            assertEquals("2015-09-18 17:00:00.000", v.get(createRawValue("_c4")).asRawValue().getString());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void addNullValues()
            throws Exception
    {
        try {
            recordWriter.open(schema);

            // values are not null
            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                    1442595600L, null, null, null, null, null)) {
                recordWriter.add(page);
            }

            MsgpackGZFileBuilder builder = recordWriter.getBuilder();
            builder.finish();

            // record count 1
            assertEquals(1, builder.getRecordCount());

            Unpacker u = msgpack.createUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            MapValue v = u.readValue().asMapValue();

            // compare actual values
            assertTrue(v.get(createRawValue("_c0")).isNilValue());
            assertTrue(v.get(createRawValue("_c1")).isNilValue());
            assertTrue(v.get(createRawValue("_c2")).isNilValue());
            assertTrue(v.get(createRawValue("_c3")).isNilValue());
            assertTrue(v.get(createRawValue("_c4")).isNilValue());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void doAbortNorthing()
    {
        recordWriter.abort();
        // no error happen
    }

    @Test
    public void checkTaskReport()
    {
        assertTrue(recordWriter.commit().isEmpty());
    }
}
