package org.embulk.output.td;

import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import org.embulk.EmbulkTestRuntime;
import org.embulk.output.td.TdOutputPlugin.PluginTask;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.embulk.output.td.TestTdOutputPlugin.config;
import static org.embulk.output.td.TestTdOutputPlugin.fieldWriters;
import static org.embulk.output.td.TestTdOutputPlugin.plugin;
import static org.embulk.output.td.TestTdOutputPlugin.pluginTask;
import static org.embulk.output.td.TestTdOutputPlugin.schema;
import static org.embulk.output.td.TestTdOutputPlugin.recordWriter;
import static org.embulk.output.td.TestTdOutputPlugin.tdClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.msgpack.value.ValueFactory.newString;

public class TestRecordWriter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Logger log;
    private Schema schema;
    private TdOutputPlugin plugin; // mock
    private PluginTask task;
    private RecordWriter recordWriter; // mock

    @Before
    public void createResources()
    {
        log = runtime.getExec().getLogger(TestRecordWriter.class);

        schema = schema("time", Types.LONG, "_c0", Types.LONG, "_c1", Types.STRING,
                "_c2", Types.BOOLEAN, "_c3", Types.DOUBLE, "_c4", Types.TIMESTAMP);

        plugin = plugin();
        task = pluginTask(config().set("session_name", "my_session").set("tmpdir", plugin.getEnvironmentTempDirectory()));
    }

    @Test
    public void checkOpenAndClose()
            throws Exception
    {
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));

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
        TDClient client = spy(plugin.newTDClient(task));
        recordWriter = recordWriter(task, client, fieldWriters(log, task, schema));

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
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));

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

            MessageUnpacker u = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            Map<Value, Value> v = u.unpackValue().asMapValue().map();

            // compare actual values
            assertEquals(1442595600L, v.get(newString("time")).asIntegerValue().toLong());
            assertEquals(0L, v.get(newString("_c0")).asIntegerValue().toLong());
            assertEquals("v", v.get(newString("_c1")).asStringValue().toString());
            assertEquals(true, v.get(newString("_c2")).asBooleanValue().getBoolean());
            assertEquals(0.0, v.get(newString("_c3")).asFloatValue().toFloat(), 0.000001);
            assertEquals("2015-09-18 17:00:00.000", v.get(newString("_c4")).asStringValue().toString());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void addNullValues()
            throws Exception
    {
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));

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

            MessageUnpacker u = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            Map<Value, Value> v = u.unpackValue().asMapValue().map();

            // compare actual values
            assertTrue(v.get(newString("_c0")).isNilValue());
            assertTrue(v.get(newString("_c1")).isNilValue());
            assertTrue(v.get(newString("_c2")).isNilValue());
            assertTrue(v.get(newString("_c3")).isNilValue());
            assertTrue(v.get(newString("_c4")).isNilValue());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void checkGeneratedTimeValueByOption()
            throws Exception
    {
        schema = schema("_c0", Types.LONG, "_c1", Types.STRING,
                "_c2", Types.BOOLEAN, "_c3", Types.DOUBLE, "_c4", Types.TIMESTAMP);
        task = pluginTask(config()
                .set("session_name", "my_session")
                .set("time_value", ImmutableMap.of("from", 0L, "to", 0L))
                .set("tmpdir", plugin.getEnvironmentTempDirectory())
        );
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));

        try {
            recordWriter.open(schema);

            // values are not null
            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                    0L, "v", true, 0.0, Timestamp.ofEpochSecond(1442595600L))) {
                recordWriter.add(page);
            }

            MsgpackGZFileBuilder builder = recordWriter.getBuilder();
            builder.finish();

            // record count 1
            assertEquals(1, builder.getRecordCount());

            MessageUnpacker u = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            Map<Value, Value> v = u.unpackValue().asMapValue().map();

            // compare actual values
            assertEquals(0L, v.get(newString("time")).asIntegerValue().toLong());
            assertEquals(0L, v.get(newString("_c0")).asIntegerValue().toLong());
            assertEquals("v", v.get(newString("_c1")).asStringValue().toString());
            assertEquals(true, v.get(newString("_c2")).asBooleanValue().getBoolean());
            assertEquals(0.0, v.get(newString("_c3")).asFloatValue().toFloat(), 0.000001);
            assertEquals("2015-09-18 17:00:00.000", v.get(newString("_c4")).asStringValue().toString());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void checkGeneratedTimeOffsetByOption()
            throws Exception
    {
        schema = schema("_c0", Types.LONG, "_c1", Types.STRING,
                "_c2", Types.BOOLEAN, "_c3", Types.DOUBLE, "_c4", Types.TIMESTAMP);
        task = pluginTask(config()
                .set("session_name", "my_session")
                .set("time_column", "_c4")
                .set("time_offset", ImmutableMap.of("value", 86400L, "unit", "sec"))
                .set("tmpdir", plugin.getEnvironmentTempDirectory())
        );
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));

        try {
            recordWriter.open(schema);

            // values are not null
            for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                    0L, "v", true, 0.0, Timestamp.ofEpochSecond(1442595600L))) {
                recordWriter.add(page);
            }

            MsgpackGZFileBuilder builder = recordWriter.getBuilder();
            builder.finish();

            // record count 1
            assertEquals(1, builder.getRecordCount());

            MessageUnpacker u = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(builder.getFile())));
            Map<Value, Value> v = u.unpackValue().asMapValue().map();

            // compare actual values
            assertEquals(1442595600L + 86400L, v.get(newString("time")).asIntegerValue().toLong());
            assertEquals(0L, v.get(newString("_c0")).asIntegerValue().toLong());
            assertEquals("v", v.get(newString("_c1")).asStringValue().toString());
            assertEquals(true, v.get(newString("_c2")).asBooleanValue().getBoolean());
            assertEquals(0.0, v.get(newString("_c3")).asFloatValue().toFloat(), 0.000001);
            assertEquals("2015-09-19 17:00:00.000", v.get(newString("_c4")).asStringValue().toString());

        }
        finally {
            recordWriter.close();
        }
    }

    @Test
    public void doAbortNorthing()
    {
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));
        recordWriter.abort();
        // no error happen
    }

    @Test
    public void checkTaskReport()
    {
        recordWriter = recordWriter(task, tdClient(plugin, task), fieldWriters(log, task, schema));
        assertTrue(recordWriter.commit().isEmpty());
    }
}
