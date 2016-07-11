package org.embulk.output.td;

import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import static org.embulk.output.td.TestTdOutputPlugin.config;
import static org.embulk.output.td.TestTdOutputPlugin.pluginTask;
import static org.embulk.output.td.TestTdOutputPlugin.schema;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTimeValueGenerator
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Logger log;
    private ConfigSource config;
    private Schema schema;

    @Before
    public void createResources()
    {
        log = Exec.getLogger(TestTimeValueGenerator.class);
        config = config();
    }

    @Test
    public void validateTimeValue()
    {
        // incremental_time
        { // {from: 0, to: 0} # default incremental_time
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("from", 0L, "to", 0L))), schema, false);
        }
        { // {from: 0} # default incremental_time
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("from", 0L))), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
        { // {to: 0} # default incremental_time
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("to", 0L))), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
        { // {from: 0, to: 0, mode: incremental_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("from", 0L, "to", 0L, "mode", "incremental_time"))), schema, false);
        }
        { // {from: 0, mode: incremental_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("from", 0L, "mode", "incremental_time"))), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
        { // {to: 0, mode: incremental_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("to", 0L, "mode", "incremental_time"))), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
        { // {mode: incremental_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("mode", "incremental_time"))), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        // fixed_time
        { // {value: 0, mode: fixed_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("value", 0L, "mode", "fixed_time"))), schema, false);
        }
        { // {mode: fixed_time}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("mode", "fixed_time"))), schema, false);
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
        { // {value: 0}
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                new FieldWriterSet(log, pluginTask(config.set("time_value", ImmutableMap.of("value", 0L))), schema, false);
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }
}
