package org.embulk.output.td;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkImportSession.ImportStatus;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.client.model.TDTableType;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.TaskReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.output.td.TdOutputPlugin.PluginTask;
import org.embulk.output.td.TdOutputPlugin.HttpProxyTask;
import org.embulk.output.td.TdOutputPlugin.TimestampColumnOption;
import org.embulk.output.td.TdOutputPlugin.UnixTimestampUnit;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;

import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.COMMITTED;
import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.COMMITTING;
import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.PERFORMING;
import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.READY;
import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.UNKNOWN;
import static com.treasuredata.client.model.TDBulkImportSession.ImportStatus.UPLOADING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestTdOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config; // not mock
    private TdOutputPlugin plugin; // mock

    @Before
    public void createResources()
    {
        config = config();
        plugin = plugin();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = this.config.deepCopy();
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(true, task.getUseSsl());
        assertFalse(task.getHttpProxy().isPresent());
        assertEquals(TdOutputPlugin.Mode.APPEND, task.getMode());
        assertEquals(true, task.getAutoCreateTable());
        assertFalse(task.getSession().isPresent());
        assertEquals(TdOutputPlugin.ConvertTimestampType.STRING, task.getConvertTimestampType());
        assertFalse(task.getTimeColumn().isPresent());
        assertFalse(task.getTimeValue().isPresent());
        assertEquals(TdOutputPlugin.UnixTimestampUnit.SEC, task.getUnixTimestampUnit());
        assertFalse(task.getTempDir().isPresent());
        assertEquals(2, task.getUploadConcurrency());
        assertEquals(16384, task.getFileSplitSize());
        assertEquals("%Y-%m-%d %H:%M:%S.%3N", task.getDefaultTimestampFormat());
        assertTrue(task.getColumnOptions().isEmpty());
        assertFalse(task.getStopOnInvalidRecord());
        assertEquals(10, task.getDisplayedErrorRecordsCountLimit());
        assertEquals(20, task.getRetryLimit());
        assertEquals(1000, task.getRetryInitialIntervalMillis());
        assertEquals(90000, task.getRetryMaxIntervalMillis());
    }

    @Test
    public void checkRetryValues()
    {
        ConfigSource config = this.config.deepCopy()
                .set("retry_limit", 17)
                .set("retry_initial_interval_millis", 4822)
                .set("retry_max_interval_millis", 19348);
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(17, task.getRetryLimit());
        assertEquals(4822, task.getRetryInitialIntervalMillis());
        assertEquals(19348, task.getRetryMaxIntervalMillis());
    }

    @Test
    public void checkUnixTimestampUnit()
    {
        { // sec
            assertEquals(UnixTimestampUnit.SEC, UnixTimestampUnit.of("sec"));
            assertEquals(UnixTimestampUnit.SEC.toString(), "sec");
            assertEquals(UnixTimestampUnit.SEC.getFractionUnit(), 1);
        }

        { // milli
            assertEquals(UnixTimestampUnit.MILLI, UnixTimestampUnit.of("milli"));
            assertEquals(UnixTimestampUnit.MILLI.toString(), "milli");
            assertEquals(UnixTimestampUnit.MILLI.getFractionUnit(), 1000);
        }

        { // micro
            assertEquals(UnixTimestampUnit.MICRO, UnixTimestampUnit.of("micro"));
            assertEquals(UnixTimestampUnit.MICRO.toString(), "micro");
            assertEquals(UnixTimestampUnit.MICRO.getFractionUnit(), 1000000);
        }

        { // nano
            assertEquals(UnixTimestampUnit.NANO, UnixTimestampUnit.of("nano"));
            assertEquals(UnixTimestampUnit.NANO.toString(), "nano");
            assertEquals(UnixTimestampUnit.NANO.getFractionUnit(), 1000000000);
        }

        { // invalid_unit
            try {
                UnixTimestampUnit.of("invalid_unit");
                fail();
            }
            catch (Throwable e) {
                e.printStackTrace();
                assertTrue(e instanceof ConfigException);
            }
        }
    }

    @Test
    public void transaction()
    {
        doReturn("session_name").when(plugin).buildBulkImportSessionName(any(PluginTask.class), any(ExecSession.class));
        ConfigDiff configDiff = Exec.newConfigDiff().set("last_session", "session_name");
        doReturn(configDiff).when(plugin).doRun(any(TDClient.class), any(Schema.class), any(PluginTask.class), any(OutputPlugin.Control.class));
        Schema schema = schema("time", Types.LONG, "c0", Types.STRING, "c1", Types.STRING);

        { // auto_create_table is true
            ConfigSource config = this.config.deepCopy().set("auto_create_table", "true");
            doNothing().when(plugin).createTableIfNotExists(any(TDClient.class), anyString(), anyString());
            assertEquals("session_name", plugin.transaction(config, schema, 0, new OutputPlugin.Control()
            {
                @Override
                public List<TaskReport> run(TaskSource taskSource)
                {
                    return Lists.newArrayList(Exec.newTaskReport());
                }
            }).get(String.class, "last_session"));
        }

        { // auto_create_table is false
            ConfigSource config = this.config.deepCopy().set("auto_create_table", "false");
            doNothing().when(plugin).validateTableExists(any(TDClient.class), anyString(), anyString());
            assertEquals("session_name", plugin.transaction(config, schema, 0, new OutputPlugin.Control()
            {
                @Override
                public List<TaskReport> run(TaskSource taskSource)
                {
                    return Lists.newArrayList(Exec.newTaskReport());
                }
            }).get(String.class, "last_session"));
        }
    }

    @Test
    public void resume()
            throws Exception
    {
        PluginTask task = pluginTask(config);
        task.setSessionName("session_name");
        task.setLoadTargetTableName("my_table");
        task.setDoUpload(true);
        doReturn(true).when(plugin).startBulkImportSession(any(TDClient.class), anyString(), anyString(), anyString());
        doNothing().when(plugin).completeBulkImportSession(any(TDClient.class), any(Schema.class), any(PluginTask.class), anyInt());
        Schema schema = schema("time", Types.LONG, "c0", Types.STRING, "c1", Types.STRING);

        ConfigDiff configDiff = plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });

        assertEquals("session_name", configDiff.get(String.class, "last_session"));
    }

    @Test
    public void cleanup()
    {
        PluginTask task = pluginTask(config);
        task.setSessionName("session_name");
        task.setLoadTargetTableName("my_table");
        task.setDoUpload(true);
        TDClient client = spy(plugin.newTDClient(task));
        doNothing().when(client).deleteBulkImportSession(anyString());
        doReturn(client).when(plugin).newTDClient(task);
        Schema schema = schema("time", Types.LONG, "c0", Types.STRING, "c1", Types.STRING);

        plugin.cleanup(task.dump(), schema, 0, Lists.newArrayList(Exec.newTaskReport()));
        // no error happens
    }

    @Test
    public void checkColumnOptions()
    {
        TimestampColumnOption columnOption = config.loadConfig(TimestampColumnOption.class);
        ImmutableMap<String, TimestampColumnOption> columnOptions = ImmutableMap.of(
                "c0", columnOption, "c1", columnOption
        );

        { // schema includes column options' keys
            Schema schema = schema("c0", Types.LONG, "c1", Types.LONG);
            plugin.checkColumnOptions(schema, columnOptions);
            // no error happens
        }

        { // schema doesn't include one of column options' keys
            Schema schema = schema("c0", Types.LONG);
            try {
                plugin.checkColumnOptions(schema, columnOptions);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SchemaConfigException);
            }
        }
    }

    @Test
    public void newTDClient()
    {
        { // no proxy setting
            PluginTask task = pluginTask(config);
            TDClient client = plugin.newTDClient(task);
            // Expect no error happens
        }

        { // proxy setting
            PluginTask task = pluginTask(config.deepCopy()
                    .set("http_proxy", ImmutableMap.of("host", "xxx", "port", "8080", "user", "foo", "password", "PASSWORD")));
            TDClient client = plugin.newTDClient(task);
            // Expect no error happens
        }

        { // proxy setting without user/password
            PluginTask task = pluginTask(config.deepCopy()
                    .set("http_proxy", ImmutableMap.of("host", "xxx", "port", "8080")));
            TDClient client = plugin.newTDClient(task);
            // Expect no error happens
        }
    }

    @Test
    public void createTableIfNotExists()
    {
        PluginTask task = pluginTask(config);
        TDClient client = spy(plugin.newTDClient(task));

        { // database exists but table doesn't exist
            doNothing().when(client).createTable(anyString(), anyString());
            plugin.createTableIfNotExists(client, "my_db", "my_table");
            // no error happens
        }

        { // table already exists
            doThrow(conflict()).when(client).createTable(anyString(), anyString());
            plugin.createTableIfNotExists(client, "my_db", "my_table");
            // no error happens
        }

        { // database and table don't exist
            { // createTable -> createDB -> createTable
                doThrow(notFound()).doNothing().when(client).createTable(anyString(), anyString());
                doNothing().when(client).createDatabase(anyString());
                plugin.createTableIfNotExists(client, "my_db", "my_table");
                // no error happens
            }

            { // createTable -> createDB -> createTable
                doThrow(notFound()).doNothing().when(client).createTable(anyString(), anyString());
                doThrow(conflict()).when(client).createDatabase(anyString());
                plugin.createTableIfNotExists(client, "my_db", "my_table");
                // no error happens
            }

            { // createTable -> createDB -> createTable
                doThrow(notFound()).doThrow(conflict()).when(client).createTable(anyString(), anyString());
                doNothing().when(client).createDatabase(anyString());
                plugin.createTableIfNotExists(client, "my_db", "my_table");
                // no error happens
            }

            { // createTable -> createDB -> createTable
                doThrow(notFound()).doThrow(conflict()).when(client).createTable(anyString(), anyString());
                doThrow(conflict()).when(client).createDatabase(anyString());
                plugin.createTableIfNotExists(client, "my_db", "my_table");
                // no error happens
            }
        }
    }

    @Test
    public void validateTableExists()
    {
        PluginTask task = pluginTask(config);
        TDClient client = spy(plugin.newTDClient(task));
        TDTable table = newTable("my_table", "[]");

        { // table exists
            doReturn(ImmutableList.of(table)).when(client).listTables(anyString());
            plugin.validateTableExists(client, "my_db", "my_table");
            // no error happens
        }

        { // table doesn't exist
            doReturn(ImmutableList.of()).when(client).listTables(anyString());
            try {
                plugin.validateTableExists(client, "my_db", "my_table");
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // database doesn't exist
            doThrow(notFound()).when(client).listTables(anyString());
            try {
                plugin.validateTableExists(client, "my_db", "my_table");
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }

    @Test
    public void buildBulkImportSessionName()
    {
        { // session option is specified
            PluginTask task = pluginTask(config.deepCopy().set("session", "my_session"));
            assertEquals("my_session", plugin.buildBulkImportSessionName(task, Exec.session()));
        }

        { // session is not specified as option
            PluginTask task = pluginTask(config);
            assertTrue(plugin.buildBulkImportSessionName(task, Exec.session()).startsWith("embulk_"));
        }
    }

    @Test
    public void startBulkImportSession()
    {
        PluginTask task = pluginTask(config);
        TDClient client = spy(plugin.newTDClient(task));
        doNothing().when(client).createBulkImportSession(anyString(), anyString(), anyString());

        { // status is uploading and unfrozen
            doReturn(session(UPLOADING, true)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is uploading and frozen
            doReturn(session(UPLOADING, false)).when(client).getBulkImportSession("my_session");
            assertEquals(true, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is performing
            doReturn(session(PERFORMING, false)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is ready
            doReturn(session(READY, false)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is committing
            doReturn(session(COMMITTING, false)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is committed
            doReturn(session(COMMITTED, false)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }

        { // status is unkown
            doReturn(session(UNKNOWN, false)).when(client).getBulkImportSession("my_session");
            try {
                plugin.startBulkImportSession(client, "my_session", "my_db", "my_table");
                fail();
            }
            catch (Throwable t) {
            }
        }

        { // if createBulkImportSession got 409, it can be ignoreable.
            doThrow(conflict()).when(client).createBulkImportSession(anyString(), anyString(), anyString());
            doReturn(session(UPLOADING, true)).when(client).getBulkImportSession("my_session");
            assertEquals(false, plugin.startBulkImportSession(client, "my_session", "my_db", "my_table"));
        }
    }

    @Test
    public void newProxyConfig()
    {
        // confirm if proxy system properties override proxy setting by http_proxy config option.

        HttpProxyTask proxyTask = Exec.newConfigSource()
                .set("host", "option_host")
                .set("port", 8080)
                .loadConfig(HttpProxyTask.class);

        String originalProxyHost = System.getProperty("http.proxyHost");
        try {
            System.setProperty("http.proxyHost", "property_host");
            Optional<ProxyConfig> proxyConfig = plugin.newProxyConfig(Optional.of(proxyTask));
            assertEquals("property_host", proxyConfig.get().getHost());
            assertEquals(80, proxyConfig.get().getPort());
        }
        finally {
            if (originalProxyHost != null) {
                System.setProperty("http.proxyHost", originalProxyHost);
            }
        }
    }

    @Test
    public void completeBulkImportSession()
    {
        PluginTask task = pluginTask(config);
        Schema schema = schema("c0", Types.LONG);

        doReturn(session(UNKNOWN, false)).when(plugin).waitForStatusChange(any(TDClient.class), anyString(), any(ImportStatus.class), any(ImportStatus.class), anyString());
        doReturn(new HashMap<String, TDColumnType>()).when(plugin).updateSchema(any(TDClient.class), any(Schema.class), any(PluginTask.class));

        TDClient client = spy(plugin.newTDClient(task));
        doNothing().when(client).freezeBulkImportSession(anyString());
        doNothing().when(client).performBulkImportSession(anyString());
        doNothing().when(client).commitBulkImportSession(anyString());

        { // uploading + unfreeze
            doReturn(session(UPLOADING, false)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // uploading + frozen
            doReturn(session(UPLOADING, true)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // performing
            doReturn(session(PERFORMING, false)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // ready
            doReturn(session(READY, false)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // committing
            doReturn(session(COMMITTING, false)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // committed
            doReturn(session(COMMITTED, false)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }

        { // unknown
            doReturn(session(UNKNOWN, false)).when(client).getBulkImportSession(anyString());
            try {
                plugin.completeBulkImportSession(client, schema, task, 0);
                fail();
            }
            catch (Throwable t) {
            }
        }

        { // if freezeBulkImportSession got 409, it can be ignoreable.
            doThrow(conflict()).when(client).freezeBulkImportSession(anyString());
            doReturn(session(UPLOADING, true)).when(client).getBulkImportSession(anyString());
            plugin.completeBulkImportSession(client, schema, task, 0);
            // no error happens
        }
    }

    @Test
    public void waitForStatusChange()
    {
        PluginTask task = pluginTask(config);
        TDClient client = spy(plugin.newTDClient(task));

        { // performing -> ready
            doReturn(session(PERFORMING, false)).doReturn(session(READY, false)).when(client).getBulkImportSession("my_session");
            plugin.waitForStatusChange(client, "my_session", PERFORMING, READY, "");
        }

        { // committing -> committed
            doReturn(session(COMMITTING, false)).doReturn(session(COMMITTED, false)).when(client).getBulkImportSession("my_session");
            plugin.waitForStatusChange(client, "my_session", COMMITTING, COMMITTED, "");
        }
    }

    @Test
    public void open()
    {
        PluginTask task = pluginTask(config);
        task.setSessionName("session_name");
        task.setLoadTargetTableName("my_table");
        task.setDoUpload(true);
        task.setTempDir(plugin.getEnvironmentTempDirectory());
        Schema schema = schema("time", Types.LONG, "c0", Types.STRING, "c1", Types.STRING);

        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        // Expect no error happens.
    }

    public static ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("apikey", "xxx")
                .set("endpoint", "api.treasuredata.com")
                .set("database", "my_db")
                .set("table", "my_table");
    }

    public static Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            String name = (String) nameAndTypes[i];
            Type type = (Type) nameAndTypes[i + 1];
            builder.add(name, type);
        }
        return builder.build();
    }

    public static PluginTask pluginTask(ConfigSource config)
    {
        return config.loadConfig(PluginTask.class);
    }

    public static TdOutputPlugin plugin()
    {
        return spy(new TdOutputPlugin());
    }

    public static TDClient tdClient(TdOutputPlugin plugin, PluginTask task)
    {
        return spy(plugin.newTDClient(task));
    }

    public static TDTable newTable(String name, String schema)
    {
        return new TDTable("", name, TDTableType.LOG, schema, 0, 0, "", "", "", "");
    }

    public static FieldWriterSet fieldWriters(Logger log, PluginTask task, Schema schema)
    {
        return spy(FieldWriterSet.createWithValidation(log, task, schema, false));
    }

    public static RecordWriter recordWriter(PluginTask task, TDClient client, FieldWriterSet fieldWriters)
    {
        return spy(new RecordWriter(task, 0, client, fieldWriters));
    }

    static TDClientHttpNotFoundException notFound()
    {
        return new TDClientHttpNotFoundException("not found");
    }

    static TDClientHttpConflictException conflict()
    {
        return new TDClientHttpConflictException("conflict");
    }

    private static TDBulkImportSession session(ImportStatus status, boolean uploadFrozen)
    {
        return new TDBulkImportSession("my_session", "my_db", "my_table", status, uploadFrozen, "0", 0, 0, 0, 0);
    }
}
