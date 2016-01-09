package org.embulk.output.td;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.validation.constraints.Min;
import javax.validation.constraints.Max;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.api.TdApiClient;
import com.treasuredata.api.TdApiClientConfig;
import com.treasuredata.api.TdApiClientConfig.HttpProxyConfig;
import com.treasuredata.api.TdApiConflictException;
import com.treasuredata.api.TdApiNotFoundException;
import com.treasuredata.api.model.TDBulkImportSession;
import com.treasuredata.api.model.TDBulkImportSession.ImportStatus;
import com.treasuredata.api.model.TDTable;
import com.treasuredata.api.model.TDColumn;
import com.treasuredata.api.model.TDColumnType;
import com.treasuredata.api.model.TDPrimitiveColumnType;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.format.DateTimeFormat;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;
import org.slf4j.Logger;

public class TdOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("apikey")
        public String getApiKey();

        @Config("endpoint")
        @ConfigDefault("\"api.treasuredata.com\"")
        public String getEndpoint();

        @Config("use_ssl")
        @ConfigDefault("true")
        public boolean getUseSsl();

        @Config("http_proxy")
        @ConfigDefault("null")
        public Optional<HttpProxyTask> getHttpProxy();

        //  TODO connect_timeout, read_timeout, send_timeout

        @Config("mode")
        @ConfigDefault("\"append\"")
        public Mode getMode();

        @Config("auto_create_table")
        @ConfigDefault("true")
        public boolean getAutoCreateTable();

        @Config("database")
        public String getDatabase();

        @Config("table")
        public String getTable();

        public void setLoadTargetTableName(String name);
        public String getLoadTargetTableName();

        @Config("session")
        @ConfigDefault("null")
        public Optional<String> getSession();

        @Config("default_timestamp_type_convert_to")
        @ConfigDefault("\"string\"")
        public ConvertTimestampType getConvertTimestampType();

        @Config("time_column")
        @ConfigDefault("null")
        public Optional<String> getTimeColumn();

        @Config("time_value")
        @ConfigDefault("null")
        public Optional<TimeValueConfig> getTimeValue(); // TODO allow timestamp format such as {from: "2015-01-01 00:00:00 UTC", to: "2015-01-02 00:00:00 UTC"} as well as unixtime integer

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        public UnixTimestampUnit getUnixTimestampUnit();

        @Config("tmpdir")
        @ConfigDefault("\"/tmp\"")
        public String getTempDir();

        @Config("upload_concurrency")
        @ConfigDefault("2")
        @Min(1)
        @Max(8)
        public int getUploadConcurrency();

        @Config("file_split_size")
        @ConfigDefault("16384") // default 16MB (unit: kb)
        public long getFileSplitSize();

        @Override
        @Config("default_timestamp_format")
        // SQL timestamp with milliseconds is, by defualt, used because Hive and Presto use
        // those format. As timestamp type, Presto
        //   * cannot parse SQL timestamp with timezone like '2015-02-03 04:05:06.789 UTC'
        //   * cannot parse SQL timestamp with nanoseconds like '2015-02-03 04:05:06.789012345'
        //   * cannot parse SQL timestamp with microseconds like '2015-02-03 04:05:06.789012'
        //   * can parse SQL timestamp with milliseconds like '2015-02-03 04:05:06.789'
        // On the other hand, Hive
        //   * cannot parse SQL timestamp with timezone like '2015-02-03 04:05:06.789 UTC'
        //   * can parse SQL timestamp with nanoseconds like '2015-02-03 04:05:06.789012345'
        //   * can parse SQL timestamp with microseconds like '2015-02-03 04:05:06.789012'
        //   * can parse SQL timestamp with milliseconds like '2015-02-03 04:05:06.789'
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%3N\"")
        public String getDefaultTimestampFormat();

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, TimestampColumnOption> getColumnOptions();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("displayed_error_records_count_limit")
        @ConfigDefault("10")
        @Min(0)
        int getDisplayedErrorRecordsCountLimit();

        public boolean getDoUpload();
        public void setDoUpload(boolean doUpload);

        public String getSessionName();
        public void setSessionName(String session);
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {}

    public enum Mode
    {
        APPEND, REPLACE;

        @JsonCreator
        public static Mode fromConfig(String value)
        {
            switch(value) {
            case "append":
                return APPEND;
            case "replace":
                return REPLACE;
            default:
                throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are [append, replace]", value));
            }
        }

        @JsonValue
        public String toString()
        {
            switch(this) {
            case APPEND:
                return "append";
            case REPLACE:
                return "replace";
            default:
                throw new IllegalStateException();
            }
        }
    }

    public interface HttpProxyTask
            extends Task
    {
        @Config("host")
        public String getHost();

        @Config("port")
        public int getPort();

        @Config("use_ssl")
        @ConfigDefault("false")
        public boolean getUseSsl();
    }

    public static class TimeValueConfig
    {
        @JsonCreator
        public static TimeValueConfig of(@JsonProperty("from") long from, @JsonProperty("to") long to)
        {
            if (from < 0 || from > 253402300799L || to < 0 || to > 253402300799L) {
                throw new ConfigException(String.format("time_value 'from' and 'to' should be [0, 253402300799]: {from: %d, to: %d}", from, to));
            }
            if (from > to) {
                throw new ConfigException(String.format("time_value 'to' should be greater than or equal to 'from': {from: %d, to: %d}", from, to));
            }

            TimeValueConfig config = new TimeValueConfig(from, to);
            return config;
        }

        private final long from;
        private final long to;

        public TimeValueConfig(long from, long to)
        {
            this.from = from;
            this.to = to;
        }

        @JsonProperty("from")
        public long getFrom()
        {
            return from;
        }

        @JsonProperty("to")
        public long getTo()
        {
            return to;
        }

        @JsonValue
        public Map<String, Object> getMap()
        {
            return ImmutableMap.<String, Object>of("from", from, "to", to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof TimeValueConfig)) {
                return false;
            }

            TimeValueConfig that = (TimeValueConfig) obj;
            return Objects.equals(from, that.from) && Objects.equals(to, that.to);
        }

    }

    public static enum ConvertTimestampType
    {
        STRING(-1),
        //SEC_DOUBLE(-1),  // TODO
        SEC(1);
        //MILLI(1000),  // TODO
        //MICRO(1000000),  // TODO
        //NANO(1000000000);  // TODO

        private final int unit;

        private ConvertTimestampType(int unit)
        {
            this.unit = unit;
        }

        public int getFractionUnit()
        {
            return unit;
        }

        @JsonCreator
        public static ConvertTimestampType of(String s)
        {
            switch (s) {
            case "string": return STRING;
            //case "sec_double": return SEC_DOUBLE;
            case "sec": return SEC;
            //case "milli": return MILLI;
            //case "micro": return MICRO;
            //case "nano": return NANO;
            default:
                throw new ConfigException(
                        String.format("Unknown convert_timestamp_type '%s'. Supported units are string, sec, milli, micro, nano, and sec_double", s));
            }
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public static enum UnixTimestampUnit
    {
        SEC(1),
        MILLI(1000),
        MICRO(1000000),
        NANO(1000000000);

        private final int unit;

        private UnixTimestampUnit(int unit)
        {
            this.unit = unit;
        }

        public int getFractionUnit()
        {
            return unit;
        }

        @JsonCreator
        public static UnixTimestampUnit of(String s)
        {
            switch (s) {
            case "sec": return SEC;
            case "milli": return MILLI;
            case "micro": return MICRO;
            case "nano": return NANO;
            default:
                throw new ConfigException(
                        String.format("Unknown unix_timestamp_unit '%s'. Supported units are sec, milli, micro, and nano", s));
            }
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private final Logger log;

    public TdOutputPlugin()
    {
        this.log = Exec.getLogger(getClass());
    }

    public ConfigDiff transaction(final ConfigSource config, final Schema schema, int processorCount,
                                  OutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        // check column_options is valid or not
        checkColumnOptions(schema, task.getColumnOptions());

        // generate session name
        task.setSessionName(buildBulkImportSessionName(task, Exec.session()));

        try (TdApiClient client = newTdApiClient(task)) {
            String databaseName = task.getDatabase();
            String tableName = task.getTable();

            switch (task.getMode()) {
            case APPEND:
                if (task.getAutoCreateTable()) {
                    // auto_create_table is valid only with append mode (replace mode always creates a new table)
                    createTableIfNotExists(client, databaseName, tableName);
                }
                else {
                    // check if the database and/or table exist or not
                    validateTableExists(client, databaseName, tableName);
                }
                task.setLoadTargetTableName(tableName);
                break;

            case REPLACE:
                task.setLoadTargetTableName(
                        createTemporaryTableWithPrefix(client, databaseName, makeTablePrefix(task)));
                break;
            }

            // validate FieldWriterSet configuration before transaction is started
            RecordWriter.validateSchema(log, task, schema);

            return doRun(client, schema, task, control);
        }
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (TdApiClient client = newTdApiClient(task)) {
            return doRun(client, schema, task, control);
        }
    }

    @VisibleForTesting
    ConfigDiff doRun(TdApiClient client, Schema schema, PluginTask task, OutputPlugin.Control control)
    {
        boolean doUpload = startBulkImportSession(client, task.getSessionName(), task.getDatabase(), task.getLoadTargetTableName());
        task.setDoUpload(doUpload);
        control.run(task.dump());
        completeBulkImportSession(client, schema, task, 0);  // TODO perform job priority

        // commit
        switch (task.getMode()) {
        case APPEND:
            // already done
            break;
        case REPLACE:
            // rename table
            renameTable(client, task.getDatabase(), task.getLoadTargetTableName(), task.getTable());
        }

        ConfigDiff configDiff = Exec.newConfigDiff();
        configDiff.set("last_session", task.getSessionName());
        return configDiff;
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<TaskReport> successTaskReports)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (TdApiClient client = newTdApiClient(task)) {
            String sessionName = task.getSessionName();
            log.info("Deleting bulk import session '{}'", sessionName);
            client.deleteBulkImportSession(sessionName);
        }
    }

    private String makeTablePrefix(PluginTask task)
    {
        return task.getTable() + "_" + task.getSessionName();
    }

    @VisibleForTesting
    void checkColumnOptions(Schema schema, Map<String, TimestampColumnOption> columnOptions)
    {
        for (String columnName : columnOptions.keySet()) {
            schema.lookupColumn(columnName); // throws SchemaConfigException
        }
    }

    @VisibleForTesting
    public TdApiClient newTdApiClient(final PluginTask task)
    {
        Optional<HttpProxyConfig> httpProxyConfig = newHttpProxyConfig(task.getHttpProxy());
        TdApiClientConfig config = new TdApiClientConfig(task.getEndpoint(), task.getUseSsl(), httpProxyConfig);
        TdApiClient client = new TdApiClient(task.getApiKey(), config);
        try {
            client.start();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return client;
    }

    private Optional<HttpProxyConfig> newHttpProxyConfig(Optional<HttpProxyTask> task)
    {
        Optional<HttpProxyConfig> httpProxyConfig;
        if (task.isPresent()) {
            HttpProxyTask pt = task.get();
            httpProxyConfig = Optional.of(new HttpProxyConfig(pt.getHost(), pt.getPort(), pt.getUseSsl()));
        }
        else {
            httpProxyConfig = Optional.absent();
        }
        return httpProxyConfig;
    }

    @VisibleForTesting
    void createTableIfNotExists(TdApiClient client, String databaseName, String tableName)
    {
        log.debug("Creating table \"{}\".\"{}\" if not exists", databaseName, tableName);
        try {
            client.createTable(databaseName, tableName);
            log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
        }
        catch (TdApiNotFoundException e) {
            try {
                client.createDatabase(databaseName);
                log.debug("Created database \"{}\"", databaseName);
            }
            catch (TdApiConflictException ex) {
                // ignorable error
            }
            try {
                client.createTable(databaseName, tableName);
                log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
            }
            catch (TdApiConflictException exe) {
                // ignorable error
            }
        }
        catch (TdApiConflictException e) {
            // ignorable error
        }
    }

    @VisibleForTesting
    String createTemporaryTableWithPrefix(TdApiClient client, String databaseName, String tablePrefix)
            throws TdApiConflictException
    {
        String tableName = tablePrefix;
        while (true) {
            log.debug("Creating temporal table \"{}\".\"{}\"", databaseName, tableName);
            try {
                client.createTable(databaseName, tableName);
                log.debug("Created temporal table \"{}\".\"{}\"", databaseName, tableName);
                return tableName;
            }
            catch (TdApiConflictException e) {
                log.debug("\"{}\".\"{}\" table already exists. Renaming temporal table.", databaseName, tableName);
                tableName += "_";
            }
        }
    }

    @VisibleForTesting
    void validateTableExists(TdApiClient client, String databaseName, String tableName)
    {
        try {
            for (TDTable table : client.getTables(databaseName)) {
                if (table.getName().equals(tableName)) {
                    return;
                }
            }
            throw new ConfigException(String.format("Table \"%s\".\"%s\" doesn't exist", databaseName, tableName));
        }
        catch (TdApiNotFoundException ex) {
            throw new ConfigException(String.format("Database \"%s\" doesn't exist", databaseName), ex);
        }
    }

    @VisibleForTesting
    String buildBulkImportSessionName(PluginTask task, ExecSession exec)
    {
        if (task.getSession().isPresent()) {
            return task.getSession().get();
        }
        else {
            Timestamp time = exec.getTransactionTime(); // TODO implement Exec.getTransactionUniqueName()
            return String.format("embulk_%s_%09d",
                    DateTimeFormat.forPattern("yyyyMMdd_HHmmss").withZoneUTC().print(time.getEpochSecond() * 1000),
                    time.getNano());
        }
    }

    // return false if all files are already uploaded
    @VisibleForTesting
    boolean startBulkImportSession(TdApiClient client,
            String sessionName, String databaseName, String tableName)
    {
        log.info("Create bulk_import session {}", sessionName);
        TDBulkImportSession session;
        try {
            client.createBulkImportSession(sessionName, databaseName, tableName);
        }
        catch (TdApiConflictException ex) {
            // ignorable error
        }
        session = client.getBulkImportSession(sessionName);
        // TODO check associated databaseName and tableName

        switch (session.getStatus()) {
        case UPLOADING:
            if (session.getUploadFrozen()) {
                return false;
            }
            return true;
        case PERFORMING:
            return false;
        case READY:
            return false;
        case COMMITTING:
            return false;
        case COMMITTED:
            return false;
        case UNKNOWN:
        default:
            throw new RuntimeException("Unknown bulk import status");
        }
    }

    @VisibleForTesting
    void completeBulkImportSession(TdApiClient client, Schema schema, PluginTask task, int priority)
    {
        String sessionName = task.getSessionName();
        TDBulkImportSession session = client.getBulkImportSession(sessionName);

        switch (session.getStatus()) {
        case UPLOADING:
            if (!session.getUploadFrozen()) {
                // freeze
                try {
                    client.freezeBulkImportSession(sessionName);
                }
                catch (TdApiConflictException e) {
                    // ignorable error
                }
            }
            // perform
            client.performBulkImportSession(sessionName, priority);

            // pass
        case PERFORMING:
            log.info("Performing bulk import session '{}'", sessionName);
            session = waitForStatusChange(client, sessionName,
                    ImportStatus.PERFORMING, ImportStatus.READY,
                    "perform");
            log.info("    job id: {}", session.getJobId());

            // pass
        case READY:
            // TODO add an option to make the transaction failed if error_records or error_parts is too large

            // add Embulk's columns to the table schema
            Map<String, TDColumnType> newColumns = updateSchema(client, schema, task);
            log.info("Committing bulk import session '{}'", sessionName);
            log.info("    valid records: {}", session.getValidRecords());
            log.info("    error records: {}", session.getErrorRecords());
            log.info("    valid parts: {}", session.getValidParts());
            log.info("    error parts: {}", session.getErrorParts());
            if (!newColumns.isEmpty()) {
                log.info("    new columns:");
            }
            for (Map.Entry<String, TDColumnType> pair : newColumns.entrySet()) {
                log.info("      - {}: {}", pair.getKey(), pair.getValue());
            }

            if (session.getErrorRecords() > 0L) {
                showBulkImportErrorRecords(client, sessionName, (int) Math.min(session.getErrorRecords(), task.getDisplayedErrorRecordsCountLimit()));
            }

            if (session.getErrorRecords() > 0 && task.getStopOnInvalidRecord()) {
                throw new DataException(String.format("Stop committing because the perform job skipped %d error records", session.getErrorRecords()));
            }

            // commit
            client.commitBulkImportSession(sessionName);

            // pass
        case COMMITTING:
            session = waitForStatusChange(client, sessionName,
                    ImportStatus.COMMITTING, ImportStatus.COMMITTED,
                    "commit");

            // pass
        case COMMITTED:
            return;

        case UNKNOWN:
            throw new RuntimeException("Unknown bulk import status");
        }
    }

    Map<String, TDColumnType> updateSchema(TdApiClient client, Schema inputSchema, PluginTask task)
    {
        String databaseName = task.getDatabase();

        TDTable table = findTable(client, databaseName, task.getTable());
        if (table == null) {
            return new HashMap<>();
        }

        final Map<String, TDColumnType> guessedSchema = new HashMap<>();
        inputSchema.visitColumns(new ColumnVisitor() {
            public void booleanColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDPrimitiveColumnType.LONG);
            }

            public void longColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDPrimitiveColumnType.LONG);;
            }

            public void doubleColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDPrimitiveColumnType.DOUBLE);
            }

            public void stringColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDPrimitiveColumnType.STRING);
            }

            public void timestampColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDPrimitiveColumnType.STRING);
            }
        });

        Map<String, Integer> usedNames = new HashMap<>();
        for (TDColumn existent : table.getColumns()) {
            usedNames.put(new String(existent.getKey()), 1);
            guessedSchema.remove(existent.getName()); // don't change type of existent columns
        }
        guessedSchema.remove("time"); // don't change type of 'time' column

        List<TDColumn> newSchema = new ArrayList<>(table.getColumns());
        for (Map.Entry<String, TDColumnType> pair : guessedSchema.entrySet()) {
            String key = renameColumn(pair.getKey());

            if (!usedNames.containsKey(key)) {
                usedNames.put(key, 1);
            } else {
                int next = usedNames.get(key);
                key = key + "_" + next;
                usedNames.put(key, next + 1);
            }

            newSchema.add(new TDColumn(pair.getKey(), pair.getValue(), key.getBytes(StandardCharsets.UTF_8)));
        }

        client.updateSchema(databaseName, task.getLoadTargetTableName(), newSchema);
        return guessedSchema;
    }

    private static TDTable findTable(TdApiClient client, String databaseName, String tableName)
    {
        for (TDTable table : client.getTables(databaseName)) {
            if (table.getName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("\\A[a-z_][a-z0-9_]*\\z");
    private static final Pattern COLUMN_NAME_SQUASH_PATTERN = Pattern.compile("(?:[^a-zA-Z0-9_]|(?:\\A[^a-zA-Z_]))+");

    private static String renameColumn(String origName)
    {
        if (COLUMN_NAME_PATTERN.matcher(origName).matches()) {
            return origName;
        }
        return COLUMN_NAME_SQUASH_PATTERN.matcher(origName).replaceAll("_").toLowerCase();
    }

    void showBulkImportErrorRecords(TdApiClient client, String sessionName, int recordCountLimit)
    {
        log.info("Show {} error records", recordCountLimit);
        try (InputStream in = client.getBulkImportErrorRecords(sessionName)) {
            Unpacker unpacker = new MessagePack().createUnpacker(new GZIPInputStream(in));
            UnpackerIterator records = unpacker.iterator();
            for (int i = 0; i < recordCountLimit; i++) {
                log.info("    {}", records.next());
            }
        }
        catch (Exception ignored) {
            log.info("Stop downloading error records", ignored);
        }
    }

    @VisibleForTesting
    TDBulkImportSession waitForStatusChange(TdApiClient client, String sessionName,
            ImportStatus current, ImportStatus expecting, String operation)
    {
        TDBulkImportSession importSession;
        while (true) {
            importSession = client.getBulkImportSession(sessionName);

            if (importSession.is(expecting)) {
                return importSession;

            }
            else if (importSession.is(current)) {
                // in progress

            }
            else {
                throw new RuntimeException(String.format("Failed to %s bulk import session '%s'",
                            operation, sessionName));
            }

            try {
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {
            }
        }
    }

    @VisibleForTesting
    void renameTable(TdApiClient client, String databaseName, String oldName, String newName)
    {
        log.debug("Renaming table \"{}\".\"{}\" to \"{}\"", databaseName, oldName, newName);
        client.renameTable(databaseName, oldName, newName, true);
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        RecordWriter closeLater = null;
        try {
            FieldWriterSet fieldWriters = new FieldWriterSet(log, task, schema);
            closeLater = new RecordWriter(task, taskIndex, newTdApiClient(task), fieldWriters);
            RecordWriter recordWriter = closeLater;
            recordWriter.open(schema);
            closeLater = null;
            return recordWriter;

        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (closeLater != null) {
                closeLater.close();
            }
        }
    }
}
