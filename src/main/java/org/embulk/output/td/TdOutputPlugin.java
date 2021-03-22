package org.embulk.output.td;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientBuilder;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkImportSession.ImportStatus;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDTable;
import org.embulk.EmbulkVersion;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.td.writer.FieldWriterSet;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Optional.fromNullable;
import static java.lang.Integer.parseInt;

public class TdOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("apikey")
        String getApiKey();

        @Config("endpoint")
        @ConfigDefault("\"api.treasuredata.com\"")
        String getEndpoint();

        @Config("use_ssl")
        @ConfigDefault("true")
        boolean getUseSsl();

        @Config("http_proxy")
        @ConfigDefault("null")
        Optional<HttpProxyTask> getHttpProxy();

        //  TODO connect_timeout, read_timeout, send_timeout

        @Config("mode")
        @ConfigDefault("\"append\"")
        Mode getMode();

        @Config("auto_create_table")
        @ConfigDefault("true")
        boolean getAutoCreateTable();

        @Config("database")
        String getDatabase();

        @Config("table")
        String getTable();

        void setLoadTargetTableName(String name);
        String getLoadTargetTableName();

        @Config("session")
        @ConfigDefault("null")
        Optional<String> getSession();

        @Config("default_timestamp_type_convert_to")
        @ConfigDefault("\"string\"")
        ConvertTimestampType getConvertTimestampType();

        @Config("time_column")
        @ConfigDefault("null")
        Optional<String> getTimeColumn();

        @Config("time_value")
        @ConfigDefault("null")
        Optional<TimeValueConfig> getTimeValue(); // TODO allow timestamp format such as {from: "2015-01-01 00:00:00 UTC", to: "2015-01-02 00:00:00 UTC"} as well as unixtime integer
        void setTimeValue(Optional<TimeValueConfig> timeValue);

        @Config("ignore_alternative_time_if_time_exists")
        @ConfigDefault("false")
        boolean getIgnoreAlternativeTimeIfTimeExists();

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        UnixTimestampUnit getUnixTimestampUnit();

        @Config("tmpdir")
        @ConfigDefault("null")
        Optional<String> getTempDir();
        void setTempDir(Optional<String> dir);

        @Config("upload_concurrency")
        @ConfigDefault("2")
        @Min(1)
        @Max(8)
        int getUploadConcurrency();

        @Config("file_split_size")
        @ConfigDefault("16384") // default 16MB (unit: kb)
        long getFileSplitSize();

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
        String getDefaultTimestampFormat();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, ColumnOption> getColumnOptions();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("displayed_error_records_count_limit")
        @ConfigDefault("10")
        @Min(0)
        int getDisplayedErrorRecordsCountLimit();

        @Config("retry_limit")
        @ConfigDefault("20")
        int getRetryLimit();

        @Config("retry_initial_interval_millis")
        @ConfigDefault("1000")
        int getRetryInitialIntervalMillis();

        @Config("retry_max_interval_millis")
        @ConfigDefault("90000")
        int getRetryMaxIntervalMillis();

        @Config("pool_name")
        @ConfigDefault("null")
        Optional<String> getPoolName();

        @Config("additional_http_headers")
        @ConfigDefault("null")
        Optional<Map<String, String>> getAdditionalHttpHeaders();

        @Config("port")
        @ConfigDefault("null")
        Optional<Integer> getPort();
        void setPort(Optional<Integer> port);

        boolean getDoUpload();
        void setDoUpload(boolean doUpload);

        String getSessionName();
        void setSessionName(String session);
    }

    public interface ColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption, TypeColumnOption
    {}

    public enum Mode
    {
        APPEND, REPLACE, TRUNCATE;

        @JsonCreator
        public static Mode fromConfig(String value)
        {
            switch(value) {
            case "append":
                return APPEND;
            case "replace":
                return REPLACE;
            case "truncate":
                return TRUNCATE;
            default:
                throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are [append, replace, truncate]", value));
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
            case TRUNCATE:
                return "truncate";
            default:
                throw new IllegalStateException();
            }
        }
    }

    public interface HttpProxyTask
            extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        int getPort();

        @Config("use_ssl")
        @ConfigDefault("false")
        boolean getUseSsl();

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();
    }

    // FIXME: Better class naming?
    public interface TypeColumnOption
    {
        // keep backward compatible
        @Config("type")
        @ConfigDefault("null")
        Optional<String> getType();

        // keep backward compatible
        @Config("value_type")
        @ConfigDefault("null")
        Optional<String> getValueType();
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

    static final String TASK_REPORT_UPLOADED_PART_NUMBER = "uploaded_part_number";

    private final Logger log;

    public TdOutputPlugin()
    {
        this.log = LoggerFactory.getLogger(getClass());
    }

    public ConfigDiff transaction(final ConfigSource config, final Schema schema, int processorCount,
                                  OutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        // check column_options is valid or not
        checkColumnOptions(schema, task.getColumnOptions());

        // generate session name
        task.setSessionName(buildBulkImportSessionName(task));

        if (!task.getTempDir().isPresent()) {
            task.setTempDir(Optional.of(getEnvironmentTempDirectory()));
        }

        checkEmbulkVersion();

        try (TDClient client = newTDClient(task)) {
            String databaseName = task.getDatabase();
            String tableName = task.getTable();

            switch (task.getMode()) {
            case APPEND:
                if (task.getAutoCreateTable()) {
                    // auto_create_table is valid only with append mode
                    createTableIfNotExists(client, databaseName, tableName);
                }
                else {
                    // check if the database and/or table exist or not
                    validateTableExists(client, databaseName, tableName);
                }
                task.setLoadTargetTableName(tableName);
                break;

            case REPLACE:
            case TRUNCATE:
                // replace and truncate modes always create a new table if the table doesn't exist
                createTableIfNotExists(client, databaseName, tableName);
                task.setLoadTargetTableName(createTemporaryTableWithPrefix(client, databaseName, makeTablePrefix(task)));
                break;
            }

            // validate FieldWriterSet configuration before transaction is started
            validateFieldWriterSet(log, task, schema);

            return doRun(client, schema, task, control);
        }
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (TDClient client = newTDClient(task)) {
            return doRun(client, schema, task, control);
        }
    }

    @VisibleForTesting
    ConfigDiff doRun(TDClient client, Schema schema, PluginTask task, OutputPlugin.Control control)
    {
        boolean doUpload = startBulkImportSession(client, task.getSessionName(), task.getDatabase(), task.getLoadTargetTableName());
        task.setDoUpload(doUpload);
        List<TaskReport> taskReports = control.run(task.dump());
        if (!isNoUploadedParts(taskReports)) {
            completeBulkImportSession(client, schema, task, 0);  // TODO perform job priority
        }
        else {
            // if no parts, it skips submitting requests for perform and commit.
            log.info("Skip performing and committing bulk import session '{}' since no parts are uploaded.", task.getSessionName());
            Map<String, TDColumnType> newColumns = updateSchema(client, schema, task);
            printNewAddedColumns(newColumns);
        }

        // commit
        switch (task.getMode()) {
        case APPEND:
            // already done
            break;
        case REPLACE:
        case TRUNCATE:
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
        try (TDClient client = newTDClient(task)) {
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
    void checkColumnOptions(Schema schema, Map<String, ColumnOption> columnOptions)
    {
        for (String columnName : columnOptions.keySet()) {
            schema.lookupColumn(columnName); // throws SchemaConfigException
        }
    }

    @VisibleForTesting
    public TDClient newTDClient(final PluginTask task)
    {
        TDClientBuilder builder = TDClient.newBuilder();
        builder.setApiKey(task.getApiKey());
        builder.setEndpoint(task.getEndpoint());
        builder.setUseSSL(task.getUseSsl());
        builder.setConnectTimeoutMillis(60000); // default 15000
        builder.setReadTimeoutMillis(60000); // default 60000
        builder.setRetryLimit(task.getRetryLimit());
        builder.setRetryInitialIntervalMillis(task.getRetryInitialIntervalMillis());
        builder.setRetryMaxIntervalMillis(task.getRetryMaxIntervalMillis());

        if (task.getPort().isPresent()) {
            builder.setPort(task.getPort().get());
        }

        if (task.getAdditionalHttpHeaders().isPresent()) {
            builder.setHeaders(buildMultiMapHeaders(task.getAdditionalHttpHeaders().get()));
        }

        Optional<ProxyConfig> proxyConfig = newProxyConfig(task.getHttpProxy());
        if (proxyConfig.isPresent()) {
            builder.setProxy(proxyConfig.get());
        }

        return builder.build();
    }

    private Multimap<String, String> buildMultiMapHeaders(Map<String, String> headers)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            multimap.put(entry.getKey(), entry.getValue());
        }
        return multimap;
    }

    @VisibleForTesting
    Optional<ProxyConfig> newProxyConfig(Optional<HttpProxyTask> task)
    {
        // This plugin searches http proxy settings and configures them to TDClient. The order of proxy setting searching is:
        // 1. System properties
        // 2. http_proxy config option provided by this plugin

        Properties props = System.getProperties();
        if (props.containsKey("http.proxyHost") || props.containsKey("https.proxyHost")) {
            boolean useSsl = props.containsKey("https.proxyHost");
            String proto = !useSsl ? "http" : "https";
            String host = props.getProperty(proto + ".proxyHost");
            int port = parseInt(props.getProperty(proto + ".proxyPort", !useSsl ? "80" : "443"));
            Optional<String> user = fromNullable(props.getProperty(proto + ".proxyUser"));
            Optional<String> password = fromNullable(props.getProperty(proto + ".proxyPassword"));
            return Optional.of(new ProxyConfig(host, port, useSsl, user, password));
        }
        else if (task.isPresent()) {
            HttpProxyTask proxyTask = task.get();
            return Optional.of(new ProxyConfig(proxyTask.getHost(), proxyTask.getPort(), proxyTask.getUseSsl(),
                    proxyTask.getUser(), proxyTask.getPassword()));
        }
        else {
            return Optional.absent();
        }
    }

    @VisibleForTesting
    void createTableIfNotExists(TDClient client, String databaseName, String tableName)
    {
        log.debug("Creating table \"{}\".\"{}\" if not exists", databaseName, tableName);
        try {
            client.createTable(databaseName, tableName);
            log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
        }
        catch (TDClientHttpNotFoundException e) {
            try {
                client.createDatabase(databaseName);
                log.debug("Created database \"{}\"", databaseName);
            }
            catch (TDClientHttpConflictException ex) {
                // ignorable error
            }
            try {
                client.createTable(databaseName, tableName);
                log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
            }
            catch (TDClientHttpConflictException exe) {
                // ignorable error
            }
        }
        catch (TDClientHttpConflictException e) {
            // ignorable error
        }
    }

    @VisibleForTesting
    String createTemporaryTableWithPrefix(TDClient client, String databaseName, String tablePrefix)
            throws TDClientHttpConflictException
    {
        String tableName = tablePrefix;
        while (true) {
            log.debug("Creating temporal table \"{}\".\"{}\"", databaseName, tableName);
            try {
                client.createTable(databaseName, tableName);
                log.debug("Created temporal table \"{}\".\"{}\"", databaseName, tableName);
                return tableName;
            }
            catch (TDClientHttpConflictException e) {
                log.debug("\"{}\".\"{}\" table already exists. Renaming temporal table.", databaseName, tableName);
                tableName += "_";
            }
        }
    }

    @VisibleForTesting
    void validateTableExists(TDClient client, String databaseName, String tableName)
    {
        try {
            client.showTable(databaseName, tableName);
        }
        catch (TDClientHttpNotFoundException ex) {
            throw new ConfigException(String.format("Database \"%s\" or table \"%s\" doesn't exist", databaseName, tableName), ex);
        }
    }

    @VisibleForTesting
    String buildBulkImportSessionName(PluginTask task)
    {
        if (task.getSession().isPresent()) {
            return task.getSession().get();
        }
        else {
            Timestamp time = Exec.getTransactionTime(); // TODO implement Exec.getTransactionUniqueName()
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                    .withZone(ZoneOffset.UTC);
            return String.format("embulk_%s_%09d_%s",
                    dateTimeFormatter.format(time.getInstant()),
                    time.getNano(), UUID.randomUUID().toString().replace('-', '_'));
        }
    }

    // return false if all files are already uploaded
    @VisibleForTesting
    boolean startBulkImportSession(TDClient client,
            String sessionName, String databaseName, String tableName)
    {
        log.info("Create bulk_import session {}", sessionName);
        TDBulkImportSession session;
        try {
            client.createBulkImportSession(sessionName, databaseName, tableName);
        }
        catch (TDClientHttpConflictException ex) {
            // ignorable error
        }
        session = client.getBulkImportSession(sessionName);
        // TODO check associated databaseName and tableName

        switch (session.getStatus()) {
        case UPLOADING:
            if (session.isUploadFrozen()) {
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
    void completeBulkImportSession(TDClient client, Schema schema, PluginTask task, int priority)
    {
        String sessionName = task.getSessionName();
        TDBulkImportSession session = client.getBulkImportSession(sessionName);

        switch (session.getStatus()) {
        case UPLOADING:
            if (!session.isUploadFrozen()) {
                // freeze
                try {
                    client.freezeBulkImportSession(sessionName);
                }
                catch (TDClientHttpConflictException e) {
                    // ignorable error
                }
            }
            // perform
            client.performBulkImportSession(sessionName, task.getPoolName()); // TODO use priority

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
            printNewAddedColumns(newColumns);

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

    Map<String, TDColumnType> updateSchema(TDClient client, Schema inputSchema, PluginTask task)
    {
        String databaseName = task.getDatabase();
        TDTable table = findTable(client, databaseName, task.getTable());

        final Map<String, TDColumnType> guessedSchema = new LinkedHashMap<>();
        inputSchema.visitColumns(new ColumnVisitor() {
            public void booleanColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.STRING);
            }

            public void longColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.LONG);
            }

            public void doubleColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.DOUBLE);
            }

            public void stringColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.STRING);
            }

            public void timestampColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.STRING);
            }

            public void jsonColumn(Column column)
            {
                guessedSchema.put(column.getName(), TDColumnType.STRING);
            }
        });

        Map<String, Integer> usedNames = new HashMap<>();
        if (task.getMode() != Mode.REPLACE) {
            for (TDColumn existent : table.getColumns()) {
                usedNames.put(existent.getName(), 1);
                guessedSchema.remove(existent.getKeyString()); // don't change type of existent columns
            }
        }
        guessedSchema.remove("time"); // don't change type of 'time' column
        // 'v' column is special column in TD's table, it is reserved column to be used in Hive
        // by executing `SELECT *` query, thus it must not be appended
        // otherwise 422 response code will be responded.
        guessedSchema.remove("v");

        List<TDColumn> newSchema;
        if (task.getMode() != Mode.REPLACE) {
            newSchema = new ArrayList<>(table.getColumns());
        }
        else {
            newSchema = Lists.newArrayList();
        }

        final Map<String, TDColumnType> appliedColumnOptionSchema = applyColumnOptions(guessedSchema, task.getColumnOptions());
        for (Map.Entry<String, TDColumnType> pair : appliedColumnOptionSchema.entrySet()) {
            String key = renameColumn(pair.getKey());

            if (!usedNames.containsKey(key)) {
                usedNames.put(key, 1);
            }
            else {
                int next = usedNames.get(key);
                key = key + "_" + next;
                usedNames.put(key, next + 1);
            }

            newSchema.add(new TDColumn(key, pair.getValue(), pair.getKey().getBytes(StandardCharsets.UTF_8)));
        }

        client.appendTableSchema(databaseName, task.getLoadTargetTableName(), newSchema);
        return appliedColumnOptionSchema;
    }

    void printNewAddedColumns(Map<String, TDColumnType> newColumns)
    {
        if (!newColumns.isEmpty()) {
            log.info("    new columns:");
        }
        for (Map.Entry<String, TDColumnType> pair : newColumns.entrySet()) {
            log.info("      - {}: {}", pair.getKey(), pair.getValue());
        }
    }

    private static TDTable findTable(TDClient client, String databaseName, String tableName)
    {
        try {
            return client.showTable(databaseName, tableName);
        }
        catch (TDClientHttpNotFoundException e) {
            return null;
        }
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

    void showBulkImportErrorRecords(TDClient client, String sessionName, final int recordCountLimit)
    {
        log.info("Show {} error records", recordCountLimit);
        client.getBulkImportErrorRecords(sessionName, new Function<InputStream, Void>()
        {
            @Override
            public Void apply(InputStream input)
            {
                int errorRecordCount = 0;
                try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input))) {
                    while (unpacker.hasNext()) {
                        Value v = unpacker.unpackValue();
                        log.info("    {}", v.toJson());
                        errorRecordCount += 1;

                        if (errorRecordCount >= recordCountLimit) {
                            break;
                        }
                    }
                }
                catch (IOException ignored) {
                    log.info("Stop downloading error records");
                }
                return null;
            }
        });
    }

    private void checkEmbulkVersion()
    {
        // Embulk v0.8.21 and lower version uses old Jackson and doesn't works with this plugin
        // https://github.com/embulk/embulk/pull/615
        boolean isValidVersion = true;
        try {
            // Embulk v0.8.18 or lower version doesn't have class org.embulk.EmbulkVersion
            Class.forName("org.embulk.EmbulkVersion");
            String[] versionNumber = EmbulkVersion.VERSION.split("-"); // to split e.g. "0.8.26-SNAPSHOT" or "0.8.30-ALPHA1"
            if (versionNumber[0] != null) {
                String[] versions = versionNumber[0].split("\\.");
                if (versions.length == 3) {
                    if (Integer.valueOf(versions[1]) <= 8 && Integer.valueOf(versions[2]) < 22) {
                        isValidVersion = false;
                    }
                }
            }
        }
        catch (ClassNotFoundException ex) {
            isValidVersion = false;
        }
        if (!isValidVersion) {
            throw new ConfigException("embulk-output-td v0.4.x+ only supports Embulk v0.8.22 or higher versions");
        }
    }

    private Map<String, TDColumnType> applyColumnOptions(Map<String, TDColumnType> schema, Map<String, ColumnOption> columnOptions)
    {
        return Maps.asMap(schema.keySet(), key -> {
            if (columnOptions.containsKey(key)) {
                Optional<String> columnType = columnOptions.get(key).getType();
                if (columnType.isPresent()) {
                    return TDColumnType.parseColumnType(columnType.get());
                }
            }

            return schema.get(key);
        });
    }

    @VisibleForTesting
    TDBulkImportSession waitForStatusChange(TDClient client, String sessionName,
            ImportStatus current, ImportStatus expecting, String operation)
    {
        TDBulkImportSession importSession;
        while (true) {
            importSession = client.getBulkImportSession(sessionName);

            if (importSession.getStatus() == expecting) {
                return importSession;

            }
            else if (importSession.getStatus() == current) {
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

    boolean isNoUploadedParts(List<TaskReport> taskReports)
    {
        int partNumber = 0;
        for (TaskReport taskReport : taskReports) {
            if (!taskReport.has(TASK_REPORT_UPLOADED_PART_NUMBER)) {
                return false;
            }
            partNumber += taskReport.get(int.class, TASK_REPORT_UPLOADED_PART_NUMBER);
        }
        return partNumber == 0;
    }

    @VisibleForTesting
    void renameTable(TDClient client, String databaseName, String oldName, String newName)
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
            FieldWriterSet fieldWriters = createFieldWriterSet(log, task, schema);
            closeLater = new RecordWriter(task, taskIndex, newTDClient(task), fieldWriters);
            RecordWriter recordWriter = closeLater;
            recordWriter.open(schema);
            closeLater = null;
            return recordWriter;

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (closeLater != null) {
                closeLater.close();
            }
        }
    }

    @VisibleForTesting
    String getEnvironmentTempDirectory()
    {
        return System.getProperty("java.io.tmpdir");
    }

    protected FieldWriterSet createFieldWriterSet(Logger log, PluginTask task, Schema schema)
    {
        return FieldWriterSet.createWithValidation(log, task, schema, true);
    }

    protected void validateFieldWriterSet(Logger log, PluginTask task, Schema schema)
    {
        FieldWriterSet.createWithValidation(log, task, schema, false);
    }
}
