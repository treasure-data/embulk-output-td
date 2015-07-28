package org.embulk.output.td;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.Min;
import javax.validation.constraints.Max;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.treasuredata.api.TdApiClient;
import com.treasuredata.api.TdApiClientConfig;
import com.treasuredata.api.TdApiClientConfig.HttpProxyConfig;
import com.treasuredata.api.TdApiConflictException;
import com.treasuredata.api.TdApiNotFoundException;
import com.treasuredata.api.model.TDBulkImportSession;
import com.treasuredata.api.model.TDBulkImportSession.ImportStatus;
import com.treasuredata.api.model.TDTable;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.output.td.RecordWriter.FieldWriterSet;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
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

        //  TODO mode[append, replace]

        @Config("auto_create_table")
        @ConfigDefault("true")
        public boolean getAutoCreateTable();

        @Config("database")
        public String getDatabase();

        @Config("table")
        public String getTable();

        @Config("session")
        @ConfigDefault("null")
        public Optional<String> getSession();

        @Config("time_column")
        @ConfigDefault("null")
        public Optional<String> getTimeColumn();

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
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone(); // data connector's default timezone

        @Override
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%3N\"")
        public String getDefaultTimestampFormat(); // data connector's default timestamp format

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, TimestampColumnOption> getColumnOptions();

        public boolean getDoUpload();
        public void setDoUpload(boolean doUpload);

        public String getSessionName();
        public void setSessionName(String session);
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {}

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
                        String.format("Unknown unix_timestamp_unit '%s'. Supported units are sec, milli, micro, and nano"));
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

        // TODO mode check

        // check column_options is valid or not
        for (String columnName : task.getColumnOptions().keySet()) {
            schema.lookupColumn(columnName); // throws SchemaConfigException
        }

        // generate session name
        task.setSessionName(buildBulkImportSessionName(task, Exec.session()));

        try (TdApiClient client = newTdApiClient(task)) {
            String databaseName = task.getDatabase();
            String tableName = task.getTable();
            if (task.getAutoCreateTable()) {
                createTableIfNotExists(client, databaseName, tableName);
            } else {
                // check if the database and/or table exist or not
                validateTableExists(client, databaseName, tableName);
            }

            // validate FieldWriterSet configuration before transaction is started
            RecordWriter.validateSchema(log, task, schema);

            return doRun(client, task, control);
        }
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (TdApiClient client = newTdApiClient(task)) {
            return doRun(client, task, control);
        }
    }

    private ConfigDiff doRun(TdApiClient client, PluginTask task, OutputPlugin.Control control)
    {
        boolean doUpload = startBulkImportSession(client, task.getSessionName(), task.getDatabase(), task.getTable());
        task.setDoUpload(doUpload);
        control.run(task.dump());
        completeBulkImportSession(client, task.getSessionName(), 0);  // TODO perform job priority

        ConfigDiff configDiff = Exec.newConfigDiff();
        configDiff.set("last_session", task.getSessionName());
        return configDiff;
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<CommitReport> successCommitReports)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (TdApiClient client = newTdApiClient(task)) {
            String sessionName = task.getSessionName();
            log.info("Deleting bulk import session '{}'", sessionName);
            client.deleteBulkImportSession(sessionName);
        }
    }

    private TdApiClient newTdApiClient(final PluginTask task)
    {
        Optional<HttpProxyConfig> httpProxyConfig = newHttpProxyConfig(task.getHttpProxy());
        TdApiClientConfig config = new TdApiClientConfig(task.getEndpoint(), task.getUseSsl(), httpProxyConfig);
        TdApiClient client = new TdApiClient(task.getApiKey(), config);
        try {
            client.start();
        } catch (IOException e) {
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
        } else {
            httpProxyConfig = Optional.absent();
        }
        return httpProxyConfig;
    }

    private void createTableIfNotExists(TdApiClient client, String databaseName, String tableName)
    {
        log.debug("Creating table \"{}\".\"{}\" if not exists", databaseName, tableName);
        try {
            client.createTable(databaseName, tableName);
            log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
        } catch (TdApiNotFoundException e) {
            try {
                client.createDatabase(databaseName);
                log.debug("Created database \"{}\"", databaseName);
            } catch (TdApiConflictException ex) {
                // ignorable error
            }
            try {
                client.createTable(databaseName, tableName);
                log.debug("Created table \"{}\".\"{}\"", databaseName, tableName);
            } catch (TdApiConflictException exe) {
                // ignorable error
            }
        } catch (TdApiConflictException e) {
            // ignorable error
        }
    }

    private void validateTableExists(TdApiClient client, String databaseName, String tableName)
    {
        try {
            for (TDTable table : client.getTables(databaseName)) {
                if (table.getName().equals(tableName)) {
                    return;
                }
            }
            throw new ConfigException(String.format("Table \"%s\".\"%s\" doesn't exist", databaseName, tableName));
        } catch (TdApiNotFoundException ex) {
            throw new ConfigException(String.format("Database \"%s\" doesn't exist", databaseName), ex);
        }
    }

    private String buildBulkImportSessionName(PluginTask task, ExecSession exec)
    {
        if (task.getSession().isPresent()) {
            return task.getSession().get();
        } else {
            Timestamp time = exec.getTransactionTime(); // TODO implement Exec.getTransactionUniqueName()
            return String.format("embulk_%s_%09d",
                    DateTimeFormat.forPattern("yyyyMMdd_HHmmss").withZoneUTC().print(time.getEpochSecond() * 1000),
                    time.getNano());
        }
    }

    // return false if all files are already uploaded
    private boolean startBulkImportSession(TdApiClient client,
            String sessionName, String databaseName, String tableName)
    {
        log.info("Create bulk_import session {}", sessionName);
        TDBulkImportSession session;
        try {
            client.createBulkImportSession(sessionName, databaseName, tableName);
        } catch (TdApiConflictException ex) {
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

    private void completeBulkImportSession(TdApiClient client, String sessionName, int priority)
    {
        TDBulkImportSession session = client.getBulkImportSession(sessionName);

        switch (session.getStatus()) {
        case UPLOADING:
            if (!session.getUploadFrozen()) {
                // freeze
                try {
                    client.freezeBulkImportSession(sessionName);
                } catch (TdApiConflictException e) {
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
            // commit
            log.info("Committing bulk import session '{}'", sessionName);
            log.info("    valid records: {}", session.getValidRecords());
            log.info("    error records: {}", session.getErrorRecords());
            log.info("    valid parts: {}", session.getValidParts());
            log.info("    error parts: {}", session.getErrorParts());
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

    private TDBulkImportSession waitForStatusChange(TdApiClient client, String sessionName,
            ImportStatus current, ImportStatus expecting, String operation)
    {
        TDBulkImportSession importSession;
        while (true) {
            importSession = client.getBulkImportSession(sessionName);

            if (importSession.is(expecting)) {
                return importSession;

            } else if (importSession.is(current)) {
                // in progress

            } else {
                throw new RuntimeException(String.format("Failed to %s bulk import session '%s'",
                            operation, sessionName));
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        RecordWriter closeLater = null;
        try {
            FieldWriterSet fieldWriters = new FieldWriterSet(log, task, schema);
            RecordWriter recordWriter = closeLater = new RecordWriter(task, taskIndex, newTdApiClient(task), fieldWriters);
            recordWriter.open(schema);
            closeLater = null;
            return recordWriter;

        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            if (closeLater != null) {
                closeLater.close();
            }
        }
    }
}
