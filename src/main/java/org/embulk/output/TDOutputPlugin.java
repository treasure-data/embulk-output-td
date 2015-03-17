package org.embulk.output;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.treasuredata.api.TDApiClient;
import com.treasuredata.api.TDApiClientOptions;
import com.treasuredata.api.TDApiConflictException;
import com.treasuredata.api.TDApiException;
import com.treasuredata.api.TDApiNotFoundException;
import com.treasuredata.api.model.TDBulkImportSession;
import com.treasuredata.api.model.TDBulkImportSession.ImportStatus;
import com.treasuredata.api.model.TDDatabase;
import com.treasuredata.api.model.TDTable;
import org.joda.time.format.DateTimeFormat;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter.FormatterTask;
import org.slf4j.Logger;

public class TDOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, FormatterTask
    {
        @Config("apikey")
        public String getApiKey();

        @Config("endpoint")
        public String getEndpoint();

        @Config("use_ssl")
        @ConfigDefault("true")
        public boolean getUseSsl();

        //  TODO http_proxy
        //  TODO connect_timeout, read_timeout, send_timeout

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

        @Config("tmpdir")
        @ConfigDefault("/tmp")
        public String getTempDir();

        public boolean getDoUpload();
        public void setDoUpload(boolean doUpload);

        public String getSessionName();
        public void setSessionName(String session);
    }

    private final Logger log;

    @Inject
    public TDOutputPlugin()
    {
        this.log = Exec.getLogger(getClass());
    }

    public ConfigDiff transaction(final ConfigSource config, final Schema schema, int processorCount,
                                  OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        task.setSessionName(buildBulkImportSessionName(task, Exec.session()));

        try (TDApiClient client = newTDApiClient(task)) {
            String databaseName = task.getDatabase();
            String tableName = task.getTable();
            if (task.getAutoCreateTable()) {
                createDatabaseIfNotExists(client, databaseName);
                createTableIfNotExists(client, databaseName, tableName);
            } else {
                // check if the database and/or table exist or not
                if (!checkDatabaseExists(client, databaseName)) {
                    log.debug("Database '{}' doesn't exist", databaseName);
                    throw new TDApiException(String.format("Database '%s' doesn't exist", databaseName));
                }
                if (!checkTableExists(client, databaseName, tableName)) {
                    log.debug("Table {}.{} doesn't exist", databaseName, tableName);
                    throw new TDApiException(String.format("Table %s.%s doesn't exist", databaseName, tableName));
                }
            }

            // validate MessagePackRecordOutput configuration before transaction is started
            createMessagePackPageOutput(task, schema, client).close();

            return doRun(client, task, control);
        }
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        try (TDApiClient client = newTDApiClient(task)) {
            return doRun(client, task, control);
        }
    }

    private ConfigDiff doRun(TDApiClient client, PluginTask task, OutputPlugin.Control control)
    {
        boolean doUpload = startBulkImportSession(client, task.getSessionName(), task.getDatabase(), task.getTable());
        task.setDoUpload(doUpload);

        control.run(task.dump());

        completeBulkImportSession(client, task.getSessionName(), 0);  // TODO perform job priority

        return Exec.newConfigDiff();
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<CommitReport> successCommitReports)
    {
        // TODO delete last_session
    }

    private TDApiClient newTDApiClient(final PluginTask task)
    {
        TDApiClientOptions config = new TDApiClientOptions(task.getEndpoint(), task.getUseSsl());
        TDApiClient client = new TDApiClient(task.getApiKey(), config);
        try {
            client.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return client;
    }

    private void createDatabaseIfNotExists(TDApiClient client, String databaseName)
    {
        if (checkDatabaseExists(client, databaseName)) {
            log.debug("Database '{}' exists", databaseName);
            return;
        }

        log.info("Creating database '{}'", databaseName);
        try {
            client.createDatabase(databaseName);
        } catch (TDApiConflictException e) {
            // ignorable error
        }
    }

    private void createTableIfNotExists(TDApiClient client, String databaseName, String tableName)
    {
        if (checkTableExists(client, databaseName, tableName)) {
            log.debug("Table {}.{} exists", databaseName, tableName);
            return;
        }

        log.info("Create table {}.{} because it doesn't exist", databaseName, tableName);
        try {
            client.createTable(databaseName, tableName);
        } catch (TDApiConflictException e) {
            // ignore
        }
    }

    private boolean checkDatabaseExists(TDApiClient client, String name)
    {
        for (TDDatabase db : client.getDatabases()) {
            if (db.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkTableExists(TDApiClient client, String databaseName, String tableName)
    {
        for (TDTable table : client.getTables(databaseName)) {
            if (table.getName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private String buildBulkImportSessionName(PluginTask task, ExecSession exec)
    {
        if (task.getSession().isPresent()) {
            return task.getSession().get();
        } else {
            // TODO implement Exec.getTransactionUniqueName()
            Timestamp time = exec.getTransactionTime();
            return DateTimeFormat.forPattern("embulk_yyyyMMdd_HHmmss_").withZoneUTC()
                .print(time.getEpochSecond() * 1000)
                + String.format("%09d", time.getNano());
        }
    }

    // return false if all files are already uploaded
    private boolean startBulkImportSession(TDApiClient client,
            String sessionName, String databaseName, String tableName)
    {
        TDBulkImportSession session;
        try {
            client.createBulkImportSession(sessionName, databaseName, tableName);
        } catch (TDApiConflictException ex) {
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

    private void completeBulkImportSession(TDApiClient client, String sessionName, int priority)
    {
        TDBulkImportSession session = client.getBulkImportSession(sessionName);

        switch (session.getStatus()) {
        case UPLOADING:
            if (!session.getUploadFrozen()) {
                // freeze
                try {
                    client.freezeBulkImportSession(sessionName);
                } catch (TDApiConflictException e) {
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
            log.info("    job id: ", session.getJobId());

            // pass
        case READY:
            // TODO add an option to make the transaction failed if error_records or error_parts is too large
            // commit
            log.info("Committing bulk import session '{}'", sessionName);
            log.info("    valid records: ", session.getValidRecords());
            log.info("    error records: ", session.getErrorRecords());
            log.info("    valid parts: ", session.getValidParts());
            log.info("    error parts: ", session.getErrorParts());
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

    private TDBulkImportSession waitForStatusChange(TDApiClient client, String sessionName,
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

    public TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, int processorIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final TDApiClient client = newTDApiClient(task);

        MessagePackPageOutput pageOutput = createMessagePackPageOutput(task, schema, client);
        try {
            pageOutput.open(schema);
            return pageOutput;

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private MessagePackPageOutput createMessagePackPageOutput(final PluginTask task, final Schema schema, final TDApiClient client)
    {
        MessagePackRecordOutput recordOutput = new MessagePackRecordOutput(task, schema);
        MessagePackPageOutput pageOutput = new MessagePackPageOutput(task, client, recordOutput);
        return pageOutput;
    }
}
