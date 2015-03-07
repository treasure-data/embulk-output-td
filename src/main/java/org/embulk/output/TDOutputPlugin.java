package org.embulk.output;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.treasuredata.api.TDApiClient;
import com.treasuredata.api.TDApiClientConfig;
import com.treasuredata.api.TDApiConflictException;
import com.treasuredata.api.model.TDBulkImportSession;
import com.treasuredata.api.model.TDBulkImportSession.ImportStatus;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
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

        @Config("session")
        @ConfigDefault("null")
        public Optional<String> getSession();
        public void setSession(String session);

        //  TODO auto_create_table

        @Config("database")
        public String getDatabase();

        @Config("table")
        public String getTable();

        //  TODO use_ssl
        //  TODO http_proxy
        //  TODO connect_timeout, read_timeout, send_timeout

        @Config("tmpdir")
        @ConfigDefault("/tmp")
        public String getTempDir();
    }

    private final Injector injector;
    private final Logger log;

    @Inject
    public TDOutputPlugin(Injector injector)
    {
        this.injector = injector;
        this.log = Exec.getLogger(getClass());
    }

    public ConfigDiff transaction(final ConfigSource config, final Schema schema, int processorCount,
                                  OutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
        final TDApiClient client = createTDApiClient(task);
        try {

            //  TODO check connectivity
            //  TODO check if the database exists
            //  TODO check if the table exists

            //  check MessagePackRecordOutput configuration before transaction is started
            createMessagePackPageOutput(task, schema, client);

            //  TODO should change the behavior of the method with 'getOrCreateBulkImportSession'
            final String sessionName = newBulkImportSession(task, client);
            task.setSession(sessionName);
            //  TODO check the status of the session

            //  TODO retryable (idempotent) output:
            //  TODO return resume(task.dump(), schema, processorCount, control);

            control.run(task.dump()); //  TODO upload part files

            commitBulkImportSession(task, client, sessionName);

        } finally {
            client.close();
        }

        ConfigDiff configDiff = Exec.newConfigDiff();
        return configDiff;
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("td output plugin does not support the resume feature yet.");
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<CommitReport> successCommitReports)
    {
        //  TODO
    }

    private TDApiClient createTDApiClient(final PluginTask task)
    {
        TDApiClientConfig config = new TDApiClientConfig(task.getEndpoint());
        TDApiClient client = new TDApiClient(config);
        try {
            client.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return client;
    }

    private String newBulkImportSession(final PluginTask task, final TDApiClient client)
    {
        final String apikey = task.getApiKey();
        final String databaseName = task.getDatabase();
        final String tableName = task.getTable();
        final Optional<String> sessionName = task.getSession();
        try {
            if (sessionName.isPresent()) {
                client.createBulkImportSession(apikey, sessionName.get(), databaseName, tableName);
                return sessionName.get();
            } else {
                return client.createBulkImportSession(apikey, databaseName, tableName);
            }

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void commitBulkImportSession(final PluginTask task, final TDApiClient client, final String sessionName)
    {
        final String apikey = task.getApiKey();

        TDBulkImportSession importSession;

        try {
            client.freezeBulkImportSession(apikey, sessionName);
        } catch (TDApiConflictException e) {
            // ignore
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        try {
            client.performBulkImportSession(apikey, sessionName, 0); // TODO: priority
        } catch (IOException e) {
            // check performing status anyway
        }
        importSession = waitCondition(client, apikey, sessionName, ImportStatus.READY, ImportStatus.PERFORMING);
        if (importSession.isPeformError()) {
            throw new RuntimeException("Data Import failed : " + importSession.getErrorMessage());
        }

        //  TODO check error_records

        try {
            client.commitBulkImportSession(apikey, sessionName);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        waitCondition(client, apikey, sessionName, ImportStatus.COMMITTED, ImportStatus.COMMITTING);

        //  should not delete session
    }

    private TDBulkImportSession waitCondition(final TDApiClient client, final String apikey, final String sessionName,
                                              final ImportStatus expecting, final ImportStatus current)
    {
        TDBulkImportSession importSession = null;
        try {
            importSession = client.getBulkImportSession(apikey, sessionName);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        while (true) {
            if (importSession.is(expecting)) {
                break;
            }
            else if (importSession.is(current)) {
                // still working
            }
            else {
                throw new RuntimeException("Data Import failed : " + expecting);
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }

            try {
                importSession = client.getBulkImportSession(apikey, sessionName);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        return importSession;
    }

    public TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, int processorIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final TDApiClient client = createTDApiClient(task);

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
