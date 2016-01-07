package com.treasuredata.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.api.model.TDBulkImportSession;
import com.treasuredata.api.model.TDColumn;
import com.treasuredata.api.model.TDDatabase;
import com.treasuredata.api.model.TDDatabaseList;
import com.treasuredata.api.model.TDTable;
import com.treasuredata.api.model.TDTableList;
import com.treasuredata.api.model.TDTableType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TdApiClient
        implements Closeable
{
    private final String apiKey;
    private final TdApiClientConfig config;
    private final HttpClient http;
    private final ObjectMapper objectMapper;

    public TdApiClient(String apiKey, TdApiClientConfig config)
    {
        this.apiKey = apiKey;
        this.config = config;

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SslContextFactory sslContextFactory = new SslContextFactory();
        http = new HttpClient(sslContextFactory);
        http.setConnectTimeout(10 * 1000); //  TODO get from options
        http.setIdleTimeout(60 * 1000); //  TODO get from options
        http.setMaxConnectionsPerDestination(10);  //  TODO get from options
        http.setCookieStore(new HttpCookieStore.Empty());

        // ProxyConfiguration
        if (config.getHttpProxyConfig().isPresent()) {
            TdApiClientConfig.HttpProxyConfig httpProxyConfig = config.getHttpProxyConfig().get();
            String host = httpProxyConfig.getHost();
            int port = httpProxyConfig.getPort();
            boolean isSecure = httpProxyConfig.isSecure();

            ProxyConfiguration proxyConfig = http.getProxyConfiguration();
            proxyConfig.getProxies().add(new HttpProxy(new Origin.Address(host, port), isSecure));
        }
    }

    private TdApiClient(TdApiClient copy, String apiKey)
    {
        this.apiKey = apiKey;
        this.config = copy.config;
        this.http = copy.http;
        this.objectMapper = copy.objectMapper;
    }

    public TdApiClient withApikey(String apikey)
    {
        return new TdApiClient(this, apikey);
    }

    @PostConstruct
    public void start() throws IOException
    {
        try {
            http.start();
        }
        catch (Exception e) {
            throw new IOException("Failed to start http client", e);
        }
    }

    @PreDestroy
    public void close()
    {
        try {
            http.stop();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop http client", e);
        }
    }

    public List<TDDatabase> getDatabases()
    {
        Request request = prepareExchange(HttpMethod.GET,
                buildUrl("/v3/database/list"));
        ContentResponse response = executeExchange(request);
        TDDatabaseList databaseList = parseResponse(response.getContent(), TDDatabaseList.class);
        return databaseList.getDatabases();
    }

    public TDDatabase createDatabase(String databaseName)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/database/create", databaseName));
        ContentResponse response = executeExchange(request);
        TDDatabase database = parseResponse(response.getContent(), TDDatabase.class);
        return database;
    }

    public List<TDTable> getTables(String databaseName)
    {
        Request request = prepareExchange(HttpMethod.GET,
                buildUrl("/v3/table/list/", databaseName));
        ContentResponse response = executeExchange(request);
        TDTableList tables = parseResponse(response.getContent(), TDTableList.class);
        return tables.getTables();
    }

    public TDTable createTable(String databaseName, String tableName)
    {
        return createTable(databaseName, tableName, TDTableType.LOG);
    }

    public TDTable createTable(String databaseName, String tableName, TDTableType tableType)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/table/create", databaseName, tableName, tableType.name().toLowerCase()));
        ContentResponse response = executeExchange(request);
        TDTable table = parseResponse(response.getContent(), TDTable.class);
        return table;
    }

    public void deleteTable(String databaseName, String tableName)
            throws IOException
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/table/delete", databaseName, tableName));
        ContentResponse response = executeExchange(request);
    }

    public void renameTable(String databaseName, String oldName, String newName, boolean overwrite)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/table/rename", databaseName, oldName, newName),
                ImmutableMap.<String, String>of(),
                ImmutableMap.of("overwrite", Boolean.toString(overwrite)));
        ContentResponse response = executeExchange(request);
    }

    public void updateSchema(String databaseName, String tableName, List<TDColumn> newSchema)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/table/update-schema", databaseName, tableName),
                ImmutableMap.<String, String>of(),
                ImmutableMap.of("schema", formatRequestParameterObject(newSchema)));
        ContentResponse response = executeExchange(request);
    }

    public void createBulkImportSession(String sessionName, String databaseName, String tableName)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/bulk_import/create", sessionName, databaseName, tableName));
        ContentResponse response = executeExchange(request);
        // TODO return TDBulkImportSession
    }

    public TDBulkImportSession getBulkImportSession(String sessionName)
    {
        Request request = prepareExchange(HttpMethod.GET,
                buildUrl("/v3/bulk_import/show", sessionName));
        ContentResponse response = executeExchange(request);
        TDBulkImportSession session = parseResponse(response.getContent(), TDBulkImportSession.class);
        return session;
    }

    @Deprecated
    public void uploadBulkImport(String sessionName, File path)
            throws IOException
    {
        String name = path.getName().replace(".", "_");
        Request request = prepareExchange(HttpMethod.PUT,
                buildUrl("/v3/bulk_import/upload_part", sessionName, name));
        request.file(path.toPath());
        ContentResponse response = executeExchange(request);
    }

    public void uploadBulkImportPart(String sessionName, String uniquePartName, File path)
            throws IOException
    {
        Request request = prepareExchange(HttpMethod.PUT,
                buildUrl("/v3/bulk_import/upload_part", sessionName, uniquePartName));
        request.file(path.toPath());
        ContentResponse response = executeExchange(request);
    }

    public void freezeBulkImportSession(String sessionName)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/bulk_import/freeze", sessionName));
        ContentResponse response = executeExchange(request);
    }

    public void performBulkImportSession(String sessionName, int priority)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/bulk_import/perform", sessionName),
                ImmutableMap.<String, String>of(),
                ImmutableMap.of("priority", String.valueOf(priority)));
        ContentResponse response = executeExchange(request);
    }

    public void commitBulkImportSession(String sessionName)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/bulk_import/commit", sessionName));
        ContentResponse response = executeExchange(request);
    }

    public void deleteBulkImportSession(String sessionName)
    {
        Request request = prepareExchange(HttpMethod.POST,
                buildUrl("/v3/bulk_import/delete", sessionName));
        ContentResponse response = executeExchange(request);
    }

    public InputStream getBulkImportErrorRecords(String sessionName)
    {
        // TODO use td-client-java v0.7

        Request request = prepareExchange(HttpMethod.GET,
                buildUrl("/v3/bulk_import/error_records", sessionName));
        InputStreamResponseListener listener = new InputStreamResponseListener();
        request.send(listener);
        try {
            listener.get(60000, TimeUnit.MILLISECONDS); // 60 sec.
            return listener.getInputStream();
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private Request prepareExchange(HttpMethod method, String url)
    {
        return prepareExchange(method, url, Collections.<String, String>emptyMap(),
                Collections.<String, String>emptyMap());
    }

    private Request prepareExchange(HttpMethod method,
                                    String url, Map<String, String> headers,
                                    Map<String, String> query)
    {
        String queryString = null;
        if (!query.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : query.entrySet()) {
                sb.append(encode(entry.getKey())).append("=").append(encode(entry.getValue()));
            }
            queryString = sb.toString();
        }

        if (method == HttpMethod.GET && queryString != null) {
            url = url + "?" + queryString;
        }
        Request request = http.newRequest(url);
        request.method(method);
        request.agent(config.getAgentName());
        request.header("Authorization", "TD1 " + apiKey);
        //request.timeout(60, TimeUnit.SECONDS);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.header(entry.getKey(), entry.getValue());
        }
        String dateHeader = setDateHeader(request);
        if (method != HttpMethod.GET && queryString != null) {
            request.content(new StringContentProvider(queryString),
                    "application/x-www-form-urlencoded");
        }

        return request;
    }

    private static String encode(String s)
    {
        try {
            return URLEncoder.encode(s, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private static final ThreadLocal<SimpleDateFormat> RFC2822_FORMAT =
        new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue()
            {
                return new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH);
            }
        };

    private static String setDateHeader(Request request)
    {
        Date currentDate = new Date();
        String dateHeader = RFC2822_FORMAT.get().format(currentDate);
        request.header("Date", dateHeader);
        return dateHeader;
    }

    private static final ThreadLocal<MessageDigest> SHA1 =
        new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue()
            {
                try {
                    return MessageDigest.getInstance("SHA-1");
                }
                catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("SHA-1 digest algorithm must be available but not found", e);
                }
            }
        };

    private static final char[] hexChars = new char[16];
    static {
        for (int i = 0; i < 16; i++) {
            hexChars[i] = Integer.toHexString(i).charAt(0);
        }
    }

    @VisibleForTesting
    static String sha1HexFromString(String string)
    {
        MessageDigest sha1 = SHA1.get();
        sha1.reset();
        sha1.update(string.getBytes());
        byte[] bytes = sha1.digest();

        // convert binary to hex string
        char[] array = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = (int) bytes[i];
            array[i * 2] = hexChars[(b & 0xf0) >> 4];
            array[i * 2 + 1] = hexChars[b & 0x0f];
        }
        return new String(array);
    }

    private String buildUrl(String path, String... params)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(config.getUseSsl() ? "https://" : "http://");
        sb.append(config.getEndpoint());
        sb.append(path);
        try {
            for (String param : params) {
                sb.append("/");
                sb.append(URLEncoder.encode(param, "UTF-8"));
            }
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
        return sb.toString();
    }

    private ContentResponse executeExchange(Request request)
    {
        int retryLimit = 10;
        int retryWait = 1000;

        Exception firstException = null;

        try {
            while (true) {
                Exception exception;

                try {
                    ContentResponse response = request.send();

                    int status = response.getStatus();
                    switch(status) {
                    case 200:
                        return response;
                    case 404:
                        throw new TdApiNotFoundException(status, response.getContent());
                    case 409:
                        throw new TdApiConflictException(status, response.getContent());
                    }

                    if (status / 100 != 5) {  // not 50x
                        throw new TdApiResponseException(status, response.getContent());
                    }

                    // retry on 50x and other errors
                    exception = new TdApiResponseException(status, response.getContent());

                }
                catch (TdApiException e) {
                    throw e;

                }
                catch (Exception e) {
                    // retry on RuntimeException
                    exception = e;
                }

                if (firstException == null) {
                    firstException = exception;
                }

                if (retryLimit <= 0) {
                    if (firstException instanceof TdApiException) {
                        throw (TdApiException) firstException;
                    }
                    throw new TdApiExecutionException(firstException);
                }

                retryLimit -= 1;
                Thread.sleep(retryWait);
                retryWait *= 2;
            }
        }
        catch (InterruptedException e) {
            throw new TdApiExecutionInterruptedException(e);
        }
    }

    private String formatRequestParameterObject(Object obj)
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        try {
            objectMapper.writeValue(bo, obj);
            return new String(bo.toByteArray(), "UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            try {
                bo.close();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private byte[] formatPostRequestContent(String... kvs)
    {
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < kvs.length; i += 2) {
                if (i > 0) {
                    sb.append("&");
                }
                sb.append(encode(kvs[i]))
                  .append("=")
                  .append(encode(kvs[i + 1]));
            }
            return sb.toString().getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
    }

    private <T> T parseResponse(byte[] content, Class<T> valueType)
    {
        try {
            return objectMapper.readValue(content, valueType);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
