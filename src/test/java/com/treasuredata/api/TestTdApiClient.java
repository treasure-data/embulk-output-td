package com.treasuredata.api;

import com.treasuredata.api.model.TDDatabase;
import org.bigtesting.fixd.ServerFixture;
import org.bigtesting.fixd.core.Method;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTdApiClient
{
    private ServerFixture server;
    private TdApiClient client;
    private TdApiClientConfig clientConfig;
    private String apikey = "apikey";

    @Before
    public void startServer()
            throws Exception
    {
        server = new ServerFixture(9490);
        server.start();
    }

    @After
    public void stopServer()
            throws Exception
    {
        server.stop();
    }

    @Before
    public void startTdApiClient()
            throws Exception
    {
        clientConfig = new TdApiClientConfig("localhost:9490", false);
        client = new TdApiClient(apikey, clientConfig);
        client.start();
    }

    @After
    public void stopTdApiClient()
            throws Exception
    {
        client.close();;
    }

    private static final String DATABASE_LIST_JSON =
    "{" +
        "\"databases\":[" +
            "{\"name\":\"test1\"}," +
            "{\"name\":\"test2\"}" +
        "]" +
    "}";

    @Test
    public void getDatabases() throws Exception
    {
        server.handle(Method.GET, "/v3/database/list").with(200, "text/json", DATABASE_LIST_JSON);
        List<TDDatabase> dbs = client.getDatabases();
        assertEquals(2, dbs.size());
        assertEquals("test1", dbs.get(0).getName());
        assertEquals("test2", dbs.get(1).getName());
    }

    @Test(expected = TdApiNotFoundException.class)
    public void notFoundDatabases()
            throws Exception
    {
        server.handle(Method.GET, "/v3/database/list").with(404, "text/json", "{\"message\":\"not found\"}");
        client.getDatabases();
        fail();
    }
}
