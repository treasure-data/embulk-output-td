package com.treasuredata.api;

public class TDApiClientConfig
        implements TDApiConstants
{
    private String endpoint;
    //  TODO https settings

    public TDApiClientConfig(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public String getAgentName() {
        return AGENT_NAME;
    }
}
