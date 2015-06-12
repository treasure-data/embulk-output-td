package com.treasuredata.api;

public class TdApiClientConfig
        implements TdApiConstants
{
    private String endpoint;
    private boolean useSsl;

    // TODO Builder
    // TODO clone

    public TdApiClientConfig(String endpoint, boolean useSsl)
    {
        this.endpoint = endpoint;
        this.useSsl = useSsl;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public boolean getUseSsl()
    {
        return useSsl;
    }

    public String getAgentName() {
        return AGENT_NAME;
    }
}
