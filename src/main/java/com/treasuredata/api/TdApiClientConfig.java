package com.treasuredata.api;

import com.google.common.base.Optional;

public class TdApiClientConfig
        extends TdApiConstants
{
    public static class HttpProxyConfig
    {
        private String host;
        private int port;
        private boolean secure;

        public HttpProxyConfig(String host, int port, boolean secure)
        {
            this.host = host;
            this.port = port;
            this.secure = secure;
        }

        public String getHost()
        {
            return host;
        }

        public int getPort()
        {
            return port;
        }

        public boolean isSecure()
        {
            return secure;
        }
    }

    private String endpoint;
    private boolean useSsl;
    private Optional<HttpProxyConfig> httpProxyConfig;

    // TODO Builder
    // TODO clone

    public TdApiClientConfig(String endpoint, boolean useSsl)
    {
        this(endpoint, useSsl, Optional.<HttpProxyConfig>absent());
    }

    public TdApiClientConfig(String endpoint, boolean useSsl, Optional<HttpProxyConfig> httpProxyConfig)
    {
        this.endpoint = endpoint;
        this.useSsl = useSsl;
        this.httpProxyConfig = httpProxyConfig;
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

    public Optional<HttpProxyConfig> getHttpProxyConfig()
    {
        return httpProxyConfig;
    }

    public String getAgentName()
    {
        return AGENT_NAME;
    }
}
