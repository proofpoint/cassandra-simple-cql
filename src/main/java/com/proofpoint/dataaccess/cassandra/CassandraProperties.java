package com.proofpoint.dataaccess.cassandra;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CassandraProperties
{
    private String name = "name";
    private String hosts;
    private int port = 9042;
    private String clusterUsername;
    private String clusterPassword;
    private String datacenter;
    private boolean useCassandraSSL = false;
    private Duration readTimeout = Duration.succinctDuration(180, TimeUnit.SECONDS);
    private Duration connectTimeout = Duration.succinctDuration(180, TimeUnit.SECONDS);
    private Duration connectRetryPeriod = Duration.succinctDuration(1, TimeUnit.MINUTES);
    private String truststoreKey;
    private String truststorePath;
    private int maxRequestsPerConnectionLocal = 10000;
    private int maxRequestsPerConnectionRemote = 1000;

    // TODO: this should be automatically derived from @Named annotation
    public CassandraProperties named(String name)
    {
        this.name = name;
        return this;
    }

    @Config("cassandra-hosts")
    public void setHosts(String hosts)
    {
        this.hosts = hosts;
    }

    @Config("cassandra-port")
    public void setPort(int port)
    {
        this.port = port;
    }

    @Config("cassandra-username")
    public void setClusterUsername(String clusterUsername)
    {
        this.clusterUsername = clusterUsername;
    }

    @Config("cassandra-password")
    public void setClusterPassword(String clusterPassword)
    {
        this.clusterPassword = clusterPassword;
    }

    @Config("local-datacenter")
    public void setLocalDatacenter(String datacenter)
    {
        this.datacenter = datacenter;
    }

    @Config("cassandra-use-ssl")
    public void setUseCassandraSSL(boolean useCassandraSSL)
    {
        this.useCassandraSSL = useCassandraSSL;
    }

    @Config("ssl.truststore.key")
    public void setTruststoreKey(String key)
    {
        this.truststoreKey = key;
    }

    @Config("ssl.truststore.path")
    public void setTruststorePath(String path)
    {
        this.truststorePath = path;
    }

    //    @MinDuration("1s")
//    @MaxDuration("15m")
    @Config("cassandra-read-timeout")
    public void setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
    }

    //    @MinDuration("1s")
//    @MaxDuration("15m")
    @Config("cassandra-connect-timeout")
    public void setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
    }

    //    @MinDuration("1s")
//    @MaxDuration("15m")
    @Config("cassandra-connect-retry")
    public void setConnectRetryPeriod(Duration connectRetryPeriod)
    {
        this.connectRetryPeriod = connectRetryPeriod;
    }

    public int getMaxRequestsPerConnectionLocal()
    {
        return maxRequestsPerConnectionLocal;
    }

    @Config("cassandra-max-concurrent-requests-local")
    public void setMaxRequestsPerConnectionLocal(int maxRequestsPerConnectionLocal)
    {
        this.maxRequestsPerConnectionLocal = maxRequestsPerConnectionLocal;
    }

    public int getMaxRequestsPerConnectionRemote()
    {
        return maxRequestsPerConnectionRemote;
    }

    @Config("cassandra-max-concurrent-requests-remote")
    public void setMaxRequestsPerConnectionRemote(int maxRequestsPerConnectionRemote)
    {
        this.maxRequestsPerConnectionRemote = maxRequestsPerConnectionRemote;
    }


    public CassandraProperties()
    {

    }

    public CassandraProperties(
            @JsonProperty("local-datacenter") String datacenter,
            @JsonProperty("cassandra-port") int port,
            @JsonProperty("cassandra-hosts") String hosts,
            @JsonProperty("cassandra-username") String clusterUsername,
            @JsonProperty("cassandra-password") String clusterPassword,
            @JsonProperty("cassandra-use-ssl") boolean useCassandraSSL,
            @JsonProperty("cassandra-read-timeout") Duration readTimeout,
            @JsonProperty("cassandra-connect-timeout") Duration connectTimeout,
            @JsonProperty("cassandra-connect-retry") Duration connectRetryPeriod
    )
    {
        this.port = port;
        this.hosts = hosts;
        this.clusterUsername = clusterUsername;
        this.clusterPassword = clusterPassword;
        this.datacenter = datacenter;
        this.useCassandraSSL = useCassandraSSL;
        if (readTimeout != null) {
            this.readTimeout = readTimeout;
        }
        if (connectTimeout != null) {
            this.connectTimeout = connectTimeout;
        }
        if (connectRetryPeriod != null) {
            this.connectRetryPeriod = connectRetryPeriod;
        }
    }

    @NotNull
    public int getPort()
    {
        return port;
    }

    @NotNull
    @Min(1)
    public String getName()
    {
        return name;
    }

    @NotNull
    @Size(min = 1)
    public String getHosts()
    {
        return hosts;
    }

    public String[] getHostsAsArray()
    {
        return hosts.split("\\s*,\\s*");
    }

    @Nullable
    public String getClusterUsername()
    {
        return clusterUsername;
    }

    @Nullable
    public String getClusterPassword()
    {
        return clusterPassword;
    }

    @NotNull
    public String getLocalDatacenter()
    {
        return datacenter;
    }

    public boolean getUseCassandraSSL()
    {
        return useCassandraSSL;
    }

    public String getTruststoreKey()
    {
        return truststoreKey;
    }

    public String getTruststorePath()
    {
        return truststorePath;
    }

    @NotNull
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @NotNull
    public Duration getConnectRetryPeriod()
    {
        return connectRetryPeriod;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraProperties that = (CassandraProperties) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(hosts, that.hosts) &&
                Objects.equals(clusterUsername, that.clusterUsername) &&
                Objects.equals(clusterPassword, that.clusterPassword) &&
                Objects.equals(datacenter, that.datacenter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, hosts, clusterUsername, clusterPassword, datacenter);
    }


}

