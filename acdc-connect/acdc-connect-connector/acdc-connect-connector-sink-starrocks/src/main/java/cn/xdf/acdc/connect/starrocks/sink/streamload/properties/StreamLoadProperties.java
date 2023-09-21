package cn.xdf.acdc.connect.starrocks.sink.streamload.properties;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.http.protocol.HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE;

public class StreamLoadProperties implements Serializable {
    private final String[] loadUrls;
    private final String username;
    @JSONField(serialize = false)
    private final String password;
    //private final String version;
    // can be null
    //private final StarRocksVersion starRocksVersion;
    
    private final String labelPrefix;
    
    private final StreamLoadTableProperties defaultTableProperties;
    private final Map<String, StreamLoadTableProperties> tablePropertiesMap;
    
    
    // manager settings
    /**
     * ms
     * manager线程扫描频率
     */
    private final long scanningFrequency;
    /**
     * 最大缓存空间
     */
    private final long maxCacheBytes;
    /**
     * ms
     * 期望的单表延时时长
     */
    private final long expectDelayTime;
    
    // http client settings
    /**
     * ms
     */
    private final int connectTimeout;
    private final int waitForContinueTimeoutMs;
    private final int ioThreadCount;
    
    // default strategy settings
    /**
     * ms
     * 多少时间范围内被视为一直写
     */
    private final long writingThreshold;
    /**
     * 当region占比高于多少时，触发flush
     */
    private final float regionBufferRatio;
    private final float youngThreshold;
    private final float oldThreshold;
    
    private final Map<String, String> headers;
    
    private StreamLoadProperties(Builder builder) {
        this.loadUrls = builder.loadUrls;
        this.username = builder.username;
        this.password = builder.password;
        
        this.labelPrefix = builder.labelPrefix;
        
        this.defaultTableProperties = builder.defaultTableProperties;
        this.tablePropertiesMap = builder.tablePropertiesMap;
        
        this.scanningFrequency = builder.scanningFrequency;
        this.maxCacheBytes = builder.maxCacheBytes;
        this.expectDelayTime = builder.expectDelayTime;
        
        this.connectTimeout = builder.connectTimeout;
        this.waitForContinueTimeoutMs = builder.waitForContinueTimeoutMs;
        this.ioThreadCount = builder.ioThreadCount;
        
        this.writingThreshold = builder.writingThreshold;
        this.regionBufferRatio = builder.regionBufferRatio;
        this.youngThreshold = builder.youngThreshold;
        this.oldThreshold = builder.oldThreshold;
        
        this.headers = Collections.unmodifiableMap(builder.headers);
    }
    
    
    public String[] getLoadUrls() {
        return loadUrls;
    }
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }

   /* public String getVersion() {
        return version;
    }*/
    
    public String getLabelPrefix() {
        return labelPrefix;
    }
    
    public StreamLoadTableProperties getTableProperties(String uniqueKey) {
        return tablePropertiesMap.getOrDefault(uniqueKey, defaultTableProperties);
    }
    
    
    public long getScanningFrequency() {
        return scanningFrequency;
    }
    
    public long getMaxCacheBytes() {
        return maxCacheBytes;
    }
    
    public long getExpectDelayTime() {
        return expectDelayTime;
    }
    
    public int getConnectTimeout() {
        return connectTimeout;
    }
    
    public int getWaitForContinueTimeoutMs() {
        return waitForContinueTimeoutMs;
    }
    
    
    public int getIoThreadCount() {
        return ioThreadCount;
    }
    
    public long getWritingThreshold() {
        return writingThreshold;
    }
    
    public float getRegionBufferRatio() {
        return regionBufferRatio;
    }
    
    public float getYoungThreshold() {
        return youngThreshold;
    }
    
    public float getOldThreshold() {
        return oldThreshold;
    }
    
    public Map<String, String> getHeaders() {
        return headers;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String jdbcUrl;
        private String[] loadUrls;
        private String username;
        private String password;
        //private String version;
        
        private boolean enableTransaction;
        
        private String labelPrefix = "";
        
        private long scanningFrequency = 50L;
        private long maxCacheBytes = (long) (Runtime.getRuntime().freeMemory() * 0.7);
        private long expectDelayTime = 300000L;
        
        private StreamLoadTableProperties defaultTableProperties;
        private Map<String, StreamLoadTableProperties> tablePropertiesMap = new HashMap<>();
        
        private int connectTimeout = 60000;
        private int waitForContinueTimeoutMs = DEFAULT_WAIT_FOR_CONTINUE;
        private int ioThreadCount = Runtime.getRuntime().availableProcessors();
        
        private long writingThreshold = 50L;
        private float regionBufferRatio = 0.6F;
        private float youngThreshold = 0.1F;
        private float oldThreshold = 0.9F;
        
        private Map<String, String> headers = new HashMap<>();
        
        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }
        
        public Builder loadUrls(String... loadUrls) {
            this.loadUrls = Arrays.stream(loadUrls)
                    .filter(Objects::nonNull)
                    .map(url -> {
                        if (!url.startsWith("http")) {
                            return "http://" + url;
                        }
                        return url;
                    })
                    .toArray(String[]::new);
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder labelPrefix(String labelPrefix) {
            this.labelPrefix = labelPrefix;
            return this;
        }
        
        public Builder defaultTableProperties(StreamLoadTableProperties tableProperties) {
            this.defaultTableProperties = tableProperties;
            return this;
        }
        
        public Builder addTableProperties(StreamLoadTableProperties tableProperties) {
            if (defaultTableProperties == null) {
                defaultTableProperties = tableProperties;
            }
            tablePropertiesMap.put(tableProperties.getUniqueKey(), tableProperties);
            return this;
        }
        
        public Builder scanningFrequency(long scanningFrequency) {
            if (scanningFrequency < 50) {
                throw new IllegalArgumentException("scanningFrequency `" + scanningFrequency + "ms` set failed, must greater or equals to 50");
            }
            this.scanningFrequency = scanningFrequency;
            return this;
        }
        
        public Builder cacheMaxBytes(long maxCacheBytes) {
            if (maxCacheBytes <= 0) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, must greater to 0");
            }
            if (maxCacheBytes > Runtime.getRuntime().maxMemory()) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, current maxMemory is " + Runtime.getRuntime().maxMemory());
            }
            this.maxCacheBytes = maxCacheBytes;
            return this;
        }
        
        public Builder expectDelayTime(long expectDelayTime) {
            if (expectDelayTime <= 0) {
                throw new IllegalArgumentException("expectDelayTime `" + expectDelayTime + "ms` set failed, must greater to 0");
            }
            this.expectDelayTime = expectDelayTime;
            return this;
        }
        
        public Builder connectTimeout(int connectTimeout) {
            if (connectTimeout < 100 || connectTimeout > 60000) {
                throw new IllegalArgumentException("connectTimeout `" + connectTimeout + "ms` set failed, must range in [100, 60000]");
            }
            this.connectTimeout = connectTimeout;
            return this;
        }
        
        public Builder waitForContinueTimeoutMs(int waitForContinueTimeoutMs) {
            if (waitForContinueTimeoutMs < DEFAULT_WAIT_FOR_CONTINUE || waitForContinueTimeoutMs > 60000) {
                throw new IllegalArgumentException("waitForContinueTimeoutMs `" + waitForContinueTimeoutMs +
                        "ms` set failed, must be in range in [100, 60000]");
            }
            this.waitForContinueTimeoutMs = waitForContinueTimeoutMs;
            return this;
        }
        
        
        public Builder ioThreadCount(int ioThreadCount) {
            if (ioThreadCount <= 0) {
                throw new IllegalArgumentException("ioThreadCount `" + ioThreadCount + "` set failed, must greater to 0");
            }
            this.ioThreadCount = ioThreadCount;
            return this;
        }
        
        
        public Builder addHeaders(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }
        
        public StreamLoadProperties build() {
            StreamLoadProperties streamLoadProperties = new StreamLoadProperties(this);
            
            if (streamLoadProperties.getYoungThreshold() >= streamLoadProperties.getOldThreshold()) {
                throw new IllegalArgumentException(String.format("oldThreshold(`%s`) must greater to youngThreshold(`%s`)",
                        streamLoadProperties.getOldThreshold(), streamLoadProperties.getYoungThreshold()));
            }
            
            if (streamLoadProperties.getExpectDelayTime() < streamLoadProperties.getScanningFrequency()) {
                throw new IllegalArgumentException(String.format("expectDelayTime(`%s`) must greater to scanningFrequency(`%s`)",
                        streamLoadProperties.getExpectDelayTime(), streamLoadProperties.getScanningFrequency()));
            }
            return streamLoadProperties;
        }
        
    }
}
