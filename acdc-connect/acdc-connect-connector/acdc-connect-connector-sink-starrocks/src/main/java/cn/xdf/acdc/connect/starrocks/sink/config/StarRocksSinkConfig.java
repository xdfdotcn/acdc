/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xdf.acdc.connect.starrocks.sink.config;

import cn.xdf.acdc.connect.starrocks.sink.serialize.StarRocksDelimiterParser;
import cn.xdf.acdc.connect.starrocks.sink.streamload.StreamLoadDataFormat;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadProperties;
import cn.xdf.acdc.connect.starrocks.sink.streamload.properties.StreamLoadTableProperties;
import com.google.common.base.Joiner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.http.protocol.HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE;

public class StarRocksSinkConfig extends AbstractConfig {
    
    private static final String COMMA = ",";
    
    private static final long KILO_BYTES_SCALE = 1024L;
    
    // MB
    public static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    
    // GB
    public static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;
    
    // config group
    private static final String URL_GROUP = "Url";
    
    private static final String DATA_COLLECTION_GROUP = "DataCollection";
    
    private static final String AUTHORIZATION_GROUP = "Authorization";
    
    private static final String THRESHOLD_VALUE_GROUP = "ThresholdValue";
    
    private static final String PREFIX_GROUP = "Prefix";
    
    private static final String COLUMNS_GROUP = "Columns";
    
    private static final String HTTP_CLIENT_GROUP = "HttpClient";
    
    // jdbc url
    private static final String JDBC_URL = "jdbc.url";
    private static final String JDBC_URL_DEFAULT = "";
    
    private static final String JDBC_URL_DOC = "Url of the jdbc like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.";
    
    private static final String JDBC_URL_DISPLAY = "Url of the jdbc";
    
    // load url
    public static final String LOAD_URL = "load.url";
    
    private static final String LOAD_URL_DOC = "Url of the stream load, if you you don't specify the http/https prefix, the default http. like: `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`.";
    
    private static final String LOAD_URL_DISPLAY = "Url of the stream load";
    
    // db name
    private static final String DATABASE_NAME = "database.name";
    
    private static final String DATABASE_NAME_DOC = "Database name of the stream load.";
    
    private static final String DATABASE_NAME_DISPLAY = "Database name of the stream load";
    
    // table name
    public static final String TABLE_NAME = "table.name";
    
    private static final String TABLE_NAME_DOC = "Table name of the stream load.";
    
    private static final String TABLE_NAME_DISPLAY = "Table name of the stream load";
    
    // username
    private static final String USERNAME = "username";
    
    private static final String USERNAME_DOC = "StarRocks user name.";
    
    private static final String USERNAME_DISPLAY = "StarRocks user name";
    
    // password
    private static final String PASSWORD = "password";
    
    private static final String PASSWORD_DOC = "StarRocks user password.";
    
    private static final String PASSWORD_DISPLAY = "StarRocks user password";
    
    // label prefix
    private static final String SINK_LABEL_PREFIX = "sink.label.prefix";
    
    private static final String SINK_LABEL_PREFIX_DOC = "The prefix of the stream load label. Available values are within [-_A-Za-z0-9].";
    
    private static final String SINK_LABEL_PREFIX_DISPLAY = "The prefix of the stream load label";
    
    // http client connect timeout
    private static final String SINK_CONNECT_TIMEOUT = "sink.connect.timeout.ms";
    
    private static final String SINK_CONNECT_TIMEOUT_DOC = "Timeout in millisecond for connecting to the `load.url`.";
    
    private static final String SINK_CONNECT_TIMEOUT_DISPLAY = "Timeout in millisecond for connecting to the `load.url`";
    
    private static final int SINK_CONNECT_TIMEOUT_DEFAULT = 1000;
    
    // http client
    private static final String SINK_WAIT_FOR_CONTINUE_TIMEOUT = "sink.wait.for.continue.timeout.ms";
    
    private static final String SINK_WAIT_FOR_CONTINUE_TIMEOUT_DOC = "Timeout in millisecond for connecting to the `load.url`.";
    
    private static final String SINK_WAIT_FOR_CONTINUE_TIMEOUT_DISPLAY = "Timeout in millisecond for connecting to the `load.url`";
    
    private static final int SINK_WAIT_FOR_CONTINUE_TIMEOUT_DEFAULT = 30000;
    
    // io thread count
    private static final String SINK_IO_THREAD_COUNT = "sink.io.thread.count";
    
    private static final String SINK_IO_THREAD_COUNT_DOC = "Stream load thread count.";
    
    private static final String SINK_IO_THREAD_COUNT_DISPLAY = "Stream load thread count";
    
    private static final Integer SINK_IO_THREAD_COUNT_DEFAULT = 2;
    
    // http stream load,chunk limit
    private static final String SINK_CHUNK_LIMIT = "sink.chunk.limit";
    
    private static final String SINK_CHUNK_LIMIT_DOC = "Data chunk size in a http request for stream load.";
    
    private static final String SINK_CHUNK_LIMIT_DISPLAY = "Data chunk size in a http request for stream load";
    
    private static final Long SINK_CHUNK_LIMIT_DEFAULT = 3 * GIGA_BYTES_SCALE;
    
    // manager scan frequency
    private static final String SINK_SCAN_FREQUENCY = "sink.scan.frequency.ms";
    
    private static final String SINK_SCAN_FREQUENCY_DOC = "Scan frequency in milliseconds.";
    
    private static final String SINK_SCAN_FREQUENCY_DISPLAY = "Scan frequency in milliseconds";
    
    private static final Long SINK_SCAN_FREQUENCY_DEFAULT = 50L;
    
    // flush size
    private static final String SINK_BATCH_MAX_SIZE = "sink.buffer.flush.max.bytes";
    
    private static final String SINK_BATCH_MAX_SIZE_DOC = "Max data bytes of the flush.";
    
    private static final String SINK_BATCH_MAX_SIZE_DISPLAY = "Max data bytes of the flush";
    
    // 90MB
    private static final Long SINK_BATCH_MAX_SIZE_DEFAULT = 90L * MEGA_BYTES_SCALE;
    
    // commit interval
    private static final String SINK_BATCH_FLUSH_INTERVAL = "sink.buffer.flush.interval.ms";
    
    private static final String SINK_BATCH_FLUSH_INTERVAL_DOC = "Flush interval of the row batch in millisecond.";
    
    private static final String SINK_BATCH_FLUSH_INTERVAL_DISPLAY = "Flush interval of the row batch in millisecond";
    
    // 5 minutes
    private static final Long SINK_BATCH_FLUSH_INTERVAL_DEFAULT = 300000L;
    
    // columns
    private static final String SINK_COLUMNS = "sink.columns";
    
    private static final String SINK_COLUMNS_DOC = "Columns to be synchronized.";
    
    private static final String SINK_COLUMNS_DISPLAY = "Columns to be synchronized";
    
    private static final String SINK_CSV_ROW_SEPARATOR_DEFAULT = "\n";
    
    private static final String SINK_CSV_COLUMN_SEPARATOR_DEFAULT = "\t";
    
    // wild stream load properties' prefix
    private static final String SINK_PROPERTIES_PREFIX = "sink.properties.";
    
    private final Map<String, String> streamLoadProps = new HashMap<>();
    
    private final List<StreamLoadTableProperties> tablePropertiesList = new ArrayList<>();
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    JDBC_URL,
                    ConfigDef.Type.STRING,
                    JDBC_URL_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    JDBC_URL_DOC,
                    URL_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    JDBC_URL_DISPLAY
            )
            .define(
                    LOAD_URL,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new LoadUrlValidator(),
                    ConfigDef.Importance.HIGH,
                    LOAD_URL_DOC,
                    URL_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    LOAD_URL_DISPLAY
            )
            .define(
                    DATABASE_NAME,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DATABASE_NAME_DOC,
                    DATA_COLLECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    DATABASE_NAME_DISPLAY
            )
            .define(
                    TABLE_NAME,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    TABLE_NAME_DOC,
                    DATA_COLLECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    TABLE_NAME_DISPLAY
            )
            .define(
                    USERNAME,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    USERNAME_DOC,
                    AUTHORIZATION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    USERNAME_DISPLAY
            )
            .define(
                    PASSWORD,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    PASSWORD_DOC,
                    AUTHORIZATION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    PASSWORD_DISPLAY
            )
            .define(
                    SINK_LABEL_PREFIX,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    SINK_LABEL_PREFIX_DOC,
                    PREFIX_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    SINK_LABEL_PREFIX_DISPLAY
            )
            .define(
                    SINK_CONNECT_TIMEOUT,
                    ConfigDef.Type.INT,
                    SINK_CONNECT_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SINK_CONNECT_TIMEOUT_DOC,
                    HTTP_CLIENT_GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    SINK_CONNECT_TIMEOUT_DISPLAY
            )
            .define(
                    SINK_WAIT_FOR_CONTINUE_TIMEOUT,
                    ConfigDef.Type.INT,
                    SINK_WAIT_FOR_CONTINUE_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SINK_WAIT_FOR_CONTINUE_TIMEOUT_DOC,
                    HTTP_CLIENT_GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    SINK_WAIT_FOR_CONTINUE_TIMEOUT_DISPLAY
            )
            .define(
                    SINK_IO_THREAD_COUNT,
                    ConfigDef.Type.INT,
                    SINK_IO_THREAD_COUNT_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SINK_IO_THREAD_COUNT_DOC,
                    THRESHOLD_VALUE_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_IO_THREAD_COUNT_DISPLAY
            )
            .define(
                    SINK_CHUNK_LIMIT,
                    ConfigDef.Type.LONG,
                    SINK_CHUNK_LIMIT_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SINK_CHUNK_LIMIT_DOC,
                    THRESHOLD_VALUE_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_CHUNK_LIMIT_DISPLAY
            )
            .define(
                    SINK_SCAN_FREQUENCY,
                    ConfigDef.Type.LONG,
                    SINK_SCAN_FREQUENCY_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SINK_SCAN_FREQUENCY_DOC,
                    THRESHOLD_VALUE_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_SCAN_FREQUENCY_DISPLAY
            )
            .define(
                    SINK_BATCH_MAX_SIZE,
                    ConfigDef.Type.LONG,
                    SINK_BATCH_MAX_SIZE_DEFAULT,
                    new BatchMaxSizeValidator(),
                    ConfigDef.Importance.HIGH,
                    SINK_BATCH_MAX_SIZE_DOC,
                    THRESHOLD_VALUE_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_BATCH_MAX_SIZE_DISPLAY
            )
            .define(
                    SINK_BATCH_FLUSH_INTERVAL,
                    ConfigDef.Type.LONG,
                    SINK_BATCH_FLUSH_INTERVAL_DEFAULT,
                    new BatchFlushIntervalValidator(),
                    ConfigDef.Importance.HIGH,
                    SINK_BATCH_FLUSH_INTERVAL_DOC,
                    THRESHOLD_VALUE_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_BATCH_FLUSH_INTERVAL_DISPLAY
            )
            .define(
                    SINK_COLUMNS,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    SINK_COLUMNS_DOC,
                    COLUMNS_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SINK_COLUMNS_DISPLAY
            );
    
    public StarRocksSinkConfig(final Map<String, String> props) {
        super(CONFIG_DEF, props);
        parseSinkStreamLoadProperties();
    }
    
    
    public String getJdbcUrl() {
        return getString(JDBC_URL);
    }
    
    public String getDatabaseName() {
        return getString(DATABASE_NAME);
    }
    
    public String getTableName() {
        return getString(TABLE_NAME);
    }
    
    public String getUsername() {
        return getString(USERNAME);
    }
    
    public String getPassword() {
        return getString(PASSWORD);
    }
    
    public List<String> getLoadUrlList() {
        return getList(LOAD_URL);
    }
    
    public String getLabelPrefix() {
        return getString(SINK_LABEL_PREFIX);
    }
    
    public long getSinkMaxFlushInterval() {
        return getLong(SINK_BATCH_FLUSH_INTERVAL);
    }
    
    public long getSinkMaxBytes() {
        return getLong(SINK_BATCH_MAX_SIZE);
    }
    
    public int getConnectTimeout() {
        int connectTimeout = getInt(SINK_CONNECT_TIMEOUT);
        if (connectTimeout < 100) {
            return 100;
        }
        return Math.min(connectTimeout, 60000);
    }
    
    public int getWaitForContinueTimeout() {
        int waitForContinueTimeoutMs = getInt(SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        if (waitForContinueTimeoutMs < DEFAULT_WAIT_FOR_CONTINUE) {
            return DEFAULT_WAIT_FOR_CONTINUE;
        }
        return Math.min(waitForContinueTimeoutMs, 600000);
    }
    
    public String getColumnsString() {
        return Joiner.on(COMMA).join(getColumnsList());
    }
    
    public List<String> getColumnsList() {
        return getList(SINK_COLUMNS);
    }
    
    public int getIoThreadCount() {
        return getInt(SINK_IO_THREAD_COUNT);
    }
    
    public long getChunkLimit() {
        return getLong(SINK_CHUNK_LIMIT);
    }
    
    public long getScanFrequency() {
        return getLong(SINK_SCAN_FREQUENCY);
    }
    
    
    public Map<String, String> getSinkStreamLoadProperties() {
        return streamLoadProps;
    }
    
    private void parseSinkStreamLoadProperties() {
        Map<String, Object> headerProps = originalsWithPrefix(SINK_PROPERTIES_PREFIX);
        
        if (Objects.nonNull(headerProps) && !headerProps.isEmpty()) {
            headerProps.forEach((k, v) -> streamLoadProps.put(k, String.valueOf(v)));
        }
    }
    
    public String getCsvRowSeparator() {
        return SINK_CSV_ROW_SEPARATOR_DEFAULT;
    }
    
    public String getCsvColumnSeparator() {
        return SINK_CSV_COLUMN_SEPARATOR_DEFAULT;
    }
    
    public StreamLoadProperties getProperties() {
        StreamLoadDataFormat dataFormat = new StreamLoadDataFormat.CSVFormat(StarRocksDelimiterParser
                .parse(getCsvRowSeparator(), SINK_CSV_ROW_SEPARATOR_DEFAULT));
        
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(getDatabaseName())
                .table(getTableName())
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(getChunkLimit());
        
        defaultTablePropertiesBuilder.columns(getColumnsString());
        
        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls(getLoadUrlList().toArray(new String[0]))
                .jdbcUrl(getJdbcUrl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .cacheMaxBytes(getSinkMaxBytes())
                .connectTimeout(getConnectTimeout())
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .username(getUsername())
                .password(getPassword())
                .expectDelayTime(getSinkMaxFlushInterval())
                .addHeaders(getSinkStreamLoadProperties());
        
        for (StreamLoadTableProperties tableProperties : tablePropertiesList) {
            builder.addTableProperties(tableProperties);
        }
        
        return builder.build();
    }
}
