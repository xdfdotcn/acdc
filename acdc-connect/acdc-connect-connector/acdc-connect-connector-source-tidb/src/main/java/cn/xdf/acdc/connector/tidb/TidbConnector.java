package cn.xdf.acdc.connector.tidb;

import cn.xdf.acdc.connector.tidb.source.TidbConnectorConfig;
import cn.xdf.acdc.connector.tidb.source.TidbConnectorTask;
import cn.xdf.acdc.connector.tidb.util.Version;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TidbConnector extends SourceConnector {
    
    public static final String TASK_ID_PREFIX = "task_";
    
    public static final String TASK_ID = "task.id";
    
    @Immutable
    private Map<String, String> properties;
    
    public TidbConnector() {
    }
    
    @Override
    public String version() {
        return Version.getVersion();
    }
    
    @Override
    public void start(final Map<String, String> props) {
        this.properties = props;
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return TidbConnectorTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProperties = new HashMap<>(properties);
            taskProperties.put(TASK_ID, TASK_ID_PREFIX + i);
            taskConfigs.add(taskProperties);
        }
        return taskConfigs;
    }
    
    @Override
    public void stop() {
    }
    
    @Override
    public ConfigDef config() {
        return TidbConnectorConfig.configDef();
    }
    
    @Override
    public Config validate(final Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);
        
        // Validate all the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);
        
        return new Config(new ArrayList<>(results.values()));
    }
    
    private Map<String, ConfigValue> validateAllFields(final Configuration config) {
        return config.validate(TidbConnectorConfig.ALL_FIELDS);
    }
}
