package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceMysqlConstant;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MysqlSourceConnectorProcessServiceImpl extends AbstractSourceConnectorProcessServiceImpl {

    public static final Set<String> ENCRYPT_CONF_ITEM_SET = Sets.newHashSet(
        SourceMysqlConstant.DATABASE_PASSWORD,
        SourceMysqlConstant.DATABASE_HISTORY_CONSUMER_SASL_JAAS_CONFIG,
        SourceMysqlConstant.DATABASE_HISTORY_PRODUCER_SASL_JAAS_CONFIG
    );

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.MYSQL;
    }

    @Override
    RdbInstanceDO checkOrInitDataSource(final RdbDO rdb, final RdbDatabaseDO rdbDatabase) {
        Long rdbId = rdb.getId();
        return rdbInstanceService.findDataSourceInstanceByRdbId(rdbId)
            .orElseThrow(() -> new NotFoundException(ErrorMsg.E_103, String.format("rdbId: %s", rdbId)));
    }

    @Override
    public Map<String, String> fetchConfig(
        final RdbDO rdb,
        final RdbInstanceDO rdbInstance,
        final RdbDatabaseDO rdbDatabase,
        final RdbTableDO rdbTable,
        final KafkaTopicDO kafkaTopic,
        final List<String> uniqueKeys
    ) {

        Map<String, String> configMap = Maps.newLinkedHashMap();
        // 配置生成
        String serverName = ConnectorUtil.getSourceServerName(dataSystemType(), rdb.getName(), rdbDatabase.getName());
        String schemaHistoryTopic = ConnectorUtil
            .getSchemaHistoryTopic(DataSystemType.MYSQL, rdb.getName(), rdbDatabase.getName());

        configMap.put(CommonConstant.NAME, serverName);
        configMap.put(SourceMysqlConstant.DATABASE_HOSTNAME, rdbInstance.getHost());
        configMap.put(SourceMysqlConstant.DATABASE_PORT, String.valueOf(rdbInstance.getPort()));
        configMap.put(SourceMysqlConstant.DATABASE_USER, rdb.getUsername());
        configMap.put(SourceMysqlConstant.DATABASE_PASSWORD, rdb.getPassword());
        configMap.put(SourceMysqlConstant.DATABASE_SERVER_NAME, serverName);

        configMap.put(SourceConstant.DATABASE_INCLUDE, rdbDatabase.getName());
        configMap.put(SourceConstant.MESSAGE_KEY_COLUMNS,
            ConnectorUtil.getMessageKeyColumns(rdbDatabase.getName(), rdbTable.getName(), uniqueKeys));
        configMap.put(SourceConstant.TABLE_INCLUDE_LIST,
            ConnectorUtil.getTableInclude(rdbDatabase.getName(), rdbTable.getName()));

        configMap.put(SourceMysqlConstant.DATABASE_HISTORY_KAFKA_TOPIC, schemaHistoryTopic);

        setKafkaSASL(kafkaTopic.getKafkaCluster(), configMap);

        return configMap;
    }

    private void setKafkaSASL(
        final KafkaClusterDO kafkaCluster,
        final Map<String, String> configMap
    ) {
        try {
            Map<String, String> adminConfig = objectMapper
                .readValue(kafkaCluster.getSecurityConfiguration(), Map.class);
            configMap.put(
                SourceMysqlConstant.DATABASE_HISTORY_CONSUMER_SASL_JAAS_CONFIG,
                adminConfig.get(SaslConfigs.SASL_JAAS_CONFIG)
            );
            configMap.put(SourceMysqlConstant.DATABASE_HISTORY_PRODUCER_SASL_JAAS_CONFIG,
                adminConfig.get(SaslConfigs.SASL_JAAS_CONFIG)

            );

            configMap.put(
                SourceMysqlConstant.DATABASE_HISTORY_KAFKA_BOOTSTRAP_SERVERS,
                kafkaCluster.getBootstrapServers()
            );
        } catch (JsonProcessingException e) {
            throw new SystemBizException(e);
        }
    }

    @Override
    public Set<String> getEncryptConfigItemSet() {
        return ENCRYPT_CONF_ITEM_SET;
    }
}
