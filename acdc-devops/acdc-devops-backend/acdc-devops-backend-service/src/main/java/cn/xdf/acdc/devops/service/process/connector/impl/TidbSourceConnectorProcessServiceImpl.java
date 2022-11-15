package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseTidbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceTidbConstant;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseTidbService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TidbSourceConnectorProcessServiceImpl extends AbstractSourceConnectorProcessServiceImpl {

    public static final Set<String> ENCRYPT_CONF_ITEM_SET = Sets.newHashSet(
        SourceTidbConstant.SOURCE_KAFKA_SASL_JAAS_CONFIG
    );

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private RdbDatabaseTidbService rdbDatabaseTidbService;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.TIDB;
    }

    @Override
    RdbInstanceDO checkOrInitDataSource(final RdbDO rdb, final RdbDatabaseDO rdbDatabase) {
        String topic = ConnectorUtil.geSourceTidbTopic(rdb.getName(), rdbDatabase.getName());
        KafkaClusterDO tidbKafkaCluster = kafkaClusterService.findTicdcKafkaCluster()
            .orElseThrow(() -> new NotFoundException(String.format("clusterType: %s", KafkaClusterType.TICDC)));

        Optional<KafkaTopicDO> topicOpt = kafkaTopicService
            .findByKafkaClusterIdAndName(tidbKafkaCluster.getId(), topic);

        if (!topicOpt.isPresent()) {
            // tidb 不存在消费主题,则添加消费主题
            KafkaTopicDO kafkaTopic = kafkaTopicService.save(KafkaTopicDO.builder()
                .name(topic)
                .kafkaCluster(tidbKafkaCluster)
                .creationTime(new Date().toInstant())
                .updateTime(new Date().toInstant())
                .build());

            rdbDatabaseTidbService.save(RdbDatabaseTidbDO.builder()
                .kafkaTopic(kafkaTopic)
                .rdbDatabase(rdbDatabase)
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build()
            );
        }
        return new RdbInstanceDO();
    }

    @Override
    Map<String, String> fetchConfig(
        final RdbDO rdb,
        final RdbInstanceDO rdbInstance,
        final RdbDatabaseDO rdbDatabase,
        final RdbTableDO rdbTable,
        final KafkaTopicDO kafkaTopic,
        final List<String> uniqueKeys
    ) {

        String consumeTopic = ConnectorUtil.geSourceTidbTopic(rdb.getName(), rdbDatabase.getName());
        Map<String, String> configMap = Maps.newLinkedHashMap();
        String serverName = ConnectorUtil
            .getSourceServerName(DataSystemType.TIDB, rdb.getName(), rdbDatabase.getName());

        configMap.put(CommonConstant.NAME, serverName);

        configMap.put(SourceTidbConstant.DATABASE_SERVER_NAME, serverName);
        configMap.put(SourceConstant.DATABASE_INCLUDE, rdbDatabase.getName());
        configMap.put(SourceConstant.TABLE_INCLUDE_LIST,
            ConnectorUtil.getTableInclude(rdbDatabase.getName(), rdbTable.getName()));
        configMap.put(SourceConstant.MESSAGE_KEY_COLUMNS,
            ConnectorUtil.getMessageKeyColumns(rdbDatabase.getName(), rdbTable.getName(), uniqueKeys));

        configMap.put(SourceTidbConstant.SOURCE_KAFKA_TOPIC, consumeTopic);
        configMap.put(SourceTidbConstant.SOURCE_KAFKA_GROUP_ID, serverName);

        setKafkaSASL(configMap);

        return configMap;
    }

    private void setKafkaSASL(
        final Map<String, String> configMap
    ) {

        KafkaClusterDO tidbKafkaCluster = kafkaClusterService.findTicdcKafkaCluster()
                .orElseThrow(() -> new NotFoundException(String.format("clusterType: %s", KafkaClusterType.TICDC)));

        try {
            Map<String, String> adminConfig = objectMapper.readValue(tidbKafkaCluster.getSecurityConfiguration(), Map.class);
            configMap.put(
                SourceTidbConstant.SOURCE_KAFKA_SASL_JAAS_CONFIG,
                adminConfig.get(SaslConfigs.SASL_JAAS_CONFIG)
            );
            configMap.put(
                SourceTidbConstant.SOURCE_KAFKA_BOOTSTRAP_SERVERS,
                tidbKafkaCluster.getBootstrapServers()
            );
        } catch (JsonProcessingException e) {
            throw new SystemBizException(e);
        }
    }

    @Override
    public SourceConnectorInfoDTO getSourceDetail(final Long connectorId) {
        SourceConnectorInfoDTO sourceInfo = super.getSourceDetail(connectorId);
        RdbDatabaseTidbDO rdbDatabaseTidb = rdbDatabaseTidbService.findByRdbDataBaseId(sourceInfo.getSrcDatabaseId())
            .orElseThrow(() -> new NotFoundException(String.format("databaseId: %s", sourceInfo.getSrcDatabaseId())));
        // tidb 消费主题
        KafkaTopicDO kafkaTopic = rdbDatabaseTidb.getKafkaTopic();
        sourceInfo.setKafkaTopic(kafkaTopic.getName());
        return sourceInfo;
    }

    @Override
    public Set<String> getEncryptConfigItemSet() {
        return ENCRYPT_CONF_ITEM_SET;
    }
}
