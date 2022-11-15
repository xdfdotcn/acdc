package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO.LogicalDelDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkCreationDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaConverterType;
import cn.xdf.acdc.devops.repository.KafkaSinkConnectorRepository;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkKafkaConstant;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaClusterService;
import cn.xdf.acdc.devops.service.entity.KafkaSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.error.AlreadyExistsException;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.SpecificConfigurationProcessService;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Service
public class KafkaSinkConnectorProcessServiceImpl extends AbstractSinkConnectorProcessServiceImpl {

    public static final Set<String> ENCRYPT_CONF_ITEM_SET = Sets.newHashSet(
            SinkKafkaConstant.SINK_KAFKA_SASL_JAAS_CONFIG
    );

    private static final String JSON_KEY_KAFKA_CONVERTER_TYPE = "kafkaConverterType";

    @Autowired
    private KafkaClusterService kafkaClusterService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private KafkaSinkConnectorService kafkaSinkConnectorService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private SpecificConfigurationProcessService specificConfigurationProcessService;

    @Autowired
    private KafkaSinkConnectorRepository kafkaSinkConnectorRepository;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.KAFKA;
    }

    @Override
    public ConnectorDTO createSink(final SinkCreationDTO creation) {
        Long dataSetId = creation.getDataSetId();
        Long createdKafkaTopicId = creation.getCreatedKafkaTopicId();
        List<FieldMappingDTO> fieldMappings = creation.getFieldMappingList();
        KafkaConverterType kafkaConverterType = getKafkaConverterType(creation);

        // 重复创建校验
        kafkaSinkConnectorService.findByKafkaTopicId(dataSetId).ifPresent(table -> {
            throw new AlreadyExistsException(ErrorMsg.E_102, String.format("dataSetId: %s", dataSetId));
        });

        KafkaTopicDO dataTopic = kafkaTopicService.findById(createdKafkaTopicId)
                .orElseThrow(() -> new NotFoundException(String.format("createdKafkaTopicId: %s", createdKafkaTopicId)));

        KafkaTopicDO sinkTopic = kafkaTopicService.findById(dataSetId)
                .orElseThrow(() -> new NotFoundException(String.format("dataSetId: %s", dataSetId)));

        KafkaClusterDO kafkaCluster = sinkTopic.getKafkaCluster();

        // save connector
        String connectorName = ConnectorUtil.getKafkaSinkConnectorName(kafkaCluster.getName(), sinkTopic.getName());
        ConnectorDO connector = connectorService.save(connectorName, dataSystemType(), ConnectorType.SINK);

        List<ConnectorDataExtensionDO> dataExtensions = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        List<SinkConnectorColumnMappingDO> columnMappings = FieldMappingDTO.toSinkColumnMappingList(fieldMappings);
        String filterExpress = FieldMappingDTO.findRowFilterExpress(fieldMappings).orElse(null);

        // save sink connector
        SinkConnectorDO sinkConnector = sinkConnectorService.save(dataTopic.getId(), connector.getId(), filterExpress);

        // save  extensions and column mapping
        sinkConnectorService.saveExtensionsAndColumnMappings(sinkConnector.getId(), dataExtensions, columnMappings);

        // save hive sink connector
        kafkaSinkConnectorService.save(KafkaSinkConnectorDO.builder()
                .keyConverter(kafkaConverterType.name())
                .valueConverter(kafkaConverterType.name())
                .sinkConnector(sinkConnector)
                .kafkaTopic(sinkTopic)
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build()
        );

        // save config
        Map<String, String> configMap = fetchConfig(
                kafkaCluster,
                kafkaConverterType,
                dataTopic,
                sinkTopic,
                filterExpress,
                columnMappings);
        saveConfig(connector.getId(), configMap);

        return new ConnectorDTO(connector);
    }

    protected Map<String, String> fetchConfig(
            final KafkaClusterDO kafkaCluster,
            final KafkaConverterType kafkaConverterType,
            final KafkaTopicDO dataTopic,
            final KafkaTopicDO sinkTopic,
            final String filterExpress,
            final List<SinkConnectorColumnMappingDO> columnMappings
    ) {
        Map configMap = Maps.newLinkedHashMap();

        String connectorName = ConnectorUtil.getKafkaSinkConnectorName(kafkaCluster.getName(), sinkTopic.getName());
        String destinations = sinkTopic.getName();
        // 基础配置
        configMap.put(CommonConstant.NAME, connectorName);
        configMap.put(SinkConstant.TOPICS, dataTopic.getName());
        configMap.put(SinkConstant.DESTINATIONS, destinations);
//        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + destinations + SinkKafkaConstant.EXCLUDE_META_FIELDS, "false");

        setConfFieldMapping(destinations, columnMappings, configMap);

//        setConfFieldWhitelist(destinations, columnMappings, configMap);

        setFilterExpression(destinations, filterExpress, configMap);

        setConfLogicalDel(destinations, configMap);

        setKafkaConsumeConfig(kafkaCluster, kafkaConverterType, configMap);

        return configMap;
    }

    private void setKafkaConsumeConfig(
            final KafkaClusterDO kafkaCluster,
            final KafkaConverterType kafkaConverterType,
            final Map<String, String> configMap
    ) {

        try {
            Map<String, String> adminConfig = objectMapper
                    .readValue(kafkaCluster.getSecurityConfiguration(), Map.class);

            // security
            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaCluster.getBootstrapServers()
            );
            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    adminConfig.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
            );

            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + SaslConfigs.SASL_MECHANISM,
                    adminConfig.get(SaslConfigs.SASL_MECHANISM)
            );

            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
                    adminConfig.get(SaslConfigs.SASL_JAAS_CONFIG)
            );

            // convert
            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + SinkKafkaConstant.KEY_CONVERTER,
                    kafkaConverterType.getConverterClass()
            );
            configMap.put(SinkKafkaConstant.KAFKA_CONFIG_PREFIX + SinkKafkaConstant.VALUE_CONVERTER,
                    kafkaConverterType.getConverterClass()
            );

        } catch (JsonProcessingException e) {
            throw new SystemBizException(e);
        }
    }

    @Override
    public Page<SinkConnectorInfoDTO> querySinkForSource(final Long sourceConnectorId, final Pageable pageable) {
        Page<Map<String, Object>> resultMap = kafkaSinkConnectorRepository.findSinkForSource(pageable, sourceConnectorId);
        return resultMap.map(it -> new SinkConnectorInfoDTO(
                Long.valueOf(String.valueOf(it.get("id"))),
                (String) it.get("name"),
                (String) it.get("kafka_topic"),
                (String) it.get("cluster_name"),
                (String) it.get("database_name"),
                (String) it.get("data_set_name")
        ));
    }

    @Override
    public SinkConnectorInfoDTO getSinkDetail(final Long connectorId) {
        return Optional.of(kafkaSinkConnectorService.findSinkDetail(connectorId))
                .map(SinkConnectorInfoDTO::new).get();
    }

    @Override
    protected void flushConfigWhenEdit(
            final SinkConnectorDO sinkConnector,
            final String filterExpress,
            final List<ConnectorDataExtensionDO> extensions,
            final List<SinkConnectorColumnMappingDO> columnMappings,
            final LogicalDelDTO logicalDel) {

        Long sinkConnectorId = sinkConnector.getId();
        Long connectorId = sinkConnector.getConnector().getId();

        KafkaSinkConnectorDO kafkaSinkConnector = kafkaSinkConnectorService.findBySinkConnectorId(sinkConnectorId)
                .orElseThrow(() -> new NotFoundException(String.format("sinkConnectorId: %s", sinkConnectorId)));

        KafkaTopicDO dataTopic = sinkConnector.getKafkaTopic();
        KafkaTopicDO sinkTopic = kafkaSinkConnector.getKafkaTopic();
        KafkaClusterDO kafkaCluster = sinkTopic.getKafkaCluster();
        KafkaConverterType kafkaConverterType = KafkaConverterType.valueOf(kafkaSinkConnector.getValueConverter());
        Map<String, String> configMap = fetchConfig(
                kafkaCluster,
                kafkaConverterType,
                dataTopic,
                sinkTopic,
                filterExpress,
                columnMappings);
        saveConfig(connectorId, configMap);
    }

    @Override
    protected void doEditFieldMapping(
            final SinkConnectorDO sinkConnector,
            final String filterExpress,
            final List<ConnectorDataExtensionDO> dataExtensions,
            final List<SinkConnectorColumnMappingDO> columnMappings,
            final LogicalDelDTO logicalDel) {

        // do nothing
    }

    private KafkaConverterType getKafkaConverterType(final SinkCreationDTO creation) {
        String converter = specificConfigurationProcessService
                .kafkaSpecificConfDeserialize(creation.getSpecificConfiguration()).getKafkaConverterType();
        return KafkaConverterType.valueOf(converter);
    }

    @Override
    public Set<String> getEncryptConfigItemSet() {
        return ENCRYPT_CONF_ITEM_SET;
    }
}
