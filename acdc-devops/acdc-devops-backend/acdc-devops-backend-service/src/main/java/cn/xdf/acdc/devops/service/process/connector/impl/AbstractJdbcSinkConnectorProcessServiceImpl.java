package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO.LogicalDelDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkCreationDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.JdbcSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.repository.JdbcSinkConnectorRepository;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkJdbcConstant;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.JdbcSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.error.AlreadyExistsException;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class AbstractJdbcSinkConnectorProcessServiceImpl extends AbstractSinkConnectorProcessServiceImpl {

    public static final Set<String> ENCRYPT_CONF_ITEM_SET = Sets.newHashSet(
            SinkJdbcConstant.CONNECTION_PASSWORD
    );

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private JdbcSinkConnectorService jdbcSinkConnectorService;

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Autowired
    private JdbcSinkConnectorRepository jdbcSinkConnectorRepository;

    @Override
    public ConnectorDTO createSink(final SinkCreationDTO sinkCreationDTO) {
        Long datasetId = sinkCreationDTO.getDataSetId();
        Long instanceId = sinkCreationDTO.getInstanceId();
        Long createdKafkaTopicId = sinkCreationDTO.getCreatedKafkaTopicId();

        List<FieldMappingDTO> fieldMappings = sinkCreationDTO.getFieldMappingList();

        RdbTableDO rdbTable = rdbTableService.findById(datasetId)
                .orElseThrow(() -> new NotFoundException(String.format("tableId: %s", datasetId)));

        KafkaTopicDO kafkaTopic = kafkaTopicService.findById(createdKafkaTopicId)
                .orElseThrow(() -> new NotFoundException(String.format("topic: %s", createdKafkaTopicId)));

        RdbInstanceDO rdbInstance = rdbInstanceService.findById(instanceId)
                .orElseThrow(() -> new NotFoundException(String.format("instanceId: %s", instanceId)));

        RdbDatabaseDO rdbDatabase = rdbTable.getRdbDatabase();
        RdbDO rdb = rdbDatabase.getRdb();

        // 1. 校验重复创建sink
        jdbcSinkConnectorService.findByRdbTableId(rdbTable.getId()).ifPresent(table -> {
            throw new AlreadyExistsException(ErrorMsg.E_102, String.format("Already existed, rdbTable: %s", rdbTable));
        });

        String connectorName = ConnectorUtil
                .getRdbSinkConnectorName(dataSystemType(), rdb.getName(), rdbDatabase.getName(), rdbTable.getName());

        // 2. save connector
        ConnectorDO connector = connectorService.save(connectorName, dataSystemType(), ConnectorType.SINK);

        List<ConnectorDataExtensionDO> dataExtensions = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        List<SinkConnectorColumnMappingDO> columnMappings = FieldMappingDTO.toSinkColumnMappingList(fieldMappings);
        String filterExpress = FieldMappingDTO.findRowFilterExpress(fieldMappings).orElse(null);
        LogicalDelDTO logicalDelDTO = FieldMappingDTO.findLogicalDelColumn(fieldMappings).orElse(new LogicalDelDTO());

        // 3. save sink connector
        SinkConnectorDO sinkConnector = sinkConnectorService.save(kafkaTopic.getId(), connector.getId(), filterExpress);

        // 4. save extensions and column mappings
        sinkConnectorService.saveExtensionsAndColumnMappings(sinkConnector.getId(), dataExtensions, columnMappings);

        // 5. save jdbc sink connector
        jdbcSinkConnectorService.save(JdbcSinkConnectorDO.builder()
                .sinkConnector(sinkConnector)
                .rdbInstance(rdbInstance)
                .rdbTable(rdbTable)
                .logicalDeletionColumn(logicalDelDTO.getLogicalDeletionColumn())
                .logicalDeletionColumnValueDeletion(logicalDelDTO.getLogicalDeletionColumnValueDeletion())
                .logicalDeletionColumnValueNormal(logicalDelDTO.getLogicalDeletionColumnValueNormal())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build()
        );

        Map<String, String> configMap = fetchConfig(
                kafkaTopic,
                rdb,
                rdbInstance,
                rdbDatabase,
                rdbTable,
                filterExpress,
                logicalDelDTO,
                dataExtensions,
                columnMappings
        );
        // 6. save config
        saveConfig(connector.getId(), configMap);
        return new ConnectorDTO(connector);
    }

    @Override
    public Page<SinkConnectorInfoDTO> querySinkForSource(
            final Long sourceConnectorId,
            final Pageable pageable) {
        Page<Map<String, Object>> resultMap = jdbcSinkConnectorRepository.findSinkForSource(pageable, sourceConnectorId);
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
        return Optional.of(jdbcSinkConnectorService.findSinkDetail(connectorId))
                .map(SinkConnectorInfoDTO::new).get();
    }

    protected Map<String, String> fetchConfig(
            final KafkaTopicDO kafkaTopic,
            final RdbDO rdb,
            final RdbInstanceDO rdbInstance,
            final RdbDatabaseDO rdbDatabase,
            final RdbTableDO rdbTable,
            final String filterExpress,
            final LogicalDelDTO logicalDel,
            final List<ConnectorDataExtensionDO> extensions,
            final List<SinkConnectorColumnMappingDO> columnMappings) {
        Map configMap = Maps.newLinkedHashMap();

        String jdbcUrl = ConnectorUtil.getJdbcUrl(
                rdbInstance.getHost(),
                rdbInstance.getPort(),
                rdbDatabase.getName(),
                dataSystemType());

        String table = rdbTable.getName();
        String topic = kafkaTopic.getName();

        String connectorName = ConnectorUtil.getRdbSinkConnectorName(
                dataSystemType(),
                rdb.getName(),
                rdbDatabase.getName(),
                rdbTable.getName()
        );

        configMap.put(CommonConstant.NAME, connectorName);
        configMap.put(SinkConstant.TOPICS, topic);

        configMap.put(SinkConstant.DESTINATIONS, rdbTable.getName());

        configMap.put(SinkJdbcConstant.CONNECTION_URL, jdbcUrl);
        configMap.put(SinkJdbcConstant.CONNECTION_USER, rdb.getUsername());
        configMap.put(SinkJdbcConstant.CONNECTION_PASSWORD, rdb.getPassword());

        // 扩展字段
        setConfFieldAdd(table, extensions, configMap);

        // 字段映射
        setConfFieldMapping(table, columnMappings, configMap);

        // 字段白名单
        setConfFieldWhitelist(table, columnMappings, configMap);

        // 字段过滤表达式
        setFilterExpression(table, filterExpress, configMap);

        // 逻辑删除字段配置
        setConfLogicalDel(
                table,
                logicalDel.getLogicalDeletionColumn(),
                logicalDel.getLogicalDeletionColumnValueDeletion(),
                logicalDel.getLogicalDeletionColumnValueNormal(),
                configMap
        );
        return configMap;
    }

    @Override
    protected void flushConfigWhenEdit(
            final SinkConnectorDO sinkConnector,
            final String filterExpress,
            final List<ConnectorDataExtensionDO> extensions,
            final List<SinkConnectorColumnMappingDO> columnMappings,
            final LogicalDelDTO logicalDel) {

        Long sinkConnectorId = sinkConnector.getId();
        JdbcSinkConnectorDO jdbcSinkConnector = jdbcSinkConnectorService.findBySinkConnectorId(sinkConnectorId)
                .orElseThrow(() -> new NotFoundException(String.format("sinkConnectorId: %s", sinkConnectorId)));

        KafkaTopicDO kafkaTopic = sinkConnector.getKafkaTopic();
        RdbTableDO rdbTable = jdbcSinkConnector.getRdbTable();
        RdbDatabaseDO rdbDatabase = rdbTable.getRdbDatabase();
        RdbDO rdb = rdbDatabase.getRdb();
        RdbInstanceDO rdbInstance = jdbcSinkConnector.getRdbInstance();
        Map<String, String> configMap = fetchConfig(
                kafkaTopic,
                rdb,
                rdbInstance,
                rdbDatabase,
                rdbTable,
                filterExpress,
                logicalDel,
                extensions,
                columnMappings
        );

        saveConfig(sinkConnector.getConnector().getId(), configMap);
    }

    @Override
    protected void doEditFieldMapping(
            final SinkConnectorDO sinkConnector,
            final String filterExpress,
            final List<ConnectorDataExtensionDO> dataExtensions,
            final List<SinkConnectorColumnMappingDO> columnMappings,
            final LogicalDelDTO logicalDel) {

        Long sinkConnectorId = sinkConnector.getId();
        JdbcSinkConnectorDO jdbcSinkConnector = jdbcSinkConnectorService.findBySinkConnectorId(sinkConnectorId)
                .orElseThrow(() -> new NotFoundException(String.format("sinkConnectorId: %s", sinkConnectorId)));

        jdbcSinkConnector.setUpdateTime(Instant.now());
        jdbcSinkConnector.setLogicalDeletionColumn(logicalDel.getLogicalDeletionColumn());
        jdbcSinkConnector.setLogicalDeletionColumnValueDeletion(logicalDel.getLogicalDeletionColumnValueDeletion());
        jdbcSinkConnector.setLogicalDeletionColumnValueNormal(logicalDel.getLogicalDeletionColumnValueNormal());

        jdbcSinkConnectorService.save(jdbcSinkConnector);
    }

    @Override
    public Set<String> getEncryptConfigItemSet() {
        return ENCRYPT_CONF_ITEM_SET;
    }
}
