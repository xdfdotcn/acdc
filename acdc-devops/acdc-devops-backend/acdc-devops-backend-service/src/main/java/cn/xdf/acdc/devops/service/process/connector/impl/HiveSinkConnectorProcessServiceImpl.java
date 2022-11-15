package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO.LogicalDelDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkCreationDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.HiveSinkConnectorRepository;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkHiveConstant;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.HiveDatabaseService;
import cn.xdf.acdc.devops.service.entity.HiveService;
import cn.xdf.acdc.devops.service.entity.HiveSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.HiveTableService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
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
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class HiveSinkConnectorProcessServiceImpl extends AbstractSinkConnectorProcessServiceImpl {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private HiveDatabaseService hiveDatabaseService;

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    private HiveSinkConnectorService hiveSinkConnectorService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private HiveSinkConnectorRepository hiveSinkConnectorRepository;

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.HIVE;
    }

    @Override
    public ConnectorDTO createSink(final SinkCreationDTO sinkCreationDTO) {
        Long tableId = sinkCreationDTO.getDataSetId();
        Long createdKafkaTopicId = sinkCreationDTO.getCreatedKafkaTopicId();
        List<FieldMappingDTO> fieldMappings = sinkCreationDTO.getFieldMappingList();

        HiveTableDO hiveTable = hiveTableService.findById(tableId)
                .orElseThrow(() -> new NotFoundException(String.format("tableId: %s", tableId)));
        HiveDatabaseDO hiveDatabase = hiveTable.getHiveDatabase();
        HiveDO hive = hiveDatabase.getHive();

        KafkaTopicDO kafkaTopic = kafkaTopicService.findById(createdKafkaTopicId)
                .orElseThrow(() -> new NotFoundException(String.format("topic: %s", createdKafkaTopicId)));

        // 重复创建校验
        hiveSinkConnectorService.findByHiveTableId(hiveTable.getId()).ifPresent(table -> {
            throw new AlreadyExistsException(ErrorMsg.E_102, String.format("hiveTable: %s", hiveTable));
        });

        // save connector
        String connectorName = ConnectorUtil.getHiveSinkConnectorName(hiveDatabase.getName(), hiveTable.getName());
        ConnectorDO connector = connectorService.save(connectorName, dataSystemType(), ConnectorType.SINK);

        List<ConnectorDataExtensionDO> dataExtensions = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        List<SinkConnectorColumnMappingDO> columnMappings = FieldMappingDTO.toSinkColumnMappingList(fieldMappings);
        String filterExpress = FieldMappingDTO.findRowFilterExpress(fieldMappings).orElse(null);

        // save sink connector
        SinkConnectorDO sinkConnector = sinkConnectorService.save(kafkaTopic.getId(), connector.getId(), filterExpress);

        // save  extensions and column mapping
        sinkConnectorService.saveExtensionsAndColumnMappings(sinkConnector.getId(), dataExtensions, columnMappings);

        // save hive sink connector
        hiveSinkConnectorService.save(HiveSinkConnectorDO.builder()
                .sinkConnector(sinkConnector)
                .hiveTable(hiveTable)
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build()
        );

        // save config
        Map<String, String> configMap = fetchConfig(
                kafkaTopic,
                hive,
                hiveDatabase,
                hiveTable,
                filterExpress,
                dataExtensions,
                columnMappings);

        connectorConfigurationService.saveConfig(connector.getId(), configMap);

        return new ConnectorDTO(connector);
    }

    protected Map<String, String> fetchConfig(
            final KafkaTopicDO kafkaTopic,
            final HiveDO hive,
            final HiveDatabaseDO hiveDatabase,
            final HiveTableDO hiveTable,
            final String filterExpress,
            final List<ConnectorDataExtensionDO> extensions,
            final List<SinkConnectorColumnMappingDO> columnMappings
    ) {
        Map configMap = Maps.newLinkedHashMap();

        String database = hiveDatabase.getName();
        String table = hiveTable.getName();
        String topic = kafkaTopic.getName();
        String connectorName = ConnectorUtil.getHiveSinkConnectorName(hiveDatabase.getName(), hiveTable.getName());
        String databaseTable = new StringBuilder().append(database).append(CommonConstant.DOT).append(table).toString();

        // 基础配置
        configMap.put(CommonConstant.NAME, connectorName);
        configMap.put(SinkConstant.TOPICS, topic);
        configMap.put(SinkConstant.DESTINATIONS, databaseTable);

        // 扩展字段
        setConfFieldAdd(databaseTable, extensions, configMap);

        // 字段映射
        setConfFieldMapping(databaseTable, columnMappings, configMap);

        // 字段白名单
        setConfFieldWhitelist(databaseTable, columnMappings, configMap);

        // 字段过滤表达式
        setFilterExpression(databaseTable, filterExpress, configMap);

        // 逻辑删除,"NONE" 可以逃过core中物理删除的逻辑
        setConfLogicalDel(databaseTable, configMap);

        // hdfs 配置
        setHdfsConfig(hive, configMap);

        return configMap;
    }

    private void setHdfsConfig(final HiveDO hive, final Map<String, String> configMap) {
        HdfsDO hdfs = hive.getHdfs();
        String cluster = hdfs.getName();
        String hdfsUser = hive.getHdfsUser();
        Set<HdfsNamenodeDO> hdfsNameNodes = hdfs.getHdfsNamenodes();
        List<String> nameNodes = hdfsNameNodes.stream()
                .map(HdfsNamenodeDO::getName).collect(Collectors.toList());

        configMap.put(SinkHiveConstant.HADOOP_USER, hdfsUser);

        configMap.put(SinkHiveConstant.STORE_URL, SinkHiveConstant.HDFS_URL_SCHEMA + cluster);

        configMap.put(SinkHiveConstant.HDFS_NAME_SERVICES, cluster);
        configMap.put(SinkHiveConstant.HDFS_HA_NAMENODES + CommonConstant.DOT + cluster,
                ConnectorUtil.joinOnComma(nameNodes));

        hdfsNameNodes.forEach(it -> configMap.put(
                SinkHiveConstant.HDFS_NAME_NODE_RPC + CommonConstant.DOT + cluster + CommonConstant.DOT + it.getName(),
                ConnectorUtil.joinOn(CommonConstant.PORT_SEPARATOR, it.getRpcAddress(), it.getRpcPort()
                )));

        configMap.put(SinkHiveConstant.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER + CommonConstant.DOT + cluster,
                hdfs.getClientFailoverProxyProvider());
        configMap.put(SinkHiveConstant.HIVE_METASTORE_URIS, hive.getMetastoreUris());
    }

    @Override
    public Page<SinkConnectorInfoDTO> querySinkForSource(final Long sourceConnectorId, final Pageable pageable) {
        final Page<Map<String, Object>> resultMap = hiveSinkConnectorRepository.findSinkForSource(pageable, sourceConnectorId);
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
        return Optional.of(hiveSinkConnectorService.findSinkDetail(connectorId))
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

        HiveSinkConnectorDO hiveSinkConnector = hiveSinkConnectorService.findBySinkConnectorId(sinkConnectorId)
                .orElseThrow(() -> new NotFoundException(String.format("sinkConnectorId: %s", sinkConnectorId)));

        KafkaTopicDO kafkaTopic = sinkConnector.getKafkaTopic();
        HiveTableDO hiveTable = hiveSinkConnector.getHiveTable();
        HiveDatabaseDO hiveDatabase = hiveTable.getHiveDatabase();
        HiveDO hive = hiveDatabase.getHive();

        Map<String, String> configMap = fetchConfig(
                kafkaTopic,
                hive,
                hiveDatabase,
                hiveTable,
                filterExpress,
                extensions,
                columnMappings);

        connectorConfigurationService.saveConfig(connectorId, configMap);
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

    @Override
    public Set<String> getEncryptConfigItemSet() {
        // hive 目前没有加密配置项
        return Sets.newHashSet();
    }
}
