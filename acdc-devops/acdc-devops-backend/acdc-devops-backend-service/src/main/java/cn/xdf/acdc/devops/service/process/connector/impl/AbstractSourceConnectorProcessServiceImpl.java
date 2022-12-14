package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.CreationResult;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.service.config.RdbJdbcConfig;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SourceConstant;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.RdbDatabaseService;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connector.SourceConnectorProcessService;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Transactional
public abstract class AbstractSourceConnectorProcessServiceImpl extends AbstractConnectorConfigProcessServiceImpl implements SourceConnectorProcessService {

    // CHECKSTYLE:OFF
    @Autowired
    protected RdbJdbcConfig rdbJdbcConfig;
    // CHECKSTYLE:ON

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbDatabaseService rdbDatabaseService;

    @Autowired
    private RdbTableService rdbTableService;

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Override
    public SourceCreationResultDTO createSourceIfAbsent(final SourceCreationDTO sourceCreation) {
        Long tableId = sourceCreation.getTableId();
//        UserDO currentUser = ((ACDCUserDTO) SecurityUtils.getCurrentUserDetails()).dbUser();

        RdbTableDO rdbTable = rdbTableService.findById(tableId)
                .orElseThrow(() -> new NotFoundException(String.format("tableId: %s", tableId)));
        RdbDatabaseDO rdbDatabase = rdbTable.getRdbDatabase();
        RdbDO rdb = rdbDatabase.getRdb();
        RdbInstanceDO rdbInstance = checkOrInitDataSource(rdb, rdbDatabase);
        Long databaseId = rdbDatabase.getId();

        String dataTopic = ConnectorUtil.getDataTopic(dataSystemType(), rdb.getName(), rdbDatabase.getName(), rdbTable.getName());
        String schemaHistoryTopic = ConnectorUtil.getSchemaHistoryTopic(dataSystemType(), rdb.getName(), rdbDatabase.getName());
        String sourceServerTopic = ConnectorUtil.getSourceServerTopic(dataSystemType(), rdb.getName(), rdbDatabase.getName());
        String connectorName = ConnectorUtil.getSourceServerName(dataSystemType(), rdb.getName(), rdbDatabase.getName());

        CreationResult<ConnectorDO> connectorCreation = connectorService
                .saveSourceIfAbsent(databaseId, connectorName, dataSystemType(), ConnectorType.SOURCE);

        // TODO ????????????????????????????????????,???????????????
        CreationResult<SourceRdbTableDO> sourceRdbTableCreation = sourceRdbTableService.saveIfAbsent(rdbTable.getId(), connectorCreation.getResult().getId(), dataTopic);

        KafkaTopicDO kafkaTopic = sourceRdbTableCreation.getResult().getKafkaTopic();

        // ????????????????????????,????????????????????????????????????
        // TODO ??????????????????????????????,?????????case???????????????????????????,????????????topic?????????,???????????????????????????????????????
        List<String> pks = sourceCreation.getPrimaryFields().stream()
                .map(FieldDTO::getName)
                .collect(Collectors.toList());

        // 1. source ??????????????????,???????????????source,??????????????????,?????????????????? source
        if (!connectorCreation.isPresent()
                && !sourceRdbTableCreation.isPresent()) {
            Map<String, String> configMap = fetchConfig(rdb, rdbInstance, rdbDatabase, rdbTable, kafkaTopic, pks);
            saveConfig(connectorCreation.getResult().getId(), configMap);
        }

        // 2. ????????????source??????????????????,????????????????????????,?????????????????????,????????????????????????
        if (connectorCreation.isPresent()
                && !sourceRdbTableCreation.isPresent()) {
            List<ConnectorConfigurationDO> configurations = connectorConfigurationService
                    .findByConnectorId(connectorCreation.getResult().getId());

            saveConfig(
                    connectorCreation.getResult().getId(),
                    patchConfig(configurations, rdbDatabase.getName(), rdbTable.getName(), pks)
            );
        }

        // 3. ??????????????????????????????

        return SourceCreationResultDTO.builder()
                .createdKafkaTopicId(kafkaTopic.getId())
                .createdConnector(new ConnectorDTO(connectorCreation.getResult()))
                .dataTopic(dataTopic)
                .schemaHistoryTopic(schemaHistoryTopic)
                .sourceServerTopic(sourceServerTopic)
                .build();
    }

    private List<ConnectorConfigurationDO> saveConfig(
            final Long connectorId,
            final Map<String, String> configMap
    ) {
        return connectorConfigurationService.saveConfig(connectorId, configMap);
    }

    protected Map<String, String> patchConfig(
            final List<ConnectorConfigurationDO> configurations,
            final String database,
            final String table,
            final List<String> pks) {

        Map<String, String> configMap = configurations.stream().collect(Collectors.toMap(dto -> dto.getName(), dto -> dto.getValue()));

        // include table list
        String newTableIncludeListConf = ConnectorUtil.joinOn(
                CommonConstant.COMMA,
                configMap.get(SourceConstant.TABLE_INCLUDE_LIST),
                ConnectorUtil.getTableInclude(database, table)
        );
        configMap.put(SourceConstant.TABLE_INCLUDE_LIST, newTableIncludeListConf);

        //message.key.columns
        String newMessageKeyColumnsConf = ConnectorUtil.joinOn(
                CommonConstant.SEMICOLON,
                configMap.get(SourceConstant.MESSAGE_KEY_COLUMNS),
                ConnectorUtil.getMessageKeyColumns(database, table, pks)
        );
        configMap.put(SourceConstant.MESSAGE_KEY_COLUMNS, newMessageKeyColumnsConf);

        return configMap;
    }

    @Override
    public SourceConnectorInfoDTO getSourceDetail(final Long connectorId) {
        ConnectorDO connectorDO = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        SourceRdbTableDO sourceRdbTable = sourceRdbTableService.findByConnectorId(connectorId)
                .stream().findFirst().orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        RdbTableDO srcTable = sourceRdbTable.getRdbTable();
        KafkaTopicDO kafkaTopic = sourceRdbTable.getKafkaTopic();
        RdbDatabaseDO srcDbDatabase = srcTable.getRdbDatabase();
        RdbDO srcCluster = srcDbDatabase.getRdb();

        return SourceConnectorInfoDTO.builder()
                .connectorId(connectorId)
                .name(connectorDO.getName())
                .srcCluster(srcCluster.getName())
                .srcDatabase(srcDbDatabase.getName())
                .srcDatabaseId(srcDbDatabase.getId())
                .srcDataSet(srcTable.getName())
                .srcDataSystemType(DataSystemType.nameOf(srcCluster.getRdbType()).getName())
                .kafkaTopic(kafkaTopic.getName())
                .build();
    }

    abstract RdbInstanceDO checkOrInitDataSource(RdbDO rdb, RdbDatabaseDO rdbDatabase);

    abstract Map<String, String> fetchConfig(
            RdbDO rdb,
            RdbInstanceDO rdbInstance,
            RdbDatabaseDO rdbDatabase,
            RdbTableDO rdbTable,
            KafkaTopicDO kafkaTopic,
            List<String> uniqueKeys
    );
}
