package cn.xdf.acdc.devops.service.process.widetable.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDataSystemResourceProjectMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableColumnDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDataSystemResourceProjectMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import cn.xdf.acdc.devops.repository.WideTableRepository;
import cn.xdf.acdc.devops.service.config.ACDCInnerProperties;
import cn.xdf.acdc.devops.service.config.ACDCWideTableProperties;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationConstant;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl.ConnectionColumnConfigurationGeneratorManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.requisition.DataSystemResourcePermissionRequisitionBatchService;
import cn.xdf.acdc.devops.service.process.widetable.IdGenerator;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import cn.xdf.acdc.devops.service.process.widetable.sql.SqlSelectNodeRecursion;
import cn.xdf.acdc.devops.service.process.widetable.sql.WideTableSqlService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kafka.connect.data.Schema;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WideTableServiceImpl implements WideTableService {
    
    public static final String IS_DELETED_FIELD_NAME = "__is_deleted";
    
    public static final String OFFSET_FIELD_NAME = "__event_offset";
    
    public static final Map<String, String> INNER_STARROCKS_SOURCE = new HashMap<>();
    
    public static final Map<String, DataFieldDefinition> INNER_STARROCKS_ADDITIONAL_FIELDS = new HashMap<>();
    
    static {
        INNER_STARROCKS_SOURCE.put("INNER_CONNECTION_TYPE", "STARROCKS_SOURCE");
        
        DataFieldDefinition isDeletedDefinition = new DataFieldDefinition(
                IS_DELETED_FIELD_NAME, ConnectionColumnConfigurationConstant.META_LOGICAL_DEL_TYPE, Schema.BOOLEAN_SCHEMA, false, null, new HashMap<>(), new HashSet<>()
        );
        INNER_STARROCKS_ADDITIONAL_FIELDS.put(IS_DELETED_FIELD_NAME, isDeletedDefinition);
        
        DataFieldDefinition eventOffsetDefinition = new DataFieldDefinition(
                OFFSET_FIELD_NAME, ConnectionColumnConfigurationConstant.META_KAFKA_RECORD_OFFSET_TYPE, Schema.INT64_SCHEMA, false, null, new HashMap<>(), new HashSet<>()
        );
        INNER_STARROCKS_ADDITIONAL_FIELDS.put(OFFSET_FIELD_NAME, eventOffsetDefinition);
    }
    
    @Autowired
    private ACDCWideTableProperties acdcWideTableProperties;
    
    @Autowired
    private ACDCInnerProperties acdcInnerProperties;
    
    @Autowired
    private ConnectionService connectionService;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Autowired
    private ConnectionColumnConfigurationGeneratorManager connectionColumnConfigurationGeneratorManager;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private WideTableSqlService wideTableSqlService;
    
    @Autowired
    private WideTableRepository wideTableRepository;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private I18nService i18n;
    
    @Autowired
    private DataSystemResourcePermissionRequisitionBatchService dataSystemResourcePermissionRequisitionBatchService;
    
    public WideTableServiceImpl() {
    }
    
    @Transactional
    @Override
    public void createInnerConnectionIfNeeded(final Long id) {
        WideTableDO wideTableDO = getWideTableDOById(id);
        Set<WideTableDataSystemResourceProjectMappingDO> wideTableDataSystemResourceProjectMappings = wideTableDO.getWideTableDataSystemResourceProjectMappings();
        Set<ConnectionDO> connections = wideTableDataSystemResourceProjectMappings.stream().map(wideTableDataSystemResourceProjectMappingDO -> {
            DataSystemResourceDO dataSystemResource = wideTableDataSystemResourceProjectMappingDO.getDataSystemResource();
            ProjectDO project = wideTableDataSystemResourceProjectMappingDO.getProject();
            // create inner connection for source table if needed
            Long connectionId = createInnerConnectionForSourceTableIfNeeded(dataSystemResource.getId(), project.getId());
            return new ConnectionDO(connectionId);
        }).collect(Collectors.toSet());
        
        wideTableDO.setConnections(connections);
        wideTableRepository.save(wideTableDO);
    }
    
    @SneakyThrows
    private Long createInnerConnectionForSourceTableIfNeeded(final Long sourceTableDataSystemResourceId, final Long projectId) {
        String specificConfiguration = objectMapper.writeValueAsString(INNER_STARROCKS_SOURCE);
        
        // 1. 查询是否有connection，有则返回
        List<ConnectionDTO> existConnection = connectionService.query(getConnectionQuery(sourceTableDataSystemResourceId, specificConfiguration));
        if (!CollectionUtils.isEmpty(existConnection)) {
            // todo 暂时不考虑内部connection停止的情况
            return existConnection.get(0).getId();
        }
        
        // 2. 创建inner connection (建立内部表，建立链路)
        // 2.1 创建表
        DataSystemResourceDetailDTO sourceTable = dataSystemResourceService.getDetailById(sourceTableDataSystemResourceId);
        DataSystemMetadataService sourceDataSystemMetadataService = dataSystemServiceManager.getDataSystemMetadataService(sourceTable.getDataSystemType());
        DataCollectionDefinition sourceTableDefinition = sourceDataSystemMetadataService.getDataCollectionDefinition(sourceTableDataSystemResourceId);
        
        DataSystemResourceDTO database = dataSystemResourceService.getChildren(acdcWideTableProperties.getStarrocksClusterId(), DataSystemResourceType.STARROCKS_DATABASE)
                .stream()
                .filter(dataSystemResourceDTO -> acdcWideTableProperties.getDatabaseName().equals(dataSystemResourceDTO.getName()))
                .findFirst()
                .orElseThrow(() -> new ServerErrorException("Could not find inner starrocks database: wide_table."));
        sourceTableDefinition.getLowerCaseNameToDataFieldDefinitions().putAll(INNER_STARROCKS_ADDITIONAL_FIELDS);
        
        DataSystemMetadataService starrocksdataSystemMetadataService = dataSystemServiceManager.getDataSystemMetadataService(DataSystemType.STARROCKS);
        DataSystemResourceDTO sinkTable = starrocksdataSystemMetadataService.createDataCollectionByDataDefinition(database.getId(), getInnerStarrocksTableName(sourceTable), sourceTableDefinition);
        
        // 2.2 创建并启动connection
        ConnectionDetailDTO connection = getConnection(projectId, specificConfiguration, sourceTable, sinkTable);
        final List<ConnectionDetailDTO> connections = connectionService.batchCreate(Lists.newArrayList(connection), acdcInnerProperties.getUserDomainAccount());
        return connections.get(0).getId();
    }
    
    private ConnectionDetailDTO getConnection(final Long projectId, final String specificConfiguration, final DataSystemResourceDetailDTO sourceTable, final DataSystemResourceDTO sinkTable) {
        ConnectionDetailDTO connection = new ConnectionDetailDTO()
                .setSourceDataSystemType(sourceTable.getDataSystemType())
                .setSourceProjectId(projectId)
                .setSourceDataCollectionId(sourceTable.getId())
                .setSinkDataSystemType(DataSystemType.STARROCKS)
                .setSinkProjectId(acdcInnerProperties.getProjectId())
                .setSinkDataCollectionId(sinkTable.getId())
                .setSpecificConfiguration(specificConfiguration)
                .setVersion(ConnectionDO.DEFAULT_CONNECTION_VERSION)
                .setRequisitionState(RequisitionState.APPROVED)
                .setActualState(ConnectionState.STOPPED)
                .setDesiredState(ConnectionState.RUNNING);
        
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations =
                connectionColumnConfigurationGeneratorManager.generateConnectionColumnConfiguration(sourceTable.getId(), sinkTable.getId());
        connectionColumnConfigurations = connectionColumnConfigurations.stream().peek(column -> {
                    if (IS_DELETED_FIELD_NAME.equals(column.getSinkColumnName())) {
                        column.setSourceColumnName(ConnectionColumnConfigurationConstant.META_LOGICAL_DELETION);
                        column.setSourceColumnType(ConnectionColumnConfigurationConstant.META_LOGICAL_DEL_TYPE);
                    }
                    if (OFFSET_FIELD_NAME.equals(column.getSinkColumnName())) {
                        column.setSourceColumnName(ConnectionColumnConfigurationConstant.META_KAFKA_RECORD_OFFSET);
                        column.setSourceColumnType(ConnectionColumnConfigurationConstant.META_KAFKA_RECORD_OFFSET_TYPE);
                    }
                    column.setId(null);
                    column.setRowId(null);
                }
        ).filter(column -> Strings.isNotBlank(column.getSourceColumnName())).collect(Collectors.toList());
        connection.setConnectionColumnConfigurations(connectionColumnConfigurations);
        return connection;
    }
    
    private ConnectionQuery getConnectionQuery(final Long sourceTableDataSystemResourceId, final String specificConfiguration) {
        return new ConnectionQuery()
                .setSourceDataCollectionId(sourceTableDataSystemResourceId)
                .setSpecificConfiguration(specificConfiguration)
                .setDeleted(false);
    }
    
    private String getInnerStarrocksTableName(final DataSystemResourceDetailDTO table) {
        return table.getName() + Symbol.UNDERLINE + table.getId();
    }
    
    @Transactional
    @Override
    public void updateActualState(final Long id, final WideTableState newActualState) {
        WideTableDO wideTableDO = getWideTableDOById(id);
        wideTableDO.setActualState(newActualState);
        wideTableRepository.save(wideTableDO);
    }
    
    @Transactional
    @Override
    public void updateRequisitionState(final Long id, final RequisitionState requisitionState) {
        WideTableDO wideTableDO = getWideTableDOById(id);
        wideTableDO.setRequisitionState(requisitionState);
        wideTableRepository.save(wideTableDO);
    }
    
    @Transactional
    @Override
    public List<WideTableDTO> query(final WideTableQuery wideTableQuery) {
        return wideTableRepository.query(wideTableQuery).stream().map(WideTableDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<WideTableDetailDTO> queryDetail(final WideTableQuery wideTableQuery) {
        return wideTableRepository.query(wideTableQuery).stream().map(WideTableDetailDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public void updateBatchId(final Long id, final Long batchId) {
        WideTableDO wideTableDO = getWideTableDOById(id);
        wideTableDO.setRequisitionBatch(new DataSystemResourcePermissionRequisitionBatchDO().setId(batchId));
        wideTableRepository.save(wideTableDO);
    }
    
    private WideTableDO getWideTableDOById(final Long id) {
        Optional<WideTableDO> optionalWideTableDO = wideTableRepository.findById(id);
        return optionalWideTableDO.orElseThrow(
                () -> new ServerErrorException(String.format("WideTable could not be found with id: %s.", id))
        );
    }
    
    @Transactional
    @Override
    public WideTableDetailDTO beforeCreation(final WideTableDetailDTO wideTableDetail) {
        WideTableSubqueryDO wideTableSubqueryDO = generateSubQuery(
                new IdGenerator.MemoryIdGenerator(),
                wideTableDetail.getSelectStatement(),
                wideTableDetail.getDataCollectionIdProjectIdMappings().keySet()
        );
        
        return new WideTableDetailDTO(wideTableDetail.toDO().setSubquery(wideTableSubqueryDO));
    }
    
    @Transactional
    @Override
    public WideTableDetailDTO create(final WideTableDetailDTO wideTableDetail) {
        boolean existsByNameAndProjectId = wideTableRepository.existsByNameAndProjectId(wideTableDetail.getName(), wideTableDetail.getProjectId());
        if (existsByNameAndProjectId) {
            throw new EntityExistsException(String.format("Wide table exist with name: %s, project_id: %s.", wideTableDetail.getName(), wideTableDetail.getProjectId()));
        }
        
        WideTableSubqueryDO wideTableSubqueryDO = generateSubQuery(
                new IdGenerator.DbIdGenerator(),
                wideTableDetail.getSelectStatement(),
                wideTableDetail.getDataCollectionIdProjectIdMappings().keySet()
        );
        
        WideTableDO wideTableDO = wideTableDetail.toDO()
                .setSubquery(wideTableSubqueryDO)
                .setRequisitionState(RequisitionState.APPROVING)
                .setActualState(WideTableState.DISABLED)
                .setDesiredState(WideTableState.READY);
        
        Set<WideTableColumnDO> wideTableColumns = wideTableDetail.getWideTableColumns()
                .stream()
                .map(wideTableColumnDTO -> wideTableColumnDTO.toDO().setWideTable(wideTableDO))
                .collect(Collectors.toSet());
        wideTableDO.setWideTableColumns(wideTableColumns);
        return new WideTableDetailDTO(wideTableRepository.save(wideTableDO));
    }
    
    @Transactional
    @Override
    public WideTableDetailDTO getDetailById(final Long wideTableId) {
        return new WideTableDetailDTO(wideTableRepository.getOne(wideTableId));
    }
    
    @Transactional
    @Override
    public Page<WideTableDTO> pagedQuery(final WideTableQuery query) {
        return wideTableRepository.pagedQuery(query).map(WideTableDTO::new);
    }
    
    @Transactional
    @Override
    public void disable(final Long wideTableId) {
        WideTableDO wideTableDO = getWideTableDOById(wideTableId);
        wideTableDO.setDesiredState(WideTableState.DISABLED);
        wideTableRepository.save(wideTableDO);
    }
    
    @Transactional
    @Override
    public void enable(final Long wideTableId) {
        WideTableDO wideTableDO = getWideTableDOById(wideTableId);
        wideTableDO.setDesiredState(WideTableState.READY);
        wideTableRepository.save(wideTableDO);
    }
    
    @Transactional
    @Override
    public List<ConnectionDTO> getConnectionsByWideTableId(final Long wideTableId) {
        WideTableDO wideTableDO = getWideTableDOById(wideTableId);
        Set<ConnectionDO> connections = wideTableDO.getConnections();
        if (connections == null) {
            return new ArrayList<>();
        }
        return connections.stream()
                .map(ConnectionDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public DataSystemResourcePermissionRequisitionBatchDetailDTO getRequisitionByWideTableId(final Long wideTableId) {
        WideTableDO wideTableDO = getWideTableDOById(wideTableId);
        if (wideTableDO.getRequisitionBatch() == null) {
            return null;
        }
        return new DataSystemResourcePermissionRequisitionBatchDetailDTO(wideTableDO.getRequisitionBatch());
    }
    
    protected WideTableSubqueryDO generateSubQuery(
            final IdGenerator idGenerator,
            final String sql,
            final Set<Long> tableResourceIds
    ) {
        Map<String, Map<String, DataFieldDefinition>> dataFieldDefinitionTable = new HashMap<>();
        Map<String, DataSystemResourceDTO> dataSystemResourceMap = new HashMap<>();
        Map<String, List<DataFieldDefinition>> validateDataFieldDefinitionMap = new HashMap<>();
        Map<String, Map<String, List<DataFieldDefinition>>> validateDataFieldDefinitionTable = new HashMap<>();
        for (Long resourceId : tableResourceIds) {
            DataSystemType sourceDataSystemType = dataSystemResourceService.getDataSystemType(resourceId);
            DataSystemResourceDTO dataSystemResource = dataSystemResourceService.getById(resourceId);
            String tableName = dataSystemResource.getName() + SystemConstant.Symbol.UNDERLINE + dataSystemResource.getId();
            
            DataCollectionDefinition sourceDataCollectionDefinition = dataSystemServiceManager
                    .getDataSystemMetadataService(sourceDataSystemType)
                    .getDataCollectionDefinition(resourceId);
            
            dataFieldDefinitionTable.put(
                    tableName,
                    sourceDataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions()
            );
            dataSystemResourceMap.put(tableName, dataSystemResource);
            
            validateDataFieldDefinitionMap.put(tableName,
                    new ArrayList<>(sourceDataCollectionDefinition
                            .getLowerCaseNameToDataFieldDefinitions().values())
            );
        }
        validateDataFieldDefinitionTable.put(SubQueryPersistenceSqlNodeHandler.DEFAULT_SCHEMA_NAME, validateDataFieldDefinitionMap);
        
        SqlNode sqlNode;
        try {
            sqlNode = wideTableSqlService.validate(validateDataFieldDefinitionTable, sql);
        } catch (CalciteException | SqlParseException e) {
            // String message = ExceptionUtils.getStackTrace(e);
            //String message = Throwables.getStackTraceAsString(e);
            String message = e.getMessage();
            throw new ClientErrorException(message);
            // CHECKSTYLE:OFF
        } catch (Exception e) {
            // CHECKSTYLE:ON
            throw new ClientErrorException(e.getMessage());
        }
        
        SqlSelectNodeRecursion recursion = SqlSelectNodeRecursion.create();
        WideTableSubqueryDO subQuery = recursion.recurse(
                sqlNode,
                new SubQueryPersistenceSqlNodeHandler(
                        idGenerator,
                        dataFieldDefinitionTable,
                        dataSystemResourceMap,
                        i18n
                )
        );
        
        return subQuery;
    }
    
    @Transactional
    @Override
    public void updateRequisitionStateToRefused(final Long wideTableId) {
        this.updateRequisitionState(wideTableId, RequisitionState.REFUSED);
    }
    
    @Transactional
    @Override
    public void createDataSystemResourceAndUpdateWideTable(final Long wideTableId) {
        WideTableDetailDTO wideTableDetail = this.getDetailById(wideTableId);
        if (wideTableDetail.getDataCollectionId() == null) {
            DataSystemResourceDetailDTO dataSystemResourceDetailDTO = new DataSystemResourceDetailDTO()
                    .setName(wideTableDetail.getName())
                    .setDataSystemType(DataSystemType.ACDC_WIDE_TABLE)
                    .setResourceType(DataSystemResourceType.ACDC_WIDE_TABLE)
                    .setProjects(Lists.newArrayList(new ProjectDTO(wideTableDetail.getProjectId())));
            WideTableDO wideTableDO = getWideTableDOById(wideTableId);
            wideTableDO.setDataCollection(dataSystemResourceDetailDTO.toDO());
            wideTableRepository.save(wideTableDO);
        }
        
        this.updateRequisitionState(wideTableId, RequisitionState.APPROVED);
    }
    
    @Transactional
    @Override
    public void createWideTableRequisitionBatch(final WideTableDTO wideTableDTO) {
        Map<Long, Long> dataSystemResourceIdProjectIdMap = wideTableDTO.getWideTableDataSystemResourceProjectMappings().stream().collect(
                Collectors.toMap(WideTableDataSystemResourceProjectMappingDTO::getDataSystemResourceId, WideTableDataSystemResourceProjectMappingDTO::getProjectId)
        );
        
        DataSystemResourcePermissionRequisitionBatchDetailDTO requisition = this.getRequisitionByWideTableId(wideTableDTO.getId());
        if (requisition == null) {
            Long batchId = dataSystemResourcePermissionRequisitionBatchService.create(
                    wideTableDTO.getUserId(),
                    wideTableDTO.getDescription(),
                    wideTableDTO.getSinkProjectId(),
                    dataSystemResourceIdProjectIdMap
            );
            this.updateBatchId(wideTableDTO.getId(), batchId);
        }
    }
    
    @Transactional
    @Override
    public void updateWideTableActualState(final Long id, final WideTableState newActualState) {
        this.updateActualState(id, newActualState);
    }
    
    @Transactional
    @Override
    public void startInnerConnectionAndUpdateActualStateToLoading(final Long wideTableId) {
        // 建立并启动所有依赖的connection
        if (!this.getConnectionsByWideTableId(wideTableId).isEmpty()) {
            return;
        }
        this.createInnerConnectionIfNeeded(wideTableId);
        this.updateActualState(wideTableId, WideTableState.LOADING);
    }
}
