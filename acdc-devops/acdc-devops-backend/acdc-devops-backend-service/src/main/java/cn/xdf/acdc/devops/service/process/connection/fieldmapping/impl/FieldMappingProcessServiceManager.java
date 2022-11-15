package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;
import cn.xdf.acdc.devops.service.entity.ConnectionColumnConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectionService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorColumnMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingProcessService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldMappingProcessServiceManager implements FieldMappingProcessService {

    private static final Map<DataSystemType, FieldService> FIELD_SERVICE_MAP = Maps.newHashMap();

    private static final HashBasedTable<DataSystemType, DataSystemType, FieldMappingService> FIELD_MAPPING_SERVICE_MAP = HashBasedTable.create();

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private SinkConnectorColumnMappingService sinkConnectorColumnMappingService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConnectionColumnConfigurationService connectionColumnConfigurationService;

    public FieldMappingProcessServiceManager(
            final List<FieldService> fieldServices,
            final List<FieldMappingService> fieldMappingServices) {

        init(fieldServices, fieldMappingServices);
    }

    //为了单元测试
    void init(final List<FieldService> fieldServices, final List<FieldMappingService> fieldMappingServices) {
        // 字段处理 service
        fieldServices.forEach(it -> initFieldServices(it.supportAppTypes(), it));
        // 字段映射 service
        fieldMappingServices.forEach(it -> initFieldMappingServices(it.supportSrcAppTypes(), it.supportSinkAppTypes(), it));
    }

    private void initFieldServices(final Set<DataSystemType> supportDataSystemTypes, final FieldService fieldService) {
        supportDataSystemTypes.forEach(it -> FIELD_SERVICE_MAP.put(it, fieldService));
    }

    private void initFieldMappingServices(
            final Set<DataSystemType> supportSrcDataSystemTypes,
            final Set<DataSystemType> supportSinkDataSystemTypes,
            final FieldMappingService fieldMappingService
    ) {
        // O(N^2) 笛卡尔, 同一个FieldMappingService, 需要支持多种类型的 source 和 sink 的组合
        for (DataSystemType supportSrcDataSystemType : supportSrcDataSystemTypes) {
            for (DataSystemType supportSinkDataSystemType : supportSinkDataSystemTypes) {
                FIELD_MAPPING_SERVICE_MAP.put(supportSrcDataSystemType, supportSinkDataSystemType, fieldMappingService);
            }
        }
    }

    /**
     * 拉取字段映射.
     *
     * @param srcDataSet  srcDataSet
     * @param sinkDataSet sinkDataSet
     * @return List
     */
    public List<FieldMappingDTO> fetchFieldMapping(
            final DataSetDTO srcDataSet,
            final DataSetDTO sinkDataSet) {

        // 1. get field list
        Map<String, FieldDTO> srcFieldMap = FIELD_SERVICE_MAP.get(srcDataSet.getDataSystemType()).descDataSet(srcDataSet);
        Map<String, FieldDTO> sinkFieldMap = FIELD_SERVICE_MAP.get(sinkDataSet.getDataSystemType()).descDataSet(sinkDataSet);

        // 2. diffing and sort
        return FIELD_MAPPING_SERVICE_MAP.get(srcDataSet.getDataSystemType(), sinkDataSet.getDataSystemType())
                .diffingField(srcFieldMap, sinkFieldMap);
    }

    @Override
    public List<FieldMappingDTO> getFieldMapping4Connector(final Long connectorId) {

        ConnectorDO connector = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        ConnectorClassDO connectorClass = connector.getConnectorClass();
        DataSystemType dataSystemType = connectorClass.getDataSystemType();

        Preconditions.checkArgument(ConnectorType.SINK.equals(connectorClass.getConnectorType()));

        SinkConnectorDO sinkConnector = sinkConnectorService.findByConnectorId(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        String filterExpression = sinkConnector.getFilterExpression();
        List<SinkConnectorColumnMappingDO> columnMappings = sinkConnectorColumnMappingService
                .findBySinkConnectorId(sinkConnector.getId());

        // TODO 兼容老版本的Connector 的编辑和详情, 新版本修改 connection即可
        Map<String, String> fieldFilterMap = FieldMappingDTO.convertRowFilterExpressToMap(filterExpression);

        return columnMappings.stream().map(it -> {
            FieldDTO sinkField = FieldMappingDTO.formatToField(it.getSinkColumnName());
            FieldDTO sourceField = FieldMappingDTO.formatToField(it.getSourceColumnName());
            String rowFilterExpress = fieldFilterMap.get(sourceField.getName());
            FieldMappingDTO fieldMapping = FieldMappingDTO.builder()
                    .id(it.getId())
                    .sinkField(sinkField)
                    .sourceField(sourceField)
                    .matchStatus(FieldMappingDTO.IS_MATCH)
                    .build();
            if (!Strings.isNullOrEmpty(rowFilterExpress)) {
                List<String> splitList = Splitter.on(FieldMappingDTO.BLANK).splitToList(fieldFilterMap.get(sourceField.getName()));
                fieldMapping.setFilterOperator(splitList.get(1));
                fieldMapping.setFilterValue(splitList.get(2));
            }
            return fieldMapping;
        }).sorted(Comparator.comparing(it -> FIELD_MAPPING_SERVICE_MAP.get(DataSystemType.MYSQL, dataSystemType).editSequence(it))
        ).collect(Collectors.toList());
    }

    @Override
    public List<FieldMappingDTO> getFieldMapping4Connection(final Long connectionId) {
        ConnectionDO connection = connectionService.findById(connectionId)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connectionId)));

        DataSystemType sourceDataSystemType = connection.getSourceDataSystemType();
        DataSystemType sinkDataSystemType = connection.getSinkDataSystemType();

        ConnectionColumnConfigurationQuery query = ConnectionColumnConfigurationQuery.builder()
                .connectionId(connection.getId())
                .version(connection.getVersion())
                .build();

        List<FieldMappingDTO> fieldMappings = connectionColumnConfigurationService.query(query).stream()
                .map(FieldMappingDTO::toFieldMapping).collect(Collectors.toList());

        return fieldMappings.stream().sorted(Comparator.comparing(it -> FIELD_MAPPING_SERVICE_MAP.get(sourceDataSystemType, sinkDataSystemType).editSequence(it))
        ).collect(Collectors.toList());
    }

    /**
     * 获取 FieldService.
     *
     * @param dataSystemType appType
     * @return FieldService
     */
    public FieldService getFieldService(final DataSystemType dataSystemType) {
        return Optional.of(FIELD_SERVICE_MAP.get(dataSystemType)).get();
    }

    /**
     * 根据已经保存的字段映射配置，按照数据库中查询的最新的表结构重新生成字段映射关系.
     *
     * @param connectionId connection id
     * @return field mapping dto list
     */
    public List<FieldMappingDTO> getConnectionColumnConfigurationsMergedWithCurrentDdl(final Long connectionId) {
        ConnectionDO connection = connectionService.findById(connectionId)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connectionId)));

        DataSetDTO sourceDataSet = getDataSet(connection.getSourceDataSystemType(), connection.getSourceDataSetId());
        DataSetDTO sinkDataSet = getDataSet(connection.getSinkDataSystemType(), connection.getSinkDataSetId());
        // 获取最新上游表的所有字段 field name -> field
        Map<String, FieldDTO> sourceNameToFieldWithCurrentDdl = getFieldMapWithCurrentDdl(sourceDataSet);
        Map<String, FieldDTO> sinkNameToFieldWithCurrentDdl = getFieldMapWithCurrentDdl(sinkDataSet);
        // 获取数据库中保存的表结构的映射关系，表结构可能已经发生了变更
        List<FieldMappingDTO> existFieldMappings = getFieldMapping4Connection(connectionId);

        return FIELD_MAPPING_SERVICE_MAP.get(sourceDataSet.getDataSystemType(), sinkDataSet.getDataSystemType())
                .diffingField(sourceNameToFieldWithCurrentDdl, sinkNameToFieldWithCurrentDdl, existFieldMappings);
    }

    private Map<String, FieldDTO> getFieldMapWithCurrentDdl(final DataSetDTO dataSet) {
        Map<String, FieldDTO> sinkNameToFieldWithCurrentDdl = FIELD_SERVICE_MAP.get(dataSet.getDataSystemType()).descDataSet(dataSet);
        // 返回的map中key都转成了小写，这里需要获取原来的字段名称，故重新构建map
        return sinkNameToFieldWithCurrentDdl.values().stream().collect(Collectors.toMap(FieldDTO::getName, fieldDTO -> fieldDTO));
    }

    private DataSetDTO getDataSet(final DataSystemType dataSystemType, final Long dataSetId) {
        return DataSetDTO.builder().dataSystemType(dataSystemType).dataSetId(dataSetId).build();
    }
}
