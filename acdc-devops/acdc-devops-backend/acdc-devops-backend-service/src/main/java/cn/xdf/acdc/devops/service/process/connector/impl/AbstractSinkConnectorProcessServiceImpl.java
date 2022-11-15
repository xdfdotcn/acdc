package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO.LogicalDelDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkConstant;
import cn.xdf.acdc.devops.service.constant.connector.SinkJdbcConstant;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorDataExtensionService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorColumnMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorDataExtensionMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.process.connector.SinkConnectorProcessService;
import cn.xdf.acdc.devops.service.util.ConnectorUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

@Transactional
public abstract class AbstractSinkConnectorProcessServiceImpl extends AbstractConnectorConfigProcessServiceImpl implements SinkConnectorProcessService {

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private ConnectorDataExtensionService connectorDataExtensionService;

    @Autowired
    private SinkConnectorDataExtensionMappingService sinkConnectorDataExtensionMappingService;

    @Autowired
    private SinkConnectorColumnMappingService sinkConnectorColumnMappingService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Autowired
    private ConnectorService connectorService;

    protected void setFilterExpression(
        final String table,
        final String expression,
        final Map<String, String> configMap) {
        if (!Strings.isNullOrEmpty(expression)) {
            configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.ROW_FILTER, expression);
        }
    }

    protected void setConfFieldAdd(
        final String table,
        final List<ConnectorDataExtensionDO> extensions,
        final Map<String, String> configMap
    ) {

        if (CollectionUtils.isEmpty(extensions)) {
            return;
        }

        List<String> sinkFieldAdds = extensions.stream()
            .map(field -> new StringBuilder()
                .append(field.getName())
                .append(CommonConstant.COLON)
                .append(field.getValue())
                .toString()).collect(Collectors.toList());

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.FIELDS_ADD,
            ConnectorUtil.joinOnComma(sinkFieldAdds));
    }

    protected void setConfFieldMapping(
        final String table,
        final List<SinkConnectorColumnMappingDO> fieldMappings,
        final Map<String, String> configMap
    ) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldMappings));
        List<String> sinkFieldMappings = fieldMappings.stream()
            .filter(it -> !FieldMappingDTO.META_FIELD_FILTER_SET.contains(FieldMappingDTO.formatToField(it.getSourceColumnName()).getName()))
            .map(mapping -> new StringBuilder()
                .append(FieldMappingDTO.formatToField(mapping.getSourceColumnName()).getName())
                .append(CommonConstant.COLON)
                .append(FieldMappingDTO.formatToField(mapping.getSinkColumnName()).getName())
                .toString()).collect(Collectors.toList());

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.FIELDS_MAPPING,
            ConnectorUtil.joinOnComma(sinkFieldMappings));
    }

    protected void setConfFieldWhitelist(
        final String table,
        final List<SinkConnectorColumnMappingDO> fieldMappings,
        final Map<String, String> configMap) {

        Preconditions.checkArgument(!CollectionUtils.isEmpty(fieldMappings));

        List<String> sourceFields = fieldMappings.stream()
            // 白名单: 使用source字段,排除 source 增加的元数据字段
            .filter(it -> !FieldMappingDTO.META_FIELD_LIST.contains(FieldMappingDTO.formatToField(it.getSourceColumnName()).getName()))
            .map(it -> FieldMappingDTO.formatToField(it.getSourceColumnName()).getName()).collect(Collectors.toList());

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.FIELDS_WHITELIST,
            ConnectorUtil.joinOnComma(sourceFields));
    }

    protected void setConfLogicalDel(
        final String table,
        final String column,
        final String deletionValue,
        final String normaValue,
        final Map<String, String> configMap

    ) {
        // 没有配置逻辑删除
        if (Strings.isNullOrEmpty(column)) {
            return;
        }

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkJdbcConstant.TABLES_CONFIG_DELETE_LOGICAL_FIELD_NAME,
            column
        );

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkJdbcConstant.TABLES_CONFIG_DELETE_LOGICAL_FIELD_VALUE_DELETED,
            deletionValue
        );

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkJdbcConstant.TABLES_CONFIG_DELETE_LOGICAL_FIELD_VALUE_NORMAL,
            normaValue
        );

        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.DESTINATIONS_CONFIG_DELETE_MODE,
            SinkConstant.DESTINATIONS_CONFIG_DELETE_MODE_VALUE
        );
    }

    protected void setConfLogicalDel(
        final String table,
        final Map<String, String> configMap

    ) {
        configMap.put(SinkConstant.DESTINATIONS_CONFIG_PREFIX + table + SinkConstant.DESTINATIONS_CONFIG_DELETE_MODE,
            SinkConstant.DESTINATIONS_CONFIG_DELETE_MODE_VALUE_NONE
        );
    }

    protected void saveConfig(
        final Long connectorId,
        final Map<String, String> configMap) {
        connectorConfigurationService.saveConfig(connectorId, configMap);
    }

    @Override
    public void editFieldMapping(
        final Long connectorId,
        final List<FieldMappingDTO> fieldMappings) {
        SinkConnectorDO sinkConnector = sinkConnectorService.findByConnectorId(connectorId)
            .orElseThrow(() -> new NotFoundException(String.format("clusterId: %s", connectorId)));

        List<Long> dataExtensionIds = sinkConnectorDataExtensionMappingService
            .findBySinkConnectorId(sinkConnector.getId()).stream()
            .map(it -> it.getConnectorDataExtension().getId()).collect(Collectors.toList());

        // 删除字段映射
        sinkConnectorColumnMappingService.deleteBySinkConnectorId(sinkConnector.getId());

        // 删除扩展字段
        connectorDataExtensionService.deleteByIdIn(dataExtensionIds);
        sinkConnectorDataExtensionMappingService.deleteBySinkConnectorId(sinkConnector.getId());

        // 更新过滤条件
        String filterExpress = FieldMappingDTO.findRowFilterExpress(fieldMappings).orElse(null);
        sinkConnector.setFilterExpression(filterExpress);
        sinkConnector.setUpdateTime(Instant.now());
        sinkConnectorService.save(sinkConnector);

        // 更新扩展字段和字段映射
        List<ConnectorDataExtensionDO> dataExtensions = FieldMappingDTO.toConnectorDataExtensionList(fieldMappings);
        List<SinkConnectorColumnMappingDO> columnMappings = FieldMappingDTO.toSinkColumnMappingList(fieldMappings);
        LogicalDelDTO logicalDel = FieldMappingDTO.findLogicalDelColumn(fieldMappings).orElse(new LogicalDelDTO());
        sinkConnectorService.saveExtensionsAndColumnMappings(sinkConnector.getId(), dataExtensions, columnMappings);

        // 更新 connector
        ConnectorDO toEditConnector = sinkConnector.getConnector();
        toEditConnector.setUpdateTime(Instant.now());
        connectorService.save(toEditConnector);

        // 子类实现特殊处理逻辑
        doEditFieldMapping(sinkConnector, filterExpress, dataExtensions, columnMappings, logicalDel);

        // 所有子类都应该执行刷新配置的逻辑
        flushConfigWhenEdit(sinkConnector, filterExpress, dataExtensions, columnMappings, logicalDel);
    }

    protected abstract void flushConfigWhenEdit(
        SinkConnectorDO sinkConnector,
        String filterExpress,
        List<ConnectorDataExtensionDO> extensions,
        List<SinkConnectorColumnMappingDO> columnMappings,
        LogicalDelDTO logicalDel
    );

    protected abstract void doEditFieldMapping(
        SinkConnectorDO sinkConnector,
        String filterExpress,
        List<ConnectorDataExtensionDO> dataExtensions,
        List<SinkConnectorColumnMappingDO> columnMappings,
        LogicalDelDTO logicalDel
    );
}
