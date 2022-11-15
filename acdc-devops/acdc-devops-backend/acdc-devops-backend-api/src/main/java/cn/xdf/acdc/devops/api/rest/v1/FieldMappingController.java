package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl.FieldMappingProcessServiceManager;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("api")
@Transactional
public class FieldMappingController {

    @Autowired
    private FieldMappingProcessServiceManager fieldMappingProcessServiceManager;

    /**
     * 根据上游和下游数据表id 生成字段映射.
     * @param srcDataSetId upTableId
     * @param sinkDataSetId downTableId
     * @param srcDataSystemType sourceDataSystemType
     * @param sinkDataSystemType  sinkDataSystemType
     * @return Page
     */
    @GetMapping("/fieldMappings")
    public PageDTO<FieldMappingDTO> fetchFieldMappings(
        final Long srcDataSetId,
        final Long sinkDataSetId,
        final DataSystemType srcDataSystemType,
        final DataSystemType sinkDataSystemType
    ) {
        if (QueryUtil.isNullId(srcDataSetId)
            || QueryUtil.isNullId(sinkDataSetId)
        ) {
            return PageDTO.empty();
        }

        DataSetDTO srcDataSet = DataSetDTO.builder().dataSystemType(srcDataSystemType).dataSetId(srcDataSetId).build();
        DataSetDTO sinkDataSet = DataSetDTO.builder().dataSystemType(sinkDataSystemType).dataSetId(sinkDataSetId).build();

        List<FieldMappingDTO> fieldMappings = fieldMappingProcessServiceManager.fetchFieldMapping(srcDataSet, sinkDataSet);
        return PageDTO.of(fieldMappings, Long.valueOf(fieldMappings.size()));
    }

    /**
     * 获取表字段信息.
     * @param dataSetId dataSetId
     * @return List
     */
    @GetMapping("/fieldMappings/{dataSetId}/dataSetFields")
    public List<String> fetchFields(@PathVariable("dataSetId") final Long dataSetId) {

        // TODO 目前source 只有 tidb 和mysql, 暂时不用前端传入
        DataSystemType dataSystemType = DataSystemType.MYSQL;
        DataSetDTO dataSet = DataSetDTO.builder().dataSetId(dataSetId).dataSystemType(dataSystemType).build();

        return fieldMappingProcessServiceManager.getFieldService(dataSystemType)
            .descDataSet(dataSet).values().stream()
            .map(FieldMappingDTO::formatToString)
            .collect(Collectors.toList());
    }

    /**
     * 查询 sink connector 字段映射.
     * @param connectorId connectorId
     * @return Page
     */
    @GetMapping("/fieldMappings/connectors/{connectorId}")
    public PageDTO<FieldMappingDTO> getConnectorFieldMapping(
        @PathVariable("connectorId") final Long connectorId
    ) {
        if (QueryUtil.isNullId(connectorId)) {
            return PageDTO.empty();
        }

        List<FieldMappingDTO> fieldMappings = fieldMappingProcessServiceManager.getFieldMapping4Connector(connectorId);
        return PageDTO.of(fieldMappings, Long.valueOf(fieldMappings.size()));
    }

    /**
     * 查询 connection 字段映射.
     * @param connectionId connectorId
     * @return Page
     */
    @GetMapping("/connections/{connectionId}/connection-column-configurations")
    public PageDTO<FieldMappingDTO> getConnectionFieldMapping(
        @PathVariable("connectionId") final Long connectionId
    ) {
        if (QueryUtil.isNullId(connectionId)) {
            return PageDTO.empty();
        }

        List<FieldMappingDTO> fieldMappings = fieldMappingProcessServiceManager.getFieldMapping4Connection(connectionId);
        return PageDTO.of(fieldMappings, Long.valueOf(fieldMappings.size()));
    }

    /**
     * todo
     * 查询 connection 字段映射.
     * @param connectionId connectorId
     * @return Page
     */
    @GetMapping("/connections/{connectionId}/connection-column-configurations-merged-with-current-ddl")
    public PageDTO<FieldMappingDTO> getConnectionColumnConfigurationsMergedWithCurrentDdl(
            @PathVariable("connectionId") final Long connectionId) {
        if (QueryUtil.isNullId(connectionId)) {
            return PageDTO.empty();
        }

        List<FieldMappingDTO> fieldMappings = fieldMappingProcessServiceManager
                .getConnectionColumnConfigurationsMergedWithCurrentDdl(connectionId);
        return PageDTO.of(fieldMappings, fieldMappings.size());
    }
}
