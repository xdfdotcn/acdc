package cn.xdf.acdc.devops.service.process.connection.fieldmapping;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

import java.util.List;

public interface FieldMappingProcessService {

    /**
     * 获取上下游数据集的字段映射.
     * @param srcDataSet 上游数据集
     * @param sinkDataSet 下游数据集
     * @return 字段映射
     */
    List<FieldMappingDTO> fetchFieldMapping(DataSetDTO srcDataSet, DataSetDTO sinkDataSet);

    /**
     * 获取 sink connector 字段映射.
     * @param connectorId connectorId
     * @return 字段映射
     */
    List<FieldMappingDTO> getFieldMapping4Connector(Long connectorId);

    /**
     * 获取 sink connector 字段映射.
     * @param connectionId connectorId
     * @return 字段映射
     */
    List<FieldMappingDTO> getFieldMapping4Connection(Long connectionId);

    /**
     * 获取 FieldService.
     * @param dataSystemType appType
     * @return 字段映射
     */
    FieldService getFieldService(DataSystemType dataSystemType);
}
