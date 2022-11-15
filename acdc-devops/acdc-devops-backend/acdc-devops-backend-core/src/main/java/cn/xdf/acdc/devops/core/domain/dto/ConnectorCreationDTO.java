package cn.xdf.acdc.devops.core.domain.dto;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

// TODO: 考虑转化为 DetailDTO
/**
 * 申请链路.
 * <p>
 * 1. Info 扁平化信息,前端表单信息
 * <p>
 * 2. Detail 关联查询,详细信息
 * <p>
 * 3. 其他: 代表具体的业务含义
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorCreationDTO {

    private DataSystemType sourceDataSystemType;

    private DataSystemType sinkDataSystemType;

    // source dataset
    private DataSetDTO sourceDataset;

    // sink dataset
    private DataSetDTO sinkDataset;

    // field mapping
    private List<FieldMappingDTO> fieldMappings;

    public SourceCreationDTO toSourceCreationDTO() {
        return SourceCreationDTO.builder()
                .clusterId(sourceDataset.getClusterId())
                .databaseId(sourceDataset.getDatabaseId())
                .tableId(sourceDataset.getDataSetId())
                .primaryFields(FieldMappingDTO.getSourcePrimaryFields(this.fieldMappings))
                .build();
    }

    public SinkCreationDTO toSinkCreationDTO() {
        return SinkCreationDTO.builder()
                .clusterId(sinkDataset.getClusterId())
                .databaseId(sinkDataset.getDatabaseId())
                .instanceId(sinkDataset.getInstanceId())
                .dataSetId(sinkDataset.getDataSetId())
                .fieldMappingList(fieldMappings)
                .specificConfiguration(sinkDataset.getSpecificConfiguration())
                .build();
    }
}
