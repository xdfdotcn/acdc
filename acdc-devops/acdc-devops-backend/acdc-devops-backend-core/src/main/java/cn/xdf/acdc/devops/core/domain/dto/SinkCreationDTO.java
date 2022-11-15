package cn.xdf.acdc.devops.core.domain.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SinkCreationDTO {

    private Long clusterId;

    private Long instanceId;

    private Long databaseId;

    private Long dataSetId;

    private Long createdKafkaTopicId;

    private String specificConfiguration;

    private List<FieldMappingDTO> fieldMappingList;
}
