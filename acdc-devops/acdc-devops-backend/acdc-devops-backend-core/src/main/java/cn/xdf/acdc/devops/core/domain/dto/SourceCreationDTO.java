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
public class SourceCreationDTO {

    // 集群ID
    private Long clusterId;

    // 数据库ID
    private Long databaseId;

    // 表ID
    private Long tableId;

    private List<FieldDTO> primaryFields;
}
