package cn.xdf.acdc.devops.core.domain.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SinkConnectorEditDTO {

    private Long connectorId;

    // field mapping
    private List<FieldMappingDTO> fieldMappings;

}
