package cn.xdf.acdc.devops.core.domain.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectionEditDTO {

    private Long connectionId;

    private List<FieldMappingDTO> fieldMappings;
}
