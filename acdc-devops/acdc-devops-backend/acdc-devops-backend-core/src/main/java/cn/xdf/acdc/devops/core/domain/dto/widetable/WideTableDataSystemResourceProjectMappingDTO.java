package cn.xdf.acdc.devops.core.domain.dto.widetable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class WideTableDataSystemResourceProjectMappingDTO {
    
    private Long id;
    
    private Long wideTableId;
    
    private Long dataSystemResourceId;
    
    private Long projectId;
}
