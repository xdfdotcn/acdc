package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HiveDatabaseQuery {
    
    private Long id;
    
    private String name;
    
    private Boolean deleted = Boolean.FALSE;
}
