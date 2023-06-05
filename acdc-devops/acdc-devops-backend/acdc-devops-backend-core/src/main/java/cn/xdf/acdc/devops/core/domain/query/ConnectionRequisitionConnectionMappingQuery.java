package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConnectionRequisitionConnectionMappingQuery {
    
    private Long connectionRequisitionId;
    
    private Long connectionId;
    
}
