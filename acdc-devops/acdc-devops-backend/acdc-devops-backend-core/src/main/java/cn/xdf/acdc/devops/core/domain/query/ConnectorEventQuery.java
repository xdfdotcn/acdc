package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorEventQuery extends PagedQuery {
    
    public static final String SORT_FIELD = "creationTime";
    
    private Long id;
    
    private String reason;
    
    private String message;
    
    private String beginTime;
    
    private String endTime;
    
    private Integer source;
    
    private Integer level;
    
    private Long connectorId;
}
