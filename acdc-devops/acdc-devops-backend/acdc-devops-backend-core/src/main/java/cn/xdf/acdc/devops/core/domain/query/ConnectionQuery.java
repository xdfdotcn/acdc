package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectionQuery extends PagedQuery {
    
    private Date beginUpdateTime;
    
    private RequisitionState requisitionState;
    
    private ConnectionState actualState;
    
    private List<Long> connectionIds;
    
    private String domainAccount;
    
    private String sourceProjectName;
    
    private Long sourceConnectorId;
    
    private Long sourceDataCollectionId;
    
    private Set<Long> sinkDataCollectionIds;
    
    private DataSystemType sinkDataSystemType;
    
    private Long sinkConnectorId;
    
    private String sinkDataCollectionName;
    
    private String sourceDataCollectionName;
    
    private Boolean deleted;
}
