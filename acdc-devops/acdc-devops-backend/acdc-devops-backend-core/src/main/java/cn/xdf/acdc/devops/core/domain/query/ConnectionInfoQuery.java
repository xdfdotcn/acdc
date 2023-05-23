package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionInfoQuery extends PagedQuery {
    
    private Instant beginUpdateTime;
    
    private Long connectionId;
    
    private DataSystemType sinkDataSystemType;
    
    private RequisitionState requisitionState;
    
    private ConnectionState actualState;
    
    private List<Long> connectionIds;
    
    private String sinkDatasetClusterName;
    
    private String sinkDatasetDatabaseName;
    
    private String sinkDatasetName;
}
