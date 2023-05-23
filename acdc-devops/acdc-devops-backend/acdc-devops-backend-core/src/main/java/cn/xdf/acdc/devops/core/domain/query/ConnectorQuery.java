package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectorQuery extends PagedQuery {
    
    private String name;
    
    private Date beginUpdateTime;
    
    private ConnectClusterDO connectCluster;
    
    private ConnectorState actualState;
    
    private ConnectorState desiredState;
    
}
