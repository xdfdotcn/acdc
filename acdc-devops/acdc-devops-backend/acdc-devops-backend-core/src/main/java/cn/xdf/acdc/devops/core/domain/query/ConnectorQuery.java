package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectorQuery extends PagedQuery {

    private String name;

    private Date beginUpdateTime;

    private ConnectClusterDO connectCluster;

    private ConnectorState actualState;

    private ConnectorState desiredState;

}
