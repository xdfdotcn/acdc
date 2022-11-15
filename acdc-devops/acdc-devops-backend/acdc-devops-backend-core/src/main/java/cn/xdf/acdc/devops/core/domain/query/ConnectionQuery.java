package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectionQuery extends PagedQuery {

    private Instant beginUpdateTime;

    private Long connectionId;

    private DataSystemType sinkDataSystemType;

    private RequisitionState requisitionState;

    private List<Long> connectionIds;

    private String sourceProjectName;
}
