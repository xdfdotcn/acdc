package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalBatchState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 数据系统资源申请单集合.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class DataSystemResourcePermissionRequisitionBatchDTO {
    
    private Long id;
    
    private Set<Long> dataSystemResourcePermissionRequisitionIds;
    
    private ApprovalBatchState state;
    
    private Date updateTime;
    
    private boolean deleted;
    
    public DataSystemResourcePermissionRequisitionBatchDTO(final DataSystemResourcePermissionRequisitionBatchDO batchDO) {
        this.id = batchDO.getId();
        this.dataSystemResourcePermissionRequisitionIds = batchDO
                .getPermissionRequisitions()
                .stream()
                .map(DataSystemResourcePermissionRequisitionDO::getId)
                .collect(Collectors.toSet());
        this.state = batchDO.getState();
        this.updateTime = batchDO.getUpdateTime();
        this.deleted = false;
    }
}
