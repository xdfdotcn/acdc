package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionBatchDO;
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
public class DataSystemResourcePermissionRequisitionBatchDetailDTO {
    
    private Long id;
    
    private Set<DataSystemResourcePermissionRequisitionDTO> requisitions;
    
    private ApprovalBatchState state;
    
    private String domainAccount;
    
    private Date creationTime;
    
    private Date updateTime;
    
    private boolean deleted;
    
    public DataSystemResourcePermissionRequisitionBatchDetailDTO(final DataSystemResourcePermissionRequisitionBatchDO batchDO) {
        this.id = batchDO.getId();
        this.requisitions = batchDO
                .getPermissionRequisitions()
                .stream()
                .map(DataSystemResourcePermissionRequisitionDTO::new)
                .collect(Collectors.toSet());
        this.state = batchDO.getState();
        this.creationTime = batchDO.getCreationTime();
        this.updateTime = batchDO.getUpdateTime();
        this.domainAccount = batchDO.getUser().getDomainAccount();
        this.deleted = false;
    }
}