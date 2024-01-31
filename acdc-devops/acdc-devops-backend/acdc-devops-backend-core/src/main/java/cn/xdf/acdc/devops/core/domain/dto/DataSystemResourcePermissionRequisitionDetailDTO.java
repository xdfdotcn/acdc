package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourcePermissionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据系统资源申请单集合.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class DataSystemResourcePermissionRequisitionDetailDTO {
    
    private Long id;
    
    private String thirdPartyId;
    
    private String description;
    
    private ApprovalState state;
    
    private Long userId;
    
    private String userDomainAccount;
    
    private String sinkProjectName;
    
    private String sinkProjectOwnerEmail;
    
    private String sourceProjectName;
    
    private List<DataSystemResourceDTO> dataSystemResources = new ArrayList<>();
    
    public DataSystemResourcePermissionRequisitionDetailDTO(
            final DataSystemResourcePermissionRequisitionDO dataSystemResourcePermissionRequisitionDO
    ) {
        this.id = dataSystemResourcePermissionRequisitionDO.getId();
        this.thirdPartyId = dataSystemResourcePermissionRequisitionDO.getThirdPartyId();
        this.description = dataSystemResourcePermissionRequisitionDO.getDescription();
        this.state = dataSystemResourcePermissionRequisitionDO.getState();
        this.userId = dataSystemResourcePermissionRequisitionDO.getUser().getId();
        this.userDomainAccount = dataSystemResourcePermissionRequisitionDO.getUser().getDomainAccount();
        this.sinkProjectName = dataSystemResourcePermissionRequisitionDO.getSinkProject().getName();
        this.sinkProjectOwnerEmail = dataSystemResourcePermissionRequisitionDO.getSinkProject().getOwner().getEmail();
        this.sourceProjectName = dataSystemResourcePermissionRequisitionDO.getSourceProject().getName();
        
        dataSystemResourcePermissionRequisitionDO.getDataSystemResources()
                .forEach(it -> dataSystemResources.add(new DataSystemResourceDTO(it)));
    }
}
