package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectionRequisitionDetailDTO {
    
    private Long id;
    
    private String sourceApproveResult;
    
    private String dbaApproveResult;
    
    private String sourceApproverEmail;
    
    private String dbaApproverEmail;
    
    private ApprovalState state;
    
    private String description;
    
    // todo: 移除域外属性
    private String baseUrl;
    
    private Date creationTime;
    
    private Date updateTime;
    
    private Set<String> connectionSinkProjectNames = new HashSet<>();
    
    private List<ConnectionDetailDTO> connections = new ArrayList<>();
    
    // 三方系统id
    private String thirdPartyId;
    
    public ConnectionRequisitionDetailDTO(final ConnectionRequisitionDO connectionRequisitionDO) {
        this.id = connectionRequisitionDO.getId();
        this.thirdPartyId = connectionRequisitionDO.getThirdPartyId();
        this.sourceApproveResult = connectionRequisitionDO.getSourceApproveResult();
        this.dbaApproveResult = connectionRequisitionDO.getDbaApproveResult();
        this.sourceApproverEmail = Objects.isNull(connectionRequisitionDO.getSourceApproverUser())
                ? SystemConstant.EMPTY_STRING : connectionRequisitionDO.getSourceApproverUser().getEmail();
        this.dbaApproverEmail = Objects.isNull(connectionRequisitionDO.getDbaApproverUser())
                ? SystemConstant.EMPTY_STRING : connectionRequisitionDO.getDbaApproverUser().getEmail();
        this.state = connectionRequisitionDO.getState();
        this.description = connectionRequisitionDO.getDescription();
        this.creationTime = connectionRequisitionDO.getCreationTime();
        this.updateTime = connectionRequisitionDO.getUpdateTime();
        
        connectionRequisitionDO.getConnectionRequisitionConnectionMappings().forEach(each -> {
            connections.add(new ConnectionDetailDTO(each.getConnection()));
        });
        
        connections.forEach(each -> connectionSinkProjectNames.add(each.getSinkProjectName()));
    }
    
    public ConnectionRequisitionDetailDTO(final String description, final List<ConnectionDetailDTO> connectionDetailDTOS) {
        this.state = ApprovalState.APPROVING;
        this.description = description;
        this.connections.addAll(connectionDetailDTOS);
        connectionDetailDTOS.forEach(each -> connectionSinkProjectNames.add(each.getSinkProjectName()));
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectionRequisitionDO.
     */
    public ConnectionRequisitionDO toDO() {
        ConnectionRequisitionDO connectionRequisitionDO = new ConnectionRequisitionDO();
        
        Set<ConnectionRequisitionConnectionMappingDO> mappings = new HashSet();
        this.connections.forEach(each -> {
            ConnectionRequisitionConnectionMappingDO mapping = new ConnectionRequisitionConnectionMappingDO();
            mapping.setConnection(each.toDO());
            mapping.setConnectionRequisition(connectionRequisitionDO);
            mapping.setConnectionVersion(each.getVersion());
            mappings.add(mapping);
        });
        
        connectionRequisitionDO.setConnectionRequisitionConnectionMappings(mappings);
        connectionRequisitionDO.setDescription(this.description);
        connectionRequisitionDO.setState(this.state);
        connectionRequisitionDO.setCreationTime(this.creationTime);
        connectionRequisitionDO.setUpdateTime(this.updateTime);
        return connectionRequisitionDO;
    }
}
