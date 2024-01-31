package cn.xdf.acdc.devops.core.domain.dto.widetable;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class WideTableDTO {
    
    private Long id;
    
    private String name;
    
    private String selectStatement;
    
    private String dataCollectionName;
    
    private WideTableState actualState;
    
    private WideTableState desiredState;
    
    private Set<Long> relatedConnectionIds;
    
    private Set<WideTableDataSystemResourceProjectMappingDTO> wideTableDataSystemResourceProjectMappings;
    
    private Long requisitionBatchId;
    
    private RequisitionState requisitionState;
    
    private Long userId;
    
    private Long sinkProjectId;
    
    private String description;
    
    private Date creationTime;
    
    private Date updateTime;
    
    private boolean deleted;
    
    public WideTableDTO(final WideTableDO wideTableDO) {
        this.id = wideTableDO.getId();
        this.name = wideTableDO.getName();
        this.selectStatement = wideTableDO.getSelectStatement();
        if (Objects.nonNull(wideTableDO.getDataCollection())) {
            this.dataCollectionName = wideTableDO.getDataCollection().getName();
        }
        this.actualState = wideTableDO.getActualState();
        this.desiredState = wideTableDO.getDesiredState();
        
        this.relatedConnectionIds = fillWithEmptyIfNull(wideTableDO.getConnections())
                .stream().map(ConnectionDO::getId).collect(Collectors.toSet());
        this.wideTableDataSystemResourceProjectMappings = fillWithEmptyIfNull(wideTableDO.getWideTableDataSystemResourceProjectMappings())
                .stream().map(mappings -> new WideTableDataSystemResourceProjectMappingDTO(
                        mappings.getId(),
                        mappings.getWideTable().getId(),
                        mappings.getDataSystemResource().getId(),
                        mappings.getProject().getId())
                ).collect(Collectors.toSet());
        
        this.requisitionBatchId = wideTableDO.getRequisitionBatch() == null ? null : wideTableDO.getRequisitionBatch().getId();
        this.requisitionState = wideTableDO.getRequisitionState();
        this.userId = wideTableDO.getUser().getId();
        this.sinkProjectId = wideTableDO.getProject().getId();
        this.description = wideTableDO.getDescription();
        this.creationTime = wideTableDO.getCreationTime();
        this.updateTime = wideTableDO.getUpdateTime();
        this.deleted = wideTableDO.getDeleted();
    }
    
    private <T> Set<T> fillWithEmptyIfNull(final Set<T> set) {
        if (set == null) {
            return new HashSet<>();
        }
        return set;
    }
}
