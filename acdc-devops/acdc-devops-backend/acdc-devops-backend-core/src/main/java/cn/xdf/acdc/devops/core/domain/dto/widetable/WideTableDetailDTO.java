package cn.xdf.acdc.devops.core.domain.dto.widetable;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableDataSystemResourceProjectMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class WideTableDetailDTO {
    
    private Long id;
    
    private String name;
    
    private WideTableState actualState;
    
    private RequisitionState requisitionState;
    
    private WideTableState desiredState;
    
    private Map<Long, Long> dataCollectionIdProjectIdMappings;
    
    private String selectStatement;
    
    private Long userId;
    
    private Long projectId;
    
    private String description;
    
    private WideTableSubqueryDTO subQuery;
    
    private String userDomainAccount;
    
    private Date creationTime;
    
    private Long dataCollectionId;
    
    private Set<WideTableColumnDTO> wideTableColumns;
    
    public WideTableDetailDTO(final WideTableDO wideTableDO) {
        this.id = wideTableDO.getId();
        this.name = wideTableDO.getName();
        this.requisitionState = wideTableDO.getRequisitionState();
        this.actualState = wideTableDO.getActualState();
        this.desiredState = wideTableDO.getDesiredState();
        this.selectStatement = wideTableDO.getSelectStatement();
        this.description = wideTableDO.getDescription();
        this.subQuery = new WideTableSubqueryDTO(wideTableDO.getSubquery());
        this.userDomainAccount = wideTableDO.getUser().getDomainAccount();
        this.creationTime = wideTableDO.getCreationTime();
        this.projectId = wideTableDO.getProject().getId();
        
        if (wideTableDO.getDataCollection() != null) {
            this.dataCollectionId = wideTableDO.getDataCollection().getId();
        }
        
        if (!CollectionUtils.isEmpty(wideTableDO.getWideTableColumns())) {
            this.wideTableColumns = wideTableDO.getWideTableColumns().stream()
                    .map(WideTableColumnDTO::new).collect(Collectors.toSet());
        }
    }
    
    /**
     * To DataSystemResourceDO.
     *
     * @return DataSystemResourceDO
     */
    public WideTableDO toDO() {
        WideTableDO result = new WideTableDO();
        
        Set<WideTableDataSystemResourceProjectMappingDO> wideTableDataSystemResourceProjectMappings = dataCollectionIdProjectIdMappings.entrySet().stream()
                .map(entry -> getWideTableDataSystemResourceProjectMappingDO(entry, result))
                .collect(Collectors.toSet());
        return result.setId(this.id)
                .setName(this.name)
                .setWideTableDataSystemResourceProjectMappings(wideTableDataSystemResourceProjectMappings)
                .setSelectStatement(this.selectStatement)
                .setUser(new UserDO().setId(this.userId))
                .setProject(new ProjectDO().setId(this.projectId))
                .setDescription(this.description);
    }
    
    private WideTableDataSystemResourceProjectMappingDO getWideTableDataSystemResourceProjectMappingDO(final Entry<Long, Long> entry, final WideTableDO wideTableDO) {
        return new WideTableDataSystemResourceProjectMappingDO()
                .setWideTable(wideTableDO)
                .setDataSystemResource(new DataSystemResourceDO(entry.getKey()))
                .setProject(new ProjectDO(entry.getValue()));
    }
}
