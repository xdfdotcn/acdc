package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.enumeration.QueryScope;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.Instant;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ProjectQuery extends PagedQuery {
    
    private Long id;
    
    private String name;
    
    private String ownerDomainAccount;
    
    private String memberDomainAccount;
    
    private String description;
    
    private Long owner;
    
    private Integer source;
    
    private Long originalId;
    
    private Instant creationTime;
    
    private QueryScope scope;
    
    private Set<Long> projectIds;
    
    private Boolean deleted;
}
