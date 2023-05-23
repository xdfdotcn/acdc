package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class UserQuery extends PagedQuery {
    
    private Set<Long> userIds;
    
    private String email;
    
    private String domainAccount;
    
    private Long projectId;
}
