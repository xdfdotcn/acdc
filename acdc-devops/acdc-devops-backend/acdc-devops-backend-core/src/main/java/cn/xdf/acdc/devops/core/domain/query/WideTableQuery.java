package cn.xdf.acdc.devops.core.domain.query;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class WideTableQuery extends PagedQuery {
    
    private String name;
    
    private Long dataCollectionId;
    
    private Date beginUpdateTime;
    
    private String domainAccount;
    
    private Boolean deleted;
}
