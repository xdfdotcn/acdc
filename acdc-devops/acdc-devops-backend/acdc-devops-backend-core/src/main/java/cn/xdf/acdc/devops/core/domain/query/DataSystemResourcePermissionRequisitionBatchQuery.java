package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class DataSystemResourcePermissionRequisitionBatchQuery extends PagedQuery {
    
    private Date beginUpdateTime;
}
