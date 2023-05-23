package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionRequisitionQuery extends PagedQuery {
    
    private Long connectionId;
}
