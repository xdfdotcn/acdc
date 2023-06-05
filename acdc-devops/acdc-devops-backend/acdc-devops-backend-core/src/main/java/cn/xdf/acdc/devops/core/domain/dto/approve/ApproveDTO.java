package cn.xdf.acdc.devops.core.domain.dto.approve;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ApproveDTO {
    
    private String approveResult;
    
    private Boolean approved;
}
