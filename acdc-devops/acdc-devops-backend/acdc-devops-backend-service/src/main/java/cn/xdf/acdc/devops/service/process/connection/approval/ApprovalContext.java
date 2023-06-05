package cn.xdf.acdc.devops.service.process.connection.approval;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ApprovalContext {
    
    private Long id;
    
    private String description;
    
    private String operatorId;
}
