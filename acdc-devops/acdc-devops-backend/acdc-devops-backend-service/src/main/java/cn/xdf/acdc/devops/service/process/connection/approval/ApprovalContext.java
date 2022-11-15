package cn.xdf.acdc.devops.service.process.connection.approval;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ApprovalContext {

    private Long id;

    private String description;

    private String operatorId;
}
