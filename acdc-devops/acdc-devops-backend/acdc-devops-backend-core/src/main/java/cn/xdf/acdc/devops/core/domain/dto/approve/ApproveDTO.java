package cn.xdf.acdc.devops.core.domain.dto.approve;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApproveDTO {

    private String approveResult;

    private Boolean approved;
}
