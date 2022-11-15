package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.core.util.DateUtil;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectionRequisitionDTO {

    private Long id;

    private ApprovalState state;

    private String description;

    private String sourceApproveResult;

    private String sourceApproverEmail;

    private String dbaApproveResult;

    private String dbaApproverEmail;

    private Instant updateTime;

    private String updateTimeFormat;

    private Instant creationTime;

    private String creationTimeFormat;

    public static ConnectionRequisitionDTO toConnectionRequisitionDTO(final ConnectionRequisitionDO requisition) {
        return ConnectionRequisitionDTO.builder()
            .id(requisition.getId())
            .state(requisition.getState())
            .description(requisition.getDescription())
            .updateTime(requisition.getUpdateTime())
            .creationTime(requisition.getCreationTime())
            .build();
    }

    public static ConnectionRequisitionDTO toConnectionRequisitionDTO(
        final ConnectionRequisitionDO requisition,
        final String sourceApproverEmail,
        final String dbaApproverEmail
    ) {
        return ConnectionRequisitionDTO.builder()
            .id(requisition.getId())
            .state(requisition.getState())
            .description(requisition.getDescription())
            .updateTime(requisition.getUpdateTime())
            .updateTimeFormat(DateUtil.formatToString(requisition.getUpdateTime()))
            .creationTime(requisition.getCreationTime())
            .creationTimeFormat(DateUtil.formatToString(requisition.getCreationTime()))
            .sourceApproveResult(requisition.getSourceApproveResult())
            .sourceApproverEmail(sourceApproverEmail)
            .dbaApproverEmail(dbaApproverEmail)
            .dbaApproveResult(requisition.getDbaApproveResult())
            .build();
    }
}
