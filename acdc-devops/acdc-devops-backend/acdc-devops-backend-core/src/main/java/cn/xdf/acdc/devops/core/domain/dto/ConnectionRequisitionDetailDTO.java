package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;

import java.time.Instant;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectionRequisitionDetailDTO {

    private Long id;

    private String sourceApproveResult;

    private String dbaApproveResult;

    private String sourceApproverEmail;

    private String dbaApproverEmail;

    private ApprovalState state;

    private String description;

    private String baseUrl;

    private List<String> proposerProjects;

    private List<ConnectionDetailDTO> connections;

    // 三方系统id
    private String thirdPartyId;


    public static ConnectionRequisitionDetailDTO toConnectionRequisitionDetailDTO(
            final ConnectionRequisitionDO connectionRequisitionDO,
            final List<ConnectionDetailDTO> connectionDTOs,
            final String sourceApproverEmail,
            final String dbaApproverEmail

    ) {
        return ConnectionRequisitionDetailDTO.builder()
                .id(connectionRequisitionDO.getId())
                .thirdPartyId(connectionRequisitionDO.getThirdPartyId())
                .sourceApproveResult(connectionRequisitionDO.getSourceApproveResult())
                .dbaApproveResult(connectionRequisitionDO.getDbaApproveResult())
                .sourceApproverEmail(sourceApproverEmail)
                .dbaApproverEmail(dbaApproverEmail)
                .state(connectionRequisitionDO.getState())
                .description(connectionRequisitionDO.getDescription())
                .connections(connectionDTOs)
                .build();
    }

    public static ConnectionRequisitionDO toConnectionRequisitionDO(
            final ConnectionRequisitionDetailDTO connectionRequisitionDetailDTO) {
        return ConnectionRequisitionDO.builder()
                .description(connectionRequisitionDetailDTO.getDescription())
                .state(connectionRequisitionDetailDTO.getState())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }

    public static ConnectionRequisitionDetailDTO of(
            final String description,
            final List<ConnectionDetailDTO> connectionDetailDTOS
    ) {
        return ConnectionRequisitionDetailDTO.builder()
                .state(ApprovalState.APPROVING)
                .description(description)
                .connections(connectionDetailDTOS)
                .build();
    }

}
