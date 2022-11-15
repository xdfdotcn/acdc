package cn.xdf.acdc.devops.service.process.connection.approval;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.service.config.ACDCEmailConfig;
import cn.xdf.acdc.devops.service.utility.mail.DefaultEmailSender;
import cn.xdf.acdc.devops.service.utility.mail.EmailTemplate;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ApproveEmailSender {

    @Autowired
    private DefaultEmailSender defaultEmailSender;

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    @Autowired
    private ACDCEmailConfig acdcEmailConfig;

    /**
     * Send approve email.
     *
     * @param requisitionId requisitionId
     * @param to            to
     * @param cc            cc
     * @param template      template
     */
    public void sendApproveEmail(
            final long requisitionId,
            final List<DomainUserDTO> to,
            final List<DomainUserDTO> cc,
            final EmailTemplate template) {

        ConnectionRequisitionDetailDTO connectionRequisition = approvalStateMachine
                .getConnectionRequisitionById(requisitionId);

        // 去重,获取申请人所属的项目
        connectionRequisition.setProposerProjects(getProposerProjects(connectionRequisition));

        List<ConnectionDetailDTO> connectionDetailList = getUniqueConnectionDetails(connectionRequisition);
        removeConnectionConnectionColumnConfigurationsMetaField(connectionDetailList);
        connectionRequisition.setConnections(connectionDetailList);
        cc.add(new DomainUserDTO(acdcEmailConfig.getCcEmailAddress()));
        defaultEmailSender.sendEmail(to, cc, template, connectionRequisition);
    }

    private String getUniqueKeyFromConnectionDetail(final ConnectionDetailDTO connectionDetail) {
        return new StringBuilder()
                .append(connectionDetail.getSourceProjectId())
                .append(connectionDetail.getSourceDataSetId())
                .toString();
    }

    private List<ConnectionDetailDTO> getUniqueConnectionDetails(
            final ConnectionRequisitionDetailDTO connectionRequisition) {
        Map<String, ConnectionDetailDTO> uniqueConnectionDetailMap = Maps.newHashMap();

        connectionRequisition.getConnections()
                .forEach(it -> uniqueConnectionDetailMap.putIfAbsent(getUniqueKeyFromConnectionDetail(it), it));

        return uniqueConnectionDetailMap.values().stream().collect(Collectors.toList());
    }

    private List<String> getProposerProjects(final ConnectionRequisitionDetailDTO connectionRequisition) {
        return connectionRequisition.getConnections().stream()
                .map(ConnectionDetailDTO::getSinkProjectName)
                .collect(Collectors.toSet())
                .stream()
                .collect(Collectors.toList());
    }

    private void removeConnectionConnectionColumnConfigurationsMetaField(final List<ConnectionDetailDTO> connectionDetailList) {

        for (ConnectionDetailDTO connectionDetail : connectionDetailList) {
            List<FieldMappingDTO> newFieldMapping = connectionDetail.getConnectionColumnConfigurations().stream()
                    .filter(it -> !FieldMappingDTO.META_FIELD_LIST.contains(it.getSourceField().getName()))
                    .collect(Collectors.toList());
            connectionDetail.setConnectionColumnConfigurations(newFieldMapping);
        }
    }
}
