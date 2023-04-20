package cn.xdf.acdc.devops.service.process.connection.approval;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.service.config.ACDCEmailProperties;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationConstant;
import cn.xdf.acdc.devops.service.utility.mail.DefaultEmailSender;
import cn.xdf.acdc.devops.service.utility.mail.DomainUser;
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
    private ACDCEmailProperties acdcEmailProperties;

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
            final List<DomainUser> to,
            final List<DomainUser> cc,
            final EmailTemplate template) {

        ConnectionRequisitionDetailDTO connectionRequisition = approvalStateMachine.getConnectionRequisitionById(requisitionId);

        List<ConnectionDetailDTO> connectionDetailList = getUniqueConnectionDetails(connectionRequisition);
        removeConnectionConnectionColumnConfigurationsMetaField(connectionDetailList);
        connectionRequisition.setConnections(connectionDetailList);
        cc.add(new DomainUser(acdcEmailProperties.getCcEmailAddress()));
        defaultEmailSender.sendEmail(to, cc, template, connectionRequisition);
    }

    private String getUniqueKeyFromConnectionDetail(final ConnectionDetailDTO connectionDetail) {
        return new StringBuilder()
                .append(connectionDetail.getSourceProjectId())
                .append(connectionDetail.getSourceDataCollectionId())
                .toString();
    }

    private List<ConnectionDetailDTO> getUniqueConnectionDetails(final ConnectionRequisitionDetailDTO connectionRequisition) {
        Map<String, ConnectionDetailDTO> uniqueConnectionDetailMap = Maps.newHashMap();

        connectionRequisition.getConnections()
                .forEach(it -> uniqueConnectionDetailMap.putIfAbsent(getUniqueKeyFromConnectionDetail(it), it));

        return uniqueConnectionDetailMap.values().stream().collect(Collectors.toList());
    }

    private void removeConnectionConnectionColumnConfigurationsMetaField(final List<ConnectionDetailDTO> connectionDetailList) {
        for (ConnectionDetailDTO connectionDetail : connectionDetailList) {
            List<ConnectionColumnConfigurationDTO> columnConfigurations = connectionDetail.getConnectionColumnConfigurations().stream()
                    .filter(it -> !ConnectionColumnConfigurationConstant.META_FIELD_SET.contains(it.getSourceColumnName()))
                    .collect(Collectors.toList());
            connectionDetail.setConnectionColumnConfigurations(columnConfigurations);
        }
    }
}
