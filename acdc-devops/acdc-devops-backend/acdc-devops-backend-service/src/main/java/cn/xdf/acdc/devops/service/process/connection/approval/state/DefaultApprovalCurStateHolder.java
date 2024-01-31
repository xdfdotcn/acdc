package cn.xdf.acdc.devops.service.process.connection.approval.state;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultApprovalCurStateHolder implements ApprovalCurStateHolder {
    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;
    
    @Override
    public ApprovalState state(final Long id) {
        return connectionRequisitionService.getById(id).getState();
    }
}
