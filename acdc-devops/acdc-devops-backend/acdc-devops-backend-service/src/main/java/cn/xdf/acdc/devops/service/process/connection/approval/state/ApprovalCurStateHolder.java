package cn.xdf.acdc.devops.service.process.connection.approval.state;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.ApprovalState;

public interface ApprovalCurStateHolder {
    
    /**
     * Get state.
     *
     * @param id id
     * @return approval state
     */
    ApprovalState state(Long id);
}
