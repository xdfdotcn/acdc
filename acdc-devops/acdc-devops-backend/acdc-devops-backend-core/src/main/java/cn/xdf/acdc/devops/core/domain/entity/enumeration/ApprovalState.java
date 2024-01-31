package cn.xdf.acdc.devops.core.domain.entity.enumeration;

import com.google.common.collect.Sets;

import java.util.Set;

// 0: 待审批 1: 待数据源负责人审批 2: 数据源负责人审批拒绝 3: 待DBA负责人审批 4: DBA 负责人审批拒绝 5: 审批通过
public enum ApprovalState {
    
    // 0: 待审批
    APPROVING,
    
    // 1: 待源负责人审批
    SOURCE_OWNER_APPROVING,
    
    // 2: 源负责人审批拒绝
    SOURCE_OWNER_REFUSED,
    
    // 3: 待DBA审批
    DBA_APPROVING,
    
    // 4: DBA审批拒绝
    DBA_REFUSED,
    
    // 5: 审批通过
    APPROVED;
    
    public static final Set<ApprovalState> REFUSED_STATES = Sets.newHashSet(SOURCE_OWNER_REFUSED, DBA_REFUSED);
}
