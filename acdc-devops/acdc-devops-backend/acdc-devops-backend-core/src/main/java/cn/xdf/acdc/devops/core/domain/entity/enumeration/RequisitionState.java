package cn.xdf.acdc.devops.core.domain.entity.enumeration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// CHECKSTYLE:OFF
public enum RequisitionState {
    
    APPROVING,
    
    REFUSED,
    
    APPROVED;
    
    private static final Map<Integer, RequisitionState> CODE_MAP = new HashMap<>();
    
    static {
        CODE_MAP.put(0, APPROVING);
        CODE_MAP.put(1, REFUSED);
        CODE_MAP.put(2, APPROVED);
    }
    
    public static RequisitionState codeOf(int code) {
        RequisitionState matchType = CODE_MAP.get(code);
        return Optional.of(matchType).get();
    }
}
