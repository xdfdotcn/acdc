package cn.xdf.acdc.devops.core.domain.enumeration;

/**
 * The ApproveResult enumeration.
 */
public enum ApproveResult {
    
    AGREED("同意"),
    
    REJECTED("拒绝");
    
    private final String value;
    
    ApproveResult(final String value) {
        this.value = value;
    }
    
    /**
     * get value.
     *
     * @return value
     */
    public String getValue() {
        return value;
    }
}
