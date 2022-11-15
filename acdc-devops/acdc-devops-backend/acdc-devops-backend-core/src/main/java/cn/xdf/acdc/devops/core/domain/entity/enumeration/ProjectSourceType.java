package cn.xdf.acdc.devops.core.domain.entity.enumeration;

// CHECKSTYLE:OFF

public enum ProjectSourceType {

    USER_INPUT("user-input", 0),

    FROM_PANDORA("from-pandora", 1);

    private String name;

    private Integer code;

    ProjectSourceType(final String name, final Integer code) {
        this.name = name;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
