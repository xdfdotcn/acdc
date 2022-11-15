package cn.xdf.acdc.devops.core.domain.dto;

// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Rdb.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RdbDTO extends PageDTO {

    private Long id;

    private String name;

    private String rdbType;

    private String desc;

    private Long projectId;

    private String creationTime;

    private String updateTime;

    private String instance;

    private String username;

    private String password;

    public RdbDTO(final RdbDO rdb) {
        this.id = rdb.getId();
        this.rdbType = rdb.getRdbType();
        this.name = rdb.getName();
        this.desc = rdb.getDescription();

        this.creationTime = DateUtil.formatToString(rdb.getCreationTime());
        this.updateTime = DateUtil.formatToString(rdb.getUpdateTime());
        this.desc = rdb.getDescription();
    }

    public RdbDTO(final RdbDO rdb, final String instance) {
        this.id = rdb.getId();
        this.rdbType = rdb.getRdbType();
        this.name = rdb.getName();
        this.desc = rdb.getDescription();

        this.creationTime = DateUtil.formatToString(rdb.getCreationTime());
        this.updateTime = DateUtil.formatToString(rdb.getUpdateTime());
    }

    public static RdbDTO toRdbDTO(final RdbDO rdbDO) {
        return RdbDTO.builder()
                .id(rdbDO.getId())
                .name(rdbDO.getName())
                .username(rdbDO.getUsername())
                .rdbType(rdbDO.getRdbType())
                .desc(rdbDO.getDescription())
                .build();
    }

    public static RdbDO toRdbDO(final RdbDTO dto) {
        RdbDO rdbDO = new RdbDO();
        rdbDO.setRdbType(dto.getRdbType());
        rdbDO.setName(dto.getName());
        rdbDO.setUsername(dto.getUsername());
        rdbDO.setPassword(dto.getPassword());
        rdbDO.setDescription(dto.getDesc());
        return rdbDO;
    }
}
