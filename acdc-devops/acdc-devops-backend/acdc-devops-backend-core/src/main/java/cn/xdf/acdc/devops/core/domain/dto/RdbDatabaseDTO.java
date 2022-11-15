package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RdbDatabaseDTO extends PageDTO {

    private Long id;

    private Long clusterId;

    private String name;

    public RdbDatabaseDTO(final RdbDatabaseDO rdbDatabase) {
        this.id = rdbDatabase.getId();
        this.clusterId = rdbDatabase.getRdb().getId();
        this.name = rdbDatabase.getName();
    }

    public static RdbDatabaseDTO toRdbDatabaseDTO(final RdbDatabaseDO rdbDatabaseDO) {
        return RdbDatabaseDTO.builder()
            .id(rdbDatabaseDO.getId())
            .clusterId(rdbDatabaseDO.getRdb().getId())
            .name(rdbDatabaseDO.getName())
            .build();
    }
}
