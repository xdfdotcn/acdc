package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RdbTableDTO extends PageDTO {

    private Long id;

    private Long databaseId;

    private String name;

    public RdbTableDTO(final RdbTableDO rdbTable) {
        this.id = rdbTable.getId();
        this.databaseId = rdbTable.getRdbDatabase().getId();
        this.name = rdbTable.getName();
    }

    public static RdbTableDTO toRdbTableDTO(final RdbTableDO rdbTableDO) {
        return RdbTableDTO.builder()
            .id(rdbTableDO.getId())
            .databaseId(rdbTableDO.getRdbDatabase().getId())
            .name(rdbTableDO.getName())
            .build();
    }
}
