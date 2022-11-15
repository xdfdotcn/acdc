package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HiveTableDTO extends PageDTO {

    private Long id;

    private Long databaseId;

    private String name;

    public HiveTableDTO(final HiveTableDO hiveTable) {
        this.id = hiveTable.getId();
        this.databaseId = hiveTable.getHiveDatabase().getId();
        this.name = hiveTable.getName();
    }
}
