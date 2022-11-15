package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HiveDatabaseDTO extends PageDTO {

    private Long id;

    private Long clusterId;

    private String name;

    public HiveDatabaseDTO(final HiveDatabaseDO hiveDatabase) {
        this.id = hiveDatabase.getId();
        this.clusterId = hiveDatabase.getHive().getId();
        this.name = hiveDatabase.getName();
    }
}
