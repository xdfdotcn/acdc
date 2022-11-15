package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HiveDTO extends PageDTO {

    private Long id;

    private Long projectId;

    private Long hdfsId;

    private String name;

    public HiveDTO(final HiveDO hive) {
        this.id = hive.getId();
        this.name = hive.getName();
    }

    public static HiveDTO toHiveDTO(HiveDO hiveDO) {
        return HiveDTO.builder()
            .id(hiveDO.getId())
            .hdfsId(hiveDO.getHdfs().getId())
            .name(hiveDO.getName())
            .build();
    }
}
