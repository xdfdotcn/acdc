package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class HiveDbMetaDTO {

    private String db;

    private String table;

    private Long dbId;

    private Long tableId;

    /**
     * Get hive database DO.
     *
     * @param hiveId hive id
     * @return hive database DO
     */
    public HiveDatabaseDO toHiveDatabaseDO(final Long hiveId) {
        return HiveDatabaseDO.builder()
                .hive(HiveDO.builder()
                        .id(hiveId)
                        .build())
                .id(dbId)
                .name(db)
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }

    /**
     * Get hive table DO.
     *
     * @return hive table DO
     */
    public HiveTableDO toHiveTableDO() {
        return HiveTableDO.builder()
                .id(tableId)
                .name(table)
                .hiveDatabase(HiveDatabaseDO.builder().id(dbId).build())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }
}
