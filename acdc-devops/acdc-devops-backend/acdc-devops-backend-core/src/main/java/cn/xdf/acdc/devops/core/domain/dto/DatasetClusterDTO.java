package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.ClusterType;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Cluster.
 */
// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatasetClusterDTO extends PageDTO {

    private Long projectId;

    private Long id;

    private String name;

    private DataSystemType dataSystemType;

    private String clusterType;

    private String desc;

    public DatasetClusterDTO(final HiveDO hive) {
        this.id = hive.getId();
        this.name = hive.getName();
        this.dataSystemType = DataSystemType.HIVE;
        this.clusterType = ClusterType.HIVE.name();
    }

    public DatasetClusterDTO(final RdbDO rdb) {
        this.id = rdb.getId();
        this.name = rdb.getName();
        this.dataSystemType = DataSystemType.nameOf(rdb.getRdbType());
        this.clusterType = ClusterType.RDB.name();
        this.desc = rdb.getDescription();
    }

    public DatasetClusterDTO(final KafkaClusterDO kafkaCluster) {
        this.id = kafkaCluster.getId();
        this.name = kafkaCluster.getName();
        this.dataSystemType = DataSystemType.KAFKA;
        this.clusterType = ClusterType.KAFKA.name();
        this.desc = kafkaCluster.getDescription();
    }
}
