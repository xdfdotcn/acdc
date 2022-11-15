package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rdb instance.
 */
// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DatasetInstanceDTO extends PageDTO {

    private static final String COLON = ":";

    private Long id;

    private Long clusterId;

    private String name;

    private String tag;

    private DataSystemType dataSystemType;


    public DatasetInstanceDTO(final RdbInstanceDO rdbInstance) {
        this.id = rdbInstance.getId();
        this.name = new StringBuilder()
            .append(rdbInstance.getHost())
            .append(COLON)
            .append(rdbInstance.getPort())
            .toString();
        this.tag = rdbInstance.getRole().name();
    }

    public DatasetInstanceDTO(final HdfsDO hdfs) {
        this.id = hdfs.getId();
        this.name = createHdfsInstanceName(hdfs);
        this.tag = SystemConstant.HDFS_TAG;
    }

    public DatasetInstanceDTO(final KafkaClusterDO kafkaCluster) {
        this.id = kafkaCluster.getId();
        this.name = kafkaCluster.getBootstrapServers();
        this.tag = SystemConstant.KAFKA_TAG;
    }

    private String createHdfsInstanceName(final HdfsDO hdfs) {
        Set<HdfsNamenodeDO> nameNodes = hdfs.getHdfsNamenodes();
        List<String> hosts = nameNodes.stream().map(it -> new StringBuilder()
            .append(it.getRpcAddress())
            .append(COLON)
            .append(it.getRpcPort())
            .toString()
        ).collect(Collectors.toList());

        return Joiner.on(COLON).join(hosts);
    }
}
