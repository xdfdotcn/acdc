package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@ApiModel(description = "Hdfs集群namenode 信息.\n@author acdc")
@Entity
@Table(name = "hdfs_namenode")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class HdfsNamenodeDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "namenode 名称", required = true)
    @Column(name = "name", length = 32, nullable = false)
    private String name;

    @ApiModelProperty(value = "namenode rpc地址", required = true)
    @Column(name = "rpc_address", length = 32, nullable = false)
    private String rpcAddress;

    @ApiModelProperty(value = "namenode rpc 端口", required = true)
    @Column(name = "rpc_port", length = 10, nullable = false)
    private String rpcPort;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = {"hdfsNamenodes", "hives"}, allowSetters = true)
    private HdfsDO hdfs;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HdfsNamenodeDO)) {
            return false;
        }
        return id != null && id.equals(((HdfsNamenodeDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "HdfsNamenode{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", rpcAddress='" + getRpcAddress() + "'"
                + ", rpcPort='" + getRpcPort() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
