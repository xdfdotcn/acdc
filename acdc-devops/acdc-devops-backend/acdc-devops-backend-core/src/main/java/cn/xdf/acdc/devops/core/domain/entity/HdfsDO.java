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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ApiModel(description = "Hdfs集群信息.\n@author acdc")
@Entity
@Table(name = "hdfs")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class HdfsDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "集群名称", required = true)
    @Column(name = "name", length = 32, nullable = false)
    private String name;

    @ApiModelProperty(value = "HA集群故障转移策略配置类", required = true)
    @Column(name = "client_failover_proxy_provider", length = 100, nullable = false)
    private String clientFailoverProxyProvider;

    @ApiModelProperty("hdfs namenode集合")
    @OneToMany(mappedBy = "hdfs")
    @JsonIgnoreProperties(value = {"hdfs"}, allowSetters = true)
    private Set<HdfsNamenodeDO> hdfsNamenodes = new HashSet<>();

    @OneToMany(mappedBy = "hdfs")
    @JsonIgnoreProperties(value = {"hiveDatabases", "hdfs", "projects"}, allowSetters = true)
    private Set<HiveDO> hives = new HashSet<>();

    // functions for jpa union features
    // CHECKSTYLE:OFF

    public void setHdfsNamenodes(Set<HdfsNamenodeDO> hdfsNamenodes) {
        if (this.hdfsNamenodes != null) {
            this.hdfsNamenodes.forEach(i -> i.setHdfs(null));
        }
        if (hdfsNamenodes != null) {
            hdfsNamenodes.forEach(i -> i.setHdfs(this));
        }
        this.hdfsNamenodes = hdfsNamenodes;
    }

    public HdfsDO hdfsNamenodes(Set<HdfsNamenodeDO> hdfsNamenodes) {
        this.setHdfsNamenodes(hdfsNamenodes);
        return this;
    }

    public HdfsDO addHdfsNamenode(HdfsNamenodeDO hdfsNamenode) {
        this.hdfsNamenodes.add(hdfsNamenode);
        hdfsNamenode.setHdfs(this);
        return this;
    }

    public HdfsDO removeHdfsNamenode(HdfsNamenodeDO hdfsNamenode) {
        this.hdfsNamenodes.remove(hdfsNamenode);
        hdfsNamenode.setHdfs(null);
        return this;
    }

    public void setHives(Set<HiveDO> hives) {
        if (this.hives != null) {
            this.hives.forEach(i -> i.setHdfs(null));
        }
        if (hives != null) {
            hives.forEach(i -> i.setHdfs(this));
        }
        this.hives = hives;
    }

    public HdfsDO addHive(HiveDO hive) {
        this.hives.add(hive);
        hive.setHdfs(this);
        return this;
    }

    public HdfsDO removeHive(HiveDO hive) {
        this.hives.remove(hive);
        hive.setHdfs(null);
        return this;
    }

    // functions for jpa union features
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HdfsDO)) {
            return false;
        }
        return id != null && id.equals(((HdfsDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Hdfs{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", clientFailoverProxyProvider='" + getClientFailoverProxyProvider() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
