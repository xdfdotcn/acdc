package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.core.util.RdbInstanceUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "rdb_instance")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class RdbInstanceDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "host", length = 1024, nullable = false)
    private String host;

    @ApiModelProperty(value = "服务地址", required = true)
    @Column(name = "port", nullable = false)
    private Integer port;

    @ApiModelProperty("服务端口")
    @Column(name = "vip", length = 1024)
    private String vip;

    @ApiModelProperty("服务端口")
    @Column(name = "role")
    @Enumerated(EnumType.ORDINAL)
    private RoleType role;

    @ApiModelProperty(value = "所属数据库", required = true)
    @ManyToOne
    @JsonIgnoreProperties(value = {"rdbDatabases", "rdbInstances", "kafkaCluster", "projects"}, allowSetters = true)
    private RdbDO rdb;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RdbInstanceDO)) {
            return false;
        }

        return RdbInstanceUtil.rdbInstanceUniqueKeyOf(this).equals(RdbInstanceUtil.rdbInstanceUniqueKeyOf((RdbInstanceDO) o));
//        return id != null && id.equals(((RdbInstance) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "RdbInstance{"
                + "id=" + getId() + ", host='" + getHost() + "'"
                + ", port=" + getPort() + ", vip='" + getVip() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }

    /**
     * A pojo signature.
     *
     * @return signature
     */
    public String getSignature() {
        return host + ":" + port;
    }
}
