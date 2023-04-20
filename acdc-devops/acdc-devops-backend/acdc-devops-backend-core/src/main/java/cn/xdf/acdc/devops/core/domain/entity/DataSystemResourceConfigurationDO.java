package cn.xdf.acdc.devops.core.domain.entity;

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

@Entity
@Table(name = "data_system_resource_configuration")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class DataSystemResourceConfigurationDO extends BaseDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty("关联的数据系统资源")
    @ManyToOne(fetch = FetchType.LAZY)
    private DataSystemResourceDO dataSystemResource;

    @ApiModelProperty(value = "配置项名称", required = true)
    @Column(name = "name", length = 256, nullable = false)
    private String name;

    @ApiModelProperty(value = "配置项值", required = true)
    @Column(name = "value", length = 1024, nullable = false)
    private String value;

    public DataSystemResourceConfigurationDO(final Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataSystemResourceDO)) {
            return false;
        }
        return id != null && id.equals(((DataSystemResourceConfigurationDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Project{"
                + "id=" + getId()
                + ", name='" + getName() + "'"
                + ", value='" + getValue() + "'"
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
        return id + "," + name;
    }
}
