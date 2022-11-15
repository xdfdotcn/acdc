package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventLevel;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.EventSource;
import io.swagger.annotations.ApiModel;
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
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@ApiModel(description = "记录connector的事件，新建，status的变更")
@Entity
@Table(name = "connector_event")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class ConnectorEventDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ApiModelProperty(value = "事件来由", required = true)
    @Column(name = "reason", length = 128, nullable = false)
    private String reason;

    @ApiModelProperty(value = "事件信息", required = true)
    @Column(name = "message", length = 3072, nullable = false)
    private String message;

    @ApiModelProperty("事件来源")
    @Column(name = "source")
    @Enumerated(EnumType.ORDINAL)
    private EventSource source;

    @ApiModelProperty("事件级别")
    @Column(name = "level")
    @Enumerated(EnumType.ORDINAL)
    private EventLevel level;

    @ApiModelProperty("connector主键")
    @ManyToOne(fetch = FetchType.LAZY)
    private ConnectorDO connector;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorEventDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectorEventDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "ConnectorEvent{"
                + "id=" + getId()
                + ", reason='" + getReason() + "'"
                + ", message='" + getMessage() + "'"
                + ", creationTime='" + getCreationTime() + "'"
                + ", updateTime='" + getUpdateTime() + "'"
                + "}";
    }
}
