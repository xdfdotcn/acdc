package cn.xdf.acdc.devops.core.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "connection_requisition_connection_mapping")
@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class ConnectionRequisitionConnectionMappingDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    private ConnectionRequisitionDO connectionRequisition;

    @ManyToOne(fetch = FetchType.LAZY)
    private ConnectionDO connection;

    @Column(name = "connection_version", nullable = false)
    private Integer connectionVersion;

    public ConnectionRequisitionConnectionMappingDO(final Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionRequisitionConnectionMappingDO)) {
            return false;
        }
        return id != null && id.equals(((ConnectionRequisitionConnectionMappingDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
}
