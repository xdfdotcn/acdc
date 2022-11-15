package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "rdb_database_tidb")
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Accessors(chain = true)
public class RdbDatabaseTidbDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonIgnoreProperties(value = {"kafkaTopic"}, allowSetters = true)
    @OneToOne
    @JoinColumn(unique = true)
    private KafkaTopicDO kafkaTopic;

    @JsonIgnoreProperties(value = {"rdbDatabase"}, allowSetters = true)
    @OneToOne
    @JoinColumn(unique = true)
    private RdbDatabaseDO rdbDatabase;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RdbDatabaseTidbDO)) {
            return false;
        }
        return id != null && id.equals(((RdbDatabaseTidbDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RdbDatabaseTidb{");
        sb.append("id=").append(id);
        sb.append(", creationTime=").append(getCreationTime());
        sb.append(", updateTime=").append(getUpdateTime());
        sb.append(", kafkaTopic=").append(kafkaTopic);
        sb.append(", rdbDatabase=").append(rdbDatabase);
        sb.append('}');
        return sb.toString();
    }
}
