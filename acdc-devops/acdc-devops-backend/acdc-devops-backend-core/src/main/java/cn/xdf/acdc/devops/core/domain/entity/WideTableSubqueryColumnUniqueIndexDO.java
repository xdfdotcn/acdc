package cn.xdf.acdc.devops.core.domain.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * 子查询字段唯一键.
 */
@Data
@Entity
@ApiModel("子查询字段唯一键")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@Table(name = "wide_table_subquery_column_unique_index")
public class WideTableSubqueryColumnUniqueIndexDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("对应字段")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private WideTableSubqueryColumnDO column;
    
    @ApiModelProperty("唯一键名称")
    @Column(name = "name", nullable = false)
    private String name;
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WideTableSubqueryColumnUniqueIndexDO)) {
            return false;
        }
        return id != null && id.equals(((WideTableSubqueryColumnUniqueIndexDO) o).id);
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    @Override
    public String toString() {
        return SystemConstant.EMPTY_STRING;
    }
}
