package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
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

/**
 * 关联类型子查询的关联条件.
 */
@Data
@Entity
@Accessors(chain = true)
@ApiModel("关联类型子查询的关联条件")
@EqualsAndHashCode(callSuper = true)
@Table(name = "wide_table_subquery_join_condition")
public class WideTableSubqueryJoinConditionDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("所属子查询")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private WideTableSubqueryDO subquery;
    
    @ApiModelProperty("左边的列")
    @Column(name = "left_column", nullable = false)
    private String leftColumn;
    
    @ApiModelProperty("连接符")
    @Column(name = "operator", nullable = false)
    private String operator;
    
    @ApiModelProperty("右边的列")
    @Column(name = "right_column", nullable = false)
    private String rightColumn;
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WideTableSubqueryJoinConditionDO)) {
            return false;
        }
        return id != null && id.equals(((WideTableSubqueryJoinConditionDO) o).id);
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
