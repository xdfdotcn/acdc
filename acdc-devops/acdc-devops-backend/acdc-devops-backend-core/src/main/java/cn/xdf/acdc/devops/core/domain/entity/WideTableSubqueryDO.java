package cn.xdf.acdc.devops.core.domain.entity;

import java.io.Serializable;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSqlJoinType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSubqueryTableSourceType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 宽表子查询.
 */
@Data
@Entity
@ApiModel("宽表子查询")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Table(name = "wide_table_subquery")
public class WideTableSubqueryDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("名称")
    @Column(name = "name", nullable = false)
    private String name;
    
    @ApiModelProperty("原始查询 sql")
    @Column(name = "select_statement", nullable = false)
    @Lob
    private String selectStatement;
    
    @ApiModelProperty("where 表达式")
    @Column(name = "where_expression")
    @Lob
    private String whereExpression;
    
    @Column(name = "other_expression")
    @ApiModelProperty("其他表达式;拼接到 where 的后面")
    @Lob
    private String otherExpression;
    
    @Column(name = "table_source_type", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    @ApiModelProperty("查询目标类型;0: 原子 ATOM，1: 关联 JOINED，2: 子查询 SUBQUERY")
    private WideTableSubqueryTableSourceType tableSourceType;
    
    @Column(name = "is_real", nullable = false)
    @ApiModelProperty("0: 非真实节点，1: 真实节点，展示血缘关系使用")
    private Boolean real = Boolean.FALSE;
    
    @ApiModelProperty("查询目标为原子表时对应的实际数据集")
    @OneToOne(fetch = FetchType.LAZY)
    private DataSystemResourceDO dataSystemResource;
    
    @ApiModelProperty("关联类型子查询的左子查询")
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private WideTableSubqueryDO leftSubquery;
    
    @ApiModelProperty("关联类型子查询的右子查询")
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private WideTableSubqueryDO rightSubquery;
    
    //    @OneToOne(mappedBy = "leftSubquery", fetch = FetchType.EAGER,cascade = CascadeType.ALL)
    //    private WideTableSubqueryDO parentLeftSubquery;
    
    //    @OneToOne(mappedBy = "rightSubquery", fetch = FetchType.EAGER,cascade = CascadeType.ALL)
    //    private WideTableSubqueryDO parentRightSubquery;
    
    @ApiModelProperty("关联类型;0: LEFT，1: RIGHT，2: INNER")
    @Enumerated(EnumType.ORDINAL)
    @Column(name = "join_type")
    private WideTableSqlJoinType joinType;
    
    @ApiModelProperty("查询目标为子查询时对应子查询")
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private WideTableSubqueryDO subquery;
    
    @OneToOne(mappedBy = "subquery", fetch = FetchType.LAZY)
    private WideTableDO wideTable;
    
    @OneToMany(mappedBy = "subquery", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<WideTableSubqueryColumnDO> wideTableSubqueryColumns;
    
    @OneToMany(mappedBy = "subquery", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<WideTableSubqueryJoinConditionDO> wideTableSubqueryJoinConditions;
    
    public WideTableSubqueryDO(final Long id) {
        this.id = id;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WideTableSubqueryDO)) {
            return false;
        }
        return id != null && id.equals(((WideTableSubqueryDO) o).id);
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
