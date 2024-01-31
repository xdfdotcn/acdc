package cn.xdf.acdc.devops.core.domain.entity;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 子查询字段.
 */
@Data
@Entity
@ApiModel("子查询字段")
@Accessors(chain = true)
@Table(name = "wide_table_subquery_column")
public class WideTableSubqueryColumnDO extends BaseDO {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("子查询")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private WideTableSubqueryDO subquery;
    
    @ApiModelProperty("表达式")
    @Column(name = "expression", nullable = false)
    private String expression;
    
    @ApiModelProperty("别名")
    @Column(name = "alias", nullable = false)
    private String alias;
    
    @ApiModelProperty("类型")
    @Column(name = "type", nullable = true)
    private String type;
    
    @ApiModelProperty("是否是所在子查询的主键;0: false，1: true")
    @Column(name = "is_primary_key", nullable = false)
    private Boolean primaryKey;
    
    @OneToMany(mappedBy = "column", fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private Set<WideTableSubqueryColumnLineageDO> wideTableSubqueryColumnLineages = new HashSet<>();
    
    @OneToMany(mappedBy = "column", fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private Set<WideTableSubqueryColumnUniqueIndexDO> wideTableSubqueryColumnUniqueIndex = new HashSet<>();
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WideTableSubqueryColumnDO)) {
            return false;
        }
        return id != null && id.equals(((WideTableSubqueryColumnDO) o).id);
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

