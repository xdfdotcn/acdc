package cn.xdf.acdc.devops.core.domain.entity;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableState;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Set;

/**
 * 宽表.
 */
@Data
@Entity
@ApiModel("宽表")
@Accessors(chain = true)
@Table(name = "wide_table")
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class WideTableDO extends SoftDeletableDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("表名")
    @Column(name = "name", nullable = false)
    private String name;
    
    @ApiModelProperty("查询 sql")
    @Column(name = "select_statement")
    @Lob
    private String selectStatement;
    
    @ApiModelProperty("描述")
    @Column(name = "description")
    private String description;
    
    @ApiModelProperty("子查询id")
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL, optional = false)
    private WideTableSubqueryDO subquery;
    
    @ApiModelProperty("创建者")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private UserDO user;
    
    @ApiModelProperty("所属项目")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private ProjectDO project;
    
    @ApiModelProperty("对应的数据集")
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private DataSystemResourceDO dataCollection;
    
    @Column(name = "requisition_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    @ApiModelProperty("申请状态;0: approving 1: refused 2: approved")
    private RequisitionState requisitionState;
    
    @ApiModelProperty("对应的审批单集合")
    @OneToOne(fetch = FetchType.LAZY)
    private DataSystemResourcePermissionRequisitionBatchDO requisitionBatch;
    
    @ApiModelProperty("实际状态")
    @Column(name = "actual_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private WideTableState actualState;
    
    @ApiModelProperty("预期状态")
    @Column(name = "desired_state", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private WideTableState desiredState;
    
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "wide_table_connection_mapping",
            joinColumns = @JoinColumn(name = "wide_table_id"),
            inverseJoinColumns = @JoinColumn(name = "connection_id")
    )
    private Set<ConnectionDO> connections;
    
    @OneToMany(mappedBy = "wideTable", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<WideTableDataSystemResourceProjectMappingDO> wideTableDataSystemResourceProjectMappings;
    
    @OneToMany(mappedBy = "wideTable", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    private Set<WideTableColumnDO> wideTableColumns;
    
    public WideTableDO(final Long id) {
        this.id = id;
    }
    
    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }
    
    @Override
    public String toString() {
        return "WideTableDO{"
                + "id=" + id
                + ", name='" + name + '\''
                + ", sql='" + selectStatement + '\''
                + ", description='" + description + '\''
                + ", requisitionState=" + requisitionState
                + ", actualState=" + actualState
                + ", desiredState=" + desiredState
                + '}';
    }
}
