package cn.xdf.acdc.devops.core.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Entity
@ApiModel("宽表字段信息")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@Table(name = "wide_table_column")
public class WideTableColumnDO extends BaseDO {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("所属宽表")
    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    private WideTableDO wideTable;
    
    @ApiModelProperty("字段名")
    @Column(name = "name", nullable = false)
    private String name;
    
    @ApiModelProperty("类型")
    @Column(name = "type", nullable = false)
    private String type;
    
    @ApiModelProperty("是否主键")
    @Column(name = "is_primary_key", nullable = false)
    private Boolean isPrimaryKey;
}
