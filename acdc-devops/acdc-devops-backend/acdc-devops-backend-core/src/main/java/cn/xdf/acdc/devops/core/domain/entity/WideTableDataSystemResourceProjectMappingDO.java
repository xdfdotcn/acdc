package cn.xdf.acdc.devops.core.domain.entity;

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
 * 宽表与数据系统资源的映射;表示宽表数据的来源数据集.
 */
@Data
@Entity
@Accessors(chain = true)
@EqualsAndHashCode
@ApiModel("宽表与数据系统资源的映射;表示宽表数据的来源数据集")
@Table(name = "wide_table_data_system_resource_project_mapping")
public class WideTableDataSystemResourceProjectMappingDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("宽表")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private WideTableDO wideTable;
    
    @ApiModelProperty("数据集")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private DataSystemResourceDO dataSystemResource;
    
    @ApiModelProperty("数据集所属项目")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    private ProjectDO project;
}
