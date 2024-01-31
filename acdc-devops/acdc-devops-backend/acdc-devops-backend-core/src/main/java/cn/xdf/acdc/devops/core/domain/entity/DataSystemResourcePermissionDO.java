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
 * 数据系统权限.
 */
@Data
@Entity
@ApiModel("数据系统权限")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@Table(name = "data_system_resource_permission")
public class DataSystemResourcePermissionDO extends BaseDO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ApiModelProperty("数据系统资源")
    @ManyToOne(fetch = FetchType.LAZY)
    private DataSystemResourceDO dataSystemResource;
    
    @ApiModelProperty("被赋予权限的项目")
    @ManyToOne(fetch = FetchType.LAZY)
    private ProjectDO project;
}
