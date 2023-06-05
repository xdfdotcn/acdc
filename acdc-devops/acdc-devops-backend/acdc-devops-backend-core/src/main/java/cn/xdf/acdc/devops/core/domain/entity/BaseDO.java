package cn.xdf.acdc.devops.core.domain.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;
import java.util.Date;

@MappedSuperclass
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public abstract class BaseDO implements Serializable {
    
    @ApiModelProperty(value = "创建时间", required = true)
    @Column(name = "creation_time", updatable = false, nullable = false)
    @CreationTimestamp
    private Date creationTime;
    
    @ApiModelProperty(value = "更新时间", required = true)
    @Column(name = "update_time", nullable = false)
    @UpdateTimestamp
    private Date updateTime;
}
