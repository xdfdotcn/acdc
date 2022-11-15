package cn.xdf.acdc.devops.core.domain.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;
import java.time.Instant;

@MappedSuperclass
@NoArgsConstructor
@AllArgsConstructor
@Data
@SuperBuilder
public abstract class BaseDO implements Serializable {

    @ApiModelProperty(value = "创建时间", required = true)
    @Column(name = "creation_time", nullable = false)
    @CreationTimestamp
    private Instant creationTime;

    @ApiModelProperty(value = "更新时间", required = true)
    @Column(name = "update_time", nullable = false)
    @UpdateTimestamp
    private Instant updateTime;
}
