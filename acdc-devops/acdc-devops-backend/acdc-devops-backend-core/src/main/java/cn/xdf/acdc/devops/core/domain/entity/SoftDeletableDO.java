package cn.xdf.acdc.devops.core.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;

@MappedSuperclass
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class SoftDeletableDO extends BaseDO implements Serializable {
    
    @Column(name = "is_deleted", nullable = false)
    private Boolean deleted = Boolean.FALSE;
}
