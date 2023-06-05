package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

public interface DataSystemService {
    
    /**
     * Get data system type.
     *
     * @return data system type
     */
    DataSystemType getDataSystemType();
}
