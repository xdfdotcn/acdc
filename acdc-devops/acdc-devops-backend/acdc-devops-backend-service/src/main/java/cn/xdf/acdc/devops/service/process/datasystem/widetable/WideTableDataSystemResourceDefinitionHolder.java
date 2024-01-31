package cn.xdf.acdc.devops.service.process.datasystem.widetable;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;

public final class WideTableDataSystemResourceDefinitionHolder {
    
    private static final DataSystemResourceDefinition WIDE_TABLE_DATA_SYSTEM_RESOURCE_DEFINITION = generateWideTableDataSystemResourceDefinition();
    
    private WideTableDataSystemResourceDefinitionHolder() {
    }
    
    private static DataSystemResourceDefinition generateWideTableDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.ACDC_WIDE_TABLE, false, false, true);
    }
    
    /**
     * Get data system definition.
     *
     * @return cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return WIDE_TABLE_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
