package cn.xdf.acdc.devops.service.process.datasystem.definition;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum DataFieldDefinitionExtendPropertyName {
    
    LENGTH,
    
    SCALE,
    
    UNSIGNED,
    
    // enum or set 's candidate values
    CANDIDATES,
    
    UNSIGNED_ZEROFILL;
    
    public static Set<DataFieldDefinitionExtendPropertyName> getSupportedExtendPropertyNames() {
        return Arrays.stream(DataFieldDefinitionExtendPropertyName.values()).collect(Collectors.toSet());
    }
}
