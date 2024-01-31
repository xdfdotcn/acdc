package cn.xdf.acdc.devops.service.process.datasystem.definition;

import lombok.Getter;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Getter
public class DataFieldDefinition {
    
    private String name;
    
    private String type;
    
    private Schema connectType;
    
    private boolean isOptional;
    
    private String defaultValue;
    
    private Map<DataFieldDefinitionExtendPropertyName, Object> extendProperties = new HashMap<>();
    
    private Set<String> uniqueIndexNames = new HashSet<>();
    
    // todo: remove this function
    public DataFieldDefinition(final String name,
                               final String type,
                               final Set<String> uniqueIndexNames) {
        this(name, type, null, false, null, null, uniqueIndexNames);
    }
    
    public DataFieldDefinition(final String name,
                               final String type,
                               final Schema connectType,
                               final boolean isOptional,
                               final String defaultValue,
                               final Map<DataFieldDefinitionExtendPropertyName, Object> extendProperties,
                               final Set<String> uniqueIndexNames) {
        this.name = name;
        this.type = type;
        this.connectType = connectType;
        this.isOptional = isOptional;
        this.defaultValue = defaultValue;
        
        if (Objects.nonNull(extendProperties)) {
            this.extendProperties.putAll(extendProperties);
        }
        
        if (Objects.nonNull(uniqueIndexNames)) {
            this.uniqueIndexNames.addAll(uniqueIndexNames);
        }
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataFieldDefinition that = (DataFieldDefinition) o;
        return isOptional == that.isOptional
                && Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(connectType, that.connectType)
                && Objects.equals(defaultValue, that.defaultValue)
                && Objects.equals(extendProperties, that.extendProperties)
                && Objects.equals(uniqueIndexNames, that.uniqueIndexNames);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, type, connectType, isOptional, defaultValue, extendProperties, uniqueIndexNames);
    }
}
