package cn.xdf.acdc.devops.service.process.datasystem.definition;

import com.google.common.base.Objects;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class DataCollectionDefinition {
    
    private Map<String, DataFieldDefinition> lowerCaseNameToDataFieldDefinitions = new HashMap<>();
    
    private Map<String, List<DataFieldDefinition>> uniqueIndexNameToFieldDefinitions = new HashMap<>();
    
    private String name;
    
    public DataCollectionDefinition(final String name, final List<DataFieldDefinition> dataFieldDefinitions) {
        this.name = name;
        
        dataFieldDefinitions.forEach(fieldDefinition -> {
            lowerCaseNameToDataFieldDefinitions.put(fieldDefinition.getName().toLowerCase(), fieldDefinition);
            
            fieldDefinition.getUniqueIndexNames().forEach(uniqueIndexName -> {
                List<DataFieldDefinition> fieldsOfIndex = uniqueIndexNameToFieldDefinitions.getOrDefault(uniqueIndexName, new ArrayList());
                fieldsOfIndex.add(fieldDefinition);
                uniqueIndexNameToFieldDefinitions.put(uniqueIndexName, fieldsOfIndex);
            });
        });
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof DataCollectionDefinition)) {
            return false;
        }
        
        DataCollectionDefinition other = (DataCollectionDefinition) obj;
        
        return Objects.equal(name, other.name) && Objects.equal(lowerCaseNameToDataFieldDefinitions, other.lowerCaseNameToDataFieldDefinitions);
    }
}
