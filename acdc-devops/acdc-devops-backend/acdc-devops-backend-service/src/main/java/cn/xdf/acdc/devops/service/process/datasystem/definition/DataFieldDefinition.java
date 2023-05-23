package cn.xdf.acdc.devops.service.process.datasystem.definition;

import com.google.common.base.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

@AllArgsConstructor
@Getter
public class DataFieldDefinition {
    
    private String name;
    
    private String type;
    
    private Set<String> uniqueIndexNames;
    
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof DataFieldDefinition)) {
            return false;
        }
        
        DataFieldDefinition other = (DataFieldDefinition) obj;
        
        return Objects.equal(name, other.name)
                && Objects.equal(type, other.type)
                && Objects.equal(uniqueIndexNames, other.uniqueIndexNames);
    }
}
