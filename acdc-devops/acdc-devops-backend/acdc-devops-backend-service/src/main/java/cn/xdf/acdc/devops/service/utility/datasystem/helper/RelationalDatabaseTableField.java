package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import com.google.common.base.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Set;

/**
 * Field for relational database table.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Setter
@Accessors(chain = true)
public class RelationalDatabaseTableField {
    
    private String name;
    
    private String type;
    
    private String defaultValue;
    
    private boolean isOptional;
    
    private Set<String> uniqueIndexNames = new HashSet<>();
    
    public RelationalDatabaseTableField(final String name, final String type, final Set<String> uniqueIndexNames) {
        this.name = name;
        this.type = type;
        this.uniqueIndexNames = uniqueIndexNames;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RelationalDatabaseTableField)) {
            return false;
        }
        
        RelationalDatabaseTableField other = (RelationalDatabaseTableField) obj;
        
        return Objects.equal(name, other.name) && Objects.equal(type, other.type);
    }
}
