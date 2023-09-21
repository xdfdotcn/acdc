package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Properties;

/**
 * Relational database table.
 */
@NoArgsConstructor
@ToString
@Getter
@Setter
public class RelationalDatabaseTable {
    
    private String name;
    
    private List<RelationalDatabaseTableField> fields;
    
    private Properties properties;
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RelationalDatabaseTable)) {
            return false;
        }
        
        RelationalDatabaseTable other = (RelationalDatabaseTable) obj;
        
        return Objects.equal(name, other.name);
    }
}
