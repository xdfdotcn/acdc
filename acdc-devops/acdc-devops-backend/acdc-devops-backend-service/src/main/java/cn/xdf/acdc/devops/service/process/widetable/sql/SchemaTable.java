package cn.xdf.acdc.devops.service.process.widetable.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SchemaTable {
    
    private String schema;
    
    private String table;
}
