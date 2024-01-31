package cn.xdf.acdc.devops.service.process.widetable.sql;

import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class SchemasAndSqls {
    private Map<String, Map<String, List<DataFieldDefinition>>> schemaTableFieldsMap;
    
    private Map<String, String> sqls;
}
