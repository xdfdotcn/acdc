package cn.xdf.acdc.devops.service.process.widetable.sql.validator;

import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.sql.CalciteMysqlFactory;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class CalciteMysqlSqlValidator implements SqlValidator<Map<String, Map<String, List<DataFieldDefinition>>>> {
    public static final String CALCITE_SQL_VALIDATOR = "calcite";
    
    private CalciteMysqlFactory calciteMysqlFactory = new CalciteMysqlFactory();
    
    @Override
    public SqlNode validate(final SqlNode sqlNode, final Map<String, Map<String, List<DataFieldDefinition>>> schemaInfo) {
        return calciteMysqlFactory.createValidator(schemaInfo).validate(sqlNode);
    }
    
    @Override
    public String getName() {
        return CALCITE_SQL_VALIDATOR;
    }
}
