package cn.xdf.acdc.devops.service.process.widetable.sql.validator;

import org.apache.calcite.sql.SqlNode;

public interface SqlValidator<T> {
    
    /**
     * Validate the sql node with given parameters.
     *
     * @param sqlNode sql node
     * @param params parameters
     * @return sql node
     */
    SqlNode validate(SqlNode sqlNode, T params);
    
    /**
     * Get the validator name.
     *
     * @return validator name
     */
    String getName();
}
