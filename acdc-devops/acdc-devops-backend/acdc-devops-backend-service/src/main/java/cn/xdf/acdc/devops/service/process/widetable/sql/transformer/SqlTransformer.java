package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;

import org.apache.calcite.sql.SqlNode;

public interface SqlTransformer<T> {
    
    /**
     * Transform sql node.
     *
     * @param sqlNode sql node
     * @param params params
     * @return sql node
     */
    SqlNode transform(SqlNode sqlNode, T params);
    
    /**
     * Get the transformer name.
     *
     * @return transformer name
     */
    String getName();
}
