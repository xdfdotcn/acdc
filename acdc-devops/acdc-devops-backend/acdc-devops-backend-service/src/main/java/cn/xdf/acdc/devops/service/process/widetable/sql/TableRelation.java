package cn.xdf.acdc.devops.service.process.widetable.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TableRelation {
    
    private String first;
    
    private String second;
    
    /**
     * Get table relation instance with first and second table ordered.
     *
     * @param left left table
     * @param right right table
     * @return table relation
     */
    public static TableRelation getInstance(final String left, final String right) {
        if (left.compareTo(right) > 0) {
            return new TableRelation(left, right);
        }
        return new TableRelation(right, left);
    }
}
