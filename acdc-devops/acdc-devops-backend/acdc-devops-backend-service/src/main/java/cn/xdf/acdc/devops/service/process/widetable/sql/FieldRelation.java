package cn.xdf.acdc.devops.service.process.widetable.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FieldRelation {
    
    private TableField first;
    
    private TableField second;
    
    /**
     * Get field relation instance.
     *
     * @param left left table field
     * @param right right table field
     * @return field relation
     */
    public static FieldRelation getInstance(final TableField left, final TableField right) {
        if (left.getTable().compareTo(right.getTable()) > 0) {
            return new FieldRelation(left, right);
        }
        return new FieldRelation(right, left);
    }
}
