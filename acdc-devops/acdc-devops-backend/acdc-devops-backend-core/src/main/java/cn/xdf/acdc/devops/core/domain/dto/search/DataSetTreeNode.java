package cn.xdf.acdc.devops.core.domain.dto.search;

import cn.xdf.acdc.devops.core.domain.dto.enumeration.DataSetTreeNodeType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Builder
public class DataSetTreeNode {

    private Long id;

    private String name;

    private int isLeaf;

    private DataSetTreeNodeType nodeType;

    private DataSystemType dataSystemType;

    public DataSetTreeNode(
            final Long id,
            final String name,
            final int isLeaf,
            final DataSetTreeNodeType nodeType,
            final DataSystemType dataSystemType) {
        this.id = id;
        this.name = name;
        this.isLeaf = isLeaf;
        this.nodeType = nodeType;
        this.dataSystemType = dataSystemType;
    }

    public DataSetTreeNode(
            final Long id,
            final String name,
            final DataSetTreeNodeType nodeType) {
        this.id = id;
        this.name = name;
        this.isLeaf = 0;
        this.nodeType = nodeType;
    }
}
