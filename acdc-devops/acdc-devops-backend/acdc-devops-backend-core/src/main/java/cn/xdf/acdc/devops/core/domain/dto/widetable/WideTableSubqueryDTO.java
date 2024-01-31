package cn.xdf.acdc.devops.core.domain.dto.widetable;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSqlJoinType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.WideTableSubqueryTableSourceType;
import lombok.Data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class WideTableSubqueryDTO {
    private Long id;
    
    private String name;
    
    private String sql;
    
    private String whereExpression;
    
    private String otherExpression;
    
    private WideTableSubqueryTableSourceType tableSourceType;
    
    private Boolean real = Boolean.FALSE;
    
    private DataSystemResourceDTO dataSystemResource;
    
    private WideTableSubqueryDTO leftSubquery;
    
    private WideTableSubqueryDTO rightSubquery;
    
    private WideTableSqlJoinType joinType;
    
    private WideTableSubqueryDTO subquery;
    
    private List<WideTableSubqueryColumnDTO> columns;
    
    public WideTableSubqueryDTO(final WideTableSubqueryDO subQueryDO) {
        this.id = subQueryDO.getId();
        this.name = subQueryDO.getName();
        this.sql = subQueryDO.getSelectStatement();
        this.whereExpression = subQueryDO.getWhereExpression();
        this.otherExpression = subQueryDO.getOtherExpression();
        this.tableSourceType = subQueryDO.getTableSourceType();
        this.real = subQueryDO.getReal();
        this.columns = subQueryDO.getWideTableSubqueryColumns().stream()
                .map(WideTableSubqueryColumnDTO::new)
                .collect(Collectors.toList());
        if (Objects.nonNull(subQueryDO.getDataSystemResource())) {
            this.dataSystemResource = new DataSystemResourceDTO(subQueryDO.getDataSystemResource());
        }
        if (Objects.nonNull(subQueryDO.getLeftSubquery())) {
            this.leftSubquery = new WideTableSubqueryDTO(subQueryDO.getLeftSubquery());
        }
        if (Objects.nonNull(subQueryDO.getRightSubquery())) {
            this.rightSubquery = new WideTableSubqueryDTO(subQueryDO.getRightSubquery());
        }
        if (Objects.nonNull(subQueryDO.getSubquery())) {
            this.subquery = new WideTableSubqueryDTO(subQueryDO.getSubquery());
        }
    }
}
