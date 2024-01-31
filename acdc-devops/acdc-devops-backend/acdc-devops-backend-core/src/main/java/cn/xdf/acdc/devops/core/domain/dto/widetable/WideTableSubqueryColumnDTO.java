package cn.xdf.acdc.devops.core.domain.dto.widetable;

import cn.xdf.acdc.devops.core.domain.entity.WideTableSubqueryColumnDO;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class WideTableSubqueryColumnDTO {
    
    private Long id;
    
    private String expression;
    
    private String alias;
    
    private String type;
    
    private List<Long> parentIds;
    
    public WideTableSubqueryColumnDTO(final WideTableSubqueryColumnDO columnDO) {
        this.id = columnDO.getId();
        this.expression = columnDO.getExpression();
        this.alias = columnDO.getAlias();
        this.type = columnDO.getType();
        this.parentIds = columnDO.getWideTableSubqueryColumnLineages().stream()
                .map(it -> it.getParentColumn().getId())
                .collect(Collectors.toList());
    }
}
