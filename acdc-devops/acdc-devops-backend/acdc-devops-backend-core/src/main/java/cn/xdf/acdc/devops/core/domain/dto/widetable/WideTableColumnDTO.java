package cn.xdf.acdc.devops.core.domain.dto.widetable;

import cn.xdf.acdc.devops.core.domain.entity.WideTableColumnDO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Accessors(chain = true)
public class WideTableColumnDTO {
    
    private Long id;
    
    private String name;
    
    private String type;
    
    private Boolean isPrimaryKey;
    
    public WideTableColumnDTO(final WideTableColumnDO wideTableColumnDO) {
        this.id = wideTableColumnDO.getId();
        this.name = wideTableColumnDO.getName();
        this.type = wideTableColumnDO.getType();
        this.isPrimaryKey = wideTableColumnDO.getIsPrimaryKey();
    }
    
    /**
     * To WideTableColumnDO.
     *
     * @return WideTableColumnDO
     */
    public WideTableColumnDO toDO() {
        return new WideTableColumnDO()
                .setId(this.id)
                .setName(this.name)
                .setType(this.type)
                .setIsPrimaryKey(this.isPrimaryKey);
    }
}
