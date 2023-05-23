package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.util.StringUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ConnectionColumnConfigurationDTO {
    
    private Long id;
    
    private String rowId;
    
    private int connectionVersion;
    
    private String sourceColumnName;
    
    private String sourceColumnType;
    
    private Set<String> sourceColumnUniqueIndexNames = new HashSet<>();
    
    private String sinkColumnName;
    
    private String sinkColumnType;
    
    private Set<String> sinkColumnUniqueIndexNames = new HashSet<>();
    
    private String filterOperator;
    
    private String filterValue;
    
    private Date creationTime;
    
    private Date updateTime;
    
    public ConnectionColumnConfigurationDTO(final ConnectionColumnConfigurationDO connectionColumnConfigurationDO) {
        this.id = connectionColumnConfigurationDO.getId();
        this.connectionVersion = connectionColumnConfigurationDO.getConnectionVersion();
        
        this.sourceColumnName = connectionColumnConfigurationDO.getSourceColumnName();
        this.sourceColumnType = connectionColumnConfigurationDO.getSourceColumnType();
        this.sourceColumnUniqueIndexNames = StringUtil.convertStringToSetWithSeparator(
                connectionColumnConfigurationDO.getSourceColumnUniqueIndexNames(),
                Symbol.COMMA
        );
        
        this.sinkColumnName = connectionColumnConfigurationDO.getSinkColumnName();
        this.sinkColumnType = connectionColumnConfigurationDO.getSinkColumnType();
        this.sinkColumnUniqueIndexNames = StringUtil.convertStringToSetWithSeparator(
                connectionColumnConfigurationDO.getSinkColumnUniqueIndexNames(),
                Symbol.COMMA
        );
        
        this.filterOperator = connectionColumnConfigurationDO.getFilterOperator();
        this.filterValue = connectionColumnConfigurationDO.getFilterValue();
        this.creationTime = connectionColumnConfigurationDO.getCreationTime();
        this.updateTime = connectionColumnConfigurationDO.getUpdateTime();
    }
    
    /**
     * Convert to DO.
     *
     * @return ConnectionColumnConfigurationDO
     */
    public ConnectionColumnConfigurationDO toDO() {
        ConnectionColumnConfigurationDO result = new ConnectionColumnConfigurationDO();
        result.setId(this.id);
        result.setConnectionVersion(this.connectionVersion);
        result.setSourceColumnName(this.sourceColumnName);
        result.setSourceColumnType(this.sourceColumnType);
        result.setSourceColumnUniqueIndexNames(StringUtil.convertSetToStringWithSeparator(this.sourceColumnUniqueIndexNames, Symbol.COMMA));
        result.setSinkColumnName(this.sinkColumnName);
        result.setSinkColumnType(this.sinkColumnType);
        result.setSinkColumnUniqueIndexNames(StringUtil.convertSetToStringWithSeparator(this.sinkColumnUniqueIndexNames, Symbol.COMMA));
        result.setFilterOperator(this.filterOperator);
        result.setFilterValue(this.filterValue);
        result.setCreationTime(this.creationTime);
        result.setUpdateTime(this.updateTime);
        return result;
    }
}
