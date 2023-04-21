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
        return ConnectionColumnConfigurationDO.builder()
                .id(this.id)
                .connectionVersion(this.connectionVersion)
                .sourceColumnName(this.sourceColumnName)
                .sourceColumnType(this.sourceColumnType)
                .sourceColumnUniqueIndexNames(StringUtil.convertSetToStringWithSeparator(this.sourceColumnUniqueIndexNames, Symbol.COMMA))
                .sinkColumnName(this.sinkColumnName)
                .sinkColumnType(this.sinkColumnType)
                .sinkColumnUniqueIndexNames(StringUtil.convertSetToStringWithSeparator(this.sinkColumnUniqueIndexNames, Symbol.COMMA))
                .filterOperator(this.filterOperator)
                .filterValue(this.filterValue)
                .creationTime(this.creationTime)
                .updateTime(this.updateTime)
                .build();
    }
}
