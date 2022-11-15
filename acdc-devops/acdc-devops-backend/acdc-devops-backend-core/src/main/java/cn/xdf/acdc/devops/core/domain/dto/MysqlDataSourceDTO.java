package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MysqlDataSourceDTO extends PageDTO {

    private Long rdbId;

    private String host;

    private Integer port;

    public MysqlDataSourceDTO(final RdbInstanceDO rdbInstance) {
        this.rdbId = rdbInstance.getRdb().getId();
        this.host = rdbInstance.getHost();
        this.port = rdbInstance.getPort();
    }
}
