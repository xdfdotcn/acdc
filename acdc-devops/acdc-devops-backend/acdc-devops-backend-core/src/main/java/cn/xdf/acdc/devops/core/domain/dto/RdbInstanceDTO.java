package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RdbInstanceDTO {

    private Long id;

    private String host;

    private String port;

    private String vip;

    private RoleType roleType;

    public RdbInstanceDTO(final RdbInstanceDO rdbInstanceDO) {
        this.id = rdbInstanceDO.getId();
        this.host = rdbInstanceDO.getHost();
        this.port = String.valueOf(rdbInstanceDO.getPort());
        this.vip = String.valueOf(rdbInstanceDO.getVip());
        this.roleType = rdbInstanceDO.getRole();
    }

    /**
     * Convert to RdbInstanceDO.
     *
     * @return RdbInstanceDO
     */
    public RdbInstanceDO toRdbInstanceDO() {
        RdbInstanceDO instanceDO = new RdbInstanceDO();
        instanceDO.setHost(this.getHost());
        instanceDO.setPort(Integer.valueOf(this.getPort()));
        instanceDO.setRole(this.getRoleType());
        instanceDO.setVip(this.getVip());
        instanceDO.setCreationTime(Instant.now());
        instanceDO.setUpdateTime(Instant.now());
        return instanceDO;
    }
}
