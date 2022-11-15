package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConnectorEventDTO {

    private Long id;

    private String reason;

    private String message;

    private String creationTime;

    private String updateTime;

    private Integer source;

    private Integer level;

    private Long connectorId;

    public ConnectorEventDTO(final ConnectorEventDO connectorEvent) {
        this.id = connectorEvent.getId();
        this.reason = connectorEvent.getReason();
        this.message = connectorEvent.getMessage();
        this.creationTime = DateUtil.formatToString(connectorEvent.getCreationTime());
        this.updateTime = DateUtil.formatToString(connectorEvent.getUpdateTime());
        this.source = connectorEvent.getSource().ordinal();
        this.level = connectorEvent.getLevel().ordinal();
        this.connectorId = connectorEvent.getConnector().getId();
    }
}
