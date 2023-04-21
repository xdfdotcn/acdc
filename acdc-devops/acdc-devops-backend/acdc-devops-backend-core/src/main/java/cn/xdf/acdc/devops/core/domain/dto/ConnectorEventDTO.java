package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConnectorEventDTO {

    private Long id;

    private String reason;

    private String message;

    private Date creationTime;

    private Date updateTime;

    private Integer source;

    private Integer level;

    private Long connectorId;

    public ConnectorEventDTO(final ConnectorEventDO connectorEvent) {
        this.id = connectorEvent.getId();
        this.reason = connectorEvent.getReason();
        this.message = connectorEvent.getMessage();
        this.creationTime = connectorEvent.getCreationTime();
        this.updateTime = connectorEvent.getUpdateTime();
        this.source = connectorEvent.getSource().ordinal();
        this.level = connectorEvent.getLevel().ordinal();
        this.connectorId = connectorEvent.getConnector().getId();
    }
}
