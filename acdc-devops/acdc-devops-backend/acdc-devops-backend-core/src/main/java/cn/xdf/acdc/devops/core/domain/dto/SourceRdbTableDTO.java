package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Source rdb table.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SourceRdbTableDTO {

    private Long id;

    private Long connectorId;

    private Long rdbTableId;

    private String dataTopic;

    private String schemaHistoryTopic;

    private String sourceServerTopic;

    public SourceRdbTableDTO(final SourceRdbTableDO sourceRdbTable) {
        this.id = sourceRdbTable.getId();
        this.connectorId = sourceRdbTable.getConnector().getId();
        this.rdbTableId = sourceRdbTable.getRdbTable().getId();
        this.dataTopic = sourceRdbTable.getKafkaTopic().getName();
    }
}
