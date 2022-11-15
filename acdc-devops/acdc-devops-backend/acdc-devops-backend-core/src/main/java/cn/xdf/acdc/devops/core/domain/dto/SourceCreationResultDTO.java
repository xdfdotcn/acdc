package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SourceCreationResultDTO {

    private Long createdKafkaTopicId;

    private String dataTopic;

    private String schemaHistoryTopic;

    private String sourceServerTopic;

    private ConnectorDTO createdConnector;
}
