package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DatasetDatabaseDTO {

    private Long databaseId;

    private String databaseName;
}
