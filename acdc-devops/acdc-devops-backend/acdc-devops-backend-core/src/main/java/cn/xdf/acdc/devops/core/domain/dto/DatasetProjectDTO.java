package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Cluster.
 *
 */
// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatasetProjectDTO {

    private Long projectId;

    private String projectName;

    private String dataSystemType;

    private String desc;
}
