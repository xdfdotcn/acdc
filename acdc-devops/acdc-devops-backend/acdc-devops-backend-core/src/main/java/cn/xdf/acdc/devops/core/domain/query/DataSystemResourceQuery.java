package cn.xdf.acdc.devops.core.domain.query;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Accessors(chain = true)
public class DataSystemResourceQuery extends PagedQuery {

    private Long parentResourceId;

    private String name;

    private List<DataSystemResourceType> resourceTypes;

    private Map<String, String> resourceConfigurations;

    private Long projectId;

    private Boolean deleted;
}
