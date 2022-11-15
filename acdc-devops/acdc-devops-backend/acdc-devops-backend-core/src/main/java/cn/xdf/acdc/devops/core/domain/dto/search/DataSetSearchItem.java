package cn.xdf.acdc.devops.core.domain.dto.search;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataSetSearchItem {

    private Long id;

    private List<DataSetTreeNode> nodes;
}
