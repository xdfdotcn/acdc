package cn.xdf.acdc.devops.core.domain.dto.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataSetSearchItem {

    private Long id;

    private List<DataSetTreeNode> nodes;
}
