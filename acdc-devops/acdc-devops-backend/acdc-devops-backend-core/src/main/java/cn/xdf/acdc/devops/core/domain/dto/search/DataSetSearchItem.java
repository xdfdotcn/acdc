package cn.xdf.acdc.devops.core.domain.dto.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class DataSetSearchItem {
    
    private Long id;
    
    private List<DataSetTreeNode> nodes;
}
