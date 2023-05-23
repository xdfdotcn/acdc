package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HiveQuery {
    
    private String name;
    
    private List<Long> hiveIds;
}
