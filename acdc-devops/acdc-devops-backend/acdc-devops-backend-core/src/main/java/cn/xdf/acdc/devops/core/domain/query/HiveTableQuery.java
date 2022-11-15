package cn.xdf.acdc.devops.core.domain.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class HiveTableQuery {

    private Long id;

    private String name;

    @Builder.Default
    private Boolean deleted = Boolean.FALSE;
}
