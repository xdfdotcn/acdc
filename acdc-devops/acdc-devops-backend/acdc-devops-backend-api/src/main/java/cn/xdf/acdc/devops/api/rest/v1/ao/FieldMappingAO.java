package cn.xdf.acdc.devops.api.rest.v1.ao;

import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF

/**
 * source 与sink的字段映射 .
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldMappingAO {

    private static final String DEFAULT_DATA_TYPE = "string";

    private static final String TAB = "\t";

    private static final String EMPTY_NAME = "";

    // 主键id,目前没有主键,采用顺序生成
    private Long id;

    // 上游表ID
    private Long upRdbTableId;

    private String filterOperator = "";

    private String filterValue = "";

    // 匹配状态
    private Integer matchStatus = FieldMappingDTO.NOT_MATCH;

    // 原表数据库字段描述信息
    private String sourceField = "";

    // 目标表数据库字段描述信息
    private String sinkField = "";

    public FieldMappingAO(FieldMappingDTO fieldMappingDTO) {
        this.id = fieldMappingDTO.getId();
        this.filterOperator = fieldMappingDTO.getFilterOperator();
        this.filterValue = fieldMappingDTO.getFilterValue();
        this.matchStatus = fieldMappingDTO.getMatchStatus();
        this.sinkField = FieldMappingDTO.formatToString(fieldMappingDTO.getSinkField());

        if (!Strings.isNullOrEmpty(fieldMappingDTO.getSourceField().getName())
            && !fieldMappingDTO.getSourceField().getName().equals(FieldMappingDTO.META_NONE)
        ) {
            this.sourceField = FieldMappingDTO.formatToString(fieldMappingDTO.getSourceField());
        }
    }

    public static FieldMappingDTO toFieldMappingDTO(FieldMappingAO fieldMapping) {
        return FieldMappingDTO.builder()
            .sinkField(FieldMappingDTO.formatToField(fieldMapping.getSinkField()))
            .sourceField(FieldMappingDTO.formatToField(fieldMapping.getSourceField()))
            .filterOperator(fieldMapping.getFilterOperator())
            .filterValue(fieldMapping.getFilterValue())
            .build();
    }
}
