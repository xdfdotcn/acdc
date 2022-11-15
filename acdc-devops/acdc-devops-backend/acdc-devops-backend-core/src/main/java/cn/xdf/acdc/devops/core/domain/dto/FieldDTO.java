package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据表字段.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldDTO {

    private static final FieldDTO NULL_FIELD = new FieldDTO();

    private String name = "";

    private String dataType = "";

    private String allowNull = "";

    private String keyType = "";

    private String defaultValue = "";

    private String extra = "";

    /**
     * 判断是否为空字段.
     * @param fieldDTO  fieldDTO
     * @return 是否为空字段
     */
    public static boolean isNull(final FieldDTO fieldDTO) {
        return fieldDTO.equals(NULL_FIELD);
    }

    /**
     * 空字段.
     * @return FieldDTO
     */
    public static FieldDTO empty() {
        return new FieldDTO();
    }
}
