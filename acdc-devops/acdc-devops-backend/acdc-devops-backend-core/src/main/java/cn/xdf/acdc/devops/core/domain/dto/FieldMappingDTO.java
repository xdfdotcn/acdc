package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * 字段映射.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldMappingDTO {

    public static final String TAB = "\t";

    public static final String BLANK = " ";

    public static final String META_OP = "__op";

    public static final String META_KAFKA_RECORD_OFFSET = "__kafka_record_offset";

    public static final String META_LOGICAL_DEL = "__logical_del";

    public static final String META_DATE_TIME = "__datetime";

    public static final String META_NONE = "__none";

    public static final String META_PREFIX = "__";

    public static final int IS_MATCH = 1;

    public static final int NOT_MATCH = 2;

    public static final FieldDTO NULL_FIELD = null;

    public static final Set<String> META_FIELD_FILTER_SET = Sets.newHashSet(
            META_LOGICAL_DEL,
            META_DATE_TIME,
            META_NONE
    );

    public static final List<String> META_FIELD_LIST = Lists.newArrayList(
            META_NONE,
            META_OP,
            META_KAFKA_RECORD_OFFSET,
            META_LOGICAL_DEL,
            META_DATE_TIME
    );

    private static final String EXPRESS_AND = " and ";

    private static final String EXTENSION_FIELD_VALUE = "${datetime}";

    private static final String DEFAULT_DATA_TYPE = "string";

    private static final String EMPTY_NAME = "";

    @JsonIgnore
    private FieldDTO sourceField;

    private String sourceFieldFormat;

    @JsonIgnore
    private FieldDTO sinkField;

    private String sinkFieldFormat;

    @Builder.Default
    private String filterOperator = "";

    @Builder.Default
    private String filterValue = "";

    private Integer matchStatus;

    private Long id;

    /**
     * Determines whether the field is none.
     *
     * @param fieldName fieldName
     * @return boolean
     */
    public static boolean isNone(final String fieldName) {
        return Strings.isNullOrEmpty(fieldName) || fieldName.equals(FieldMappingDTO.META_NONE);
    }

    /**
     * 获取逻辑删除列.
     *
     * @param fieldMappings fieldMappings
     * @return LogicalDelDTO
     */
    public static Optional<LogicalDelDTO> findLogicalDelColumn(final List<FieldMappingDTO> fieldMappings) {
        return fieldMappings.stream().filter(fieldMapping -> Objects.equals(fieldMapping.getSourceField().getName(), META_LOGICAL_DEL))
                .map(LogicalDelDTO::new).findFirst();
    }

    /**
     * 获取过滤列表达式.
     *
     * @param fieldMappings fieldMappings
     * @return String
     */
    public static Optional<String> findRowFilterExpress(final List<FieldMappingDTO> fieldMappings) {

        RowFilterExpress rowFilterExpress = RowFilterExpress.newRowFilterExpress();
        fieldMappings.forEach(it -> rowFilterExpress.append(it));

        return Strings.isNullOrEmpty(rowFilterExpress.filterExpress())
                ? Optional.empty()
                : Optional.of(rowFilterExpress.filterExpress());
    }

    /**
     * 过滤列表达式转换成 Map.
     *
     * @param rowFilterExpress rowFilterExpress
     * @return Map
     */
    public static Map<String, String> convertRowFilterExpressToMap(final String rowFilterExpress) {
        if (Strings.isNullOrEmpty(rowFilterExpress)) {
            return new HashMap<>();
        }
        return Splitter.on(EXPRESS_AND).splitToList(rowFilterExpress).stream()
                .collect(Collectors.toMap(it -> Splitter.on(BLANK).splitToList(it).get(0), it -> it));
    }

    /**
     * 转换成 字段mapping 集合.
     *
     * @param fieldMappings fieldMappings
     * @return List
     */
    public static List<SinkConnectorColumnMappingDO> toSinkColumnMappingList(final List<FieldMappingDTO> fieldMappings) {
        return fieldMappings.stream()
//            .filter(fieldMapping -> !META_FIELD_FILTER_SET.contains(fieldMapping.getSourceField().getName()))
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getSinkField().getName()))
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getSourceField().getName()))
                .map(fieldMapping -> SinkConnectorColumnMappingDO.builder()
                        .sourceColumnName(formatToString(fieldMapping.getSourceField()))
                        .sinkColumnName(formatToString(fieldMapping.getSinkField()))
                        .creationTime(new Date().toInstant())
                        .updateTime(new Date().toInstant())
                        .build()
                ).collect(Collectors.toList());
    }

    /**
     * 转换成 扩展 集合.
     *
     * @param fieldMappings fieldMappings
     * @return List
     */
    public static List<ConnectorDataExtensionDO> toConnectorDataExtensionList(final List<FieldMappingDTO> fieldMappings) {
        return fieldMappings.stream()
                .filter(fieldMapping -> Objects.equals(fieldMapping.getSourceField().getName(), META_DATE_TIME))
                .map(fieldMapping -> ConnectorDataExtensionDO.builder()
                        .name(fieldMapping.getSinkField().getName())
                        .value(EXTENSION_FIELD_VALUE)
                        .creationTime(new Date().toInstant())
                        .updateTime(new Date().toInstant())
                        .build()
                ).collect(Collectors.toList());
    }

    // CHECKSTYLE:ON

    /**
     * 获取 source 端主键.
     *
     * @param fieldMappings fieldMappings
     * @return List
     */
    public static List<FieldDTO> getSourcePrimaryFields(final List<FieldMappingDTO> fieldMappings) {
        List<FieldDTO> primaryKeyFields = fieldMappings.stream().filter(fieldMapping -> {
            String key = fieldMapping.getSourceField().getKeyType();
            return FieldKeyType.nameOf(key) == FieldKeyType.PRI || FieldKeyType.nameOf(key) == FieldKeyType.UNI;
        }).map(mapping -> mapping.getSourceField()).limit(1)
                .collect(Collectors.toList());
        return primaryKeyFields;
    }

    /**
     * 获取 sink 端主键.
     *
     * @param fieldMappings fieldMappings
     * @return List
     */
    public static List<FieldDTO> getSinkPrimaryFields(final List<FieldMappingDTO> fieldMappings) {
        List<FieldDTO> primaryKeyFields = fieldMappings.stream().filter(fieldMapping -> {
            String key = fieldMapping.getSinkField().getKeyType();
            return FieldKeyType.nameOf(key) == FieldKeyType.PRI;
        }).map(mapping -> mapping.getSinkField())
                .collect(Collectors.toList());

        return primaryKeyFields;
    }

    /**
     * 字段 format 转换成 FieldDTO.
     *
     * @param format format
     * @return FieldDTO
     */
    public static FieldDTO formatToField(final String format) {
        List<String> names = Splitter.on(TAB).splitToList(format);
        Preconditions.checkArgument(!CollectionUtils.isEmpty(names) && names.size() >= 2);
        String fieldName = names.get(0);
        String dataType = names.get(1);
        String keyType = names.size() >= 3 ? names.get(2) : EMPTY_NAME;
        return FieldDTO.builder()
                .name(fieldName)
                .dataType(dataType)
                .keyType(keyType)
                .build();
    }

    /**
     * 字段 format.
     *
     * @param fieldDTO fieldDTO
     * @return string format
     */
    public static String formatToString(final FieldDTO fieldDTO) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldDTO.getName()));

        StringBuilder builder = new StringBuilder();
        builder.append(fieldDTO.getName())
                .append(TAB)
                .append(fieldDTO.getDataType());

        if (!Strings.isNullOrEmpty(fieldDTO.getKeyType())) {
            builder.append(TAB).append(fieldDTO.getKeyType());
        }
        return builder.toString();
    }

    /**
     * 元字段 format.
     *
     * @param metaField metaField
     * @return string format
     */
    public static String metaFormatToString(final String metaField) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(metaField) && metaField.startsWith(META_PREFIX));

        return new StringBuilder()
                .append(metaField)
                .append(TAB)
                .append(DEFAULT_DATA_TYPE)
                .toString();
    }

    /**
     * Convert ConnectionColumnConfigurationDO to FieldMappingDTO.
     *
     * @param conf conf
     * @return FieldMappingDTO
     */
    public static FieldMappingDTO toFieldMapping(final ConnectionColumnConfigurationDO conf) {
        return FieldMappingDTO.builder()
                .id(conf.getId())
                .sourceField(formatToField(conf.getSourceColumnName()))
                .sinkField(formatToField(conf.getSinkColumnName()))
                .filterOperator(conf.getFilterOperator())
                .filterValue(conf.getFilterValue())
                .matchStatus(IS_MATCH)
                .build();
    }

    /**
     * Convert  FieldMapping list  to ConnectionColumnConfigurationDO list.
     *
     * @param fieldMappings fieldMappings
     * @return List
     */
    public static List<ConnectionColumnConfigurationDO> toConnectionColumnConfiguration(
            final List<FieldMappingDTO> fieldMappings) {
        return fieldMappings.stream()
//            .filter(fieldMapping -> !META_FIELD_FILTER_SET.contains(fieldMapping.getSourceField().getName()))
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getSinkField().getName()))
                .filter(fieldMapping -> StringUtils.isNotBlank(fieldMapping.getSourceField().getName()))
                .map(fieldMapping -> buildConnectionColumnConfiguration(fieldMapping)
                ).collect(Collectors.toList());
    }

    private static ConnectionColumnConfigurationDO buildConnectionColumnConfiguration(
            final FieldMappingDTO fieldMapping
    ) {
        return ConnectionColumnConfigurationDO.builder()
                .sourceColumnName(formatToString(fieldMapping.getSourceField()))
                .sinkColumnName(formatToString(fieldMapping.getSinkField()))
                .filterOperator(fieldMapping.getFilterOperator().trim())
                .filterValue(fieldMapping.getFilterValue().trim())
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build();
    }

    // CHECKSTYLE:OFF
    public String getSourceFieldFormat() {
        return isNone(sourceField.getName()) ? EMPTY_NAME : formatToString(sourceField);
    }

    // CHECKSTYLE:OFF
    public void setSourceFieldFormat(String sourceFieldFormat) {
        this.sourceFieldFormat = Strings.isNullOrEmpty(sourceFieldFormat)
                ? metaFormatToString(FieldMappingDTO.META_NONE)
                : sourceFieldFormat;

        this.sourceField = formatToField(this.sourceFieldFormat);
    }

    public String getSinkFieldFormat() {
        return formatToString(sinkField);
    }

    public void setSinkFieldFormat(String sinkFieldFormat) {
        this.sinkFieldFormat = sinkFieldFormat;
        this.sinkField = formatToField(this.sinkFieldFormat);
    }

    public void setFilterOperator(String filterOperator) {
        this.filterOperator = Strings.isNullOrEmpty(filterOperator) ?
                SystemConstant.EMPTY_STRING
                : filterOperator;
    }

    public void setFilterValue(String filterValue) {
        this.filterValue = Strings.isNullOrEmpty(filterValue) ?
                SystemConstant.EMPTY_STRING
                : filterValue;

    }

    /**
     * 逻辑删除字段.
     */
    @Getter
    @Setter
    @NoArgsConstructor
    public static final class LogicalDelDTO {

        private static final String LOGICAL_DELETION_COLUMN_VALUE_DELETION = "1";

        private static final String LOGICAL_DELETION_COLUMN_VALUE_NORMAL = "0";

        private String logicalDeletionColumn;

        private String logicalDeletionColumnValueDeletion;

        private String logicalDeletionColumnValueNormal;

        public LogicalDelDTO(final FieldMappingDTO fieldMapping) {
            this.logicalDeletionColumn = fieldMapping.getSinkField().getName();
            this.logicalDeletionColumnValueDeletion = LOGICAL_DELETION_COLUMN_VALUE_DELETION;
            this.logicalDeletionColumnValueNormal = LOGICAL_DELETION_COLUMN_VALUE_NORMAL;
        }
    }

    private static final class RowFilterExpress {

        private final StringBuilder filterExpress = new StringBuilder();

        private RowFilterExpress() {

        }

        private static RowFilterExpress newRowFilterExpress() {
            return new RowFilterExpress();
        }

        private RowFilterExpress append(final FieldMappingDTO fieldMapping) {
            String filterOperator = fieldMapping.getFilterOperator();
            String filterValue = fieldMapping.getFilterValue();
            String sourceFieldName = fieldMapping.getSourceField().getName();
            if (Strings.isNullOrEmpty(filterOperator)
                    || Strings.isNullOrEmpty(filterOperator = filterOperator.trim())
                    || Strings.isNullOrEmpty(filterValue)
                    || Strings.isNullOrEmpty(filterValue = filterValue.trim())) {

                return this;
            }

            if (filterExpress.length() == 0) {
                filterExpress
                        .append(sourceFieldName)
                        .append(BLANK)
                        .append(filterOperator)
                        .append(BLANK).append(filterValue);
            } else {
                filterExpress
                        .append(EXPRESS_AND)
                        .append(sourceFieldName)
                        .append(BLANK)
                        .append(filterOperator)
                        .append(BLANK)
                        .append(filterValue);
            }

            return this;
        }

        private String filterExpress() {
            return this.filterExpress.toString();
        }
    }
}
