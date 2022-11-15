package cn.xdf.acdc.devops.service.process.connection.fieldmapping;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface FieldMappingService {

    /**
     * Source app type.
     *
     * @return Set
     */
    Set<DataSystemType> supportSrcAppTypes();

    /**
     * Sink app type.
     *
     * @return Set
     */
    Set<DataSystemType> supportSinkAppTypes();

    /**
     * Diffing filed.
     *
     * @param srcFieldMap srcFieldMap
     * @param sinkFieldMap sinkFieldMap
     * @return List
     */
    default List<FieldMappingDTO> diffingField(
        Map<String, FieldDTO> srcFieldMap,
        Map<String, FieldDTO> sinkFieldMap
    ) {
        List<FieldMappingDTO> fieldMappings = sinkFieldMap.entrySet().stream()
            .map(entry -> buildFiledMapping(entry, srcFieldMap))
            // 优先展示匹配到的字段，主键字排前面
            .sorted(Comparator.comparing(this::newSequence))
            .collect(Collectors.toList());

        // react 前端组件需要存在id作为 unique key
        appendIdForMappings(fieldMappings, () -> 1);

        return fieldMappings;
    }

    /**
     * Diffing field.
     *
     * @param sourceNameToFieldWithCurrentDdl sourceNameToFieldWithCurrentDdl
     * @param sinkNameToFieldWithCurrentDdl sinkNameToFieldWithCurrentDdl
     * @param existFieldMappingConfig existFieldMappingConfig
     * @return field mappings
     */
    default List<FieldMappingDTO> diffingField(
        Map<String, FieldDTO> sourceNameToFieldWithCurrentDdl,
        Map<String, FieldDTO> sinkNameToFieldWithCurrentDdl,
        List<FieldMappingDTO> existFieldMappingConfig
    ) {
        Map<String, FieldMappingDTO> sinkNameToFieldMappingByExistConfig =
            existFieldMappingConfig.stream().collect(
                Collectors.toMap(fieldMapping -> fieldMapping.getSinkField().getName(), fieldMapping -> fieldMapping));
        List<FieldMappingDTO> fieldMappings = sinkNameToFieldWithCurrentDdl.entrySet().stream()
            .map(
                entry -> buildFiledMapping(entry, sourceNameToFieldWithCurrentDdl, sinkNameToFieldMappingByExistConfig))
            // 优先展示匹配到的字段，主键字排前面
            .sorted(Comparator.comparing(this::newSequence))
            .collect(Collectors.toList());

        // react 前端组件需要存在id作为 unique key
        appendIdForMappings(fieldMappings, () -> 1);

        return fieldMappings;
    }

    /**
     * Get sequence fro each mappings.
     *
     * @param fieldMapping fieldMapping
     * @return sequence
     */
    String newSequence(FieldMappingDTO fieldMapping);

    /**
     * Get sequence fro each mappings.
     *
     * @param fieldMapping columnMapping
     * @return sequence
     */
    String editSequence(FieldMappingDTO fieldMapping);

    /**
     * Build fieldMapping with exist config.
     *
     * @param sinkFieldEntry sink field entry
     * @param sourceNameToFieldWithCurrentDdl source name to field with current ddl
     * @param sinkNameToFieldMappingByExistConfig sink name to field mapping by exist config
     * @return FieldMappingDTO
     */
    default FieldMappingDTO buildFiledMapping(
        Map.Entry<String, FieldDTO> sinkFieldEntry,
        Map<String, FieldDTO> sourceNameToFieldWithCurrentDdl,
        Map<String, FieldMappingDTO> sinkNameToFieldMappingByExistConfig) {
        String sinkFieldName = sinkFieldEntry.getKey();
        FieldDTO sinkField = sinkFieldEntry.getValue();
        FieldMappingDTO fieldMappingByExistConfig = sinkNameToFieldMappingByExistConfig.get(sinkFieldName);

        boolean notMatched = Objects.isNull(fieldMappingByExistConfig)
            || FieldMappingDTO.isNone(fieldMappingByExistConfig.getSourceField().getName())
            || (!sourceNameToFieldWithCurrentDdl.containsKey(fieldMappingByExistConfig.getSourceField().getName())
            && !fieldMappingByExistConfig.getSourceField().getName().startsWith(FieldMappingDTO.META_PREFIX));
        if (notMatched) {
            return FieldMappingDTO.builder().sinkField(sinkField).sourceField(FieldDTO.empty())
                .matchStatus(FieldMappingDTO.NOT_MATCH).build();
        }

        String matchedSourceName = fieldMappingByExistConfig.getSourceField().getName();
        FieldDTO sourceField;
        if (fieldMappingByExistConfig.getSourceField().getName().startsWith(FieldMappingDTO.META_PREFIX)) {
            sourceField = fieldMappingByExistConfig.getSourceField();
        } else {
            sourceField = sourceNameToFieldWithCurrentDdl.get(matchedSourceName);
        }
        return FieldMappingDTO.builder()
            .sinkField(sinkField)
            .sourceField(sourceField)
            .filterOperator(fieldMappingByExistConfig.getFilterOperator())
            .filterValue(fieldMappingByExistConfig.getFilterValue())
            .matchStatus(FieldMappingDTO.IS_MATCH)
            .build();
    }

    /**
     * Build fieldMapping.
     *
     * @param sinkFieldEntry sinkFieldEntry
     * @param sourceFieldMap sourceFieldMap
     * @return FieldMappingDTO
     */
    default FieldMappingDTO buildFiledMapping(
        final Map.Entry<String, FieldDTO> sinkFieldEntry,
        final Map<String, FieldDTO> sourceFieldMap
    ) {
        String sinkFieldName = sinkFieldEntry.getKey();
        FieldDTO sinkField = sinkFieldEntry.getValue();
        FieldDTO sourceField = sourceFieldMap.get(sinkFieldName);

        int matchStatus = Objects.isNull(sourceField) ? FieldMappingDTO.NOT_MATCH : FieldMappingDTO.IS_MATCH;
        FieldDTO matchedSourceField = matchStatus == FieldMappingDTO.NOT_MATCH ? FieldDTO.empty() : sourceField;

        return FieldMappingDTO.builder()
            .sinkField(sinkField)
            .sourceField(matchedSourceField)
            .matchStatus(matchStatus)
            .build();
    }

    /**
     * Append ID for each item.
     *
     * @param mappings mappings
     * @param beginId begin id
     */
    default void appendIdForMappings(List<FieldMappingDTO> mappings, Supplier<Integer> beginId) {
        int begin = beginId.get();
        for (FieldMappingDTO mapping : mappings) {
            mapping.setId(Long.valueOf(begin++));
        }
    }
}
