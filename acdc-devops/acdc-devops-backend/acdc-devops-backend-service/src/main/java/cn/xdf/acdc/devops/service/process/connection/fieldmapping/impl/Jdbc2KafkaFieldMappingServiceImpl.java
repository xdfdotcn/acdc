package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
public class Jdbc2KafkaFieldMappingServiceImpl implements FieldMappingService {

    @Override
    public Set<DataSystemType> supportSrcAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Override
    public Set<DataSystemType> supportSinkAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.KAFKA);
    }

    @Override
    public List<FieldMappingDTO> diffingField(
        final Map<String, FieldDTO> srcFieldMap,
        final Map<String, FieldDTO> sinkFieldMap) {
        List<FieldMappingDTO> fieldMappings = srcFieldMap.entrySet().stream()
            .map(entry -> FieldMappingDTO.builder()
                .sinkField(entry.getValue())
                .sourceField(entry.getValue())
                .matchStatus(FieldMappingDTO.IS_MATCH)
                .build()
            )
            .sorted(Comparator.comparing(this::newSequence))
            .collect(Collectors.toList());

        appendIdForMappings(fieldMappings, () -> 1);

        return fieldMappings;
    }

    @Override
    public String newSequence(final FieldMappingDTO fieldMapping) {
        FieldKeyType fieldKeyType = FieldKeyType.nameOf(fieldMapping.getSinkField().getKeyType());
        return new StringBuilder()
            .append(fieldMapping.getMatchStatus())
            .append(fieldKeyType.getSort())
            .toString();
    }

    @Override
    public String editSequence(final FieldMappingDTO fieldMapping) {
        FieldKeyType fieldKeyType = FieldKeyType.nameOf(fieldMapping.getSourceField().getKeyType());
        return new StringBuilder()
            .append(fieldKeyType.getSort())
            .append(fieldMapping.getSourceField().getName())
            .toString();
    }
}
