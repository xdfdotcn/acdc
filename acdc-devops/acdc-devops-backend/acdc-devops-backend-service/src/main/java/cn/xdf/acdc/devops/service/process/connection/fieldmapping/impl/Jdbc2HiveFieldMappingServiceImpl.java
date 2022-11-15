package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.FieldKeyType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

@Service
@Transactional
public class Jdbc2HiveFieldMappingServiceImpl implements FieldMappingService {

    @Override
    public Set<DataSystemType> supportSrcAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Override
    public Set<DataSystemType> supportSinkAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.HIVE);
    }

    @Override
    public String newSequence(final FieldMappingDTO fieldMapping) {
        // 匹配状态,key
        FieldKeyType fieldKeyType = FieldKeyType.nameOf(fieldMapping.getSourceField().getKeyType());
        return new StringBuilder()
            .append(fieldMapping.getMatchStatus())
            .append(fieldKeyType.getSort())
            .toString();
    }

    @Override
    public String editSequence(final FieldMappingDTO fieldMapping) {
        // key,名称
        FieldKeyType fieldKeyType = FieldKeyType.nameOf(fieldMapping.getSourceField().getKeyType());
        return new StringBuilder()
            .append(fieldKeyType.getSort())
            .append(fieldMapping.getSinkField().getName())
            .toString();
    }
}
