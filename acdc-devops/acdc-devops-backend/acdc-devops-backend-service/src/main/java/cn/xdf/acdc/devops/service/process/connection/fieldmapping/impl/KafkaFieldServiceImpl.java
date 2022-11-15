package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Service
@Transactional
public class KafkaFieldServiceImpl implements FieldService {

    @Override
    public Map<String, FieldDTO> descDataSet(final DataSetDTO dataSet) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public Set<DataSystemType> supportAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.KAFKA);
    }
}
