package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.entity.HiveTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HiveHelperService;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
public class HiveFieldServiceImpl implements FieldService {

    @Autowired
    private HiveHelperService hiveHelperService;

    @Autowired
    private HiveTableService hiveTableService;

    @Override
    public Map<String, FieldDTO> descDataSet(final DataSetDTO dataSet) {
        Long hiveTableId = dataSet.getDataSetId();
        HiveTableDO hiveTable = hiveTableService.findById(hiveTableId)
            .orElseThrow(() -> new NotFoundException(String.format("hiveTableId: %s", hiveTableId)));

        String database = hiveTable.getHiveDatabase().getName();
        String table = hiveTable.getName();

        return hiveHelperService.descTable(database, table).stream()
            .collect(Collectors.toMap(it -> it.getName().toLowerCase(), it -> it));
    }

    @Override
    public Set<DataSystemType> supportAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.HIVE);
    }
}
