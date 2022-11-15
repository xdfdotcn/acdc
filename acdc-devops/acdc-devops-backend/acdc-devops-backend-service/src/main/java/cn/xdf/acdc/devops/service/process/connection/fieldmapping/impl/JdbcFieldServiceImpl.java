package cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldDTO;
import cn.xdf.acdc.devops.core.domain.dto.HostAndPortDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.entity.RdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
public class JdbcFieldServiceImpl implements FieldService {

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Autowired
    private RdbTableService rdbTableService;

    @Override
    public Map<String, FieldDTO> descDataSet(final DataSetDTO dataSet) {
        Long rdbTableId = dataSet.getDataSetId();
        RdbTableDO rdbTable = rdbTableService.findById(rdbTableId)
                .orElseThrow(() -> new NotFoundException(String.format("rdbTableId: %s", rdbTableId)));

        RdbDatabaseDO rdbDatabase = rdbTable.getRdbDatabase();

        RdbDO rdb = rdbDatabase.getRdb();
        Set<RdbInstanceDO> rdbInstances = rdb.getRdbInstances();
        String database = rdbDatabase.getName();
        String table = rdbTable.getName();

        return mysqlHelperService
                .descTable(HostAndPortDTO.toHostAndPortDTOs(rdbInstances), rdb.getUsername(), EncryptUtil.decrypt(rdb.getPassword()), database, table).stream()
                .collect(Collectors.toMap(fieldDTO -> fieldDTO.getName().toLowerCase(), fieldDTO -> fieldDTO));
    }

    @Override
    public Set<DataSystemType> supportAppTypes() {
        return Sets.immutableEnumSet(DataSystemType.MYSQL, DataSystemType.TIDB);
    }
}
