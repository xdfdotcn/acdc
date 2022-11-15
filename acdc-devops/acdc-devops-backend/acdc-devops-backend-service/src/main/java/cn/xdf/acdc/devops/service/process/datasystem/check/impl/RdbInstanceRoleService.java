package cn.xdf.acdc.devops.service.process.datasystem.check.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.core.util.RdbUtil;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.process.datasystem.check.MetadataCheckService;
import io.jsonwebtoken.lang.Collections;
import io.jsonwebtoken.lang.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Order(1)
@Transactional
public class RdbInstanceRoleService implements MetadataCheckService {

    private static final String MYSQL_RDB_WITHOUT_DATA_SOURCE_TITLE = "mysql rdb没有配置数据抽取实例(rdb_id,rdb_name)";

    @Autowired
    private RdbRepository rdbRepository;

    @Override
    public Map<String, List<String>> checkMetadataAndReturnErrorMessage() {
        List<RdbDO> rdbs = rdbRepository.queryAll(new RdbQuery());
        List<String> mysqlRdbWithoutDataSource = rdbs.stream()
                .filter(rdbDO ->
                        Objects.equals(rdbDO.getRdbType(), DataSystemType.MYSQL.getName())
                                // ignore rdb without instance
                                && !Collections.isEmpty(rdbDO.getRdbInstances())
                                && !RdbUtil.hasDataSourceInstance(rdbDO.getRdbInstances())
                )
                .map(RdbDO::getSignature)
                .collect(Collectors.toList());

        if (Collections.isEmpty(mysqlRdbWithoutDataSource)) {
            return new HashMap<>();
        }
        return Maps.of(MYSQL_RDB_WITHOUT_DATA_SOURCE_TITLE, mysqlRdbWithoutDataSource).build();
    }
}
