package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractJdbcDatasetProcessService;
import org.springframework.stereotype.Service;

@Service
public class MysqlDatasetProcessServiceImpl extends AbstractJdbcDatasetProcessService {

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.MYSQL;
    }
}
