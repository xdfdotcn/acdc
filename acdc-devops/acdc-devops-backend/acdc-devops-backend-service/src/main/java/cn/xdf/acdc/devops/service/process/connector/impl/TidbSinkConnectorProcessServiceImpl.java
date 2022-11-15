package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import org.springframework.stereotype.Service;

@Service
public class TidbSinkConnectorProcessServiceImpl extends AbstractJdbcSinkConnectorProcessServiceImpl {

    @Override
    public DataSystemType dataSystemType() {
        return DataSystemType.TIDB;
    }
}
