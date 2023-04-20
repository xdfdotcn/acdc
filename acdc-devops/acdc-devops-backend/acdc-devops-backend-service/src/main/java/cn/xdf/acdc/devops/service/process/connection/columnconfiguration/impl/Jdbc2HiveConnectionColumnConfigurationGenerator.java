package cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.ConnectionColumnConfigurationGenerator;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class Jdbc2HiveConnectionColumnConfigurationGenerator implements ConnectionColumnConfigurationGenerator {

    @Override
    public Set<DataSystemType> supportedSourceDataSystemTypes() {
        return Sets.immutableEnumSet(DataSystemType.MYSQL, DataSystemType.TIDB);
    }

    @Override
    public Set<DataSystemType> supportedSinkDataSystemTypes() {
        return Sets.immutableEnumSet(DataSystemType.HIVE);
    }
}
