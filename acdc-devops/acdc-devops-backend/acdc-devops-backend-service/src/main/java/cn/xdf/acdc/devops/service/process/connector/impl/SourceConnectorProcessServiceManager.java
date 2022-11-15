package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connector.SourceConnectorProcessService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SourceConnectorProcessServiceManager {

    private final Map<DataSystemType, SourceConnectorProcessService> serviceMap = Maps.newHashMap();

    public SourceConnectorProcessServiceManager(final List<SourceConnectorProcessService> services) {
        services.forEach(it -> initSourceDatasetServices(it.dataSystemType(), it));
    }

    private void initSourceDatasetServices(final DataSystemType dataSystemType, final SourceConnectorProcessService service) {
        Preconditions.checkArgument(!serviceMap.containsKey(dataSystemType), "Already existed same type service.");
        serviceMap.put(dataSystemType, service);
    }

    /**
     * Get implementation.
     *
     * @param dataSystemType dbType
     * @return SourceConnectProcessService
     */
    public SourceConnectorProcessService getJService(final DataSystemType dataSystemType) {
        return Optional.of(serviceMap.get(dataSystemType)).get();
    }
}
