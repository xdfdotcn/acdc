package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connector.SinkConnectorProcessService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SinkConnectorProcessServiceManager {

    private final Map<DataSystemType, SinkConnectorProcessService> serviceMap = Maps.newHashMap();

    public SinkConnectorProcessServiceManager(final List<SinkConnectorProcessService> services) {
        services.forEach(it -> initSourceDatasetServices(it.dataSystemType(), it));
    }

    private void initSourceDatasetServices(final DataSystemType dataSystemType, final SinkConnectorProcessService service) {
        Preconditions.checkArgument(!serviceMap.containsKey(dataSystemType), "Already existed same type service.");
        serviceMap.put(dataSystemType, service);
    }

    /**
     * Get implementation.
     *
     * @param dataSystemType dbType
     * @return SinkConnectProcessService
     */
    public SinkConnectorProcessService getJService(final DataSystemType dataSystemType) {
        return Optional.of(serviceMap.get(dataSystemType)).get();
    }
}
