package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DatasetProcessServiceManager {

    private final Map<DataSystemType, DatasetProcessService> serviceMap = Maps.newHashMap();

    public DatasetProcessServiceManager(
        final List<DatasetProcessService> services
    ) {
        services.forEach(it -> {
            Preconditions.checkArgument(!serviceMap.containsKey(it.dataSystemType()), "Already existed same type service.");
            serviceMap.put(it.dataSystemType(), it);
        });
    }

    /**
     * Get source dataset service implementation.
     * @param dataSystemType dataSystemType
     * @return DatasetProcessService
     */
    public DatasetProcessService getService(final DataSystemType dataSystemType) {
        return Optional.of(serviceMap.get(dataSystemType)).get();
    }

    /**
     * Get dataset service implementation for list.
     * @return List
     */
    public Collection<DatasetProcessService> getServices() {
        return serviceMap.values();
    }
}
