package cn.xdf.acdc.devops.service.process.sync;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(Ordered.LOWEST_PRECEDENCE)
@Slf4j
public class DynamicMetaDataSynchronizer implements SynchronizerInOrder {

    private final DataSystemResourceService dataSystemResourceService;

    private final Map<DataSystemType, DataSystemMetadataService> dataSystemTypeServiceMap;

    public DynamicMetaDataSynchronizer(final List<DataSystemMetadataService> dataSystemMetadataServices, final DataSystemResourceService dataSystemResourceService) {
        dataSystemTypeServiceMap = dataSystemMetadataServices.stream().collect(
                Collectors.toMap(DataSystemMetadataService::getDataSystemType, service -> service));
        this.dataSystemResourceService = dataSystemResourceService;
    }

    @Override
    public void sync() {
        StringBuilder exceptions = new StringBuilder();
        dataSystemResourceService.getAllRoots().forEach(cluster -> {
            try {
                dataSystemTypeServiceMap.get(cluster.getDataSystemType()).refreshDynamicDataSystemResource(cluster.getId());
            } catch (ServerErrorException exception) {
                log.warn("Refresh dynamic data system resource error: {}", exception.getMessage());
                exceptions.append("ClusterId: ").append(cluster.getId()).append(", ")
                        .append(exception.getMessage()).append(System.lineSeparator());
            }
        });
        if (exceptions.length() > 0) {
            throw new ServerErrorException("Refresh dynamic data system resource error: " + System.lineSeparator() + exceptions);
        }
    }
}
