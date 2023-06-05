package cn.xdf.acdc.devops.service.process.check.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.check.CheckerInOrder;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(1)
@Transactional
@Slf4j
public class DataSystemCheckService implements CheckerInOrder {
    
    private static final String CLUSTER_LABEL = "集群id: %d, 集群名称: %s ";
    
    private final DataSystemResourceService dataSystemResourceService;
    
    private final Map<DataSystemType, DataSystemMetadataService> dataSystemTypeServiceMap;
    
    public DataSystemCheckService(final List<DataSystemMetadataService> dataSystemMetadataServices, final DataSystemResourceService dataSystemResourceService) {
        dataSystemTypeServiceMap = dataSystemMetadataServices.stream().collect(
                Collectors.toMap(DataSystemMetadataService::getDataSystemType, service -> service));
        this.dataSystemResourceService = dataSystemResourceService;
    }
    
    @Override
    public Map<String, List<String>> checkMetadataAndReturnErrorMessage() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        dataSystemResourceService.getAllRoots().forEach(cluster -> {
            try {
                dataSystemTypeServiceMap.get(cluster.getDataSystemType()).checkDataSystem(cluster.getId());
            } catch (ServerErrorException e) {
                if (!result.containsKey(e.getMessage())) {
                    result.put(e.getMessage(), new ArrayList<>());
                }
                result.get(e.getMessage()).add(getSignature(cluster));
            }
        });
        return result;
    }
    
    private String getSignature(final DataSystemResourceDTO cluster) {
        return String.format(CLUSTER_LABEL, cluster.getId(), cluster.getName());
    }
}
