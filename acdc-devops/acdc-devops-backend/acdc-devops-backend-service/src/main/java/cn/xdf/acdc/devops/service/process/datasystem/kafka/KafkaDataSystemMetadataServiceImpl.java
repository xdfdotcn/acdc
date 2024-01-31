package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaDataSystemMetadataServiceImpl implements DataSystemMetadataService {
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private KafkaHelperService kafkaHelperService;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return KafkaDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    @Transactional
    public DataCollectionDefinition getDataCollectionDefinition(final Long dataCollectionId) {
        // TODO: as sink, we should return a empty field definition.
        // but as source, we should return a field definition with two field named: message key, message value
        
        DataSystemResourceDTO dataSystemResource = dataSystemResourceService.getById(dataCollectionId);
        return new DataCollectionDefinition(dataSystemResource.getName(), Collections.emptyList());
    }
    
    @Override
    public void checkDataSystem(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(rootDataSystemResourceId);
        checkDataSystem(clusterResource);
    }
    
    @Override
    public void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        if (DataSystemResourceType.KAFKA_CLUSTER.equals(dataSystemResourceDetail.getResourceType())) {
            Map<String, Object> adminClientConfiguration = KafkaConfigurationUtil.generateDecryptAdminClientConfiguration(dataSystemResourceDetail);
            kafkaHelperService.checkAdminClientConfig(adminClientConfiguration);
        }
    }
    
    @Override
    public void refreshDynamicDataSystemResource(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(rootDataSystemResourceId);
        Set<String> topics = kafkaHelperService.listTopics(KafkaConfigurationUtil.generateDecryptAdminClientConfiguration(clusterResource));
        List<DataSystemResourceDetailDTO> actualTopics = topics.stream()
                .map(each -> {
                    DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO();
                    resource.setName(each);
                    resource.setDataSystemType(DataSystemType.KAFKA);
                    resource.setResourceType(DataSystemResourceType.KAFKA_TOPIC);
                    resource.setParentResource(
                            new DataSystemResourceDetailDTO(rootDataSystemResourceId)
                    );
                    resource.getProjects().addAll(clusterResource.getProjects());
                    return resource;
                })
                .collect(Collectors.toList());
        dataSystemResourceService.mergeAllChildrenByName(actualTopics, DataSystemResourceType.KAFKA_TOPIC, rootDataSystemResourceId);
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.KAFKA;
    }
    
    @Override
    public DataSystemResourceDTO createDataCollectionByDataDefinition(final Long parentId, final String dataCollectionName, final DataCollectionDefinition dataCollectionDefinition) {
        throw new UnsupportedOperationException();
    }
}
