package cn.xdf.acdc.devops.service.process.datasystem.es;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Strings;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.EsHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Service
public class EsDataSystemMetadataServiceImpl implements DataSystemMetadataService {
    
    @Autowired
    private EsHelperService helperService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private I18nService i18n;
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.ELASTICSEARCH;
    }
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return EsDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    public DataCollectionDefinition getDataCollectionDefinition(
            final Long dataCollectionId
    ) {
        DataSystemResourceDTO indexResource = dataSystemResourceService.getById(dataCollectionId);
        String index = indexResource.getName();
        
        DataSystemResourceDetailDTO clusterResourceDetail = dataSystemResourceService
                .getDetailParent(dataCollectionId, DataSystemResourceType.ELASTICSEARCH_CLUSTER);
        ClusterConfig config = getClusterConfig(clusterResourceDetail);
        
        List<DataFieldDefinition> dataFieldDefinitions = helperService
                .getIndexMapping(config.nodeServers, config.usernameAndPassword, index)
                .stream()
                .map(it -> new DataFieldDefinition(it.getName(), it.getType(), new HashSet<>()))
                .collect(Collectors.toList());
        
        return new DataCollectionDefinition(index, dataFieldDefinitions);
    }
    
    @Override
    public DataSystemResourceDTO createDataCollectionByDataDefinition(final Long parentId, final String dataCollectionName, final DataCollectionDefinition dataCollectionDefinition) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void checkDataSystem(
            final Long rootDataSystemResourceId
    ) {
        DataSystemResourceDetailDTO dataSystemResourceDetail = dataSystemResourceService
                .getDetailById(rootDataSystemResourceId);
        
        this.checkDataSystem(dataSystemResourceDetail);
    }
    
    @Override
    public void checkDataSystem(
            final DataSystemResourceDetailDTO dataSystemResourceDetail
    ) {
        if (CollectionUtils.isEmpty(dataSystemResourceDetail.getDataSystemResourceConfigurations())) {
            return;
        }
        
        ClusterConfig config = getClusterConfig(dataSystemResourceDetail);
        UsernameAndPassword usernameAndPassword = config.usernameAndPassword;
        String nodeServers = config.nodeServers;
        if (Strings.isNullOrEmpty(nodeServers)
                || Objects.isNull(usernameAndPassword)
                || Strings.isNullOrEmpty(usernameAndPassword.getUsername())
                || Strings.isNullOrEmpty(usernameAndPassword.getPassword())
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
        
        helperService.checkCluster(nodeServers, config.usernameAndPassword);
    }
    
    @Override
    public void refreshDynamicDataSystemResource(
            final Long rootDataSystemResourceId
    ) {
        DataSystemResourceDetailDTO clusterResourceDetail = dataSystemResourceService
                .getDetailById(rootDataSystemResourceId);
        
        refreshIndexs(clusterResourceDetail);
    }
    
    protected void refreshIndexs(final DataSystemResourceDetailDTO clusterResourceDetail) {
        ClusterConfig config = getClusterConfig(clusterResourceDetail);
        
        List<String> actualIndexNames = helperService
                .getClusterAllIndex(config.nodeServers, config.usernameAndPassword);
        
        List<DataSystemResourceDetailDTO> actualDatabases = generateResourceDetails(
                actualIndexNames,
                DataSystemResourceType.ELASTICSEARCH_INDEX,
                clusterResourceDetail
        );
        
        dataSystemResourceService.mergeAllChildrenByName(
                actualDatabases,
                DataSystemResourceType.ELASTICSEARCH_INDEX,
                clusterResourceDetail.getId()
        );
    }
    
    private List<DataSystemResourceDetailDTO> generateResourceDetails(
            final List<String> resourceNames,
            final DataSystemResourceType dataSystemResourceType,
            final DataSystemResourceDetailDTO parentResourceDetail
    ) {
        return resourceNames.stream()
                .map(each -> {
                    DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                            .setName(each)
                            .setDataSystemType(DataSystemType.ELASTICSEARCH)
                            .setResourceType(dataSystemResourceType)
                            .setParentResource(
                                    new DataSystemResourceDetailDTO(parentResourceDetail.getId())
                            );
                    resource.getProjects().addAll(parentResourceDetail.getProjects());
                    return resource;
                })
                .collect(Collectors.toList());
    }
    
    private ClusterConfig getClusterConfig(
            final DataSystemResourceDetailDTO resourceDetail
    ) {
        Map<String, DataSystemResourceConfigurationDTO> configMap = resourceDetail
                .getDataSystemResourceConfigurations();
        
        String username = configMap.get(Cluster.USERNAME.getName()).getValue();
        String password = configMap.get(Cluster.PASSWORD.getName()).getValue();
        String nodeServers = configMap.get(Cluster.NODE_SERVERS.getName()).getValue();
        
        UsernameAndPassword usernameAndPassword = new UsernameAndPassword(username, EncryptUtil.decrypt(password));
        
        return new ClusterConfig(nodeServers, usernameAndPassword);
    }
    
    @Setter
    @Getter
    @AllArgsConstructor
    private static class ClusterConfig {
        
        private String nodeServers;
        
        private UsernameAndPassword usernameAndPassword;
    }
}
