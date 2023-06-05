package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HiveHelperService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.DataSystem;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import joptsimple.internal.Strings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HiveDataSystemMetadataServiceImpl implements DataSystemMetadataService {
    
    @Autowired
    private HiveHelperService hiveHelperService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private I18nService i18n;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return HiveDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    @Transactional
    public DataCollectionDefinition getDataCollectionDefinition(final Long dataSystemResourceId) {
        DataSystemResourceDTO tableResource = dataSystemResourceService.getById(dataSystemResourceId);
        DataSystemResourceDTO databaseResource = dataSystemResourceService.getParent(dataSystemResourceId, DataSystemResourceType.HIVE_DATABASE);
        String table = tableResource.getName();
        String database = databaseResource.getName();
        
        List<DataFieldDefinition> dataFieldDefinitions = hiveHelperService
                .descTable(database, table)
                .stream()
                .map(it -> new DataFieldDefinition(
                        it.getName(),
                        it.getType(),
                        new HashSet<>(it.getUniqueIndexNames())
                )).collect(Collectors.toList());
        
        return new DataCollectionDefinition(table, dataFieldDefinitions);
    }
    
    @Override
    @Transactional
    public void checkDataSystem(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO hiveClusterResourceDetail = dataSystemResourceService.getDetailById(rootDataSystemResourceId);
        checkDataSystem(hiveClusterResourceDetail);
    }
    
    @Override
    @Transactional
    public void checkDataSystem(final DataSystemResourceDetailDTO hiveClusterResourceDetail) {
        // TODO 校验逻辑应该放到录入数据系统的service内部,本次迭代不涉及到校验元数据的部分的逻辑
        Map<String, DataSystemResourceConfigurationDTO> dataSystemResourceConfigurations =
                hiveClusterResourceDetail.getDataSystemResourceConfigurations();
        
        DataSystemResourceType resourceType = hiveClusterResourceDetail.getResourceType();
        if (DataSystemResourceType.HIVE == resourceType) {
            String hadoopUser = getConfigurationValue(dataSystemResourceConfigurations, Hive.HDFS_HADOOP_USER);
            if (Strings.isNullOrEmpty(hadoopUser)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HDFS_HADOOP_USER.getName()));
            }
            String clientFailoverProxyProvider = getConfigurationValue(dataSystemResourceConfigurations, Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER);
            if (Strings.isNullOrEmpty(clientFailoverProxyProvider)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName()));
            }
            
            String metastoreUris = getConfigurationValue(dataSystemResourceConfigurations, Hive.HIVE_METASTORE_URIS);
            if (Strings.isNullOrEmpty(metastoreUris)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HIVE_METASTORE_URIS.getName()));
            }
            
            String hdfsNameServices = getConfigurationValue(dataSystemResourceConfigurations, Hive.HDFS_NAME_SERVICES);
            if (Strings.isNullOrEmpty(hdfsNameServices)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HDFS_NAME_SERVICES.getName()));
            }
            
            String hdfsNameNodes = getConfigurationValue(dataSystemResourceConfigurations, Hive.HDFS_NAME_NODES);
            if (Strings.isNullOrEmpty(hdfsNameNodes)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HDFS_NAME_NODES.getName()));
            }
        }
    }
    
    @Override
    @Transactional
    public void refreshDynamicDataSystemResource(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO hiveResourceDetail = dataSystemResourceService.getDetailById(rootDataSystemResourceId);
        
        refreshDatabases(hiveResourceDetail);
        refreshTables(hiveResourceDetail);
    }
    
    protected void refreshDatabases(final DataSystemResourceDetailDTO hiveResourceDetail) {
        List<String> actualDatabaseNames = hiveHelperService.showDatabases();
        List<DataSystemResourceDetailDTO> actualDatabases = generateResourceDetails(actualDatabaseNames, DataSystemResourceType.HIVE_DATABASE, hiveResourceDetail);
        dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.HIVE_DATABASE, hiveResourceDetail.getId());
    }
    
    protected void refreshTables(final DataSystemResourceDetailDTO hiveResourceDetail) {
        List<DataSystemResourceDetailDTO> databaseResources = dataSystemResourceService.getDetailChildren(hiveResourceDetail.getId(), DataSystemResourceType.HIVE_DATABASE);
        databaseResources.forEach(each -> refreshTablesOfDatabase(each));
    }
    
    protected void refreshTablesOfDatabase(final DataSystemResourceDetailDTO databaseResourceDetail) {
        List<String> actualTableNames = hiveHelperService.showTables(databaseResourceDetail.getName());
        List<DataSystemResourceDetailDTO> actualTables = generateResourceDetails(actualTableNames, DataSystemResourceType.HIVE_TABLE, databaseResourceDetail);
        dataSystemResourceService.mergeAllChildrenByName(actualTables, DataSystemResourceType.HIVE_TABLE, databaseResourceDetail.getId());
    }
    
    private List<DataSystemResourceDetailDTO> generateResourceDetails(
            final List<String> resourceNames,
            final DataSystemResourceType dataSystemResourceType,
            final DataSystemResourceDetailDTO parentResourceDetail) {
        return resourceNames.stream()
                .map(each -> {
                    DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                            .setName(each)
                            .setDataSystemType(DataSystemType.HIVE)
                            .setResourceType(dataSystemResourceType)
                            .setParentResource(
                                    new DataSystemResourceDetailDTO(parentResourceDetail.getId())
                            );
                    resource.getProjects().addAll(parentResourceDetail.getProjects());
                    return resource;
                })
                .collect(Collectors.toList());
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.HIVE;
    }
    
    private String getConfigurationValue(
            final Map<String, DataSystemResourceConfigurationDTO> resourceConfiguration,
            final ConfigurationDefinition definition) {
        return resourceConfiguration.getOrDefault(definition.getName(), new DataSystemResourceConfigurationDTO()).getValue();
    }
}
