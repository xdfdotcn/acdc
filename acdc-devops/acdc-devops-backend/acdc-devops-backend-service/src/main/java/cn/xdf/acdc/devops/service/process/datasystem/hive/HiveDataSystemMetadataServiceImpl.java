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
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hdfs;
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
        throw new UnsupportedOperationException();
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
            String hadoopUser = getConfigurationValue(dataSystemResourceConfigurations, Hdfs.HDFS_HADOOP_USER);
            if (Strings.isNullOrEmpty(hadoopUser)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hdfs.HDFS_HADOOP_USER.getName()));
            }
            String clientFailoverProxyProvider = getConfigurationValue(dataSystemResourceConfigurations, Hdfs.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER);
            if (Strings.isNullOrEmpty(clientFailoverProxyProvider)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hdfs.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName()));
            }

            String metastoreUris = getConfigurationValue(dataSystemResourceConfigurations, Hive.HIVE_METASTORE_URIS);
            if (Strings.isNullOrEmpty(metastoreUris)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hive.HIVE_METASTORE_URIS.getName()));
            }

            String hdfsNameServices = getConfigurationValue(dataSystemResourceConfigurations, Hdfs.HDFS_NAME_SERVICES);
            if (Strings.isNullOrEmpty(hdfsNameServices)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hdfs.HDFS_NAME_SERVICES.getName()));
            }

            String hdfsNameNodes = getConfigurationValue(dataSystemResourceConfigurations, Hdfs.HDFS_NAME_NODES);
            if (Strings.isNullOrEmpty(hdfsNameNodes)) {
                throw new ServerErrorException(i18n.msg(DataSystem.INVALID_CONFIGURATION, Hdfs.HDFS_NAME_NODES.getName()));
            }
        }
    }

    @Override
    @Transactional
    public void refreshDynamicDataSystemResource(final Long rootDataSystemResourceId) {
        refreshDatabases(rootDataSystemResourceId);
        refreshTables(rootDataSystemResourceId);
    }

    protected void refreshDatabases(final Long rootDataSystemResourceId) {
        List<DataSystemResourceDetailDTO> actualDatabases = hiveHelperService.showDatabases()
                .stream()
                .map(it -> new DataSystemResourceDetailDTO()
                        .setName(it)
                        .setDataSystemType(DataSystemType.HIVE)
                        .setResourceType(DataSystemResourceType.HIVE_DATABASE)
                        .setParentResourceId(rootDataSystemResourceId)
                ).collect(Collectors.toList());

        dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.HIVE_DATABASE, rootDataSystemResourceId);
    }

    protected void refreshTables(final Long rootDataSystemResourceId) {
        List<DataSystemResourceDTO> databaseResources = dataSystemResourceService.getChildren(rootDataSystemResourceId, DataSystemResourceType.HIVE_DATABASE);

        for (DataSystemResourceDTO databaseResource : databaseResources) {
            String databaseName = databaseResource.getName();
            Long databaseId = databaseResource.getId();
            List<DataSystemResourceDetailDTO> actualTables = hiveHelperService.showTables(databaseName)
                    .stream()
                    .map(it -> new DataSystemResourceDetailDTO()
                            .setName(it)
                            .setDataSystemType(DataSystemType.HIVE)
                            .setResourceType(DataSystemResourceType.HIVE_TABLE)
                            .setParentResourceId(databaseId)
                    )
                    .collect(Collectors.toList());

            dataSystemResourceService.mergeAllChildrenByName(actualTables, DataSystemResourceType.HIVE_TABLE, databaseId);
        }
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
