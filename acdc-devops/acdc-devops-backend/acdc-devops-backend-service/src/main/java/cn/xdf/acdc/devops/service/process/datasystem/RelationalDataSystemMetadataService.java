package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Internal;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HostAndPort;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class RelationalDataSystemMetadataService implements DataSystemMetadataService {

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Override
    public void refreshDynamicDataSystemResource(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(rootDataSystemResourceId);

        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterResource);

        List<DataSystemResourceDetailDTO> instanceResources = dataSystemResourceService.getDetailChildren(rootDataSystemResourceId, getInstanceDataSystemResourceType());
        Set<HostAndPort> hostAndPorts = getHostAndPorts(instanceResources);

        if (checkIfNeedRefreshDatabase(clusterResource)) {
            refreshDatabase(clusterResource, usernameAndPassword, hostAndPorts);
        }

        refreshTable(clusterResource, usernameAndPassword, hostAndPorts);
    }

    private boolean checkIfNeedRefreshDatabase(final DataSystemResourceDetailDTO clusterResourceDetail) {
        if (clusterResourceDetail.getDataSystemResourceConfigurations().containsKey(Internal.SOURCE.getName())) {
            String sourceValue = clusterResourceDetail.getDataSystemResourceConfigurations().get(Internal.SOURCE.getName()).getValue();
            if (Objects.nonNull(sourceValue) && MetadataSourceType.valueOf(sourceValue).equals(MetadataSourceType.FROM_PANDORA)) {
                return false;
            }
        }
        return true;
    }

    protected UsernameAndPassword getUsernameAndPassword(final DataSystemResourceDetailDTO clusterResource) {
        String username = clusterResource.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue();
        String password = clusterResource.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue();
        if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
            throw new ServerErrorException(String.format("either username or password should not be null for mysql cluster, please set them for mysql cluster id: %d, name: %s",
                    clusterResource.getId(), clusterResource.getName()));
        }
        return new UsernameAndPassword(username, EncryptUtil.decrypt(password));
    }

    protected HostAndPort getHostAndPort(final DataSystemResourceDetailDTO instance) {
        Preconditions.checkArgument(DataSystemResourceType.MYSQL_INSTANCE.equals(instance.getResourceType()), "you can not get host and port from a resource which type is not mysql instance");

        String host = instance.getDataSystemResourceConfigurations().get(Instance.HOST.getName()).getValue();
        int port = Integer.valueOf(instance.getDataSystemResourceConfigurations().get(Instance.PORT.getName()).getValue());

        Preconditions.checkArgument(!Strings.isNullOrEmpty(host), "mysql instance host must not be null");
        Preconditions.checkArgument(port > 0, "mysql instance port must bigger than 0");

        return new HostAndPort(host, port);
    }

    protected Set<HostAndPort> getHostAndPorts(final List<DataSystemResourceDetailDTO> instances) {
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        instances.forEach(each -> hostAndPorts.add(getHostAndPort(each)));
        return hostAndPorts;
    }

    private void refreshDatabase(
            final DataSystemResourceDetailDTO clusterResourceDetail,
            final UsernameAndPassword usernameAndPassword,
            final Set<HostAndPort> hostAndPorts) {
        List<String> actualDatabaseNames = mysqlHelperService.showDataBases(hostAndPorts, usernameAndPassword, this::checkDatabaseIsCreatedByUser);
        List<DataSystemResourceDetailDTO> actualDatabases = actualDatabaseNames.stream()
                .map(each -> {
                    DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                            .setName(each)
                            .setDataSystemType(getDataSystemType())
                            .setResourceType(getDatabaseDataSystemResourceType())
                            .setParentResourceId(clusterResourceDetail.getId());
                    resource.getProjects().addAll(clusterResourceDetail.getProjects());
                    return resource;
                })
                .collect(Collectors.toList());
        dataSystemResourceService.mergeAllChildrenByName(actualDatabases, getDatabaseDataSystemResourceType(), clusterResourceDetail.getId());
    }

    private void refreshTable(
            final DataSystemResourceDetailDTO clusterResourceDetail,
            final UsernameAndPassword usernameAndPassword,
            final Set<HostAndPort> hostAndPorts) {
        List<DataSystemResourceDTO> databaseResources = dataSystemResourceService.getChildren(clusterResourceDetail.getId(), getDatabaseDataSystemResourceType());
        databaseResources.forEach(each -> refreshTable(each, usernameAndPassword, hostAndPorts));
    }

    private void refreshTable(
            final DataSystemResourceDTO databaseResource,
            final UsernameAndPassword usernameAndPassword,
            final Set<HostAndPort> hostAndPorts) {
        List<String> actualTableNames = mysqlHelperService.showTables(hostAndPorts, usernameAndPassword, databaseResource.getName());
        List<DataSystemResourceDetailDTO> actualTables = actualTableNames.stream()
                .map(each -> {
                    DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                            .setName(each)
                            .setDataSystemType(getDataSystemType())
                            .setResourceType(getTableDataSystemResourceType())
                            .setParentResourceId(databaseResource.getId());
                    return resource;
                })
                .collect(Collectors.toList());
        dataSystemResourceService.mergeAllChildrenByName(actualTables, getTableDataSystemResourceType(), databaseResource.getId());
    }

    @Override
    @Transactional
    public DataCollectionDefinition getDataCollectionDefinition(final Long dataCollectionResourceId) {
        DataSystemResourceDTO tableResource = dataSystemResourceService.getById(dataCollectionResourceId);
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailParent(tableResource.getId(), getClusterDataSystemResourceType());
        DataSystemResourceDTO dataBaseResource = dataSystemResourceService.getParent(tableResource.getId(), getDatabaseDataSystemResourceType());
        List<DataSystemResourceDetailDTO> instanceResources = dataSystemResourceService.getDetailChildren(clusterResource.getId(), getInstanceDataSystemResourceType());

        Set<HostAndPort> hosts = new HashSet<>();
        instanceResources.forEach(each -> hosts.add(getHostAndPort(each)));

        String database = dataBaseResource.getName();
        String table = tableResource.getName();

        List<DataFieldDefinition> dataFieldDefinitions = mysqlHelperService
                .descTable(hosts, getUsernameAndPassword(clusterResource), database, table)
                .stream()
                .map(it -> new DataFieldDefinition(
                        it.getName(),
                        it.getType(),
                        new HashSet<>(it.getUniqueIndexNames())
                ))
                .collect(Collectors.toList());

        return new DataCollectionDefinition(table, dataFieldDefinitions);
    }

    /**
     * Check if a database is a user database, which need to sync to ACDC.
     *
     * @param databaseName database name
     * @return ture if need
     */
    protected abstract boolean checkDatabaseIsCreatedByUser(String databaseName);

    /**
     * Get database data system resource type.
     *
     * @return data system resource type
     */
    protected abstract DataSystemResourceType getDatabaseDataSystemResourceType();

    /**
     * Get cluster data system resource type.
     *
     * @return data system resource type
     */
    protected abstract DataSystemResourceType getClusterDataSystemResourceType();

    /**
     * Get instance data system resource type.
     *
     * @return data system resource type
     */
    protected abstract DataSystemResourceType getInstanceDataSystemResourceType();

    /**
     * Get table data system resource type.
     *
     * @return data system resource type
     */
    protected abstract DataSystemResourceType getTableDataSystemResourceType();
}
