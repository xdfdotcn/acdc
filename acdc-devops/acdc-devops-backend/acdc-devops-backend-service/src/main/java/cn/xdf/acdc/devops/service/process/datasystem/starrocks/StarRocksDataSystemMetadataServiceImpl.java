package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.RelationalDataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.FrontEnd;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HostAndPort;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.RelationalDatabaseTable;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.StarRocksHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StarRocksDataSystemMetadataServiceImpl extends RelationalDataSystemMetadataService {
    
    @Autowired
    private StarRocksHelperService starRocksHelperService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return StarRocksDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    public DataSystemResourceType getDatabaseDataSystemResourceType() {
        return DataSystemResourceType.STARROCKS_DATABASE;
    }
    
    @Override
    public DataSystemResourceType getClusterDataSystemResourceType() {
        return DataSystemResourceType.STARROCKS_CLUSTER;
    }
    
    @Override
    public DataSystemResourceType getInstanceDataSystemResourceType() {
        return DataSystemResourceType.STARROCKS_FRONTEND;
    }
    
    @Override
    protected DataSystemResourceType getTableDataSystemResourceType() {
        return DataSystemResourceType.STARROCKS_TABLE;
    }
    
    @Override
    public void checkDataSystem(final Long clusterId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(clusterId);
        checkDataSystem(clusterResource);
    }
    
    @Override
    public void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        if (DataSystemResourceType.STARROCKS_CLUSTER.equals(dataSystemResourceDetail.getResourceType())) {
            checkStarRocksCluster(dataSystemResourceDetail);
        }
        
        if (DataSystemResourceType.STARROCKS_FRONTEND.equals(dataSystemResourceDetail.getResourceType())) {
            checkStarRocksInstance(dataSystemResourceDetail);
        }
    }
    
    private void checkStarRocksInstance(final DataSystemResourceDetailDTO starRocksInstanceDetail) {
        DataSystemResourceDetailDTO clusterDetail = dataSystemResourceService.getDetailById(starRocksInstanceDetail.getParentResource().getId());
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        checkStarRocksInstancePermission(usernameAndPassword, starRocksInstanceDetail);
    }
    
    private void checkStarRocksCluster(final DataSystemResourceDetailDTO clusterDetail) {
        if (Objects.isNull(clusterDetail.getId())) {
            log.info("we do not need to check data system when saving a new starrocks cluster, name: {}", clusterDetail.getName());
            return;
        }
        
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        List<DataSystemResourceDetailDTO> frontEnds = dataSystemResourceService.getDetailChildren(clusterDetail.getId(), DataSystemResourceType.STARROCKS_FRONTEND);
        
        if (Objects.isNull(frontEnds) || frontEnds.isEmpty()) {
            log.info("we do not need to check data system when there is no front end in cluster, name: {}", clusterDetail.getName());
            return;
        }
        
        // 检查任一frontEnd
        DataSystemResourceDetailDTO anyFrontEnd = frontEnds.get(0);
        
        checkStarRocksInstancePermission(usernameAndPassword, anyFrontEnd);
    }
    
    private void checkStarRocksInstancePermission(final UsernameAndPassword usernameAndPassword, final DataSystemResourceDetailDTO anyFrontEnd) {
        starRocksHelperService.checkHealth(Sets.newHashSet(getHostAndPort(anyFrontEnd)), usernameAndPassword);
    }
    
    protected HostAndPort getHostAndPort(final DataSystemResourceDetailDTO frontEnd) {
        Preconditions.checkArgument(DataSystemResourceType.STARROCKS_FRONTEND.equals(frontEnd.getResourceType()), "you can't get host and port from a resource which is not a front end.");
        
        String host = frontEnd.getDataSystemResourceConfigurations().get(FrontEnd.HOST.getName()).getValue();
        int port = Integer.parseInt(frontEnd.getDataSystemResourceConfigurations().get(FrontEnd.JDBC_PORT.getName()).getValue());
        
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host), "front end host must not be null");
        Preconditions.checkArgument(port > 0, "front end port must bigger than 0");
        
        return new HostAndPort(host, port);
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.STARROCKS;
    }
    
    @Override
    protected boolean checkDatabaseIsCreatedByUser(final String databaseName) {
        return !StarRocksDataSystemConstant.Metadata.StarRocks.SYSTEM_DATABASES.contains(databaseName.toLowerCase());
    }
    
    @Override
    public DataCollectionDefinition getDataCollectionDefinition(final Long dataCollectionResourceId) {
        DataSystemResourceDTO tableResource = dataSystemResourceService.getById(dataCollectionResourceId);
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailParent(tableResource.getId(), getClusterDataSystemResourceType());
        DataSystemResourceDTO dataBaseResource = dataSystemResourceService.getParent(tableResource.getId(), getDatabaseDataSystemResourceType());
        List<DataSystemResourceDetailDTO> instanceResources = dataSystemResourceService.getDetailChildren(clusterResource.getId(), getInstanceDataSystemResourceType());
        
        Set<HostAndPort> hosts = new HashSet<>();
        instanceResources.forEach(each -> hosts.add(getHostAndPort(each)));
        
        String database = dataBaseResource.getName();
        String table = tableResource.getName();
        
        RelationalDatabaseTable relationalDatabaseTable = starRocksHelperService.descTable(hosts, getUsernameAndPassword(clusterResource), database, table);
        
        List<DataFieldDefinition> dataFieldDefinitions = relationalDatabaseTable.getFields().stream()
                .map(it -> new DataFieldDefinition(
                        it.getName(),
                        it.getType(),
                        new HashSet<>(it.getUniqueIndexNames())
                ))
                .collect(Collectors.toList());
        
        return new DataCollectionDefinition(table, dataFieldDefinitions, relationalDatabaseTable.getProperties());
    }
}
