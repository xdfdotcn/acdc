package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.relational.RelationalDataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Server;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HostAndPort;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class TidbDataSystemMetadataServiceImpl extends RelationalDataSystemMetadataService {
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private MysqlHelperService mysqlHelperService;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return TidbDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    public DataSystemResourceType getDatabaseDataSystemResourceType() {
        return DataSystemResourceType.TIDB_DATABASE;
    }
    
    @Override
    public DataSystemResourceType getClusterDataSystemResourceType() {
        return DataSystemResourceType.TIDB_CLUSTER;
    }
    
    @Override
    public DataSystemResourceType getInstanceDataSystemResourceType() {
        return DataSystemResourceType.TIDB_SERVER;
    }
    
    @Override
    protected DataSystemResourceType getTableDataSystemResourceType() {
        return DataSystemResourceType.TIDB_TABLE;
    }
    
    @Override
    public void checkDataSystem(final Long clusterId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(clusterId);
        checkDataSystem(clusterResource);
    }
    
    @Override
    public void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        if (DataSystemResourceType.TIDB_CLUSTER.equals(dataSystemResourceDetail.getResourceType())) {
            checkTidbCluster(dataSystemResourceDetail);
        }
        
        if (DataSystemResourceType.TIDB_SERVER.equals(dataSystemResourceDetail.getResourceType())) {
            checkTidbInstance(dataSystemResourceDetail);
        }
    }
    
    private void checkTidbInstance(final DataSystemResourceDetailDTO tidbInstanceDetail) {
        DataSystemResourceDetailDTO clusterDetail = dataSystemResourceService.getDetailById(tidbInstanceDetail.getParentResource().getId());
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        checkTidbInstancePermission(usernameAndPassword, tidbInstanceDetail);
    }
    
    private void checkTidbCluster(final DataSystemResourceDetailDTO clusterDetail) {
        if (Objects.isNull(clusterDetail.getId())) {
            log.info("we do not need to check data system when saving a new tidb cluster, name: {}", clusterDetail.getName());
            return;
        }
        
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        List<DataSystemResourceDetailDTO> instanceResource = dataSystemResourceService.getDetailChildren(clusterDetail.getId(), DataSystemResourceType.TIDB_SERVER);
        
        if (Objects.isNull(instanceResource) || instanceResource.isEmpty()) {
            log.info("we do not need to check data system when there is no instance in cluster, name: {}", clusterDetail.getName());
            return;
        }
        
        // 对于tidb，检查任一实例即可
        DataSystemResourceDetailDTO anyInstance = instanceResource.get(0);
        
        checkTidbInstancePermission(usernameAndPassword, anyInstance);
    }
    
    private void checkTidbInstancePermission(final UsernameAndPassword usernameAndPassword, final DataSystemResourceDetailDTO anyInstance) {
        mysqlHelperService.checkPermissions(
                getHostAndPort(anyInstance),
                usernameAndPassword,
                TidbDataSystemConstant.Metadata.UserPermissionsAndBinlogConfiguration.PERMISSIONS_FOR_UPDATE
        );
    }
    
    protected HostAndPort getHostAndPort(final DataSystemResourceDetailDTO instance) {
        Preconditions.checkArgument(DataSystemResourceType.TIDB_SERVER.equals(instance.getResourceType()), "you can't get host and port from a resource which type is not tidb instance");
        
        String host = instance.getDataSystemResourceConfigurations().get(Server.HOST.getName()).getValue();
        int port = Integer.parseInt(instance.getDataSystemResourceConfigurations().get(Server.PORT.getName()).getValue());
        
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host), "tidb instance host must not be null");
        Preconditions.checkArgument(port > 0, "tidb instance port must bigger than 0");
        
        return new HostAndPort(host, port);
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.TIDB;
    }
    
    @Override
    protected boolean checkDatabaseIsCreatedByUser(final String databaseName) {
        return !TidbDataSystemConstant.Metadata.Tidb.SYSTEM_DATABASES.contains(databaseName.toLowerCase());
    }
}
