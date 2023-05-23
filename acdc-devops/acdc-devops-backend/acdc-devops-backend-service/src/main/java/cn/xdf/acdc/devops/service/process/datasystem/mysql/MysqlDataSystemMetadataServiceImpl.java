package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.ErrorMsg.DataSystem;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.RelationalDataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.Mysql;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.UserPermissionsAndBinlogConfiguration;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HostAndPort;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class MysqlDataSystemMetadataServiceImpl extends RelationalDataSystemMetadataService {
    
    @Autowired
    private MysqlHelperService mysqlHelperService;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Override
    public DataSystemResourceDefinition getDataSystemResourceDefinition() {
        return MysqlDataSystemResourceDefinitionHolder.get();
    }
    
    @Override
    public void checkDataSystem(final Long rootDataSystemResourceId) {
        DataSystemResourceDetailDTO clusterResource = dataSystemResourceService.getDetailById(rootDataSystemResourceId);
        checkDataSystemForCluster(clusterResource);
    }
    
    @Override
    public void checkDataSystem(final DataSystemResourceDetailDTO dataSystemResourceDetail) {
        if (DataSystemResourceType.MYSQL_CLUSTER.equals(dataSystemResourceDetail.getResourceType())) {
            checkDataSystemForCluster(dataSystemResourceDetail);
            return;
        }
        
        if (DataSystemResourceType.MYSQL_INSTANCE.equals(dataSystemResourceDetail.getResourceType())) {
            checkDataSystemForInstance(dataSystemResourceDetail);
            return;
        }
    }
    
    private void checkDataSystemForCluster(final DataSystemResourceDetailDTO clusterDetail) {
        if (Objects.isNull(clusterDetail.getId())) {
            log.info("we do not need to check data system when saving a new mysql cluster, name: {}", clusterDetail.getName());
            return;
        }
        
        List<DataSystemResourceDetailDTO> instanceResource = dataSystemResourceService.getDetailChildren(clusterDetail.getId(), DataSystemResourceType.MYSQL_INSTANCE);
        if (instanceResource.isEmpty()) {
            log.info("we do not need to check data system when there is no instance in cluster, name: {}", clusterDetail.getName());
            return;
        }
        checkUserPermissionsAndBinlogConfiguration(clusterDetail, instanceResource);
    }
    
    private void checkDataSystemForInstance(final DataSystemResourceDetailDTO instanceDetail) {
        DataSystemResourceDetailDTO clusterDetail = dataSystemResourceService.getDetailById(instanceDetail.getParentResource().getId());
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        checkUserPermissionsAndBinlogConfiguration(instanceDetail, usernameAndPassword);
    }
    
    private void checkUserPermissionsAndBinlogConfiguration(final DataSystemResourceDetailDTO clusterDetail, final List<DataSystemResourceDetailDTO> instanceResource) {
        UsernameAndPassword usernameAndPassword = getUsernameAndPassword(clusterDetail);
        for (DataSystemResourceDetailDTO each : instanceResource) {
            checkUserPermissionsAndBinlogConfiguration(each, usernameAndPassword);
        }
    }
    
    private void checkUserPermissionsAndBinlogConfiguration(final DataSystemResourceDetailDTO instance, final UsernameAndPassword usernameAndPassword) {
        log.info("checking permissions and binlog configuration for rdb instance '{}'", instance);
        
        MysqlInstanceRoleType roleType = MysqlInstanceRoleType.valueOf(instance.getDataSystemResourceConfigurations().get(Instance.ROLE_TYPE.getName()).getValue());
        
        HostAndPort hostAndPort = getHostAndPort(instance);
        Map<String, String> variables = mysqlHelperService.showVariables(hostAndPort, usernameAndPassword);
        if (MysqlInstanceRoleType.MASTER.equals(roleType)) {
            log.info("checking permissions for user '{}' in master rdb instance '{}:{}'", usernameAndPassword.getUsername(), hostAndPort);
            mysqlHelperService.checkPermissions(hostAndPort, usernameAndPassword, UserPermissionsAndBinlogConfiguration.PERMISSIONS_FOR_MASTER);
            checkVariablesForMaster(variables);
        } else if (MysqlInstanceRoleType.DATA_SOURCE.equals(roleType)) {
            log.info("checking permissions for user '{}' in data source rdb instance '{}:{}'", usernameAndPassword.getUsername(), hostAndPort);
            mysqlHelperService.checkPermissions(hostAndPort, usernameAndPassword, UserPermissionsAndBinlogConfiguration.PERMISSIONS_FOR_DATASOURCE);
            log.info("checking binlog configuration for data source rdb instance '{}:{}'", hostAndPort);
            checkVariablesForDataSource(variables);
        }
        
        log.info("successfully check permissions and binlog configuration for rdb instance '{}'", instance);
    }
    
    protected void checkVariablesForMaster(final Map<String, String> variables) {
        checkSqlMode(variables);
    }
    
    protected void checkSqlMode(final Map<String, String> variables) {
        String sqlModeVariableValue = variables.getOrDefault(UserPermissionsAndBinlogConfiguration.SQL_MODE, SystemConstant.EMPTY_STRING);
        if (!sqlModeVariableValue.contains(UserPermissionsAndBinlogConfiguration.EXPECTED_SQL_MODE_VALUE)) {
            log.error("check sql_mode failed. [should have]:{} [actual]:{}", UserPermissionsAndBinlogConfiguration.EXPECTED_SQL_MODE_VALUE, sqlModeVariableValue);
            throw new ServerErrorException(String.format(DataSystem.UNEXPECTED_CONFIGURATION_VALUE,
                    UserPermissionsAndBinlogConfiguration.SQL_MODE,
                    UserPermissionsAndBinlogConfiguration.EXPECTED_SQL_MODE_VALUE,
                    sqlModeVariableValue));
        }
    }
    
    protected void checkVariablesForDataSource(final Map<String, String> variables) {
        checkBinlogConfiguration(variables);
    }
    
    protected void checkBinlogConfiguration(final Map<String, String> variables) {
        for (int i = 0; i < UserPermissionsAndBinlogConfiguration.TO_CHECK_BINLOG_CONFIGURATION.length; i++) {
            String variableValue = variables.getOrDefault(UserPermissionsAndBinlogConfiguration.TO_CHECK_BINLOG_CONFIGURATION[i], SystemConstant.EMPTY_STRING);
            
            // 使用表达式引擎Aviator计算比较结果
            executeExpression(UserPermissionsAndBinlogConfiguration.EXPECTED_BINLOG_CONFIGURATION_VALUE_EXPRESSION[i],
                    UserPermissionsAndBinlogConfiguration.TO_CHECK_BINLOG_CONFIGURATION[i],
                    UserPermissionsAndBinlogConfiguration.EXPECTED_BINLOG_CONFIGURATION_VALUE[i],
                    variableValue);
        }
    }
    
    protected void executeExpression(final String expression, final String config, final String exceptedValue, final String result) {
        Map<String, Object> param = Maps.newHashMapWithExpectedSize(1);
        if (StringUtils.isNumeric(result)) {
            param.put(UserPermissionsAndBinlogConfiguration.RESULT, Integer.parseInt(result));
        } else {
            param.put(UserPermissionsAndBinlogConfiguration.RESULT, result);
        }
        boolean executeResult = (Boolean) AviatorEvaluator.execute(expression, param);
        if (!executeResult) {
            log.error("check config failed. [config]:{} [expected]:{} [actual]:{}", config, exceptedValue, result);
            throw new ServerErrorException(String.format(DataSystem.UNEXPECTED_CONFIGURATION_VALUE, config, exceptedValue, result));
        }
    }
    
    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.MYSQL;
    }
    
    @Override
    public boolean checkDatabaseIsCreatedByUser(final String databaseName) {
        return !Mysql.SYSTEM_DATABASES.contains(databaseName.toLowerCase());
    }
    
    @Override
    public DataSystemResourceType getDatabaseDataSystemResourceType() {
        return DataSystemResourceType.MYSQL_DATABASE;
    }
    
    @Override
    public DataSystemResourceType getClusterDataSystemResourceType() {
        return DataSystemResourceType.MYSQL_CLUSTER;
    }
    
    @Override
    public DataSystemResourceType getInstanceDataSystemResourceType() {
        return DataSystemResourceType.MYSQL_INSTANCE;
    }
    
    @Override
    protected DataSystemResourceType getTableDataSystemResourceType() {
        return DataSystemResourceType.MYSQL_TABLE;
    }
}
