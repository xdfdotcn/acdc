package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.constant.connector.CommonConstant;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.AbstractDataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DeletionMode;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class HiveDataSystemSinkConnectorServiceImpl extends AbstractDataSystemSinkConnectorService {

    @Autowired
    private ConnectorClassService connectorClassService;

    @Autowired
    private DataSystemResourceService dataSystemResourceService;

    @Autowired
    private ConnectionService connectionService;

    @Override
    public void verifyDataSystemMetadata(final Long resourceId) {

    }

    @Override
    public void beforeConnectorCreation(final Long resourceId) {

    }

    @Override
    public Map<String, String> getConnectorDefaultConfiguration() {
        ConnectorClassDetailDTO connectorClassDetail = connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.HIVE, ConnectorType.SINK);

        Map<String, String> defaultConfigurations = new HashMap<>();
        connectorClassDetail.getDefaultConnectorConfigurations().forEach(each -> defaultConfigurations.put(each.getName(), each.getValue()));
        return defaultConfigurations;
    }

    @Override
    public Map<String, String> generateConnectorCustomConfiguration(final Long connectionId) {
        ConnectionDetailDTO connectionDetail = connectionService.getDetailById(connectionId);
        DataSystemResourceDTO sourceDataCollection = dataSystemResourceService.getById(connectionDetail.getSourceDataCollectionId());
        DataSystemResourceDTO sinkTable = dataSystemResourceService.getById(connectionDetail.getSinkDataCollectionId());
        DataSystemResourceDTO sinkDatabase = dataSystemResourceService.getParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.HIVE_DATABASE);
        DataSystemResourceDetailDTO hiveDetail = dataSystemResourceService.getDetailParent(connectionDetail.getSinkDataCollectionId(), DataSystemResourceType.HIVE);
        String destination = Joiner.on(Symbol.DOT).join(sinkDatabase.getName(), sinkTable.getName());

        Map<String, String> configurations = new HashMap<>();

        configurations.put(
                HiveDataSystemConstant.Connector.Sink.Configuration.TOPICS,
                sourceDataCollection.getKafkaTopicName()
        );
        configurations.put(
                HiveDataSystemConstant.Connector.Sink.Configuration.DESTINATIONS,
                destination
        );

        // override logical deletion configuration to none
        configurations.put(
                AbstractDataSystemSinkConnectorService.DESTINATIONS_PREFIX + destination + AbstractDataSystemSinkConnectorService.DELETE_LOGICAL_MODE_SUFFIX,
                DeletionMode.NONE.name()
        );

        // destination configurations
        configurations.putAll(super.generateDestinationsConfiguration(destination, connectionDetail.getConnectionColumnConfigurations()));

        // hdfs configurations
        configurations.putAll(generateHdfsAndHiveConfiguration(hiveDetail));

        return configurations;
    }

    Map<String, String> generateHdfsAndHiveConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        Map<String, String> configurations = new LinkedHashMap<>();

        String storeUrl = generateStoreUrlConfiguration(hiveDetail);
        String hadoopUser = generateHadoopUserConfiguration(hiveDetail);
        String hdfsNameServices = generateHdfsNameServicesConfiguration(hiveDetail);
        String clientFailoverProxyProvider = generateClientFailoverProxyProviderConfiguration(hiveDetail);
        Map<String, String> hdfsHaNameNodeNameAndRpcAddressMapping = getHdfsHaNameNodeNameAndRpcAddressMapping(hiveDetail);
        final String hiveMetastoreUris = generateHiveMetastoreUrisConfiguration(hiveDetail);

        // hdfs

        configurations.put(
                Configuration.HADOOP_USER,
                hadoopUser
        );
        configurations.put(
                Configuration.STORE_URL,
                storeUrl
        );

        configurations.put(
                Configuration.HDFS_NAME_SERVICES,
                hdfsNameServices
        );

        configurations.put(
                Configuration.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER + CommonConstant.DOT + hdfsNameServices,
                clientFailoverProxyProvider
        );

        // name nodes
        configurations.put(
                Configuration.HDFS_HA_NAMENODES + Symbol.DOT + hdfsNameServices,
                Joiner.on(SystemConstant.Symbol.COMMA).join(hdfsHaNameNodeNameAndRpcAddressMapping.keySet())
        );
        hdfsHaNameNodeNameAndRpcAddressMapping.forEach((k, v) -> configurations.put(Configuration.HDFS_NAME_NODE_RPC + CommonConstant.DOT + hdfsNameServices + CommonConstant.DOT + k, v));

        // hive
        configurations.put(Configuration.HIVE_METASTORE_URIS, hiveMetastoreUris);

        return configurations;
    }

    private String generateHiveMetastoreUrisConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        return hiveDetail.getDataSystemResourceConfigurations().get(Hive.HIVE_METASTORE_URIS.getName()).getValue();
    }

    private String generateClientFailoverProxyProviderConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        return hiveDetail.getDataSystemResourceConfigurations().get(Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName()).getValue();
    }

    private String generateHadoopUserConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        return hiveDetail.getDataSystemResourceConfigurations().get(Hive.HDFS_HADOOP_USER.getName()).getValue();
    }

    private String generateStoreUrlConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        String nameServices = generateHdfsNameServicesConfiguration(hiveDetail);
        return Configuration.HDFS_URL_PROTOCOL + nameServices;
    }

    private String generateHdfsNameServicesConfiguration(final DataSystemResourceDetailDTO hiveDetail) {
        String nameServices = hiveDetail.getDataSystemResourceConfigurations().get(Hive.HDFS_NAME_SERVICES.getName()).getValue();
        return nameServices;
    }

    private Map<String, String> getHdfsHaNameNodeNameAndRpcAddressMapping(final DataSystemResourceDetailDTO hiveDetail) {
        String nameNodeConfString = hiveDetail.getDataSystemResourceConfigurations().get(Hive.HDFS_NAME_NODES.getName()).getValue();
        List<String> nameNodeConfList = Splitter.on(Symbol.COMMA).splitToList(nameNodeConfString);

        Map<String, String> nameNodeNameAndRpcAddressMapping = new LinkedHashMap<>();

        for (String conf : nameNodeConfList) {
            List<String> splitToList = Splitter.on(Symbol.EQUIVALENCE).splitToList(conf);
            String name = splitToList.get(0);
            String rpcAddress = splitToList.get(1);
            nameNodeNameAndRpcAddressMapping.put(name, rpcAddress);
        }

        return nameNodeNameAndRpcAddressMapping;
    }

    @Override
    public Set<ConfigurationDefinition<?>> getConnectorSpecificConfigurationDefinitions() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getSensitiveConfigurationNames() {
        return Sets.newHashSet();
    }

    @Override
    public ConnectorClassDetailDTO getConnectorClass() {
        return connectorClassService.getDetailByDataSystemTypeAndConnectorType(DataSystemType.HIVE, ConnectorType.SINK);
    }

    @Override
    public DataSystemType getDataSystemType() {
        return DataSystemType.HIVE;
    }
}
