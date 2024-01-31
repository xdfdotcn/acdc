package cn.xdf.acdc.devops.service.process.datasystem.es;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.EsDocField;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.UsernameAndPassword;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestHelper {
    
    static final UsernameAndPassword E_U_PASSWORD = new UsernameAndPassword("es", EncryptUtil.encrypt("123"));
    
    static final UsernameAndPassword D_U_PASSWORD = new UsernameAndPassword("es", "123");
    
    static final String NODE_SERVERS = "server1:9200,server2:9200";
    
    static DataSystemResourceDetailDTO createClusterRsDetal() {
        Map<String, DataSystemResourceConfigurationDTO> config = new HashMap<>();
        config.put(
                Cluster.USERNAME.getName(),
                new DataSystemResourceConfigurationDTO()
                        .setName(Cluster.USERNAME.getName())
                        .setValue(E_U_PASSWORD.getUsername())
        );
        config.put(
                Cluster.PASSWORD.getName(),
                new DataSystemResourceConfigurationDTO()
                        .setName(Cluster.PASSWORD.getName())
                        .setValue(E_U_PASSWORD.getPassword())
        );
        
        config.put(
                Cluster.NODE_SERVERS.getName(),
                new DataSystemResourceConfigurationDTO()
                        .setName(Cluster.NODE_SERVERS.getName())
                        .setValue(NODE_SERVERS)
        );
        
        DataSystemResourceDetailDTO clusterDetail = new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("test cluster")
                .setDataSystemResourceConfigurations(config);
        
        return clusterDetail;
    }
    
    static List<EsDocField> createDocFieldLimit2() {
        return Lists.newArrayList(
                new EsDocField().setName("f1").setType("Integer"),
                new EsDocField().setName("f2").setType("Text")
        );
    }
    
    static List<String> createIndexNameLimit2() {
        return Lists.newArrayList(
                "index1",
                "index2"
        );
    }
    
    static DataSystemResourceDTO createIndexRs() {
        return new DataSystemResourceDTO()
                .setId(1L)
                .setName("index1");
    }
    
    static ConnectionDetailDTO createConnectionDetail() {
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        connectionColumnConfigurations.add(new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("source_column_name")
                .setSinkColumnName("sink_column_name")
        );
        
        return new ConnectionDetailDTO()
                .setId(1L)
                .setSourceConnectorId(2L)
                .setSinkDataCollectionId(3L)
                .setConnectionColumnConfigurations(connectionColumnConfigurations);
    }
    
    static DataSystemResourceDTO createSourceDataCollection() {
        return new DataSystemResourceDTO()
                .setId(2L)
                .setKafkaTopicName("topic_name");
    }
}
