package cn.xdf.acdc.devops.service.process.connector.impl;

import com.google.common.collect.Maps;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveSinkConnectorProcessServiceImplTest extends ConnectBaseTest {

    private HiveSinkConnectorProcessServiceImpl hiveSinkConnectProcessService;

    @Before
    public void setup() {
        hiveSinkConnectProcessService = new HiveSinkConnectorProcessServiceImpl();
    }

    @Test
    public void testFetchConfig() {

        Map<String, String> expectConfigMap = Maps.newHashMap();

        //base
        expectConfigMap.put("name", "sink-hive-test_db-test_tb");
        expectConfigMap.put("topics", "test_topic");
        expectConfigMap.put("destinations", "test_db.test_tb");

        // core

        // filter
        expectConfigMap.put("destinations.test_db.test_tb.row.filter", "id>1");

        // add
        expectConfigMap.put("destinations.test_db.test_tb.fields.add", "test1_date:${datetime},test2_date:${datetime}");

        // mapping
        expectConfigMap.put("destinations.test_db.test_tb.fields.mapping",
            "id:tid,name:tname,email:temail,__op:opt,__kafka_record_offset:version");

        // whitelist
        expectConfigMap.put("destinations.test_db.test_tb.fields.whitelist", "id,name,email");

        // logical del
        expectConfigMap.put("destinations.test_db.test_tb.delete.mode", "NONE");

        // hdfs
        expectConfigMap.put("hadoop.user", "hive");
        expectConfigMap.put("store.url", "hdfs://mycluster");
        expectConfigMap.put("__hdfs.dfs.nameservices", "mycluster");
        expectConfigMap.put("__hdfs.dfs.ha.namenodes.mycluster", "nn1,nn2");
        expectConfigMap.put("__hdfs.dfs.client.failover.proxy.provider.mycluster",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        expectConfigMap.put("__hdfs.dfs.namenode.rpc-address.mycluster.nn1", "rpc1:9900");
        expectConfigMap.put("__hdfs.dfs.namenode.rpc-address.mycluster.nn2", "rpc2:9900");

        // hive
        expectConfigMap.put("hive.metastore.uris", "thrift://test-01:9083,thrift://test-02:9083");

        Map<String, String> configMap = hiveSinkConnectProcessService.fetchConfig(
            dataKafkaTopic,
            hive,
            hiveDatabase,
            hiveTable,
            "id>1",
            extensions,
            fieldMappings);
        Assertions.assertThat(expectConfigMap).containsAllEntriesOf(configMap);
    }
}
