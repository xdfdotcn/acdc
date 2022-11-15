package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO.LogicalDelDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaConverterType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.service.config.RdbJdbcConfig;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectBaseTest {

    public static final String INNER_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule "
        + "required username=\\\"Admin\\\" password=\\\"666\\\";";

    public static final String TICDC_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule "
        + "required username=\\\"Admin\\\" password=\\\"1111\\\";";

    public static final String USER_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule "
        + "required username=\\\"Admin\\\" password=\\\"5555\\\";";

    public static final Map<String, String> INNER_KAFKA_CLUSTER_ADMIN_CONFIG = new HashMap<>();

    public static final Map<String, String> TICDC_KAFKA_CLUSTER_ADMIN_CONFIG = new HashMap<>();

    public static final Map<String, String> USER_KAFKA_CLUSTER_ADMIN_CONFIG = new HashMap<>();

    static {
        INNER_KAFKA_CLUSTER_ADMIN_CONFIG.put("security.protocol", "SASL_PLAINTEXT");
        INNER_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.mechanism", "SCRAM-SHA-512");
        INNER_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.jaas.config", EncryptUtil.encrypt(INNER_SASL_JAAS_CONFIG));

        TICDC_KAFKA_CLUSTER_ADMIN_CONFIG.put("security.protocol", "SASL_PLAINTEXT");
        TICDC_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.mechanism", "SCRAM-SHA-512");
        TICDC_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.jaas.config", EncryptUtil.encrypt(TICDC_SASL_JAAS_CONFIG));

        USER_KAFKA_CLUSTER_ADMIN_CONFIG.put("security.protocol", "SASL_PLAINTEXT");
        USER_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.mechanism", "SCRAM-SHA-512");
        USER_KAFKA_CLUSTER_ADMIN_CONFIG.put("sasl.jaas.config", EncryptUtil.encrypt(USER_SASL_JAAS_CONFIG));
    }

    // CHECKSTYLE:OFF
    protected RdbDO rdb;

    protected RdbInstanceDO rdbInstance;

    protected RdbDatabaseDO rdbDatabase;

    protected RdbTableDO rdbTable;

    protected KafkaTopicDO dataKafkaTopic;

    protected KafkaTopicDO sinkKafkaTopic;

    protected List<String> uniqueKeys;

    protected RdbJdbcConfig rdbJdbcConfig;

    protected List<SinkConnectorColumnMappingDO> fieldMappings;

    protected List<ConnectorDataExtensionDO> extensions;

    protected LogicalDelDTO logicalDelDTO;

    protected HiveDO hive;

    protected HiveDatabaseDO hiveDatabase;

    protected HiveTableDO hiveTable;

    protected KafkaClusterDO userKafkaCluster;

    protected KafkaClusterDO ticdcKafkaCluster;

    protected KafkaConverterType kafkaConverterType;

    private ObjectMapper objectMapper = new ObjectMapper();

    // CHECKSTYLE:ON

    @Before
    public void init() throws JsonProcessingException {
        mockRdb();
        mockRdbInstance();
        mockRdbDatabase();
        mockRdbTable();
        mockRdbJdbcConfig();
        mockFieldExtensions();
        mockFieldMappings();
        mockKafkaTopic();
        mockUniqueKeys();
        mockLogicalDto();
        mockHive();
        mockHiveDatabase();
        mockHiveTable();
        mockSinkKafkaTopic();
        mockKafkaCluster();
        mockKafkaConverterType();
        mockTicdcKafkaCluster();
    }

    private void mockTicdcKafkaCluster() throws JsonProcessingException {
        this.ticdcKafkaCluster = KafkaClusterDO.builder()
            .clusterType(KafkaClusterType.TICDC)
            .id(2L)
            .name("acdc-ticdc")
            .securityConfiguration(objectMapper.writeValueAsString(TICDC_KAFKA_CLUSTER_ADMIN_CONFIG))
            .build();
    }

    private void mockSinkKafkaTopic() {
        sinkKafkaTopic = new KafkaTopicDO();
        sinkKafkaTopic.setName("mysql_test_db_test_tb_json");
    }

    private void mockKafkaConverterType() {
        kafkaConverterType = KafkaConverterType.JSON;
    }

    private void mockKafkaCluster() throws JsonProcessingException {
        userKafkaCluster = new KafkaClusterDO();
        userKafkaCluster.setName("test");
        userKafkaCluster.setId(6L);
        userKafkaCluster.setClusterType(KafkaClusterType.USER);
        userKafkaCluster.setBootstrapServers("localhost:9003");
        // CHECKSTYLE:OFF
        userKafkaCluster.setSecurityConfiguration(
            objectMapper.writeValueAsString(USER_KAFKA_CLUSTER_ADMIN_CONFIG));
        // CHECKSTYLE:ON
    }

    private void mockFieldMappings() {
        fieldMappings = Lists.newArrayList(
            SinkConnectorColumnMappingDO.builder().sourceColumnName("id\tbigint(20)\tPRI")
                .sinkColumnName("tid\tbigint(20)\tPRI").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("name\tvarchar(20)")
                .sinkColumnName("tname\tvarchar(20)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("email\tvarchar(20)")
                .sinkColumnName("temail\tvarchar(20)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__logical_del\tstring")
                .sinkColumnName("yn\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__datetime\tstring")
                .sinkColumnName("test1_date\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__datetime\tstring")
                .sinkColumnName("test2_date\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__op\tstring").sinkColumnName("opt\tvarchar(15)")
                .build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__kafka_record_offset\tstring")
                .sinkColumnName("version\tvarchar(15)").build(),
            SinkConnectorColumnMappingDO.builder().sourceColumnName("__none\tstring")
                .sinkColumnName("my_field\tvarchar(15)").build()
        );
    }

    private void mockLogicalDto() {
        logicalDelDTO = new LogicalDelDTO();
        logicalDelDTO.setLogicalDeletionColumn("is_deleted");
        logicalDelDTO.setLogicalDeletionColumnValueDeletion("1");
        logicalDelDTO.setLogicalDeletionColumnValueNormal("0");
    }

    private void mockFieldExtensions() {
        extensions = Lists.newArrayList(
                new ConnectorDataExtensionDO().setName("test1_date").setValue("${datetime}"),
                new ConnectorDataExtensionDO().setName("test2_date").setValue("${datetime}")
        );
    }

    private void mockRdbJdbcConfig() {
        rdbJdbcConfig = RdbJdbcConfig.builder().user("acdc").password("acdc").build();
    }

    private void mockUniqueKeys() {
        uniqueKeys = Lists.newArrayList("id", "uid");
    }

    private void mockRdb() {
        rdb = new RdbDO();
        rdb.setId(1L);
        rdb.setName("rdb1");
        rdb.setUsername("acdc");
        rdb.setPassword(EncryptUtil.encrypt("acdc"));
    }

    private void mockRdbInstance() {
        rdbInstance = new RdbInstanceDO();
        rdbInstance.setId(1L);
        rdbInstance.setRole(RoleType.DATA_SOURCE);
        rdbInstance.setHost("localhost");
        rdbInstance.setPort(3306);
    }

    private void mockRdbDatabase() {
        rdbDatabase = new RdbDatabaseDO();
        rdbDatabase.setId(1L);
        rdbDatabase.setName("test_db");
    }

    private void mockRdbTable() {
        rdbTable = new RdbTableDO();
        rdbTable.setId(1L);
        rdbTable.setName("test_tb");
    }

    private void mockKafkaTopic() throws JsonProcessingException {
        KafkaClusterDO kafkaClusterDO = new KafkaClusterDO();
        kafkaClusterDO.setBootstrapServers("localhost:9093");
        kafkaClusterDO.setClusterType(KafkaClusterType.INNER);

        kafkaClusterDO.setId(1L);
        kafkaClusterDO.setSecurityConfiguration(objectMapper.writeValueAsString(INNER_KAFKA_CLUSTER_ADMIN_CONFIG));

        dataKafkaTopic = new KafkaTopicDO();
        dataKafkaTopic.setId(1L);
        dataKafkaTopic.setName("test_topic");
        dataKafkaTopic.setKafkaCluster(kafkaClusterDO);
    }

    private void mockHive() {
        HdfsDO hdfs = new HdfsDO();
        hdfs.setName("mycluster");
        hdfs.setClientFailoverProxyProvider(
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        hdfs.setHdfsNamenodes(Sets.newHashSet(
                HdfsNamenodeDO.builder()
                        .name("nn1")
                        .rpcAddress("rpc1")
                        .rpcPort("9900")
                        .build(),
                HdfsNamenodeDO.builder()
                        .name("nn2")
                        .rpcAddress("rpc2")
                        .rpcPort("9900")
                        .build()
        ));
        hive = new HiveDO();
        hive.setId(1L);
        hive.setName("test_hive");
        hive.setHdfs(hdfs);
        hive.setHdfsUser("hive");
        hive.setMetastoreUris("thrift://test-01:9083,thrift://test-02:9083");
    }

    private void mockHiveDatabase() {
        hiveDatabase = new HiveDatabaseDO();
        hiveDatabase.setName("test_db");
    }

    private void mockHiveTable() {
        hiveTable = new HiveTableDO();
        hiveTable.setName("test_tb");
    }
}
